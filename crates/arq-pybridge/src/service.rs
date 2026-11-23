//! Blocking calls into `RedisBackend`. The Python event loop uses a worker thread.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use arq_core::{parse_outcome, ParsedOutcome};
use arq_redis::{ControlResult, RedisBackend, RequeueResult};

use crate::decide::{action_for, CommitAction, DispatchRaw};
use crate::encode::{self, abandoned_response, error_response};
use crate::load::{config_from_json, enqueue_job, BridgeError};

static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
static CONNECTS: AtomicU64 = AtomicU64::new(0);

pub fn runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("arq-pybridge")
            .build()
            .expect("tokio runtime")
    })
}

fn wait<T: Send>(work: impl std::future::Future<Output = T> + Send) -> T {
    runtime().block_on(work)
}

pub fn connects() -> u64 {
    CONNECTS.load(Ordering::SeqCst)
}

pub fn describe_config(text: &str) -> String {
    match config_from_json(text) {
        Ok(config) => {
            let body = crate::load::config_fields(&config);
            arq_core::Json::object(vec![("config", body)]).canonical()
        }
        Err(error) => error_response(&error),
    }
}

/// Parse config, build the backend, and open a socket with a depth read.
pub fn open(url: &str, config_json: &str) -> Result<RedisBackend, BridgeError> {
    CONNECTS.fetch_add(1, Ordering::SeqCst);
    let config = config_from_json(config_json)?;
    let backend = RedisBackend::connect(url, config).map_err(BridgeError::from)?;
    let probe = backend.clone();
    wait(async move { probe.admin("default").depth().await }).map_err(BridgeError::from)?;
    Ok(backend)
}

pub fn submit(backend: &RedisBackend, spec: &str) -> String {
    match submit_job(backend, spec) {
        Ok(result) => encode::submit_response(&result),
        Err(error) => error_response(&error),
    }
}

fn submit_job(backend: &RedisBackend, spec: &str) -> Result<arq_redis::EnqueueResult, BridgeError> {
    let job = enqueue_job(spec)?;
    let backend = backend.clone();
    wait(async move { backend.enqueue(job).await }).map_err(BridgeError::from)
}

pub fn snapshot(backend: &RedisBackend, queue: &str, host: Option<&str>) -> String {
    let backend = backend.clone();
    let queue = queue.to_string();
    let host = host.map(str::to_string);
    respond(wait(async move {
        backend
            .admin(queue)
            .snapshot(host.as_deref())
            .await
            .map(|snapshot| encode::snapshot_response(&snapshot))
            .map_err(BackendOrBridge::from)
    }))
}

pub fn get(backend: &RedisBackend, queue: &str, task_id: &str) -> String {
    let backend = backend.clone();
    let queue = queue.to_string();
    let task_id = task_id.to_string();
    respond(wait(async move {
        backend
            .admin(&queue)
            .get(&task_id)
            .await
            .map(|info| encode::task_response(&queue, info.as_ref()))
            .map_err(BackendOrBridge::from)
    }))
}

pub fn peek(backend: &RedisBackend, queue: &str, state: &str, limit: u64, cursor: Option<&str>) -> String {
    page(backend, queue, "peek", state, limit, cursor)
}

pub fn dead(backend: &RedisBackend, queue: &str, limit: u64, cursor: Option<&str>) -> String {
    page(backend, queue, "dead", "", limit, cursor)
}

pub fn dead_for_tag(backend: &RedisBackend, queue: &str, tag: &str, limit: u64, cursor: Option<&str>) -> String {
    page(backend, queue, "tag", tag, limit, cursor)
}

fn page(backend: &RedisBackend, queue: &str, kind: &str, arg: &str, limit: u64, cursor: Option<&str>) -> String {
    let backend = backend.clone();
    let queue = queue.to_string();
    let kind = kind.to_string();
    let arg = arg.to_string();
    let cursor = cursor.map(str::to_string);
    respond(wait(async move {
        let admin = backend.admin(&queue);
        let page = match kind.as_str() {
            "dead" => admin.dead(limit, cursor.as_deref()).await,
            "tag" => admin.dead_for_tag(&arg, limit, cursor.as_deref()).await,
            _ => admin.peek(&arg, limit, cursor.as_deref()).await,
        }
        .map_err(BackendOrBridge::from)?;
        Ok(encode::page_response(&queue, &page))
    }))
}

pub fn command(backend: &RedisBackend, queue: &str, operation_id: &str) -> String {
    let backend = backend.clone();
    let queue = queue.to_string();
    let operation_id = operation_id.to_string();
    respond(wait(async move {
        backend
            .admin(&queue)
            .command(&operation_id)
            .await
            .map(|receipt| encode::command_response(receipt.as_ref()))
            .map_err(BackendOrBridge::from)
    }))
}

#[derive(Debug, Clone)]
pub struct DeliveryRef {
    pub queue: String,
    pub task_id: String,
    pub message_id: String,
    pub cycle_id: String,
    pub delivery_token: String,
    pub operation_id: String,
}

pub fn commit(backend: &RedisBackend, delivery: &DeliveryRef, raw: &DispatchRaw) -> String {
    match action_for(raw) {
        CommitAction::Abandon => abandoned_response(),
        CommitAction::Finish(outcome) => {
            let label = outcome_label(&outcome);
            let request = match finish_request(delivery, outcome) {
                Ok(request) => request,
                Err(error) => return error_response(&error),
            };
            let backend = backend.clone();
            respond(wait(async move {
                backend
                    .finish(request)
                    .await
                    .map(|result| encode::finish_response(&label, &result))
                    .map_err(BackendOrBridge::from)
            }))
        }
    }
}

pub fn control(backend: &RedisBackend, kind: &str, queue: &str, task_id: &str, operation_id: &str) -> String {
    let parsed = match control_ids(queue, task_id, operation_id) {
        Ok(parsed) => parsed,
        Err(error) => return error_response(&error),
    };
    let backend = backend.clone();
    let kind = kind.to_string();
    respond(wait(async move {
        let request = arq_redis::ControlRequest {
            operation_id: parsed.operation_id,
            queue: parsed.queue.clone(),
            task_id: parsed.task_id,
        };
        let result = match kind.as_str() {
            "cancel" => backend.cancel(request).await.map(ControlView::Control).map_err(BackendOrBridge::from)?,
            "kill" => backend.kill(request).await.map(ControlView::Control).map_err(BackendOrBridge::from)?,
            "settle" => backend.settle(request).await.map(ControlView::Control).map_err(BackendOrBridge::from)?,
            "requeue" => {
                let requeue = arq_redis::RequeueRequest {
                    operation_id: parsed.operation_id,
                    queue: parsed.queue.clone(),
                    task_id: parsed.task_id,
                    new_cycle_id: parsed.cycle_id,
                };
                backend.requeue(requeue).await.map(ControlView::Requeue).map_err(BackendOrBridge::from)?
            }
            _ => return Err(BridgeError::input("unknown control operation").into()),
        };
        Ok(match result {
            ControlView::Control(result) => encode::control_response(&parsed.operation_text, &parsed.task_text, &result),
            ControlView::Requeue(result) => encode::requeue_response(&parsed.operation_text, &parsed.task_text, &result),
        })
    }))
}

enum ControlView {
    Control(ControlResult),
    Requeue(RequeueResult),
}

struct ParsedControl {
    operation_id: arq_core::OperationId,
    cycle_id: arq_core::CycleId,
    task_id: arq_core::TaskId,
    queue: String,
    operation_text: String,
    task_text: String,
}

fn control_ids(queue: &str, task_id: &str, operation_id: &str) -> Result<ParsedControl, BridgeError> {
    arq_core::validate_queue(queue).map_err(|_| BridgeError::input("queue name must match [a-z0-9]+"))?;
    let task = arq_core::TaskId::parse(task_id).map_err(|_| BridgeError::input("task_id must be a canonical lowercase UUID"))?;
    let operation = arq_core::OperationId::parse(operation_id)
        .map_err(|_| BridgeError::input("operation_id must be a canonical lowercase UUID"))?;
    let cycle = arq_core::CycleId::from_uuid(operation.as_uuid());
    Ok(ParsedControl {
        operation_id: operation,
        cycle_id: cycle,
        task_id: task,
        queue: queue.to_string(),
        operation_text: operation_id.to_string(),
        task_text: task_id.to_string(),
    })
}

fn finish_request(delivery: &DeliveryRef, outcome: ParsedOutcome) -> Result<arq_redis::FinishRequest, BridgeError> {
    arq_core::validate_queue(&delivery.queue).map_err(|_| BridgeError::input("queue name must match [a-z0-9]+"))?;
    Ok(arq_redis::FinishRequest {
        operation_id: arq_core::OperationId::parse(&delivery.operation_id)
            .map_err(|_| BridgeError::input("operation_id must be a canonical lowercase UUID"))?,
        message_id: arq_core::MessageId::parse(&delivery.message_id)
            .map_err(|_| BridgeError::input("message_id must be a canonical lowercase UUID"))?,
        queue: delivery.queue.clone(),
        task_id: arq_core::TaskId::parse(&delivery.task_id)
            .map_err(|_| BridgeError::input("task_id must be a canonical lowercase UUID"))?,
        cycle_id: arq_core::CycleId::parse(&delivery.cycle_id)
            .map_err(|_| BridgeError::input("cycle_id must be a canonical lowercase UUID"))?,
        delivery_token: arq_core::DeliveryToken::parse(&delivery.delivery_token)
            .map_err(|_| BridgeError::input("delivery_token must be a canonical lowercase UUID"))?,
        proposal: outcome,
    })
}

pub fn outcome_label(outcome: &ParsedOutcome) -> String {
    match outcome {
        ParsedOutcome::Proposal(arq_core::OutcomeProposal::Ok) => "ok".into(),
        ParsedOutcome::Proposal(arq_core::OutcomeProposal::Skipped) => "skipped".into(),
        ParsedOutcome::Proposal(arq_core::OutcomeProposal::Retry { .. }) => "retry".into(),
        ParsedOutcome::Proposal(arq_core::OutcomeProposal::Dead { reason, .. }) => reason.clone(),
        ParsedOutcome::Invalid(fault) => fault.reason().into(),
    }
}

fn respond(result: Result<String, BackendOrBridge>) -> String {
    match result {
        Ok(text) => text,
        Err(BackendOrBridge::Backend(error)) => error_response(&BridgeError::from(error)),
        Err(BackendOrBridge::Bridge(error)) => error_response(&error),
    }
}

enum BackendOrBridge {
    Backend(arq_redis::BackendError),
    Bridge(BridgeError),
}

impl From<arq_redis::BackendError> for BackendOrBridge {
    fn from(error: arq_redis::BackendError) -> Self {
        Self::Backend(error)
    }
}

impl From<BridgeError> for BackendOrBridge {
    fn from(error: BridgeError) -> Self {
        Self::Bridge(error)
    }
}

/// Used by tests that already parsed an outcome string.
pub fn parsed_outcome(text: &str) -> ParsedOutcome {
    match arq_core::Json::parse(text) {
        Ok(value) => parse_outcome(&value),
        Err(_) => ParsedOutcome::Invalid(arq_core::OutcomeFault::InvalidOutcome),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decide::DispatchRaw;
    use arq_core::Json;
    use arq_redis::{ClaimRequest, ClaimResult};
    use std::sync::atomic::{AtomicU64, Ordering};

    const URL: &str = "redis://redis:6379/0";

    fn n() -> u64 {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let tick = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        tick.wrapping_add(NEXT.fetch_add(1, Ordering::Relaxed)).wrapping_mul(17) & 0x0000_ffff_ffff_ffff
    }

    fn uuid(salt: u64) -> String {
        format!("00000000-0000-4000-8000-{:012x}", n().wrapping_add(salt) & 0xffffffffffff)
    }

    fn spec(queue: &str, unique: Option<&str>, message: &str, operation: &str, cycle: &str, kwargs: &str) -> String {
        let unique = match unique {
            Some(unique) => format!(r#""{unique}""#),
            None => "null".into(),
        };
        format!(
            r#"{{"queue":"{queue}","actor_name":"process_item","kwargs":{kwargs},"headers":{{"source":"bridge"}},"unique_key":{unique},"tag":"batch-a","delay_ms":0,"message_id":"{message}","operation_id":"{operation}","cycle_id":"{cycle}","max_retries":1,"max_deliveries":5,"time_limit_ms":1000,"min_backoff_ms":10,"max_backoff_ms":20}}"#
        )
    }

    fn fresh_queue() -> String {
        format!("q{}", n() % 1_000_000_000)
    }

    fn open_default() -> RedisBackend {
        open(URL, "{}").unwrap()
    }

    fn field<'a>(value: &'a Json, key: &str) -> Option<&'a Json> {
        value.get(key)
    }

    #[test]
    fn open_rejects_a_closed_port_and_a_bad_config() {
        let before = connects();
        let error = match open("redis://127.0.0.1:1", "{}") {
            Err(error) => error,
            Ok(_) => panic!("a closed port must not open"),
        };
        assert_eq!(error.kind(), "transport");
        assert!(connects() > before);
        let rejected = match open(URL, r#"{"SIMPLE_Q_POLL_INTERVAL_MS":"1"}"#) {
            Err(error) => error,
            Ok(_) => panic!("SIMPLE_Q_ must be rejected"),
        };
        assert_eq!(rejected.kind(), "config");
        assert!(std::ptr::eq(runtime(), runtime()));
        let described = describe_config("");
        assert!(described.contains("poll_interval_ms"), "{described}");
        let rejected = describe_config(r#"{"SIMPLE_Q_LEASE_MS":"1"}"#);
        assert!(rejected.contains("error"), "{rejected}");
    }

    #[test]
    fn submit_duplicate_conflict_and_full_stay_distinct() {
        let backend = open_default();
        let queue = fresh_queue();
        let message = uuid(1);
        let operation = uuid(2);
        let cycle = uuid(3);
        let unique = format!("item:{}", n());
        let first = submit(&backend, &spec(&queue, Some(&unique), &message, &operation, &cycle, r#"{"item_id":"1"}"#));
        let parsed = Json::parse(&first).unwrap();
        assert_eq!(field(&parsed, "status").and_then(Json::as_str), Some("enqueued"));
        let again = submit(
            &backend,
            &spec(&queue, Some(&unique), &uuid(4), &uuid(5), &uuid(6), r#"{"item_id":"1"}"#),
        );
        assert_eq!(Json::parse(&again).unwrap().get("status").and_then(Json::as_str), Some("duplicate"));
        let conflict = submit(&backend, &spec(&queue, None, &uuid(7), &operation, &uuid(8), r#"{"item_id":"2"}"#));
        let conflict = Json::parse(&conflict).unwrap();
        assert_eq!(conflict.get("error").and_then(|error| error.get("kind")).and_then(Json::as_str), Some("conflict"));
        let limited = open(URL, r#"{"ARQ_MAX_READY":"1"}"#).unwrap();
        let narrow = fresh_queue();
        let _ = submit(&limited, &spec(&narrow, None, &uuid(9), &uuid(10), &uuid(11), r#"{"item_id":"1"}"#));
        let full = submit(&limited, &spec(&narrow, None, &uuid(12), &uuid(13), &uuid(14), r#"{"item_id":"2"}"#));
        assert_eq!(
            Json::parse(&full).unwrap().get("error").and_then(|error| error.get("kind")).and_then(Json::as_str),
            Some("full")
        );
        let bad = submit(&backend, "[]");
        assert!(bad.contains("input"));
        let huge = submit(
            &open(URL, r#"{"ARQ_MAX_MESSAGE_BYTES":"8"}"#).unwrap(),
            &spec(&fresh_queue(), None, &uuid(15), &uuid(16), &uuid(17), r#"{"item_id":"0123456789abcdef"}"#),
        );
        assert!(huge.contains("error"));
    }

    #[test]
    fn admin_and_control_round_trip_on_one_task() {
        let backend = open_default();
        let queue = fresh_queue();
        let message = uuid(20);
        let operation = uuid(21);
        let cycle = uuid(22);
        let created = submit(&backend, &spec(&queue, Some(&format!("item:{}", n())), &message, &operation, &cycle, r#"{"item_id":"7"}"#));
        assert!(created.contains("enqueued"));
        let fetched = Json::parse(&get(&backend, &queue, &message)).unwrap();
        assert_eq!(fetched.get("task").and_then(|task| task.get("actor_name")).and_then(Json::as_str), Some("process_item"));
        assert!(get(&backend, &queue, &uuid(99)).contains("null"));
        assert!(peek(&backend, &queue, "ready", 1, None).contains("tasks"));
        assert!(peek(&backend, &queue, "scheduled", 1, None).contains("tasks"));
        assert!(peek(&backend, &queue, "inflight", 1, None).contains("tasks"));
        assert!(peek(&backend, &queue, "nope", 1, None).contains("page"));
        assert!(peek(&backend, &queue, "ready", 0, None).contains("page"));
        assert!(peek(&backend, &queue, "ready", 1, Some("nope")).contains("page"));
        assert!(dead(&backend, &queue, 1, None).contains("tasks"));
        assert!(dead_for_tag(&backend, &queue, "batch-a", 1, None).contains("tasks"));
        assert!(snapshot(&backend, &queue, Some("redis")).contains("taken_at"));
        assert!(snapshot(&backend, &queue, None).contains("queues"));
        let cancel_op = uuid(23);
        let cancelled = control(&backend, "cancel", &queue, &message, &cancel_op);
        assert!(cancelled.contains("accepted"), "{cancelled}");
        let again = control(&backend, "kill", &queue, &message, &uuid(24));
        assert!(again.contains("already_terminal"), "{again}");
        let settled = control(&backend, "settle", &queue, &message, &uuid(25));
        assert!(settled.contains("already_terminal") || settled.contains("not_blocked"), "{settled}");
        let missing = control(&backend, "settle", &queue, &uuid(26), &uuid(27));
        assert!(missing.contains("not_found"), "{missing}");
        let receipt = command(&backend, &queue, &cancel_op);
        assert!(receipt.contains("accepted") || receipt.contains("receipt"), "{receipt}");
        assert!(command(&backend, &queue, &uuid(28)).contains("null"));
        let requeue_op = uuid(29);
        let requeued = control(&backend, "requeue", &queue, &message, &requeue_op);
        assert!(requeued.contains("requeued") || requeued.contains("duplicate") || requeued.contains("not_dead"), "{requeued}");
        assert!(control(&backend, "nope", &queue, &message, &uuid(40)).contains("input"));
        assert!(control(&backend, "cancel", "Bad", &message, &uuid(41)).contains("input"));
        assert!(control(&backend, "cancel", &queue, "nope", &uuid(42)).contains("input"));
        assert!(get(&backend, "Bad", &message).contains("error"));
    }

    #[test]
    fn commit_writes_the_proposal_and_skips_grace() {
        let backend = open_default();
        let queue = fresh_queue();
        let message = uuid(50);
        let operation = uuid(51);
        let cycle = uuid(52);
        let created = submit(&backend, &spec(&queue, None, &message, &operation, &cycle, r#"{"item_id":"1"}"#));
        assert!(created.contains("enqueued"), "{created}");
        let claim = wait(backend.claim(ClaimRequest {
            operation_id: arq_core::OperationId::parse(&uuid(53)).unwrap(),
            message_id: arq_core::MessageId::parse(&uuid(54)).unwrap(),
            queue: queue.clone(),
            worker_id: "worker".into(),
            proposed_token: arq_core::DeliveryToken::parse(&uuid(55)).unwrap(),
            lease_ms: 15_000,
        }))
        .unwrap();
        let ClaimResult::Delivered(delivery) = claim else { panic!("{claim:?}") };
        let observed = &delivery.observed;
        let reference = DeliveryRef {
            queue: queue.clone(),
            task_id: observed.task_id.unwrap().to_string(),
            message_id: observed.message_id.unwrap().to_string(),
            cycle_id: observed.cycle_id.unwrap().to_string(),
            delivery_token: observed.delivery_token.unwrap().to_string(),
            operation_id: uuid(56),
        };
        let grace = commit(&backend, &reference, &DispatchRaw::GraceExceeded);
        assert!(grace.contains("abandoned"), "{grace}");
        let still = get(&backend, &queue, &reference.task_id);
        assert!(still.contains("inflight"), "{still}");
        let invalid = commit(&backend, &reference, &DispatchRaw::Returned(r#"{"outcome":"maybe"}"#.into()));
        assert!(invalid.contains("finished") || invalid.contains("rejected"), "{invalid}");
        let after = get(&backend, &queue, &reference.task_id);
        assert!(after.contains("invalid_outcome") || after.contains("dead"), "{after}");
        let bad = commit(
            &backend,
            &DeliveryRef { operation_id: "nope".into(), ..reference.clone() },
            &DispatchRaw::Returned(r#"{"outcome":"ok"}"#.into()),
        );
        assert!(bad.contains("input"), "{bad}");
        let _ = parsed_outcome(r#"{"outcome":"ok"}"#);
        let _ = outcome_label(&parsed_outcome(r#"{"outcome":"dead","reason":"retries","error":"x"}"#));
        let _ = outcome_label(&parsed_outcome("nope"));
    }

    #[test]
    fn a_second_task_can_be_finished_ok_and_rejected() {
        let backend = open_default();
        let queue = fresh_queue();
        let message = uuid(70);
        assert!(submit(&backend, &spec(&queue, None, &message, &uuid(71), &uuid(72), r#"{"item_id":"9"}"#)).contains("enqueued"));
        let claim = wait(backend.claim(ClaimRequest {
            operation_id: arq_core::OperationId::parse(&uuid(73)).unwrap(),
            message_id: arq_core::MessageId::parse(&uuid(74)).unwrap(),
            queue: queue.clone(),
            worker_id: "worker".into(),
            proposed_token: arq_core::DeliveryToken::parse(&uuid(75)).unwrap(),
            lease_ms: 15_000,
        }))
        .unwrap();
        let ClaimResult::Delivered(delivery) = claim else { panic!("{claim:?}") };
        let observed = &delivery.observed;
        let reference = DeliveryRef {
            queue: queue.clone(),
            task_id: observed.task_id.unwrap().to_string(),
            message_id: observed.message_id.unwrap().to_string(),
            cycle_id: observed.cycle_id.unwrap().to_string(),
            delivery_token: observed.delivery_token.unwrap().to_string(),
            operation_id: uuid(76),
        };
        let finished = commit(&backend, &reference, &DispatchRaw::Returned(r#"{"outcome":"ok"}"#.into()));
        assert!(finished.contains("finished"), "{finished}");
        let raised = commit(
            &backend,
            &DeliveryRef { operation_id: uuid(77), ..reference },
            &DispatchRaw::Raised,
        );
        assert!(raised.contains("rejected") || raised.contains("finished"), "{raised}");
        let retry = outcome_label(&parsed_outcome(r#"{"outcome":"retry","delay_ms":1}"#));
        assert_eq!(retry, "retry");
        let skipped = outcome_label(&parsed_outcome(r#"{"outcome":"skipped"}"#));
        assert_eq!(skipped, "skipped");
    }
}
