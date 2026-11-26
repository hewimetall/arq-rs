//! Protocol tests for `RedisBackend` against a local Redis.
//!
//! Fixtures use only `process_item`, queue `default`, `item_id`, `item:<number>`,
//! tags `batch-a` / `batch-b`, and `redis://redis:6379/0`.

use arq_core::{
    parse_outcome, CanonicalUuid, Config, CycleId, DeliveryToken, Json, MessageId, MessageIdentity, OperationId,
    OutcomeProposal, ParsedOutcome,
};
use arq_redis::{
    BackendError, ChildRegistration, ClaimRequest, ClaimResult, ControlRequest, ControlResult, EnqueueJob,
    EnqueueResult, ExecFault, Fault, FinishRequest, FinishResult, RedisBackend, RenewRequest, RequeueRequest,
    RequeueResult, SupervisorRegistration,
};
use std::sync::{Arc, Barrier, Mutex, OnceLock};
use std::time::Duration;

const URL: &str = "redis://redis:6379/0";

static LOCK: Mutex<()> = Mutex::new(());
static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("runtime"))
}

fn harness<T>(fut: impl std::future::Future<Output = T>) -> T {
    let _guard = LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    flush();
    let result = runtime().block_on(fut);
    flush();
    result
}

fn flush() {
    let mut con = conn();
    let _: String = redis::cmd("FLUSHDB").query(&mut con).expect("FLUSHDB");
}

fn conn() -> redis::Connection {
    redis::Client::open(URL).expect("client").get_connection().expect("connection")
}

fn uuid(n: u128) -> CanonicalUuid {
    CanonicalUuid::from_random_v4(n.to_be_bytes())
}

fn ident(n: u128) -> MessageIdentity {
    MessageIdentity::new(uuid(n))
}

fn op(n: u128) -> OperationId {
    OperationId::from_uuid(uuid(1_000_000 + n))
}

fn cycle(n: u128) -> CycleId {
    CycleId::from_uuid(uuid(2_000_000 + n))
}

fn token(n: u128) -> DeliveryToken {
    DeliveryToken::from_uuid(uuid(3_000_000 + n))
}

fn message(n: u128) -> MessageId {
    MessageId::from_uuid(uuid(n))
}

fn config(pairs: &[(&str, &str)]) -> Config {
    Config::parse(pairs.iter().copied()).expect("config")
}

fn open_backend(pairs: &[(&str, &str)]) -> RedisBackend {
    RedisBackend::connect(URL, config(pairs)).expect("connect")
}

fn job(n: u128, unique: Option<&str>, delay: u64) -> EnqueueJob {
    EnqueueJob {
        operation_id: op(n),
        identity: ident(n),
        cycle_id: cycle(n),
        actor_name: "process_item".into(),
        queue: "default".into(),
        kwargs: Json::object(vec![("item_id", Json::Number(n.to_string()))]),
        headers: Json::object(vec![]),
        tag: None,
        unique_key: unique.map(str::to_string),
        delay_ms: delay,
        max_retries: 4,
        max_deliveries: 5,
        time_limit_ms: 1_000,
        min_backoff_ms: 10,
        max_backoff_ms: 20,
    }
}

fn outcome(text: &str) -> ParsedOutcome {
    parse_outcome(&Json::parse(text).expect("json"))
}

fn hget(key: &str, field: &str) -> Option<String> {
    redis::cmd("HGET").arg(key).arg(field).query(&mut conn()).expect("HGET")
}

fn get(key: &str) -> Option<String> {
    redis::cmd("GET").arg(key).query(&mut conn()).expect("GET")
}

fn key_type(key: &str) -> String {
    redis::cmd("TYPE").arg(key).query(&mut conn()).expect("TYPE")
}

fn zscore(key: &str, member: &str) -> Option<String> {
    redis::cmd("ZSCORE").arg(key).arg(member).query(&mut conn()).expect("ZSCORE")
}

fn job_key(n: u128) -> String {
    format!("q:{{default}}:job:{}", ident(n).task_id)
}

async fn enqueue(backend: &RedisBackend, job: EnqueueJob) -> EnqueueResult {
    backend.enqueue(job).await.expect("enqueue")
}

async fn claim_to(backend: &RedisBackend, n: u128, lease_ms: u64) -> ClaimResult {
    backend
        .claim(ClaimRequest {
            operation_id: op(50_000 + n),
            message_id: message(50_000 + n),
            queue: "default".into(),
            worker_id: format!("worker-{n}"),
            proposed_token: token(n),
            lease_ms,
        })
        .await
        .expect("claim")
}

fn delivered(result: &ClaimResult) -> &arq_core::ObservedTask {
    match result {
        ClaimResult::Delivered(delivery) => &delivery.observed,
        other => panic!("expected delivery, got {other:?}"),
    }
}

#[test]
fn reports_the_redis_version() {
    harness(async {
        let mut con = conn();
        let info: String = redis::cmd("INFO").arg("server").query(&mut con).unwrap();
        let version = info.lines().find(|line| line.starts_with("redis_version:")).unwrap();
        println!("redis {version}");
        assert!(version.contains("redis_version:"));
    });
}

#[test]
fn enqueue_claim_renew_and_finish_round_trip() {
    harness(async {
        let backend = open_backend(&[]);
        let watched = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen = watched.clone();
        backend.set_fault(Fault {
            before_exec: Some(Arc::new(move |keys| seen.lock().unwrap().extend(keys.iter().cloned()))),
            ..Fault::default()
        });
        let mut queued = job(1, Some("item:1"), 0);
        queued.tag = Some("batch-a".into());
        let created = enqueue(&backend, queued.clone()).await;
        let EnqueueResult::Enqueued { task_id, replayed, .. } = created else { panic!("{created:?}") };
        assert!(!replayed);
        assert_eq!(task_id, ident(1).task_id);
        let keys = watched.lock().unwrap().clone();
        assert!(keys.iter().any(|key| key.contains(":receipt:")));
        assert!(keys.iter().any(|key| key.contains(":job:")));
        assert!(keys.iter().any(|key| key.contains(":uniq:item:1")));
        assert!(keys.iter().all(|key| !key.ends_with(":ready")));
        assert_eq!(hget(&job_key(1), "version").as_deref(), Some("1"));
        assert_eq!(hget(&job_key(1), "actor_name").as_deref(), Some("process_item"));
        assert_eq!(hget(&job_key(1), "state").as_deref(), Some("ready"));
        assert_eq!(get("q:{default}:uniq:item:1").as_deref(), Some(task_id.to_string().as_str()));
        let again = enqueue(&backend, queued).await;
        assert!(matches!(again, EnqueueResult::Enqueued { replayed: true, .. }));
        assert_eq!(zcard("q:{default}:ready"), 1);

        backend.set_fault(Fault::default());
        let first = claim_to(&backend, 1, 15_000).await;
        let observed = delivered(&first);
        assert_eq!(observed.deliveries, 1);
        assert_eq!(observed.state, Some(arq_core::TaskState::Inflight));
        let started = observed.started_at_ms;
        let deadline = observed.lease_deadline_ms.unwrap();
        let renewed = backend
            .renew(RenewRequest {
                operation_id: op(80_001),
                message_id: message(1),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(1),
                delivery_token: token(1),
                lease_ms: 15_000,
            })
            .await
            .unwrap();
        let arq_redis::RenewResult::Renewed { lease_deadline_ms, started_at_ms, replayed } = renewed else {
            panic!("{renewed:?}")
        };
        assert!(!replayed);
        assert!(lease_deadline_ms > deadline);
        assert_eq!(started_at_ms, started);
        let repeat = backend
            .renew(RenewRequest {
                operation_id: op(80_001),
                message_id: message(1),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(1),
                delivery_token: token(1),
                lease_ms: 15_000,
            })
            .await
            .unwrap();
        let arq_redis::RenewResult::Renewed { lease_deadline_ms: same, started_at_ms: still, replayed } = repeat else {
            panic!("{repeat:?}")
        };
        assert!(replayed);
        assert_eq!(same, lease_deadline_ms);
        assert_eq!(still, started);

        let stale = backend
            .renew(RenewRequest {
                operation_id: op(80_002),
                message_id: message(1),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(1),
                delivery_token: token(99),
                lease_ms: 15_000,
            })
            .await
            .unwrap();
        assert!(matches!(stale, arq_redis::RenewResult::NotHeld { exit_code: 4 }));
        assert_eq!(hget(&job_key(1), "lease_deadline_ms").unwrap(), lease_deadline_ms.to_string());

        let rejected = backend
            .finish(FinishRequest {
                operation_id: op(80_003),
                message_id: message(1),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(99),
                delivery_token: token(1),
                proposal: outcome(r#"{"outcome":"ok"}"#),
            })
            .await
            .unwrap();
        assert!(matches!(rejected, FinishResult::Rejected));
        assert_eq!(hget(&job_key(1), "state").as_deref(), Some("inflight"));

        let finished = backend
            .finish(FinishRequest {
                operation_id: op(80_004),
                message_id: message(1),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(1),
                delivery_token: token(1),
                proposal: outcome(r#"{"outcome":"ok"}"#),
            })
            .await
            .unwrap();
        assert!(matches!(finished, FinishResult::Finished { replayed: false }));
        assert_eq!(hget(&job_key(1), "state").as_deref(), Some("done"));
        assert_eq!(hget(&job_key(1), "result").as_deref(), Some("ok"));
        assert!(get("q:{default}:uniq:item:1").is_none());
        assert!(zscore("q:{default}:done", &task_id.to_string()).is_some());
        let replayed = backend
            .finish(FinishRequest {
                operation_id: op(80_004),
                message_id: message(1),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(1),
                delivery_token: token(1),
                proposal: outcome(r#"{"outcome":"ok"}"#),
            })
            .await
            .unwrap();
        assert!(matches!(replayed, FinishResult::Finished { replayed: true }));
        let receipt = backend.admin("default").command(&op(1).to_string()).await.unwrap().unwrap();
        assert_eq!(receipt.status, "enqueued");
        assert_eq!(receipt.op, "enqueue");
    });
}

#[test]
fn duplicate_unique_key_keeps_one_task() {
    harness(async {
        let backend = open_backend(&[]);
        let first = job(2, Some("item:2"), 0);
        let mut second = job(3, Some("item:2"), 0);
        second.operation_id = op(3);
        let (left, right) = tokio::join!(backend.enqueue(first), backend.enqueue(second));
        let left = left.unwrap();
        let right = right.unwrap();
        let duplicates = [&left, &right].iter().filter(|item| matches!(item, EnqueueResult::Duplicate(_))).count();
        let created = [&left, &right].iter().filter(|item| matches!(item, EnqueueResult::Enqueued { .. })).count();
        assert_eq!(created, 1);
        assert_eq!(duplicates, 1);
        assert_eq!(zcard("q:{default}:ready"), 1);
        assert!(get("q:{default}:uniq:item:2").is_some());
    });
}

#[test]
fn operation_conflict_does_not_write_a_second_task() {
    harness(async {
        let backend = open_backend(&[]);
        let original = job(4, Some("item:4"), 0);
        enqueue(&backend, original.clone()).await;
        let mut changed = original;
        changed.kwargs = Json::object(vec![("item_id", Json::Number("404".into()))]);
        let error = backend.enqueue(changed).await.unwrap_err();
        assert!(matches!(error, BackendError::Conflict(_)));
        assert!(error.to_string().contains("conflicts"));
        assert_eq!(hget(&job_key(4), "kwargs").as_deref(), Some(r#"{"item_id":4}"#));
        assert!(!backend.is_halted("default"));
    });
}

#[test]
fn parallel_tasks_do_not_share_a_watch_on_ready() {
    harness(async {
        let backend = open_backend(&[]);
        let (left, right) = tokio::join!(
            backend.enqueue(job(5, Some("item:5"), 0)),
            backend.enqueue(job(6, Some("item:6"), 0))
        );
        assert!(matches!(left.unwrap(), EnqueueResult::Enqueued { .. }));
        assert!(matches!(right.unwrap(), EnqueueResult::Enqueued { .. }));
        assert_eq!(zcard("q:{default}:ready"), 2);
    });
}

#[test]
fn watch_budget_is_per_attempt_and_execabort_retries() {
    harness(async {
        let backend = open_backend(&[("ARQ_WATCH_RETRIES", "1")]);
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = hits.clone();
        backend.set_fault(Fault {
            before_exec: Some(Arc::new(move |keys| {
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let receipt = keys.iter().find(|key| key.contains(":receipt:")).cloned().unwrap();
                let mut con = conn();
                let _: () = redis::cmd("SET").arg(&receipt).arg("touched").query(&mut con).unwrap();
                let _: i64 = redis::cmd("DEL").arg(&receipt).query(&mut con).unwrap();
            })),
            ..Fault::default()
        });
        let error = backend.enqueue(job(7, Some("item:7"), 0)).await.unwrap_err();
        assert!(matches!(error, BackendError::Transaction(_)));
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(hget(&job_key(7), "state").is_none());
        assert!(!backend.is_halted("default"));

        backend.set_fault(Fault { exec: ExecFault::AbortOnce, ..Fault::default() });
        let created = enqueue(&backend, job(8, Some("item:8"), 0)).await;
        assert!(matches!(created, EnqueueResult::Enqueued { replayed: false, .. }));
        assert_eq!(hget(&job_key(8), "state").as_deref(), Some("ready"));
        assert_eq!(zcard("q:{default}:ready"), 1);
    });
}

#[test]
fn resp3_null_exec_is_a_watch_conflict() {
    harness(async {
        let config = config(&[("ARQ_WATCH_RETRIES", "1")]);
        let backend = RedisBackend::connect("redis://redis:6379/0?protocol=resp3", config).unwrap();
        backend.set_fault(Fault {
            before_exec: Some(Arc::new(|keys| {
                let receipt = keys.iter().find(|key| key.contains(":receipt:")).cloned().unwrap();
                let mut con = conn();
                let _: () = redis::cmd("SET").arg(&receipt).arg("touched").query(&mut con).unwrap();
                let _: i64 = redis::cmd("DEL").arg(&receipt).query(&mut con).unwrap();
            })),
            ..Fault::default()
        });
        let error = backend.enqueue(job(9, Some("item:9"), 0)).await.unwrap_err();
        assert!(matches!(error, BackendError::Transaction(_)), "{error}");
        assert!(!backend.is_halted("default"));
    });
}

#[test]
fn partial_exec_halts_and_does_not_repair() {
    harness(async {
        let backend = open_backend(&[]);
        backend.set_fault(Fault {
            before_exec: Some(Arc::new(|_| {
                let mut con = conn();
                let _: i64 = redis::cmd("DEL").arg("q:{default}:ready").query(&mut con).unwrap();
                let _: () = redis::cmd("SET").arg("q:{default}:ready").arg("not-a-zset").query(&mut con).unwrap();
            })),
            ..Fault::default()
        });
        let error = backend.enqueue(job(10, Some("item:10"), 0)).await.unwrap_err();
        assert!(matches!(error, BackendError::Inconsistent(_)), "{error}");
        assert!(error.to_string().contains("storage inconsistency") || error.to_string().contains("inconsistency"));
        assert_eq!(backend.storage_inconsistent_count(), 1);
        assert!(backend.is_halted("default"));
        assert_eq!(get("q:{default}:uniq:item:10").as_deref(), Some(ident(10).task_id.to_string().as_str()));
        assert_eq!(key_type("q:{default}:ready"), "string");
        assert_eq!(hget(&job_key(10), "state").as_deref(), Some("ready"));
        let halted = backend.enqueue(job(11, Some("item:11"), 0)).await.unwrap_err();
        assert!(matches!(halted, BackendError::Halted { .. }), "{halted}");
        assert_eq!(backend.storage_inconsistent_count(), 1);
        let info = backend.admin("default").get(&ident(10).task_id.to_string()).await.unwrap().unwrap();
        assert_eq!(info.state, "ready");
        assert!(!info.missing_job);
        let fresh = open_backend(&[]);
        assert!(fresh.is_halted("default"));
        assert!(get("q:{default}:halted").is_some());
        assert_eq!(fresh.storage_inconsistent_count(), 0);
        let still = fresh.admin("default").get(&ident(10).task_id.to_string()).await.unwrap().unwrap();
        assert_eq!(still.actor_name, "process_item");
        assert_eq!(get("q:{default}:uniq:item:10").as_deref(), Some(ident(10).task_id.to_string().as_str()));
        assert_eq!(key_type("q:{default}:ready"), "string");
    });
}

#[test]
fn lost_reply_recovers_each_operation_without_a_second_write() {
    harness(async {
        let backend = open_backend(&[]);
        backend.set_fault(Fault { exec: ExecFault::LoseReply, ..Fault::default() });
        let created = enqueue(&backend, job(12, Some("item:12"), 0)).await;
        let EnqueueResult::Enqueued { task_id, replayed, .. } = created else { panic!("{created:?}") };
        assert!(replayed);
        backend.set_fault(Fault::default());
        let again = enqueue(&backend, job(12, Some("item:12"), 0)).await;
        assert!(matches!(again, EnqueueResult::Enqueued { replayed: true, .. }));
        assert_eq!(zcard("q:{default}:ready"), 1);

        backend.set_fault(Fault { exec: ExecFault::LoseReply, ..Fault::default() });
        let claimed = claim_to(&backend, 12, 15_000).await;
        let observed = delivered(&claimed);
        assert_eq!(observed.deliveries, 1);
        backend.set_fault(Fault::default());
        let claimed_again = claim_to(&backend, 12, 15_000).await;
        assert_eq!(delivered(&claimed_again).deliveries, 1);

        backend.set_fault(Fault { exec: ExecFault::LoseReply, ..Fault::default() });
        let renewed = backend
            .renew(RenewRequest {
                operation_id: op(90_012),
                message_id: message(12),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(12),
                delivery_token: token(12),
                lease_ms: 15_000,
            })
            .await
            .unwrap();
        let arq_redis::RenewResult::Renewed { lease_deadline_ms, started_at_ms, replayed } = renewed else {
            panic!("{renewed:?}")
        };
        assert!(replayed);
        assert_eq!(started_at_ms, observed.started_at_ms);
        backend.set_fault(Fault::default());
        let renewed_again = backend
            .renew(RenewRequest {
                operation_id: op(90_012),
                message_id: message(12),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(12),
                delivery_token: token(12),
                lease_ms: 15_000,
            })
            .await
            .unwrap();
        let arq_redis::RenewResult::Renewed { lease_deadline_ms: same, .. } = renewed_again else {
            panic!("{renewed_again:?}")
        };
        assert_eq!(same, lease_deadline_ms);

        backend.set_fault(Fault { exec: ExecFault::LoseReply, ..Fault::default() });
        let finished = backend
            .finish(FinishRequest {
                operation_id: op(91_012),
                message_id: message(12),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(12),
                delivery_token: token(12),
                proposal: outcome(r#"{"outcome":"retry","delay_ms":0}"#),
            })
            .await
            .unwrap();
        assert!(matches!(finished, FinishResult::Finished { replayed: true }));
        assert_eq!(hget(&job_key(12), "retries").as_deref(), Some("1"));
        assert_eq!(hget(&job_key(12), "state").as_deref(), Some("ready"));
        assert_eq!(get("q:{default}:uniq:item:12").as_deref(), Some(task_id.to_string().as_str()));
        backend.set_fault(Fault::default());
        let finished_again = backend
            .finish(FinishRequest {
                operation_id: op(91_012),
                message_id: message(12),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(12),
                delivery_token: token(12),
                proposal: outcome(r#"{"outcome":"retry","delay_ms":0}"#),
            })
            .await
            .unwrap();
        assert!(matches!(finished_again, FinishResult::Finished { replayed: true }));
        assert_eq!(hget(&job_key(12), "retries").as_deref(), Some("1"));

        let claimed = claim_to(&backend, 120, 15_000).await;
        assert_eq!(delivered(&claimed).task_id, Some(task_id));
        backend.set_fault(Fault { exec: ExecFault::LoseReply, ..Fault::default() });
        let dead = backend
            .finish(FinishRequest {
                operation_id: op(92_012),
                message_id: message(12),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(12),
                delivery_token: token(120),
                proposal: outcome(r#"{"outcome":"dead","reason":"retries","error":"RuntimeError: failed"}"#),
            })
            .await
            .unwrap();
        assert!(matches!(dead, FinishResult::Finished { replayed: true }));
        assert_eq!(hget(&job_key(12), "state").as_deref(), Some("dead"));
        assert_eq!(hget(&job_key(12), "reason").as_deref(), Some("retries"));
        assert!(get("q:{default}:uniq:item:12").is_none());
    });
}

fn zcard(key: &str) -> u64 {
    let value: i64 = redis::cmd("ZCARD").arg(key).query(&mut conn()).unwrap();
    value as u64
}

#[test]
fn lost_reply_covers_requeue_and_control() {
    harness(async {
        let backend = open_backend(&[]);
        let mut queued = job(14, Some("item:14"), 0);
        queued.tag = Some("batch-b".into());
        let EnqueueResult::Enqueued { task_id, .. } = enqueue(&backend, queued).await else { panic!("enqueue") };
        let claimed = claim_to(&backend, 14, 15_000).await;
        let _ = delivered(&claimed);
        backend
            .finish(FinishRequest {
                operation_id: op(93_014),
                message_id: message(14),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(14),
                delivery_token: token(14),
                proposal: outcome(r#"{"outcome":"dead","reason":"retries","error":null}"#),
            })
            .await
            .unwrap();
        assert!(get("q:{default}:uniq:item:14").is_none());
        backend.set_fault(Fault { exec: ExecFault::LoseReply, ..Fault::default() });
        let requeued = backend
            .requeue(RequeueRequest {
                operation_id: op(94_014),
                queue: "default".into(),
                task_id,
                new_cycle_id: cycle(140),
            })
            .await
            .unwrap();
        let RequeueResult::Requeued { cycle_id, replayed } = requeued else { panic!("{requeued:?}") };
        assert!(replayed);
        assert_eq!(cycle_id, cycle(140));
        backend.set_fault(Fault::default());
        let requeued_again = backend
            .requeue(RequeueRequest {
                operation_id: op(94_014),
                queue: "default".into(),
                task_id,
                new_cycle_id: cycle(140),
            })
            .await
            .unwrap();
        let RequeueResult::Requeued { cycle_id: same, replayed } = requeued_again else { panic!("{requeued_again:?}") };
        assert!(replayed);
        assert_eq!(same, cycle_id);
        assert_eq!(hget(&job_key(14), "deliveries").as_deref(), Some("0"));
        assert_eq!(hget(&job_key(14), "state").as_deref(), Some("ready"));
        assert!(hget(&job_key(14), "history").unwrap().contains("retries"));
        assert_eq!(get("q:{default}:uniq:item:14").as_deref(), Some(task_id.to_string().as_str()));

        backend.set_fault(Fault { exec: ExecFault::LoseReply, ..Fault::default() });
        let cancelled = backend
            .cancel(ControlRequest { operation_id: op(95_014), queue: "default".into(), task_id })
            .await
            .unwrap();
        assert!(matches!(cancelled, ControlResult::Accepted { replayed: true, .. }));
        backend.set_fault(Fault::default());
        let cancelled_again = backend
            .cancel(ControlRequest { operation_id: op(95_014), queue: "default".into(), task_id })
            .await
            .unwrap();
        assert!(matches!(cancelled_again, ControlResult::Accepted { .. }));
        assert_eq!(hget(&job_key(14), "reason").as_deref(), Some("cancelled"));
        assert_eq!(hget(&job_key(14), "state").as_deref(), Some("dead"));
        assert!(zscore("q:{default}:dead", &task_id.to_string()).is_some());
        assert!(zscore("q:{default}:ready", &task_id.to_string()).is_none());
    });
}

#[test]
fn drop_before_exec_leaves_the_ready_member() {
    harness(async {
        let backend = open_backend(&[]);
        enqueue(&backend, job(15, Some("item:15"), 0)).await;
        let task = ident(15).task_id.to_string();
        let seen = Arc::new(Mutex::new(false));
        let still_ready = Arc::new(Mutex::new(false));
        let flag = seen.clone();
        let ready_flag = still_ready.clone();
        backend.set_fault(Fault {
            exec: ExecFault::DropBeforeExec,
            before_exec: Some(Arc::new(move |_| {
                *ready_flag.lock().unwrap() = zscore("q:{default}:ready", &task).is_some();
                *flag.lock().unwrap() = true;
            })),
            ..Fault::default()
        });
        let claimed = claim_to(&backend, 15, 15_000).await;
        assert!(*seen.lock().unwrap());
        assert!(*still_ready.lock().unwrap());
        backend.set_fault(Fault::default());
        assert_eq!(delivered(&claimed).state, Some(arq_core::TaskState::Inflight));
        assert!(zscore("q:{default}:ready", &ident(15).task_id.to_string()).is_none());
        assert!(zscore("q:{default}:inflight", &ident(15).task_id.to_string()).is_some());
    });
}

#[test]
fn claim_window_prefers_expired_inflight_and_skips_a_lost_race() {
    harness(async {
        let backend = open_backend(&[("ARQ_WATCH_RETRIES", "1")]);
        // Claim while the inflight window is empty, then expire those leases. The next poll
        // must spend its window of 16 on overdue inflight and leave the new ready task.
        for n in 20..36 {
            enqueue(&backend, job(n, Some(&format!("item:{n}")), 0)).await;
            let claimed = claim_to(&backend, n, 15_000).await;
            let _ = delivered(&claimed);
        }
        let mut con = conn();
        for n in 20..36 {
            let _: i64 = redis::cmd("ZADD").arg("q:{default}:inflight").arg(1).arg(ident(n).task_id.to_string()).query(&mut con).unwrap();
            let _: i64 = redis::cmd("HSET").arg(job_key(n)).arg("lease_deadline_ms").arg(1).query(&mut con).unwrap();
        }
        enqueue(&backend, job(36, Some("item:36"), 0)).await;
        let blocked = claim_to(&backend, 360, 15_000).await;
        let overdue = delivered(&blocked);
        assert_ne!(overdue.task_id, Some(ident(36).task_id));
        assert!((20..36).any(|n| overdue.task_id == Some(ident(n).task_id)));
        assert_eq!(hget(&job_key(36), "state").as_deref(), Some("ready"));

        flush();
        enqueue(&backend, job(38, Some("item:38"), 0)).await;
        let claimed = claim_to(&backend, 38, 15_000).await;
        let _ = delivered(&claimed);
        let mut con = conn();
        let _: i64 = redis::cmd("ZADD").arg("q:{default}:inflight").arg(1).arg(ident(38).task_id.to_string()).query(&mut con).unwrap();
        let _: i64 = redis::cmd("HSET").arg(job_key(38)).arg("lease_deadline_ms").arg(1).query(&mut con).unwrap();
        let _: i64 = redis::cmd("HSET").arg(job_key(38)).arg("control_op").arg("").query(&mut con).unwrap();
        enqueue(&backend, job(39, Some("item:39"), 0)).await;
        let preferred = claim_to(&backend, 390, 15_000).await;
        assert_eq!(delivered(&preferred).task_id, Some(ident(38).task_id));
        assert_eq!(delivered(&preferred).deliveries, 2);
        assert_eq!(hget(&job_key(39), "state").as_deref(), Some("ready"));

        flush();
        enqueue(&backend, job(40, Some("item:40"), 0)).await;
        enqueue(&backend, job(41, Some("item:41"), 0)).await;
        let mut con = conn();
        let _: i64 = redis::cmd("ZADD")
            .arg("q:{default}:ready")
            .arg(1)
            .arg(ident(40).task_id.to_string())
            .arg(2)
            .arg(ident(41).task_id.to_string())
            .query(&mut con)
            .unwrap();
        let _: i64 = redis::cmd("HSET").arg(job_key(40)).arg("available_at_ms").arg(1).query(&mut con).unwrap();
        let _: i64 = redis::cmd("HSET").arg(job_key(41)).arg("available_at_ms").arg(2).query(&mut con).unwrap();
        let stolen = ident(40).task_id.to_string();
        let second = ident(41).task_id.to_string();
        let phase = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let phase2 = phase.clone();
        backend.set_fault(Fault {
            before_exec: Some(Arc::new(move |keys| {
                let job = keys.iter().find(|key| key.contains(":job:")).cloned().unwrap_or_default();
                let mut con = conn();
                if job.ends_with(&stolen) && phase2.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    let _: i64 = redis::cmd("HSET")
                        .arg(&job)
                        .arg("state")
                        .arg("inflight")
                        .arg("lease_deadline_ms")
                        .arg("9999999999999")
                        .arg("delivery_token")
                        .arg(token(400).to_string())
                        .query(&mut con)
                        .unwrap();
                } else if job.ends_with(&second) {
                    let seen = phase2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if seen == 1 {
                        let _: i64 = redis::cmd("HSET").arg(&job).arg("touched").arg("1").query(&mut con).unwrap();
                        let _: i64 = redis::cmd("HDEL").arg(&job).arg("touched").query(&mut con).unwrap();
                    }
                }
            })),
            ..Fault::default()
        });
        let raced = claim_to(&backend, 410, 15_000).await;
        assert_eq!(delivered(&raced).task_id, Some(ident(41).task_id));
        assert_eq!(hget(&job_key(40), "state").as_deref(), Some("inflight"));
    });
}

#[test]
fn ready_ceiling_counts_scheduled_and_allows_a_soft_overshoot() {
    harness(async {
        let backend = open_backend(&[("ARQ_MAX_READY", "1")]);
        let created = enqueue(&backend, job(42, Some("item:42"), 60_000)).await;
        assert!(matches!(created, EnqueueResult::Enqueued { .. }));
        let error = backend.enqueue(job(43, Some("item:43"), 0)).await.unwrap_err();
        assert!(matches!(error, BackendError::Full(_)), "{error}");
        let depth = backend.admin("default").depth().await.unwrap();
        assert_eq!(depth.scheduled, 1);
        assert_eq!(depth.ready, 0);
        assert_eq!(depth.ready + depth.scheduled, depth.ready_zcard);

        flush();
        let backend = open_backend(&[("ARQ_MAX_READY", "1")]);
        let barrier = Arc::new(Barrier::new(2));
        let left = barrier.clone();
        let right = barrier.clone();
        let mut first = true;
        let gate = Arc::new(Mutex::new(first));
        let _ = &mut first;
        let gate_a = gate.clone();
        backend.set_fault(Fault {
            after_ready_zcard: Some(Arc::new(move || {
                let mut flag = gate_a.lock().unwrap();
                if *flag {
                    *flag = false;
                    drop(flag);
                    left.wait();
                } else {
                    drop(flag);
                    right.wait();
                }
            })),
            ..Fault::default()
        });
        let (one, two) = tokio::join!(
            backend.enqueue(job(44, Some("item:44"), 0)),
            backend.enqueue(job(45, Some("item:45"), 0))
        );
        assert!(matches!(one.unwrap(), EnqueueResult::Enqueued { .. }));
        assert!(matches!(two.unwrap(), EnqueueResult::Enqueued { .. }));
        assert_eq!(zcard("q:{default}:ready"), 2);
        backend.set_fault(Fault::default());
        let error = backend.enqueue(job(46, Some("item:46"), 0)).await.unwrap_err();
        assert!(matches!(error, BackendError::Full(_)));
    });
}

#[test]
fn depth_splits_ready_and_scheduled_and_pages_do_not_repeat() {
    harness(async {
        let backend = open_backend(&[]);
        enqueue(&backend, job(50, Some("item:50"), 0)).await;
        let mut later = job(51, Some("item:51"), 120_000);
        later.tag = Some("batch-a".into());
        enqueue(&backend, later).await;
        let depth = backend.admin("default").depth().await.unwrap();
        assert_eq!(depth.ready, 1);
        assert_eq!(depth.scheduled, 1);
        assert_eq!(depth.ready + depth.scheduled, depth.ready_zcard);
        assert_eq!(depth.inflight, 0);
        let ready = backend.admin("default").peek("ready", 10, None).await.unwrap();
        assert_eq!(ready.tasks.len(), 1);
        assert_eq!(ready.tasks[0].task_id, ident(50).task_id.to_string());
        let scheduled = backend.admin("default").peek("scheduled", 10, None).await.unwrap();
        assert_eq!(scheduled.tasks.len(), 1);
        assert_eq!(scheduled.tasks[0].task_id, ident(51).task_id.to_string());
        let snap = backend.admin("default").snapshot(None).await.unwrap();
        assert_eq!(snap.queues[0].ready + snap.queues[0].scheduled, snap.queues[0].ready_zcard);

        for n in 52..56 {
            enqueue(&backend, job(n, Some(&format!("item:{n}")), 0)).await;
        }
        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let page = backend.admin("default").peek("ready", 2, cursor.as_deref()).await.unwrap();
            assert!(page.tasks.len() <= 2);
            for task in page.tasks {
                assert!(!seen.contains(&task.task_id));
                seen.push(task.task_id);
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert!(seen.len() >= 5);
        let bad = backend.admin("default").peek("ready", 0, None).await.unwrap_err();
        assert!(matches!(bad, BackendError::Page { .. }));
        let bad = backend.admin("default").peek("ready", 201, None).await.unwrap_err();
        assert!(matches!(bad, BackendError::Page { .. }));
        let bad = backend.admin("default").peek("ready", 1, Some("nope")).await.unwrap_err();
        assert!(matches!(bad, BackendError::Page { .. }));
        assert!(backend.admin("default").get("not-a-uuid").await.is_err());
        assert!(backend.admin("default").get(&ident(999).task_id.to_string()).await.unwrap().is_none());
        assert!(backend.admin("default").command(&op(999).to_string()).await.unwrap().is_none());

        let mut con = conn();
        let ghost = ident(900).task_id.to_string();
        let _: i64 = redis::cmd("ZADD").arg("q:{default}:ready").arg(1).arg(&ghost).query(&mut con).unwrap();
        let page = backend.admin("default").peek("ready", 50, None).await.unwrap();
        let missing = page.tasks.iter().find(|task| task.task_id == ghost).unwrap();
        assert!(missing.missing_job);
        assert_eq!(missing.state, "missing_job");
        assert!(zscore("q:{default}:ready", &ghost).is_some());
    });
}

#[test]
fn control_settle_and_requeue_follow_the_lease() {
    harness(async {
        let backend = open_backend(&[]);
        let mut queued = job(60, Some("item:60"), 0);
        queued.tag = Some("batch-a".into());
        let EnqueueResult::Enqueued { task_id, .. } = enqueue(&backend, queued).await else { panic!("enqueue") };
        let cancelled = backend
            .cancel(ControlRequest { operation_id: op(160), queue: "default".into(), task_id })
            .await
            .unwrap();
        assert!(matches!(cancelled, ControlResult::Accepted { .. }));
        assert_eq!(hget(&job_key(60), "reason").as_deref(), Some("cancelled"));
        assert_eq!(hget(&job_key(60), "state").as_deref(), Some("dead"));
        assert!(get("q:{default}:uniq:item:60").is_none());
        let dead = backend.admin("default").dead(10, None).await.unwrap();
        assert_eq!(dead.tasks[0].task_id, task_id.to_string());
        let tagged = backend.admin("default").dead_for_tag("batch-a", 10, None).await.unwrap();
        assert_eq!(tagged.tasks.len(), 1);
        assert!(backend.admin("default").dead_for_tag("batch-b", 10, None).await.unwrap().tasks.is_empty());

        flush();
        let mut queued = job(61, Some("item:61"), 0);
        queued.tag = Some("batch-b".into());
        let EnqueueResult::Enqueued { task_id, .. } = enqueue(&backend, queued).await else { panic!("enqueue") };
        let claimed = claim_to(&backend, 61, 400).await;
        let _ = delivered(&claimed);
        let accepted = backend
            .kill(ControlRequest { operation_id: op(161), queue: "default".into(), task_id })
            .await
            .unwrap();
        assert!(matches!(accepted, ControlResult::Accepted { .. }));
        assert_eq!(hget(&job_key(61), "state").as_deref(), Some("inflight"));
        assert_eq!(hget(&job_key(61), "control_op").as_deref(), Some("kill"));
        let rejected = backend
            .finish(FinishRequest {
                operation_id: op(162),
                message_id: message(61),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(61),
                delivery_token: token(61),
                proposal: outcome(r#"{"outcome":"ok"}"#),
            })
            .await
            .unwrap();
        assert!(matches!(rejected, FinishResult::Rejected));
        enqueue(&backend, job(62, Some("item:62"), 0)).await;
        let next = claim_to(&backend, 620, 15_000).await;
        assert_eq!(delivered(&next).task_id, Some(ident(62).task_id));
        assert_eq!(hget(&job_key(61), "state").as_deref(), Some("inflight"));
        let still = backend
            .settle(ControlRequest { operation_id: op(163), queue: "default".into(), task_id })
            .await
            .unwrap();
        assert!(matches!(still, ControlResult::StillOwned), "{still:?}");
        assert_eq!(hget(&job_key(61), "state").as_deref(), Some("inflight"));
        assert!(backend.admin("default").command(&op(163).to_string()).await.unwrap().is_none());
        std::thread::sleep(Duration::from_millis(500));
        let settled = backend
            .settle(ControlRequest { operation_id: op(163), queue: "default".into(), task_id })
            .await
            .unwrap();
        assert!(matches!(settled, ControlResult::Accepted { .. }), "{settled:?}");
        assert_eq!(hget(&job_key(61), "reason").as_deref(), Some("killed"));
        assert_eq!(hget(&job_key(61), "stop_evidence").as_deref(), Some("lease_expired"));
        assert_eq!(hget(&job_key(61), "control_op").as_deref(), Some(""));
        assert_eq!(hget(&job_key(61), "state").as_deref(), Some("dead"));
        let requeued = backend
            .requeue(RequeueRequest {
                operation_id: op(164),
                queue: "default".into(),
                task_id,
                new_cycle_id: cycle(610),
            })
            .await
            .unwrap();
        let RequeueResult::Requeued { cycle_id, .. } = requeued else { panic!("{requeued:?}") };
        assert_eq!(cycle_id, cycle(610));
        assert_eq!(hget(&job_key(61), "state").as_deref(), Some("ready"));
        assert_eq!(hget(&job_key(61), "cycle_id").as_deref(), Some(cycle(610).to_string().as_str()));
        assert_eq!(get("q:{default}:uniq:item:61").as_deref(), Some(task_id.to_string().as_str()));

        let not_dead = backend
            .requeue(RequeueRequest {
                operation_id: op(165),
                queue: "default".into(),
                task_id: ident(62).task_id,
                new_cycle_id: cycle(620),
            })
            .await
            .unwrap();
        assert!(matches!(not_dead, RequeueResult::NotDead));
        let missing = backend
            .requeue(RequeueRequest {
                operation_id: op(166),
                queue: "default".into(),
                task_id: ident(999).task_id,
                new_cycle_id: cycle(999),
            })
            .await
            .unwrap();
        assert!(matches!(missing, RequeueResult::NotFound));
        let missing_again = backend
            .requeue(RequeueRequest {
                operation_id: op(166),
                queue: "default".into(),
                task_id: ident(999).task_id,
                new_cycle_id: cycle(999),
            })
            .await
            .unwrap();
        assert!(matches!(missing_again, RequeueResult::NotFound));
        let open = backend
            .settle(ControlRequest { operation_id: op(167), queue: "default".into(), task_id: ident(62).task_id })
            .await
            .unwrap();
        assert!(matches!(open, ControlResult::NotBlocked));
    });
}

#[test]
fn retry_keeps_uniq_and_max_deliveries_dead_letters() {
    harness(async {
        let backend = open_backend(&[]);
        let EnqueueResult::Enqueued { task_id, .. } = enqueue(&backend, job(70, Some("item:70"), 0)).await else { panic!("enqueue") };
        let claimed = claim_to(&backend, 70, 15_000).await;
        let _ = delivered(&claimed);
        backend
            .finish(FinishRequest {
                operation_id: op(170),
                message_id: message(70),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(70),
                delivery_token: token(70),
                proposal: outcome(r#"{"outcome":"retry","delay_ms":0}"#),
            })
            .await
            .unwrap();
        assert_eq!(get("q:{default}:uniq:item:70").as_deref(), Some(task_id.to_string().as_str()));
        assert_eq!(hget(&job_key(70), "retries").as_deref(), Some("1"));
        assert_eq!(hget(&job_key(70), "started_at_ms").as_deref(), Some(""));

        flush();
        let mut limited = job(71, Some("item:71"), 0);
        limited.max_deliveries = 1;
        let EnqueueResult::Enqueued { task_id, .. } = enqueue(&backend, limited).await else { panic!("enqueue") };
        let claimed = claim_to(&backend, 71, 15_000).await;
        assert_eq!(delivered(&claimed).deliveries, 1);
        backend
            .finish(FinishRequest {
                operation_id: op(171),
                message_id: message(71),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(71),
                delivery_token: token(71),
                proposal: outcome(r#"{"outcome":"retry","delay_ms":0}"#),
            })
            .await
            .unwrap();
        let dead = claim_to(&backend, 710, 15_000).await;
        match dead {
            ClaimResult::DeadLetter { task_id: id, reason, .. } => {
                assert_eq!(id, task_id);
                assert_eq!(reason, "max_deliveries");
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(hget(&job_key(71), "state").as_deref(), Some("dead"));
        assert!(get("q:{default}:uniq:item:71").is_none());
        assert!(zscore("q:{default}:inflight", &task_id.to_string()).is_none());

        flush();
        let EnqueueResult::Enqueued { task_id, .. } = enqueue(&backend, job(72, Some("item:72"), 0)).await else { panic!("enqueue") };
        let other = ident(720).task_id.to_string();
        let mut con = conn();
        let _: () = redis::cmd("SET").arg("q:{default}:uniq:item:72").arg(&other).query(&mut con).unwrap();
        let claimed = claim_to(&backend, 72, 15_000).await;
        let _ = delivered(&claimed);
        backend
            .finish(FinishRequest {
                operation_id: op(172),
                message_id: message(72),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(72),
                delivery_token: token(72),
                proposal: outcome(r#"{"outcome":"dead","reason":"timeout","error":"slow"}"#),
            })
            .await
            .unwrap();
        assert_eq!(get("q:{default}:uniq:item:72").as_deref(), Some(other.as_str()));
        assert_eq!(hget(&job_key(72), "reason").as_deref(), Some("timeout"));
    });
}

#[test]
fn duplicate_requeue_leaves_the_dead_task() {
    harness(async {
        let backend = open_backend(&[]);
        let EnqueueResult::Enqueued { task_id, .. } = enqueue(&backend, job(73, Some("item:73"), 0)).await else { panic!("enqueue") };
        backend
            .cancel(ControlRequest { operation_id: op(173), queue: "default".into(), task_id })
            .await
            .unwrap();
        let owner = ident(730).task_id.to_string();
        let mut con = conn();
        let _: () = redis::cmd("SET").arg("q:{default}:uniq:item:73").arg(&owner).query(&mut con).unwrap();
        let result = backend
            .requeue(RequeueRequest {
                operation_id: op(174),
                queue: "default".into(),
                task_id,
                new_cycle_id: cycle(730),
            })
            .await
            .unwrap();
        match result {
            RequeueResult::Duplicate(error) => assert_eq!(error.owner_task_id.to_string(), owner),
            other => panic!("{other:?}"),
        }
        assert_eq!(hget(&job_key(73), "state").as_deref(), Some("dead"));
        let terminal = backend
            .cancel(ControlRequest { operation_id: op(175), queue: "default".into(), task_id })
            .await
            .unwrap();
        assert!(matches!(terminal, ControlResult::Accepted { .. } | ControlResult::AlreadyTerminal));
    });
}

#[test]
fn ownership_lost_when_the_lease_has_already_ended() {
    harness(async {
        let backend = open_backend(&[]);
        enqueue(&backend, job(80, Some("item:80"), 0)).await;
        let claimed = claim_to(&backend, 80, 1).await;
        match claimed {
            ClaimResult::OwnershipLost { exit_code, task_id, .. } => {
                assert_eq!(exit_code, 4);
                assert_eq!(task_id, ident(80).task_id);
                assert_eq!(hget(&job_key(80), "state").as_deref(), Some("inflight"));
            }
            ClaimResult::Delivered(_) => {
                std::thread::sleep(Duration::from_millis(5));
                let again = backend
                    .claim(ClaimRequest {
                        operation_id: op(50_000 + 80),
                        message_id: message(50_000 + 80),
                        queue: "default".into(),
                        worker_id: "worker-80".into(),
                        proposed_token: token(80),
                        lease_ms: 1,
                    })
                    .await
                    .unwrap();
                assert!(matches!(again, ClaimResult::Delivered(_) | ClaimResult::OwnershipLost { exit_code: 4, .. }));
            }
            other => panic!("{other:?}"),
        }
    });
}

#[test]
fn maintenance_removes_old_terminal_tasks_and_refuses_a_live_uniq() {
    harness(async {
        let backend = open_backend(&[("ARQ_MAINTENANCE_BATCH", "1")]);
        for n in [81_u128, 82] {
            let EnqueueResult::Enqueued { task_id, .. } = enqueue(&backend, job(n, Some(&format!("item:{n}")), 0)).await else {
                panic!("enqueue")
            };
            let claimed = claim_to(&backend, n, 15_000).await;
            let _ = delivered(&claimed);
            backend
                .finish(FinishRequest {
                    operation_id: op(180 + n),
                    message_id: message(n),
                    queue: "default".into(),
                    task_id,
                    cycle_id: cycle(n),
                    delivery_token: token(n),
                    proposal: outcome(r#"{"outcome":"skipped"}"#),
                })
                .await
                .unwrap();
            let mut con = conn();
            let _: i64 = redis::cmd("ZADD").arg("q:{default}:done").arg(0).arg(task_id.to_string()).query(&mut con).unwrap();
        }
        let first = backend.maintain("default").await.unwrap();
        assert_eq!(first.removed, 1);
        let second = backend.maintain("default").await.unwrap();
        assert_eq!(second.removed, 1);
        assert!(hget(&job_key(81), "state").is_none());
        assert!(hget(&job_key(82), "state").is_none());

        let EnqueueResult::Enqueued { task_id, .. } = enqueue(&backend, job(83, Some("item:83"), 0)).await else { panic!("enqueue") };
        let claimed = claim_to(&backend, 83, 15_000).await;
        let _ = delivered(&claimed);
        backend
            .finish(FinishRequest {
                operation_id: op(183),
                message_id: message(83),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(83),
                delivery_token: token(83),
                proposal: outcome(r#"{"outcome":"ok"}"#),
            })
            .await
            .unwrap();
        let mut con = conn();
        let _: i64 = redis::cmd("ZADD").arg("q:{default}:done").arg(0).arg(task_id.to_string()).query(&mut con).unwrap();
        let _: () = redis::cmd("HSET").arg(job_key(83)).arg("state").arg("ready").query(&mut con).unwrap();
        let skipped = backend.maintain("default").await.unwrap();
        assert_eq!(skipped.removed, 0);
        assert_eq!(hget(&job_key(83), "state").as_deref(), Some("ready"));

        let _: () = redis::cmd("HSET").arg(job_key(83)).arg("state").arg("done").query(&mut con).unwrap();
        let _: () = redis::cmd("SET").arg("q:{default}:uniq:item:83").arg(task_id.to_string()).query(&mut con).unwrap();
        let error = backend.maintain("default").await.unwrap_err();
        assert!(matches!(error, BackendError::Inconsistent(_)), "{error}");
        assert!(!backend.is_halted("default"));
        assert_eq!(hget(&job_key(83), "state").as_deref(), Some("done"));
        assert_eq!(get("q:{default}:uniq:item:83").as_deref(), Some(task_id.to_string().as_str()));
        let again = backend.maintain("default").await.unwrap_err();
        assert!(matches!(again, BackendError::Inconsistent(_)));
    });
}

#[test]
fn registry_heartbeat_does_not_touch_the_queue_and_orphans_stay() {
    harness(async {
        let backend = open_backend(&[]);
        let EnqueueResult::Enqueued { task_id, .. } = enqueue(&backend, job(84, Some("item:84"), 0)).await else { panic!("enqueue") };
        let uniq = get("q:{default}:uniq:item:84");
        let bad = backend.register_supervisor(sample_supervisor("bad host", 7)).await.unwrap_err();
        assert!(matches!(bad, BackendError::Input(_)));
        let seen = backend.register_supervisor(sample_supervisor("worker.local", 42)).await.unwrap();
        assert!(seen > 0);
        backend
            .upsert_child(
                "worker.local",
                42,
                ChildRegistration {
                    slot: 1,
                    pid: 100,
                    ppid: 42,
                    state: "running".into(),
                    reason: String::new(),
                    exit_code: None,
                    rss_bytes: 10,
                    started_at_ms: 1,
                    ended_at_ms: None,
                    tasks_done: 3,
                    max_tasks: 9,
                    alive: true,
                },
            )
            .await
            .unwrap();
        let beat = backend.heartbeat("worker.local", 42).await.unwrap();
        assert!(beat >= seen);
        assert_eq!(get("q:{default}:uniq:item:84"), uniq);
        assert!(hget(&job_key(84), "lease_deadline_ms").is_none());
        let snap = backend.admin("default").snapshot(Some("worker.local")).await.unwrap();
        assert_eq!(snap.supervisors.len(), 1);
        assert_eq!(snap.supervisors[0].in_progress, 1);
        assert_eq!(snap.supervisors[0].children[0].tasks_done, 3);
        assert_eq!(snap.supervisors[0].queues, vec!["default".to_string()]);
        assert!(snap.orphans.is_empty());
        backend
            .upsert_child(
                "worker.local",
                42,
                ChildRegistration {
                    slot: 1,
                    pid: 100,
                    ppid: 42,
                    state: "exited".into(),
                    reason: "done".into(),
                    exit_code: Some(0),
                    rss_bytes: 10,
                    started_at_ms: 1,
                    ended_at_ms: Some(2),
                    tasks_done: 4,
                    max_tasks: 9,
                    alive: false,
                },
            )
            .await
            .unwrap();
        let snap = backend.admin("default").snapshot(Some("worker.local")).await.unwrap();
        assert_eq!(snap.supervisors[0].in_progress, 0);
        assert_eq!(snap.supervisors[0].children[0].exit_code, Some(0));
        let mut con = conn();
        let _: i64 = redis::cmd("ZREM").arg("arq:supervisors").arg("worker.local.42").query(&mut con).unwrap();
        let snap = backend.admin("default").snapshot(None).await.unwrap();
        assert!(snap.supervisors.is_empty());
        assert_eq!(snap.orphans.len(), 1);
        assert_eq!(snap.orphans[0].slot, 1);
        let again = backend.admin("default").snapshot(None).await.unwrap();
        assert_eq!(again.orphans.len(), 1);
        backend.remove_supervisor("worker.local", 42).await.unwrap();
        assert_eq!(key_type("arq:supervisor:worker.local.42"), "none");
        assert_eq!(key_type("arq:supervisor:worker.local.42:child:1"), "none");
        let snap = backend.admin("default").snapshot(Some("other-host")).await.unwrap();
        assert!(snap.orphans.is_empty());
        let _ = task_id;
    });
}

fn sample_supervisor(host: &str, pid: u32) -> SupervisorRegistration {
    SupervisorRegistration {
        host: host.into(),
        pid,
        started_at_ms: 10,
        version: "0.1.0".into(),
        queues: vec!["default".into()],
        processes: 1,
        concurrency: 1,
        max_tasks: 4,
        memory_soft_ratio: 0.8,
        memory_hard_ratio: 0.95,
        child_memory_soft: None,
        child_memory_hard: Some(32),
        cgroup: "self".into(),
        recycled: 1,
        drains: 2,
        kills: 3,
        restarts: 4,
    }
}

#[test]
fn input_and_transport_errors_stay_off_the_halt_flag() {
    harness(async {
        let backend = open_backend(&[("ARQ_WATCH_RETRIES", "1")]);
        let error = backend.enqueue(job_on("NO")).await.unwrap_err();
        assert!(matches!(error, BackendError::Input(_)));
        let error = backend
            .claim(ClaimRequest {
                operation_id: op(1),
                message_id: message(1),
                queue: "default".into(),
                worker_id: String::new(),
                proposed_token: token(1),
                lease_ms: 1000,
            })
            .await
            .unwrap_err();
        assert!(matches!(error, BackendError::Input(_)));
        assert!(!backend.is_halted("default"));
        let (driver, runtime_name) = RedisBackend::driver();
        assert!(driver.contains("redis"));
        assert!(runtime_name.contains("Runtime"));
        let down = RedisBackend::connect("redis://redis:6391/0", config(&[("ARQ_WATCH_RETRIES", "1")])).unwrap();
        let error = down.enqueue(job(85, Some("item:85"), 0)).await.unwrap_err();
        assert!(matches!(error, BackendError::Transport { .. }), "{error}");
        assert!(!down.is_halted("default"));
    });
}

fn job_on(queue: &str) -> EnqueueJob {
    let mut queued = job(86, Some("item:86"), 0);
    queued.queue = queue.into();
    queued
}

#[test]
fn invalid_outcome_and_skipped_finish_write_the_documented_fields() {
    harness(async {
        let backend = open_backend(&[]);
        let EnqueueResult::Enqueued { task_id, .. } = enqueue(&backend, job(87, None, 0)).await else { panic!("enqueue") };
        let claimed = claim_to(&backend, 87, 15_000).await;
        let _ = delivered(&claimed);
        backend
            .finish(FinishRequest {
                operation_id: op(187),
                message_id: message(87),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(87),
                delivery_token: token(87),
                proposal: outcome(r#"{"outcome":"nope"}"#),
            })
            .await
            .unwrap();
        assert_eq!(hget(&job_key(87), "reason").as_deref(), Some("invalid_outcome"));
        assert_eq!(hget(&job_key(87), "state").as_deref(), Some("dead"));
        let proposal = outcome(r#"{"outcome":"ok"}"#);
        assert!(matches!(proposal, ParsedOutcome::Invalid(_) | ParsedOutcome::Proposal(OutcomeProposal::Ok) | ParsedOutcome::Proposal(_)));
    });
}

#[test]
fn a_broken_member_is_quarantined_and_the_next_task_is_claimed() {
    harness(async {
        let backend = open_backend(&[]);
        let EnqueueResult::Enqueued {
            task_id: missing, ..
        } = enqueue(&backend, job(9300, None, 0)).await
        else {
            panic!("enqueue")
        };
        let EnqueueResult::Enqueued {
            task_id: healthy, ..
        } = enqueue(&backend, job(9301, None, 0)).await
        else {
            panic!("enqueue")
        };
        let mut con = conn();
        let _: i64 = redis::cmd("ZADD")
            .arg("q:{default}:ready")
            .arg(1)
            .arg(missing.to_string())
            .query(&mut con)
            .unwrap();
        let _: i64 = redis::cmd("DEL")
            .arg(job_key(9300))
            .query(&mut con)
            .unwrap();
        let claimed = claim_to(&backend, 9301, 15_000).await;
        assert_eq!(delivered(&claimed).task_id, Some(healthy));
        assert!(zscore("q:{default}:quarantine", &missing.to_string()).is_some());
        assert!(zscore("q:{default}:ready", &missing.to_string()).is_none());
        assert_eq!(
            hget("q:{default}:qreason", &missing.to_string()).as_deref(),
            Some("missing_hash")
        );
        assert!(!backend.is_halted("default"));
        assert!(get("q:{default}:halted").is_none());
        let fresh = open_backend(&[]);
        assert!(!fresh.is_halted("default"));
        let idle = fresh
            .claim(ClaimRequest {
                operation_id: op(9302),
                message_id: message(9302),
                queue: "default".into(),
                worker_id: "worker-9302".into(),
                proposed_token: token(9302),
                lease_ms: 15_000,
            })
            .await
            .unwrap();
        assert!(matches!(idle, ClaimResult::Idle), "{idle:?}");
        let notes = backend.drain_quarantine();
        assert!(notes
            .iter()
            .any(|(queue, reason)| queue == "default" && reason == "missing_hash"));

        let EnqueueResult::Enqueued { task_id: typed, .. } =
            enqueue(&backend, job(9400, None, 0)).await
        else {
            panic!("enqueue")
        };
        let EnqueueResult::Enqueued {
            task_id: after_type,
            ..
        } = enqueue(&backend, job(9401, None, 0)).await
        else {
            panic!("enqueue")
        };
        let _: () = redis::cmd("SET")
            .arg(job_key(9400))
            .arg("not-a-hash")
            .query(&mut con)
            .unwrap();
        let _: i64 = redis::cmd("ZADD")
            .arg("q:{default}:ready")
            .arg(1)
            .arg(typed.to_string())
            .query(&mut con)
            .unwrap();
        let claimed = claim_to(&backend, 9401, 15_000).await;
        assert_eq!(delivered(&claimed).task_id, Some(after_type));
        assert_eq!(
            hget("q:{default}:qreason", &typed.to_string()).as_deref(),
            Some("hash_type")
        );
        assert!(!backend.is_halted("default"));

        let _: i64 = redis::cmd("ZADD")
            .arg("q:{default}:ready")
            .arg(1)
            .arg("not-a-uuid")
            .query(&mut con)
            .unwrap();
        let EnqueueResult::Enqueued {
            task_id: after_id, ..
        } = enqueue(&backend, job(9500, None, 0)).await
        else {
            panic!("enqueue")
        };
        let claimed = claim_to(&backend, 9500, 15_000).await;
        assert_eq!(delivered(&claimed).task_id, Some(after_id));
        assert_eq!(
            hget("q:{default}:qreason", "not-a-uuid").as_deref(),
            Some("task_id")
        );
        assert!(zscore("q:{default}:ready", "not-a-uuid").is_none());
        assert!(!fresh.is_halted("default"));
    });
}

#[test]
fn blocked_inflight_pages_do_not_hide_a_ready_task() {
    harness(async {
        let backend = open_backend(&[]);
        let mut con = conn();
        for n in 0..17u128 {
            let id = 4_100 + n;
            let EnqueueResult::Enqueued { task_id, .. } = enqueue(&backend, job(id, None, 0)).await
            else {
                panic!("enqueue")
            };
            let member = task_id.to_string();
            let _: i64 = redis::cmd("HSET")
                .arg(job_key(id))
                .arg("control_op")
                .arg("cancel")
                .arg("state")
                .arg("inflight")
                .arg("lease_deadline_ms")
                .arg("1")
                .query(&mut con)
                .unwrap();
            let _: i64 = redis::cmd("ZREM")
                .arg("q:{default}:ready")
                .arg(&member)
                .query(&mut con)
                .unwrap();
            let _: i64 = redis::cmd("ZADD")
                .arg("q:{default}:inflight")
                .arg(1)
                .arg(&member)
                .query(&mut con)
                .unwrap();
        }
        let EnqueueResult::Enqueued { task_id, .. } = enqueue(&backend, job(4900, None, 0)).await
        else {
            panic!("enqueue")
        };
        let claimed = claim_to(&backend, 4900, 15_000).await;
        assert_eq!(delivered(&claimed).task_id, Some(task_id));
        assert_eq!(zcard("q:{default}:inflight"), 18);
        assert!(!backend.is_halted("default"));
    });
}

#[test]
fn renew_and_finish_proceed_while_the_queue_is_halted() {
    harness(async {
        let backend = open_backend(&[]);
        let EnqueueResult::Enqueued { task_id, .. } = enqueue(&backend, job(9600, None, 0)).await
        else {
            panic!("enqueue")
        };
        let claimed = claim_to(&backend, 9600, 15_000).await;
        let _ = delivered(&claimed);
        let mut con = conn();
        let _: () = redis::cmd("SET")
            .arg("q:{default}:halted")
            .arg("during-handler")
            .query(&mut con)
            .unwrap();
        assert!(backend.is_halted("default"));
        let renewed = backend
            .renew(RenewRequest {
                operation_id: op(9601),
                message_id: message(9600),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(9600),
                delivery_token: token(9600),
                lease_ms: 15_000,
            })
            .await
            .unwrap();
        assert!(
            matches!(renewed, arq_redis::RenewResult::Renewed { .. }),
            "{renewed:?}"
        );
        backend
            .finish(FinishRequest {
                operation_id: op(9602),
                message_id: message(9600),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(9600),
                delivery_token: token(9600),
                proposal: outcome(r#"{"outcome":"ok"}"#),
            })
            .await
            .unwrap();
        assert_eq!(hget(&job_key(9600), "state").as_deref(), Some("done"));
        assert_eq!(hget(&job_key(9600), "result").as_deref(), Some("ok"));
        let halted = backend.enqueue(job(9603, None, 0)).await.unwrap_err();
        assert!(matches!(halted, BackendError::Halted { .. }), "{halted}");
        let fresh = open_backend(&[]);
        assert!(fresh.is_halted("default"));
    });
}

#[path = "protocol/edges.rs"]
mod edges;
