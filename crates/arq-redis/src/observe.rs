//! Fill `ObservedTask` and `ReceiptRecord` from Redis. Core only compares them.

use crate::session::{Fail, Session};
use arq_core::{
    ControlOp, CycleId, DeliveryToken, Json, MessageId, ObservedTask, OperationId, OperationKind, QueueKeys,
    ReceiptRecord, ReceiptStatus, StopEvidence, TaskId, TaskResult, TaskState,
};

pub(crate) struct Loaded {
    pub receipt: Option<ReceiptRecord>,
    pub observed: ObservedTask,
}

pub(crate) fn load_for_task(
    session: &mut Session,
    keys: &QueueKeys,
    task_id: TaskId,
    unique_hint: Option<&str>,
    operation_id: Option<OperationId>,
) -> Result<Loaded, Fail> {
    let receipt = match operation_id {
        Some(operation_id) => read_receipt(session, keys, operation_id)?,
        None => None,
    };
    let observed = read_task(session, keys, task_id, unique_hint)?;
    Ok(Loaded { receipt, observed })
}

pub(crate) fn read_receipt(
    session: &mut Session,
    keys: &QueueKeys,
    operation_id: OperationId,
) -> Result<Option<ReceiptRecord>, Fail> {
    let key = keys.receipt(&operation_id);
    session.expect_type(&key)?;
    let Some(raw) = session.get(&key)? else {
        return Ok(None);
    };
    parse_receipt(&raw).map(Some).map_err(|detail| {
        Fail::Backend(crate::session::inconsistency(
            keys.queue(),
            Some(operation_id),
            detail,
        ))
    })
}

pub(crate) fn read_task(
    session: &mut Session,
    keys: &QueueKeys,
    task_id: TaskId,
    unique_hint: Option<&str>,
) -> Result<ObservedTask, Fail> {
    let job_key = keys.job(&task_id);
    session.expect_type(&job_key)?;
    let kind = session.key_type(&job_key)?;
    let mut observed = if kind == "none" {
        ObservedTask::absent()
    } else {
        let fields = session.hgetall(&job_key)?;
        hash_to_task(&fields)
    };
    let unique_key = observed.unique_key.clone().or_else(|| unique_hint.map(str::to_string));
    if let Some(unique_key) = unique_key.as_deref() {
        if let Ok(uniq_key) = keys.uniq(unique_key) {
            session.expect_type(&uniq_key)?;
            observed.uniq_owner = match session.get(&uniq_key)? {
                Some(owner) => TaskId::parse(&owner).ok(),
                None => None,
            };
        }
    }
    let member = task_id.to_string();
    session.expect_type(&keys.ready())?;
    session.expect_type(&keys.inflight())?;
    session.expect_type(&keys.done())?;
    session.expect_type(&keys.dead())?;
    observed.ready_score = session.zscore(&keys.ready(), &member)?;
    observed.in_inflight = session.zscore(&keys.inflight(), &member)?.is_some();
    observed.in_done = session.zscore(&keys.done(), &member)?.is_some();
    observed.in_dead = session.zscore(&keys.dead(), &member)?.is_some();
    if let Some(tag) = observed.tag.clone() {
        if let Ok(tag_key) = keys.dead_tag(&tag) {
            session.expect_type(&tag_key)?;
            observed.in_dead_tag = session.zscore(&tag_key, &member)?.is_some();
        }
    }
    Ok(observed)
}

fn hash_to_task(fields: &[(String, String)]) -> ObservedTask {
    let mut observed = ObservedTask::absent();
    if fields.is_empty() {
        observed.present = true;
        return observed;
    }
    observed.present = true;
    let get = |name: &str| fields.iter().find(|(key, _)| key == name).map(|(_, value)| value.as_str());
    observed.state = get("state").and_then(parse_state);
    observed.cycle_id = get("cycle_id").and_then(|text| CycleId::parse(text).ok());
    observed.task_id = get("task_id").and_then(|text| TaskId::parse(text).ok());
    observed.message_id = get("message_id").and_then(|text| MessageId::parse(text).ok());
    observed.delivery_token = get("delivery_token").and_then(parse_token);
    observed.worker_id = get("worker_id").unwrap_or("").to_string();
    observed.deliveries = get("deliveries").and_then(|text| text.parse().ok()).unwrap_or(0);
    observed.retries = get("retries").and_then(|text| text.parse().ok()).unwrap_or(0);
    observed.max_deliveries = get("max_deliveries").and_then(|text| text.parse().ok()).unwrap_or(0);
    observed.max_retries = get("max_retries").and_then(|text| text.parse().ok()).unwrap_or(0);
    observed.lease_deadline_ms = get("lease_deadline_ms").and_then(|text| text.parse().ok());
    observed.started_at_ms = get("started_at_ms").and_then(|text| text.parse().ok());
    observed.finished_at_ms = get("finished_at_ms").and_then(|text| text.parse().ok());
    observed.available_at_ms = get("available_at_ms").and_then(|text| text.parse().ok());
    observed.created_at_ms = get("created_at_ms").and_then(|text| text.parse().ok());
    observed.result = get("result").and_then(parse_result);
    observed.reason = get("reason").unwrap_or("").to_string();
    observed.error = get("error").unwrap_or("").to_string();
    observed.stop_evidence = get("stop_evidence").map(parse_evidence).unwrap_or(StopEvidence::Empty);
    observed.control_op = get("control_op").and_then(ControlOp::parse).unwrap_or(ControlOp::Empty);
    observed.control_cycle_id = get("control_cycle_id").and_then(|text| CycleId::parse(text).ok());
    observed.control_delivery_token = get("control_delivery_token").and_then(parse_token);
    observed.control_operation_id = get("control_operation_id").unwrap_or("").to_string();
    observed.control_accepted_at_ms = get("control_accepted_at_ms").and_then(|text| text.parse().ok());
    observed.actor_name = get("actor_name").unwrap_or("").to_string();
    observed.queue = get("queue").unwrap_or("").to_string();
    observed.kwargs_canonical = get("kwargs").unwrap_or("").to_string();
    observed.headers_canonical = get("headers").unwrap_or("").to_string();
    observed.tag = get("tag").filter(|text| !text.is_empty()).map(str::to_string);
    observed.unique_key = get("unique_key").filter(|text| !text.is_empty()).map(str::to_string);
    observed.history_contains_prior = match get("history") {
        Some(text) => Json::parse(text).ok().and_then(|json| json.as_array().map(|items| !items.is_empty())).unwrap_or(false),
        None => false,
    };
    observed
}

pub(crate) fn history_json(observed_history_field: Option<&str>) -> Json {
    match observed_history_field {
        Some(text) => Json::parse(text).unwrap_or(Json::Array(Vec::new())),
        None => Json::Array(Vec::new()),
    }
}

pub(crate) fn read_history(session: &mut Session, keys: &QueueKeys, task_id: TaskId) -> Result<Json, Fail> {
    let fields = session.hgetall(&keys.job(&task_id))?;
    let text = fields.iter().find(|(key, _)| key == "history").map(|(_, value)| value.as_str());
    Ok(history_json(text))
}

fn parse_state(text: &str) -> Option<TaskState> {
    match text {
        "ready" => Some(TaskState::Ready),
        "inflight" => Some(TaskState::Inflight),
        "done" => Some(TaskState::Done),
        "dead" => Some(TaskState::Dead),
        _ => None,
    }
}

fn parse_result(text: &str) -> Option<TaskResult> {
    match text {
        "ok" => Some(TaskResult::Ok),
        "skipped" => Some(TaskResult::Skipped),
        "" => None,
        _ => None,
    }
}

fn parse_evidence(text: &str) -> StopEvidence {
    match text {
        "observed_exit" => StopEvidence::ObservedExit,
        "lease_expired" => StopEvidence::LeaseExpired,
        _ => StopEvidence::Empty,
    }
}

fn parse_token(text: &str) -> Option<DeliveryToken> {
    if text.is_empty() {
        None
    } else {
        DeliveryToken::parse(text).ok()
    }
}

pub(crate) fn parse_receipt(raw: &str) -> Result<ReceiptRecord, String> {
    let json = Json::parse(raw).map_err(|error| error.to_string())?;
    let operation_id = required_id(&json, "operation_id", OperationId::parse)?;
    let task_id = required_id(&json, "task_id", TaskId::parse)?;
    let op = match json.get("op").and_then(Json::as_str).and_then(parse_kind) {
        Some(op) => op,
        None => return Err("receipt op".to_string()),
    };
    let cycle_id = match json.get("cycle_id") {
        Some(Json::Null) | None => None,
        Some(Json::String(text)) => Some(CycleId::parse(text).map_err(|error| error.to_string())?),
        _ => return Err("receipt cycle_id".to_string()),
    };
    let request = json.get("request").cloned().ok_or_else(|| "receipt request".to_string())?;
    let result = json.get("result").cloned().ok_or_else(|| "receipt result".to_string())?;
    let status = match result.get("status").and_then(Json::as_str).and_then(parse_status) {
        Some(status) => status,
        None => return Err("receipt status".to_string()),
    };
    Ok(ReceiptRecord {
        operation_id,
        op,
        cycle_id,
        task_id,
        request_canonical: request.canonical(),
        result_canonical: result.canonical(),
        status,
        delivery_token: result.get("delivery_token").and_then(Json::as_str).and_then(|text| DeliveryToken::parse(text).ok()),
        lease_deadline_ms: result.get("lease_deadline_ms").and_then(Json::as_u64),
        started_at_ms: match result.get("started_at_ms") {
            Some(Json::Null) | None => None,
            Some(value) => value.as_u64(),
        },
        deliveries: result.get("deliveries").and_then(Json::as_u64),
        ready_score: result.get("available_at_ms").and_then(Json::as_u64),
    })
}

fn required_id<T>(json: &Json, field: &str, parse: fn(&str) -> Result<T, arq_core::InvalidIdentifier>) -> Result<T, String> {
    let text = json.get(field).and_then(Json::as_str).ok_or_else(|| format!("receipt {field}"))?;
    parse(text).map_err(|error| error.to_string())
}

fn parse_kind(text: &str) -> Option<OperationKind> {
    Some(match text {
        "enqueue" => OperationKind::Enqueue,
        "claim" => OperationKind::Claim,
        "renew" => OperationKind::Renew,
        "finish" => OperationKind::Finish,
        "requeue" => OperationKind::Requeue,
        "cancel" => OperationKind::Cancel,
        "kill" => OperationKind::Kill,
        "settle" => OperationKind::Settle,
        _ => return None,
    })
}

fn parse_status(text: &str) -> Option<ReceiptStatus> {
    Some(match text {
        "enqueued" => ReceiptStatus::Enqueued,
        "duplicate" => ReceiptStatus::Duplicate,
        "claimed" => ReceiptStatus::Claimed,
        "dead" => ReceiptStatus::Dead,
        "renewed" => ReceiptStatus::Renewed,
        "finished" => ReceiptStatus::Finished,
        "requeued" => ReceiptStatus::Requeued,
        "accepted" => ReceiptStatus::Accepted,
        "already_terminal" => ReceiptStatus::AlreadyTerminal,
        "not_found" => ReceiptStatus::NotFound,
        "not_dead" => ReceiptStatus::NotDead,
        _ => return None,
    })
}

/// When a receipt exists, line the plan's timestamps up with the hash it already wrote.
pub(crate) fn align_requeue_score(receipt: &mut ReceiptRecord, observed: &ObservedTask) {
    if receipt.op == OperationKind::Requeue && receipt.ready_score.is_none() && receipt.cycle_id == observed.cycle_id {
        receipt.ready_score = observed.ready_score;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::Session;
    use arq_core::CanonicalUuid;

    fn id(byte: u8) -> TaskId {
        TaskId::from_uuid(CanonicalUuid::from_random_v4([byte; 16]))
    }

    #[test]
    fn parsers_and_odd_hashes() {
        let empty = hash_to_task(&[]);
        assert!(empty.present && empty.state.is_none());
        let weird = hash_to_task(&[
            ("state".into(), "nope".into()),
            ("result".into(), "nope".into()),
            ("history".into(), "{".into()),
            ("delivery_token".into(), String::new()),
            ("stop_evidence".into(), "nope".into()),
            ("tag".into(), String::new()),
            ("unique_key".into(), String::new()),
        ]);
        assert!(weird.state.is_none());
        assert!(weird.result.is_none());
        assert!(!weird.history_contains_prior);
        assert!(matches!(weird.stop_evidence, StopEvidence::Empty));
        assert!(parse_state("later").is_none());
        assert!(parse_result("later").is_none());
        assert!(parse_kind("nope").is_none());
        assert!(parse_status("nope").is_none());
        assert!(parse_token("").is_none());
        assert!(matches!(history_json(None), Json::Array(_)));
        assert!(matches!(history_json(Some("{")), Json::Array(_)));

        let operation = OperationId::from_uuid(CanonicalUuid::from_random_v4([3; 16]));
        let task = id(4);
        assert!(parse_receipt("{").is_err());
        let missing_op = format!(r#"{{"operation_id":"{operation}","task_id":"{task}","request":{{}},"result":{{"status":"enqueued"}}}}"#);
        assert!(parse_receipt(&missing_op).unwrap_err().contains("op"));
        let bad_op = format!(r#"{{"operation_id":"{operation}","task_id":"{task}","op":"nope","request":{{}},"result":{{"status":"enqueued"}}}}"#);
        assert!(parse_receipt(&bad_op).unwrap_err().contains("op"));
        let bad_cycle = format!(r#"{{"operation_id":"{operation}","task_id":"{task}","op":"enqueue","cycle_id":1,"request":{{}},"result":{{"status":"enqueued"}}}}"#);
        assert!(parse_receipt(&bad_cycle).unwrap_err().contains("cycle"));
        let bad_status = format!(r#"{{"operation_id":"{operation}","task_id":"{task}","op":"enqueue","request":{{}},"result":{{}}}}"#);
        assert!(parse_receipt(&bad_status).unwrap_err().contains("status"));
        let unknown_status = format!(r#"{{"operation_id":"{operation}","task_id":"{task}","op":"enqueue","request":{{}},"result":{{"status":"nope"}}}}"#);
        assert!(parse_receipt(&unknown_status).unwrap_err().contains("status"));

        let url = "redis://127.0.0.1:6379/1";
        let client = redis::Client::open(url).unwrap();
        let mut connection = client.get_connection().unwrap();
        let _: () = redis::cmd("FLUSHDB").query(&mut connection).unwrap();
        let keys = QueueKeys::new("coverq").unwrap();
        let task = id(5);
        let _: i64 = redis::cmd("HSET")
            .arg(keys.job(&task))
            .arg("state")
            .arg("ready")
            .arg("unique_key")
            .arg("{bad")
            .arg("tag")
            .arg("bad tag")
            .query(&mut connection)
            .unwrap();
        let mut session = Session::open(url, false).unwrap();
        let observed = read_task(&mut session, &keys, task, None).unwrap();
        assert!(observed.present);
        assert!(observed.uniq_owner.is_none());
        assert!(!observed.in_dead_tag);
    }
}
