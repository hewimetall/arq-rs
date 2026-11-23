use arq_core::*;
use std::time::Duration;

pub const TASK: &str = "3f1c0b2a-7e4d-4c1a-9b2e-6d5a4c3b2a10";
pub const OP: &str = "11111111-2222-4333-8444-555555555555";
pub const CYCLE: &str = "6b1f1c2a-1111-4222-8333-444455556666";
pub const TOKEN: &str = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
pub const CYCLE2: &str = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff";
pub const OTHER: &str = "cccccccc-dddd-4eee-8fff-000000000000";

pub fn task() -> TaskId {
    TaskId::parse(TASK).unwrap()
}
pub fn other() -> TaskId {
    TaskId::parse(OTHER).unwrap()
}
pub fn op() -> OperationId {
    OperationId::parse(OP).unwrap()
}
pub fn cycle() -> CycleId {
    CycleId::parse(CYCLE).unwrap()
}
pub fn cycle2() -> CycleId {
    CycleId::parse(CYCLE2).unwrap()
}
pub fn token() -> DeliveryToken {
    DeliveryToken::parse(TOKEN).unwrap()
}
pub fn message() -> MessageId {
    MessageId::parse(TASK).unwrap()
}
pub fn identity() -> MessageIdentity {
    MessageIdentity::new(task().as_uuid())
}

pub fn cfg() -> Config {
    Config::default()
}

pub fn cfg_with(pairs: &[(&str, &str)]) -> Config {
    Config::parse(pairs.iter().copied()).unwrap()
}

pub fn object(text: &str) -> Json {
    Json::parse(text).unwrap()
}

pub fn blank() -> ObservedTask {
    ObservedTask::absent()
}

pub fn ready_job() -> ObservedTask {
    let mut job = ObservedTask::absent();
    job.present = true;
    job.state = Some(TaskState::Ready);
    job.task_id = Some(task());
    job.message_id = Some(message());
    job.cycle_id = Some(cycle());
    job.actor_name = "process_item".into();
    job.queue = "default".into();
    job.kwargs_canonical = r#"{"item_id":"1001"}"#.into();
    job.headers_canonical = "{}".into();
    job.max_deliveries = 5;
    job.available_at_ms = Some(1_000);
    job.ready_score = Some(1_000);
    job.deliveries = 0;
    job.retries = 0;
    job
}

pub fn inflight_job() -> ObservedTask {
    let mut job = ready_job();
    job.state = Some(TaskState::Inflight);
    job.delivery_token = Some(token());
    job.worker_id = "worker-a".into();
    job.deliveries = 1;
    job.lease_deadline_ms = Some(20_000);
    job.started_at_ms = Some(1_000);
    job.ready_score = None;
    job.in_inflight = true;
    job.available_at_ms = Some(1_000);
    job
}

pub fn enq<'a>(
    config: &'a Config,
    kwargs: &'a Json,
    headers: &'a Json,
    observed: &'a ObservedTask,
    receipt: Option<&'a ReceiptRecord>,
) -> EnqueueInput<'a> {
    EnqueueInput {
        config,
        operation_id: op(),
        identity: identity(),
        cycle_id: cycle(),
        actor_name: "process_item",
        queue: "default",
        kwargs,
        headers,
        tag: Some("batch-a"),
        unique_key: Some("item:1001"),
        delay_ms: 0,
        now_ms: 1_000,
        max_retries: 3,
        max_deliveries: 5,
        time_limit_ms: 30_000,
        min_backoff_ms: 100,
        max_backoff_ms: 1_000,
        ready_zcard: 0,
        receipt,
        observed,
    }
}

pub fn claim_in<'a>(config: &'a Config, observed: &'a ObservedTask, receipt: Option<&'a ReceiptRecord>) -> ClaimInput<'a> {
    ClaimInput {
        config,
        operation_id: op(),
        message_id: message(),
        queue: "default",
        worker_id: "worker-a",
        proposed_token: token(),
        now_ms: 1_000,
        lease_ms: 15_000,
        receipt,
        observed,
    }
}

pub fn renew_in<'a>(config: &'a Config, observed: &'a ObservedTask, receipt: Option<&'a ReceiptRecord>) -> RenewInput<'a> {
    RenewInput {
        config,
        operation_id: op(),
        message_id: message(),
        queue: "default",
        task_id: task(),
        cycle_id: cycle(),
        delivery_token: token(),
        now_ms: 1_000,
        lease_ms: 15_000,
        receipt,
        observed,
    }
}

pub fn finish_in<'a>(
    config: &'a Config,
    proposal: &'a ParsedOutcome,
    observed: &'a ObservedTask,
    receipt: Option<&'a ReceiptRecord>,
) -> FinishInput<'a> {
    FinishInput {
        config,
        operation_id: op(),
        message_id: message(),
        queue: "default",
        task_id: task(),
        cycle_id: cycle(),
        delivery_token: token(),
        now_ms: 5_000,
        proposal,
        receipt,
        observed,
    }
}

pub fn requeue_in<'a>(
    config: &'a Config,
    history: &'a Json,
    observed: &'a ObservedTask,
    receipt: Option<&'a ReceiptRecord>,
) -> RequeueInput<'a> {
    RequeueInput {
        config,
        operation_id: op(),
        message_id: message(),
        queue: "default",
        task_id: task(),
        new_cycle_id: cycle2(),
        now_ms: 9_000,
        history,
        receipt,
        observed,
    }
}

pub fn control_in<'a>(config: &'a Config, observed: &'a ObservedTask, receipt: Option<&'a ReceiptRecord>) -> ControlInput<'a> {
    ControlInput {
        config,
        operation_id: op(),
        message_id: message(),
        queue: "default",
        task_id: task(),
        now_ms: 4_000,
        receipt,
        observed,
    }
}

pub fn settle_in<'a>(config: &'a Config, observed: &'a ObservedTask, receipt: Option<&'a ReceiptRecord>) -> SettleInput<'a> {
    SettleInput {
        config,
        operation_id: op(),
        message_id: message(),
        queue: "default",
        task_id: task(),
        now_ms: 8_000,
        receipt,
        observed,
    }
}

pub fn receipt(kind: OperationKind, status: ReceiptStatus, request: &Json) -> ReceiptRecord {
    ReceiptRecord {
        operation_id: op(),
        op: kind,
        cycle_id: Some(cycle()),
        task_id: task(),
        request_canonical: request.canonical(),
        result_canonical: String::new(),
        status,
        delivery_token: Some(token()),
        lease_deadline_ms: Some(16_000),
        started_at_ms: Some(1_000),
        deliveries: Some(1),
        ready_score: Some(1_000),
    }
}

pub fn touch_error(err: &dyn std::error::Error) {
    let _ = err.to_string();
    assert!(err.source().is_none());
}

pub fn secs(value: u64) -> Duration {
    Duration::from_secs(value)
}
