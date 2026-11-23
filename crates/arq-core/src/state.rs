//! Observed task state. Adapters fill this from Redis; core only compares it.

use crate::ids::{CycleId, DeliveryToken, MessageId, TaskId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Ready,
    Inflight,
    Done,
    Dead,
}

impl TaskState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Inflight => "inflight",
            Self::Done => "done",
            Self::Dead => "dead",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlOp {
    Empty,
    Cancel,
    Kill,
}

impl ControlOp {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "",
            Self::Cancel => "cancel",
            Self::Kill => "kill",
        }
    }

    pub const fn is_blocking(self) -> bool {
        matches!(self, Self::Cancel | Self::Kill)
    }

    pub const fn terminal_reason(self) -> Option<&'static str> {
        match self {
            Self::Cancel => Some("cancelled"),
            Self::Kill => Some("killed"),
            Self::Empty => None,
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "" => Some(Self::Empty),
            "cancel" => Some(Self::Cancel),
            "kill" => Some(Self::Kill),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopEvidence {
    Empty,
    ObservedExit,
    LeaseExpired,
}

impl StopEvidence {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "",
            Self::ObservedExit => "observed_exit",
            Self::LeaseExpired => "lease_expired",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskResult {
    Ok,
    Skipped,
}

impl TaskResult {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Skipped => "skipped",
        }
    }
}

/// One task as read before `MULTI`, plus the index membership of that `task_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedTask {
    pub present: bool,
    pub state: Option<TaskState>,
    pub cycle_id: Option<CycleId>,
    pub task_id: Option<TaskId>,
    pub message_id: Option<MessageId>,
    pub delivery_token: Option<DeliveryToken>,
    pub worker_id: String,
    pub deliveries: u64,
    pub retries: u64,
    pub max_deliveries: u64,
    pub max_retries: u64,
    pub lease_deadline_ms: Option<u64>,
    pub started_at_ms: Option<u64>,
    pub finished_at_ms: Option<u64>,
    pub available_at_ms: Option<u64>,
    pub created_at_ms: Option<u64>,
    pub result: Option<TaskResult>,
    pub reason: String,
    pub error: String,
    pub stop_evidence: StopEvidence,
    pub control_op: ControlOp,
    pub control_cycle_id: Option<CycleId>,
    pub control_delivery_token: Option<DeliveryToken>,
    pub control_operation_id: String,
    pub control_accepted_at_ms: Option<u64>,
    pub actor_name: String,
    pub queue: String,
    pub kwargs_canonical: String,
    pub headers_canonical: String,
    pub tag: Option<String>,
    pub unique_key: Option<String>,
    pub ready_score: Option<u64>,
    pub in_inflight: bool,
    pub in_done: bool,
    pub in_dead: bool,
    pub in_dead_tag: bool,
    pub uniq_owner: Option<TaskId>,
    pub history_contains_prior: bool,
}

impl ObservedTask {
    pub fn absent() -> Self {
        Self {
            present: false,
            state: None,
            cycle_id: None,
            task_id: None,
            message_id: None,
            delivery_token: None,
            worker_id: String::new(),
            deliveries: 0,
            retries: 0,
            max_deliveries: 0,
            max_retries: 0,
            lease_deadline_ms: None,
            started_at_ms: None,
            finished_at_ms: None,
            available_at_ms: None,
            created_at_ms: None,
            result: None,
            reason: String::new(),
            error: String::new(),
            stop_evidence: StopEvidence::Empty,
            control_op: ControlOp::Empty,
            control_cycle_id: None,
            control_delivery_token: None,
            control_operation_id: String::new(),
            control_accepted_at_ms: None,
            actor_name: String::new(),
            queue: String::new(),
            kwargs_canonical: String::new(),
            headers_canonical: String::new(),
            tag: None,
            unique_key: None,
            ready_score: None,
            in_inflight: false,
            in_done: false,
            in_dead: false,
            in_dead_tag: false,
            uniq_owner: None,
            history_contains_prior: false,
        }
    }

    pub fn indexes_contain(&self) -> bool {
        self.ready_score.is_some() || self.in_inflight || self.in_done || self.in_dead || self.in_dead_tag
    }

    /// No hash, no index member, and no uniq. A lost `EXEC` may run again.
    pub fn is_clean_slate(&self) -> bool {
        !self.present && !self.indexes_contain() && self.uniq_owner.is_none()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationKind {
    Enqueue,
    Claim,
    Renew,
    Finish,
    Requeue,
    Cancel,
    Kill,
    Settle,
}

impl OperationKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Enqueue => "enqueue",
            Self::Claim => "claim",
            Self::Renew => "renew",
            Self::Finish => "finish",
            Self::Requeue => "requeue",
            Self::Cancel => "cancel",
            Self::Kill => "kill",
            Self::Settle => "settle",
        }
    }
}

/// Status stored in a receipt. `still_owned` and `not_blocked` are not stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptStatus {
    Enqueued,
    Duplicate,
    Claimed,
    Dead,
    Renewed,
    Finished,
    Requeued,
    Accepted,
    AlreadyTerminal,
    NotFound,
    NotDead,
}

impl ReceiptStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Enqueued => "enqueued",
            Self::Duplicate => "duplicate",
            Self::Claimed => "claimed",
            Self::Dead => "dead",
            Self::Renewed => "renewed",
            Self::Finished => "finished",
            Self::Requeued => "requeued",
            Self::Accepted => "accepted",
            Self::AlreadyTerminal => "already_terminal",
            Self::NotFound => "not_found",
            Self::NotDead => "not_dead",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptRecord {
    pub operation_id: OperationId,
    pub op: OperationKind,
    pub cycle_id: Option<CycleId>,
    pub task_id: TaskId,
    pub request_canonical: String,
    pub result_canonical: String,
    pub status: ReceiptStatus,
    pub delivery_token: Option<DeliveryToken>,
    pub lease_deadline_ms: Option<u64>,
    pub started_at_ms: Option<u64>,
    pub deliveries: Option<u64>,
    pub ready_score: Option<u64>,
}

pub use crate::ids::OperationId;
