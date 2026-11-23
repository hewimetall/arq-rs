//! Operation results and read models returned by `RedisBackend`.

use arq_core::{CycleId, DeliveryToken, DuplicateTask, ObservedTask, TaskId};
use std::sync::Arc;

/// What to do with the `EXEC` reply of the next transaction. Test support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExecFault {
    #[default]
    None,
    /// `DISCARD` and close after the commands are queued, before `EXEC`.
    DropBeforeExec,
    /// Send `EXEC` and do not read the reply.
    LoseReply,
    /// Queue an illegal command so Redis answers `EXECABORT` once.
    AbortOnce,
    /// Queue an illegal command on every transaction until the caller clears it.
    AbortAlways,
}

/// Optional seams around one transaction. Production leaves this empty.
#[derive(Clone, Default)]
pub struct Fault {
    pub exec: ExecFault,
    /// Called with the keys passed to `WATCH`, after commands are queued and before `EXEC`.
    pub before_exec: Option<Arc<dyn Fn(&[String]) + Send + Sync>>,
    /// Called after `ZCARD` of the ready key and before `WATCH`.
    pub after_ready_zcard: Option<Arc<dyn Fn() + Send + Sync>>,
    /// Extra key added only to `WATCH`, so a cluster can answer `CROSSSLOT`.
    pub cross_slot_watch: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EnqueueJob {
    pub operation_id: arq_core::OperationId,
    pub identity: arq_core::MessageIdentity,
    pub cycle_id: CycleId,
    pub actor_name: String,
    pub queue: String,
    pub kwargs: arq_core::Json,
    pub headers: arq_core::Json,
    pub tag: Option<String>,
    pub unique_key: Option<String>,
    pub delay_ms: u64,
    pub max_retries: u64,
    pub max_deliveries: u64,
    pub time_limit_ms: u64,
    pub min_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnqueueResult {
    Enqueued { task_id: TaskId, cycle_id: CycleId, replayed: bool },
    Duplicate(DuplicateTask),
}

#[derive(Debug, Clone)]
pub struct ClaimRequest {
    pub operation_id: arq_core::OperationId,
    pub message_id: arq_core::MessageId,
    pub queue: String,
    pub worker_id: String,
    pub proposed_token: DeliveryToken,
    pub lease_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub observed: ObservedTask,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimResult {
    Idle,
    Delivered(Delivery),
    /// Postconditions proved this delivery and a fresh Redis `TIME` is already at the lease deadline.
    OwnershipLost { exit_code: i32, task_id: TaskId, delivery_token: DeliveryToken },
    DeadLetter { task_id: TaskId, reason: String, replayed: bool },
}

#[derive(Debug, Clone)]
pub struct RenewRequest {
    pub operation_id: arq_core::OperationId,
    pub message_id: arq_core::MessageId,
    pub queue: String,
    pub task_id: TaskId,
    pub cycle_id: CycleId,
    pub delivery_token: DeliveryToken,
    pub lease_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenewResult {
    Renewed { lease_deadline_ms: u64, started_at_ms: Option<u64>, replayed: bool },
    NotHeld { exit_code: i32 },
}

#[derive(Debug, Clone)]
pub struct FinishRequest {
    pub operation_id: arq_core::OperationId,
    pub message_id: arq_core::MessageId,
    pub queue: String,
    pub task_id: TaskId,
    pub cycle_id: CycleId,
    pub delivery_token: DeliveryToken,
    pub proposal: arq_core::ParsedOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinishResult {
    Finished { replayed: bool },
    Rejected,
}

#[derive(Debug, Clone)]
pub struct RequeueRequest {
    pub operation_id: arq_core::OperationId,
    pub queue: String,
    pub task_id: TaskId,
    pub new_cycle_id: CycleId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequeueResult {
    Requeued { cycle_id: CycleId, replayed: bool },
    Duplicate(DuplicateTask),
    NotDead,
    NotFound,
}

#[derive(Debug, Clone)]
pub struct ControlRequest {
    pub operation_id: arq_core::OperationId,
    pub queue: String,
    pub task_id: TaskId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlResult {
    Accepted { cycle_id: Option<CycleId>, replayed: bool },
    AlreadyTerminal,
    NotFound,
    StillOwned,
    NotBlocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeekState {
    Ready,
    Scheduled,
    Inflight,
}

impl PeekState {
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "ready" => Some(Self::Ready),
            "scheduled" => Some(Self::Scheduled),
            "inflight" => Some(Self::Inflight),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Scheduled => "scheduled",
            Self::Inflight => "inflight",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskInfo {
    pub task_id: String,
    pub message_id: Option<String>,
    pub state: String,
    pub deliveries: u64,
    pub retries: u64,
    pub score: Option<u64>,
    pub missing_job: bool,
    pub tag: Option<String>,
    pub reason: String,
    pub cycle_id: Option<String>,
    pub control_op: String,
    pub actor_name: String,
    pub unique_key: Option<String>,
    pub lease_deadline_ms: Option<u64>,
    pub started_at_ms: Option<u64>,
    pub available_at_ms: Option<u64>,
    pub error: String,
    pub stop_evidence: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskPage {
    pub tasks: Vec<TaskInfo>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredReceipt {
    pub operation_id: String,
    pub task_id: String,
    pub op: String,
    pub status: String,
    pub cycle_id: Option<String>,
    pub raw: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueDepth {
    pub queue: String,
    pub ready: u64,
    pub scheduled: u64,
    pub inflight: u64,
    pub done: u64,
    pub dead: u64,
    pub ready_zcard: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildView {
    pub host: String,
    pub parent_pid: u32,
    pub slot: u32,
    pub pid: u32,
    pub ppid: u32,
    pub state: String,
    pub reason: String,
    pub exit_code: Option<i32>,
    pub rss_bytes: u64,
    pub started_at_ms: u64,
    pub ended_at_ms: Option<u64>,
    pub tasks_done: u64,
    pub max_tasks: u64,
    pub alive: bool,
    pub alive_window_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisorView {
    pub host: String,
    pub pid: u32,
    pub started_at_ms: u64,
    pub version: String,
    pub queues: Vec<String>,
    pub processes: u64,
    pub concurrency: u64,
    pub max_tasks: u64,
    pub memory_soft_ratio: String,
    pub memory_hard_ratio: String,
    pub child_memory_soft: Option<u64>,
    pub child_memory_hard: Option<u64>,
    pub cgroup: String,
    pub recycled: u64,
    pub drains: u64,
    pub kills: u64,
    pub restarts: u64,
    pub last_seen_ms: u64,
    pub children: Vec<ChildView>,
    pub in_progress: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub taken_at_ms: u64,
    pub queues: Vec<QueueDepth>,
    pub supervisors: Vec<SupervisorView>,
    pub orphans: Vec<ChildView>,
}

#[derive(Debug, Clone)]
pub struct SupervisorRegistration {
    pub host: String,
    pub pid: u32,
    pub started_at_ms: u64,
    pub version: String,
    pub queues: Vec<String>,
    pub processes: u64,
    pub concurrency: u64,
    pub max_tasks: u64,
    pub memory_soft_ratio: f64,
    pub memory_hard_ratio: f64,
    pub child_memory_soft: Option<u64>,
    pub child_memory_hard: Option<u64>,
    pub cgroup: String,
    pub recycled: u64,
    pub drains: u64,
    pub kills: u64,
    pub restarts: u64,
}

#[derive(Debug, Clone)]
pub struct ChildRegistration {
    pub slot: u32,
    pub pid: u32,
    pub ppid: u32,
    pub state: String,
    pub reason: String,
    pub exit_code: Option<i32>,
    pub rss_bytes: u64,
    pub started_at_ms: u64,
    pub ended_at_ms: Option<u64>,
    pub tasks_done: u64,
    pub max_tasks: u64,
    pub alive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceReport {
    pub examined: u64,
    pub removed: u64,
}
