//! Protocol errors named by the contract.

use crate::ids::{MessageId, OperationId, TaskId};
use std::fmt;

/// A command inside `EXEC` failed, the reply length was wrong, or a receipt contradicts its postconditions.
/// The process stops mutating the queue. Nothing is repaired automatically.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageInconsistency {
    pub queue: String,
    pub operation_id: Option<OperationId>,
    pub detail: String,
}

impl fmt::Display for StorageInconsistency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "storage inconsistency in queue {}: {}",
            self.queue, self.detail
        )
    }
}

impl std::error::Error for StorageInconsistency {}

/// The `EXEC` reply was lost and the receipt plus postconditions neither prove success nor a clean retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndeterminateOperation {
    pub operation_id: OperationId,
    pub message_id: MessageId,
}

impl fmt::Display for IndeterminateOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "indeterminate operation {} for message {}",
            self.operation_id, self.message_id
        )
    }
}

impl std::error::Error for IndeterminateOperation {}

/// The same `operation_id` was reused with a different `op` or a different canonical request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationConflict {
    pub operation_id: OperationId,
    pub existing_op: String,
    pub existing_request: String,
    pub attempted_op: String,
    pub attempted_request: String,
}

impl fmt::Display for OperationConflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "operation {} conflicts: stored {} request {}, attempted {} request {}",
            self.operation_id,
            self.existing_op,
            self.existing_request,
            self.attempted_op,
            self.attempted_request
        )
    }
}

impl std::error::Error for OperationConflict {}

/// `ZCARD` of the whole ready set, including future scores, is at least `ARQ_MAX_READY`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueFull {
    pub queue: String,
    pub max_ready: u64,
    pub ready_zcard: u64,
}

impl fmt::Display for QueueFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "queue {} is full: ready zcard {} >= max_ready {}",
            self.queue, self.ready_zcard, self.max_ready
        )
    }
}

impl std::error::Error for QueueFull {}

/// `unique_key` is already held by another `task_id`. The task hash is not created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateTask {
    pub unique_key: String,
    pub task_id: TaskId,
    pub owner_task_id: TaskId,
}

impl fmt::Display for DuplicateTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "duplicate task for unique key {}: {} is held by {}",
            self.unique_key, self.task_id, self.owner_task_id
        )
    }
}

impl std::error::Error for DuplicateTask {}

/// Caller input that never reaches Redis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputError {
    Queue,
    Opaque { field: &'static str },
    Headers,
    Kwargs,
    Identity,
    Backoff,
    TimeLimit,
    MaxDeliveries,
    WorkerId,
    History,
    Payload { field: &'static str },
    Overflow { field: &'static str },
}

impl fmt::Display for InputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Queue => write!(f, "queue name must match [a-z0-9]+"),
            Self::Opaque { field } => write!(
                f,
                "{field} must be 1 to 256 UTF-8 bytes without controls, spaces, or braces"
            ),
            Self::Headers => write!(f, "headers must be a JSON object of strings"),
            Self::Kwargs => write!(f, "kwargs must be a JSON object"),
            Self::Identity => write!(f, "task_id and message_id must be the same UUID"),
            Self::Backoff => write!(f, "backoff requires 0 < min_backoff_ms <= max_backoff_ms"),
            Self::TimeLimit => write!(f, "time_limit_ms must be > 0"),
            Self::MaxDeliveries => write!(f, "max_deliveries must be >= 1"),
            Self::WorkerId => write!(f, "worker_id must be non-empty"),
            Self::History => write!(f, "history must be a JSON array"),
            Self::Payload { field } => write!(f, "{field} exceeds the configured byte limit"),
            Self::Overflow { field } => write!(f, "{field} overflowed u64"),
        }
    }
}

impl std::error::Error for InputError {}
