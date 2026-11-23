//! Failures of the concrete Redis adapter.
//!
//! Storage inconsistency is process-local: the backend stops mutating that queue
//! and does not repair indexes or clear uniq.

use arq_core::{
    IndeterminateOperation, InputError, OperationConflict, QueueFull, StorageInconsistency, TransactionConflict,
};
use std::fmt;

#[derive(Debug)]
pub enum BackendError {
    Input(InputError),
    Full(QueueFull),
    Conflict(OperationConflict),
    Inconsistent(StorageInconsistency),
    Indeterminate(IndeterminateOperation),
    Transaction(TransactionConflict),
    /// This process already observed `StorageInconsistency` for the queue.
    Halted { queue: String },
    Transport { detail: String },
    Page { detail: String },
}

impl fmt::Display for BackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Input(error) => write!(f, "{error}"),
            Self::Full(error) => write!(f, "{error}"),
            Self::Conflict(error) => write!(f, "{error}"),
            Self::Inconsistent(error) => write!(f, "{error}"),
            Self::Indeterminate(error) => write!(f, "{error}"),
            Self::Transaction(error) => write!(f, "{error}"),
            Self::Halted { queue } => write!(f, "queue {queue} is halted after storage inconsistency"),
            Self::Transport { detail } => write!(f, "redis transport: {detail}"),
            Self::Page { detail } => write!(f, "invalid page: {detail}"),
        }
    }
}

impl std::error::Error for BackendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Input(error) => Some(error),
            Self::Full(error) => Some(error),
            Self::Conflict(error) => Some(error),
            Self::Inconsistent(error) => Some(error),
            Self::Indeterminate(error) => Some(error),
            Self::Transaction(error) => Some(error),
            Self::Halted { .. } | Self::Transport { .. } | Self::Page { .. } => None,
        }
    }
}

pub(crate) fn transport(detail: impl Into<String>) -> BackendError {
    BackendError::Transport { detail: detail.into() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arq_core::{CanonicalUuid, MessageId, OperationId};
    use std::error::Error;

    fn uuid(n: u128) -> CanonicalUuid {
        CanonicalUuid::from_random_v4(n.to_be_bytes())
    }

    #[test]
    fn every_variant_displays_and_exposes_its_source() {
        let operation = OperationId::from_uuid(uuid(1));
        let message = MessageId::from_uuid(uuid(1));
        let errors = [
            BackendError::Input(InputError::Queue),
            BackendError::Input(InputError::WorkerId),
            BackendError::Full(QueueFull { queue: "default".into(), max_ready: 1, ready_zcard: 2 }),
            BackendError::Conflict(OperationConflict {
                operation_id: operation,
                existing_op: "enqueue".into(),
                existing_request: "{}".into(),
                attempted_op: "enqueue".into(),
                attempted_request: "{\"a\":1}".into(),
            }),
            BackendError::Inconsistent(StorageInconsistency {
                queue: "default".into(),
                operation_id: Some(operation),
                detail: "type".into(),
            }),
            BackendError::Indeterminate(IndeterminateOperation { operation_id: operation, message_id: message }),
            BackendError::Transaction(TransactionConflict { limit: 1 }),
            BackendError::Halted { queue: "default".into() },
            BackendError::Transport { detail: "closed".into() },
            BackendError::Page { detail: "cursor".into() },
        ];
        for error in errors {
            assert!(!error.to_string().is_empty());
            match &error {
                BackendError::Halted { .. } | BackendError::Transport { .. } | BackendError::Page { .. } => {
                    assert!(error.source().is_none());
                }
                _ => assert!(error.source().is_some()),
            }
        }
    }
}
