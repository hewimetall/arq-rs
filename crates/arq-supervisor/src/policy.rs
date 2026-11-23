//! Child exit codes, restart backoff, and storage failures.
//!
//! A connection error that is still inside the watch-retry budget is not exit 5.
//! Exit 2 does not finish the delivery and does not call `on_stop`.

use std::time::Duration;

use arq_core::{CONFIG_EXIT_CODE, OWNERSHIP_LOST_EXIT_CODE, PING_BUDGET_EXIT_CODE};
use arq_redis::BackendError;

pub const REDIS_EXIT_CODE: i32 = 5;
pub const BACKOFF_MIN: Duration = Duration::from_secs(1);
pub const BACKOFF_MAX: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildExit {
    /// Exit 0. Replace the slot immediately and reset backoff to 1s.
    Recycle,
    /// Exit 2, 4, 5, or any other non-fatal code. Restart after backoff.
    Restart,
    /// Exit 3. Stop the supervisor and do not restart the slot.
    Fatal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExitEffects {
    pub class: ChildExit,
    pub finish: bool,
    pub on_stop: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackoffStep {
    Reset,
    Wait(Duration),
    Stop,
}

pub fn exit_effects(code: Option<i32>) -> ExitEffects {
    match code {
        Some(0) => ExitEffects { class: ChildExit::Recycle, finish: true, on_stop: true },
        Some(code) if code == PING_BUDGET_EXIT_CODE => {
            ExitEffects { class: ChildExit::Restart, finish: false, on_stop: false }
        }
        Some(code) if code == CONFIG_EXIT_CODE => {
            ExitEffects { class: ChildExit::Fatal, finish: false, on_stop: false }
        }
        Some(code) if code == OWNERSHIP_LOST_EXIT_CODE => {
            ExitEffects { class: ChildExit::Restart, finish: false, on_stop: true }
        }
        Some(code) if code == REDIS_EXIT_CODE => {
            ExitEffects { class: ChildExit::Restart, finish: false, on_stop: false }
        }
        Some(_) | None => ExitEffects { class: ChildExit::Restart, finish: false, on_stop: false },
    }
}

pub fn classify_child_exit(code: Option<i32>) -> ChildExit {
    exit_effects(code).class
}

pub fn next_backoff(current: Duration) -> Duration {
    if current.is_zero() {
        return BACKOFF_MIN;
    }
    let current = clamp_backoff(current);
    if current >= BACKOFF_MAX {
        BACKOFF_MAX
    } else {
        current.saturating_mul(2).min(BACKOFF_MAX)
    }
}

pub fn backoff_step(code: Option<i32>, current: Duration) -> (BackoffStep, Duration) {
    match classify_child_exit(code) {
        ChildExit::Recycle => (BackoffStep::Reset, BACKOFF_MIN),
        ChildExit::Fatal => (BackoffStep::Stop, current),
        ChildExit::Restart => {
            let wait = clamp_backoff(current);
            (BackoffStep::Wait(wait), next_backoff(wait))
        }
    }
}

pub fn clamp_backoff(current: Duration) -> Duration {
    if current < BACKOFF_MIN {
        BACKOFF_MIN
    } else if current > BACKOFF_MAX {
        BACKOFF_MAX
    } else {
        current
    }
}

pub fn max_tasks_reached(done: u64, max_tasks: u64) -> bool {
    max_tasks > 0 && done >= max_tasks
}

/// `None` means the child keeps its loop. Exit 5 is only after the budget is spent.
pub fn connection_within_budget(used: &mut u64, limit: u64) -> Option<i32> {
    if *used < limit {
        *used = used.saturating_add(1);
        None
    } else {
        Some(REDIS_EXIT_CODE)
    }
}

/// Watch-budget exhaustion and a hard Redis failure are exit 5.
/// A transport error that still has retries left is not.
pub fn exit_for_storage(error: &BackendError, used: &mut u64, limit: u64) -> Option<i32> {
    match error {
        BackendError::Transport { .. } => connection_within_budget(used, limit),
        BackendError::Transaction(_)
        | BackendError::Inconsistent(_)
        | BackendError::Indeterminate(_)
        | BackendError::Halted { .. }
        | BackendError::Full(_)
        | BackendError::Conflict(_)
        | BackendError::Page { .. } => Some(REDIS_EXIT_CODE),
        BackendError::Input(_) => Some(CONFIG_EXIT_CODE),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arq_core::{
        CanonicalUuid, IndeterminateOperation, InputError, MessageId, OperationConflict, OperationId, QueueFull,
        StorageInconsistency, TransactionConflict,
    };

    fn uuid(n: u128) -> CanonicalUuid {
        CanonicalUuid::from_random_v4(n.to_be_bytes())
    }

    #[test]
    fn exit_codes_select_replace_backoff_or_stop() {
        assert_eq!(classify_child_exit(Some(0)), ChildExit::Recycle);
        assert_eq!(classify_child_exit(Some(2)), ChildExit::Restart);
        assert_eq!(classify_child_exit(Some(3)), ChildExit::Fatal);
        assert_eq!(classify_child_exit(Some(4)), ChildExit::Restart);
        assert_eq!(classify_child_exit(Some(5)), ChildExit::Restart);
        assert_eq!(classify_child_exit(Some(1)), ChildExit::Restart);
        assert_eq!(classify_child_exit(None), ChildExit::Restart);

        let recycle = exit_effects(Some(0));
        assert!(recycle.finish && recycle.on_stop);
        let ping = exit_effects(Some(2));
        assert!(!ping.finish && !ping.on_stop);
        let config = exit_effects(Some(3));
        assert_eq!(config.class, ChildExit::Fatal);
        assert!(!config.finish && !config.on_stop);
        let lost = exit_effects(Some(4));
        assert!(!lost.finish && lost.on_stop);
        let redis = exit_effects(Some(5));
        assert!(!redis.finish && !redis.on_stop);
        assert!(!exit_effects(None).finish);
    }

    #[test]
    fn backoff_starts_at_one_second_and_caps_at_thirty() {
        let mut current = BACKOFF_MIN;
        let mut waits = Vec::new();
        for _ in 0..8 {
            let (step, next) = backoff_step(Some(5), current);
            match step {
                BackoffStep::Wait(duration) => waits.push(duration),
                other => panic!("{other:?}"),
            }
            current = next;
        }
        assert_eq!(
            waits,
            [1, 2, 4, 8, 16, 30, 30, 30].map(Duration::from_secs).to_vec()
        );
        assert_eq!(next_backoff(Duration::ZERO), BACKOFF_MIN);
        assert_eq!(next_backoff(Duration::from_secs(20)), BACKOFF_MAX);
        assert_eq!(clamp_backoff(Duration::from_millis(1)), BACKOFF_MIN);
        assert_eq!(clamp_backoff(Duration::from_secs(90)), BACKOFF_MAX);
        assert_eq!(clamp_backoff(Duration::from_secs(4)), Duration::from_secs(4));

        let (step, next) = backoff_step(Some(0), Duration::from_secs(16));
        assert_eq!(step, BackoffStep::Reset);
        assert_eq!(next, BACKOFF_MIN);
        let (step, _) = backoff_step(Some(2), Duration::from_secs(8));
        assert_eq!(step, BackoffStep::Wait(Duration::from_secs(8)));
        let (step, _) = backoff_step(Some(4), Duration::from_secs(8));
        assert_eq!(step, BackoffStep::Wait(Duration::from_secs(8)));
        let (step, _) = backoff_step(Some(3), Duration::from_secs(8));
        assert_eq!(step, BackoffStep::Stop);
        let (step, _) = backoff_step(None, Duration::ZERO);
        assert_eq!(step, BackoffStep::Wait(BACKOFF_MIN));
    }

    #[test]
    fn max_tasks_recycles_only_after_the_budget() {
        assert!(!max_tasks_reached(0, 2));
        assert!(!max_tasks_reached(1, 2));
        assert!(max_tasks_reached(2, 2));
        assert!(!max_tasks_reached(5, 0));
    }

    #[test]
    fn a_connection_error_inside_the_watch_budget_is_not_exit_5() {
        let mut used = 0;
        assert_eq!(connection_within_budget(&mut used, 2), None);
        assert_eq!(used, 1);
        assert_eq!(connection_within_budget(&mut used, 2), None);
        assert_eq!(connection_within_budget(&mut used, 2), Some(REDIS_EXIT_CODE));
        let mut used = 0;
        assert_eq!(connection_within_budget(&mut used, 0), Some(REDIS_EXIT_CODE));
    }

    #[test]
    fn storage_failures_map_to_exit_5_or_config() {
        let operation = OperationId::from_uuid(uuid(1));
        let message = MessageId::from_uuid(uuid(2));
        let mut used = 0u64;
        assert_eq!(
            exit_for_storage(&BackendError::Transport { detail: "closed".into() }, &mut used, 1),
            None
        );
        assert_eq!(
            exit_for_storage(&BackendError::Transport { detail: "closed".into() }, &mut used, 1),
            Some(REDIS_EXIT_CODE)
        );
        let mut used = 0;
        let errors = [
            BackendError::Transaction(TransactionConflict { limit: 1 }),
            BackendError::Inconsistent(StorageInconsistency {
                queue: "default".into(),
                operation_id: Some(operation),
                detail: "type".into(),
            }),
            BackendError::Indeterminate(IndeterminateOperation { operation_id: operation, message_id: message }),
            BackendError::Halted { queue: "default".into() },
            BackendError::Full(QueueFull { queue: "default".into(), max_ready: 1, ready_zcard: 2 }),
            BackendError::Conflict(OperationConflict {
                operation_id: operation,
                existing_op: "enqueue".into(),
                existing_request: "{}".into(),
                attempted_op: "enqueue".into(),
                attempted_request: "{\"a\":1}".into(),
            }),
            BackendError::Page { detail: "cursor".into() },
        ];
        for error in &errors {
            assert_eq!(exit_for_storage(error, &mut used, 8), Some(REDIS_EXIT_CODE));
        }
        assert_eq!(
            exit_for_storage(&BackendError::Input(InputError::Queue), &mut used, 8),
            Some(CONFIG_EXIT_CODE)
        );
    }
}
