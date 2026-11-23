//! Outcome, dead-letter, shutdown, and ping decisions that do not touch Python or Redis.
//!
//! Python proposes. These functions say what Rust is allowed to commit.

use std::time::Instant;

use arq_core::{
    dispatch_exception, parse_outcome, Json, LoopEvent, OutcomeFault, OutcomeProposal, ParsedOutcome, PingDecision,
    PingWatch, PING_BUDGET_EXIT_CODE,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchRaw {
    /// JSON text of the dictionary `dispatch` returned.
    Returned(String),
    /// An exception escaped `dispatch`, including outer middleware.
    Raised,
    /// Cooperative cancel did not finish inside `cancel_grace_ms`.
    GraceExceeded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitAction {
    Finish(ParsedOutcome),
    /// The child is destroyed. Finish is not written.
    Abandon,
}

impl CommitAction {
    pub fn commits_ack(&self) -> bool {
        matches!(
            self,
            Self::Finish(ParsedOutcome::Proposal(OutcomeProposal::Ok | OutcomeProposal::Skipped))
        )
    }

    pub fn implies_retry(&self) -> bool {
        match self {
            Self::Finish(outcome) => outcome.implies_retry(),
            Self::Abandon => false,
        }
    }
}

/// Unknown dictionaries are `invalid_outcome`. Exceptions are `dispatch_error`. Grace writes nothing.
pub fn action_for(raw: &DispatchRaw) -> CommitAction {
    match raw {
        DispatchRaw::GraceExceeded => CommitAction::Abandon,
        DispatchRaw::Raised => CommitAction::Finish(dispatch_exception()),
        DispatchRaw::Returned(text) => {
            let parsed = match Json::parse(text) {
                Ok(value) => parse_outcome(&value),
                Err(_) => ParsedOutcome::Invalid(OutcomeFault::InvalidOutcome),
            };
            CommitAction::Finish(parsed)
        }
    }
}

/// `on_dead` already ran inside `dispatch` when Python decided dead.
/// Rust commits only the dictionary that left `dispatch`.
pub fn dispatch_then_commit<D, C>(dispatch: D, mut commit: C) -> CommitAction
where
    D: FnOnce() -> DispatchRaw,
    C: FnMut(&CommitAction),
{
    let raw = dispatch();
    let action = action_for(&raw);
    commit(&action);
    action
}

/// Post-commit hook for outcomes `dispatch` did not produce.
pub fn note_dead_after_commit(reason: &str) -> bool {
    matches!(reason, "max_deliveries" | "invalid_message")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownPlan {
    pub stop_new_claims: bool,
    pub renew_continues: bool,
    pub finish: bool,
    pub flush_tracing: bool,
    pub on_stop: bool,
    pub exit_code: Option<i32>,
    /// Drain never ACKs by itself. A finished proposal is a normal `finish`.
    pub ack: bool,
}

pub fn plan_shutdown(dispatch_returned: bool, has_tracing: bool) -> ShutdownPlan {
    if dispatch_returned {
        ShutdownPlan {
            stop_new_claims: true,
            renew_continues: true,
            finish: true,
            flush_tracing: has_tracing,
            on_stop: true,
            exit_code: Some(0),
            ack: false,
        }
    } else {
        ShutdownPlan {
            stop_new_claims: true,
            renew_continues: false,
            finish: false,
            flush_tracing: false,
            on_stop: false,
            exit_code: None,
            ack: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PingExit {
    pub exit_code: i32,
    pub on_stop: bool,
    pub finish: bool,
    pub flush_tracing: bool,
    pub stop_renew: bool,
    pub request_cancel: bool,
}

pub fn observe_ping(watch: &mut PingWatch, responded: bool, now: Instant) -> PingDecision {
    let event = if responded { LoopEvent::Responded } else { LoopEvent::Missed };
    watch.observe(event, now)
}

/// Code 2 does not call `on_stop`, does not finish, and does not flush tracing.
pub fn budget_effect(decision: PingDecision) -> Option<PingExit> {
    if decision.exit_code == Some(PING_BUDGET_EXIT_CODE) {
        Some(PingExit {
            exit_code: PING_BUDGET_EXIT_CODE,
            on_stop: false,
            finish: false,
            flush_tracing: false,
            stop_renew: true,
            request_cancel: true,
        })
    } else {
        None
    }
}

/// Ping is exclusive with drain and with an already requested budget exit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoopGate {
    draining: bool,
    budget_exit: bool,
}

impl LoopGate {
    pub const fn running() -> Self {
        Self { draining: false, budget_exit: false }
    }

    pub fn allow_ping(self) -> bool {
        !self.draining && !self.budget_exit
    }

    pub fn begin_drain(&mut self) {
        self.draining = true;
    }

    pub fn note_budget_exit(&mut self) {
        self.budget_exit = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn unknown_and_exception_are_not_ack() {
        let missing = action_for(&DispatchRaw::Returned(r#"{"result":1}"#.into()));
        assert!(matches!(missing, CommitAction::Finish(ParsedOutcome::Invalid(OutcomeFault::InvalidOutcome))));
        assert!(!missing.commits_ack());
        assert!(!missing.implies_retry());

        let unknown = action_for(&DispatchRaw::Returned(r#"{"outcome":"maybe"}"#.into()));
        assert!(!unknown.commits_ack());
        let bad_json = action_for(&DispatchRaw::Returned("not-json".into()));
        assert!(!bad_json.commits_ack());
        let bad_retry = action_for(&DispatchRaw::Returned(r#"{"outcome":"retry"}"#.into()));
        assert!(!bad_retry.commits_ack());
        assert!(!bad_retry.implies_retry());
        let bad_dead = action_for(&DispatchRaw::Returned(r#"{"outcome":"dead"}"#.into()));
        assert!(!bad_dead.commits_ack());

        let raised = action_for(&DispatchRaw::Raised);
        assert!(matches!(raised, CommitAction::Finish(ParsedOutcome::Invalid(OutcomeFault::DispatchError))));
        assert!(!raised.implies_retry());
        assert!(!raised.commits_ack());

        let grace = action_for(&DispatchRaw::GraceExceeded);
        assert_eq!(grace, CommitAction::Abandon);
        assert!(!grace.commits_ack());
        assert!(!grace.implies_retry());

        let ok = action_for(&DispatchRaw::Returned(r#"{"outcome":"ok"}"#.into()));
        assert!(ok.commits_ack());
        let skipped = action_for(&DispatchRaw::Returned(r#"{"outcome":"skipped"}"#.into()));
        assert!(skipped.commits_ack());
        let retry = action_for(&DispatchRaw::Returned(r#"{"outcome":"retry","delay_ms":0}"#.into()));
        assert!(retry.implies_retry());
        assert!(!retry.commits_ack());
        let dead = action_for(&DispatchRaw::Returned(r#"{"outcome":"dead","reason":"retries","error":null}"#.into()));
        assert!(!dead.commits_ack());
    }

    #[test]
    fn outer_replacement_is_what_rust_commits() {
        let order = std::cell::RefCell::new(Vec::new());
        let action = dispatch_then_commit(
            || {
                order.borrow_mut().push("on_dead");
                order.borrow_mut().push("returned");
                DispatchRaw::Returned(r#"{"outcome":"retry","delay_ms":5}"#.into())
            },
            |action| {
                order.borrow_mut().push("commit");
                assert!(action.implies_retry());
            },
        );
        assert!(action.implies_retry());
        assert_eq!(order.into_inner(), ["on_dead", "returned", "commit"]);
    }

    #[test]
    fn note_dead_is_only_the_post_commit_reasons() {
        assert!(note_dead_after_commit("max_deliveries"));
        assert!(note_dead_after_commit("invalid_message"));
        assert!(!note_dead_after_commit("invalid_outcome"));
        assert!(!note_dead_after_commit("dispatch_error"));
        assert!(!note_dead_after_commit("retries"));
        assert!(!note_dead_after_commit("timeout"));
        assert!(!note_dead_after_commit("unknown_actor"));
        assert!(!note_dead_after_commit(""));
    }

    #[test]
    fn shutdown_finishes_only_a_returned_dispatch() {
        let done = plan_shutdown(true, true);
        assert!(done.finish && done.on_stop && done.flush_tracing && done.renew_continues);
        assert_eq!(done.exit_code, Some(0));
        assert!(!done.ack);
        assert!(done.stop_new_claims);
        let quiet = plan_shutdown(true, false);
        assert!(quiet.finish && quiet.on_stop && !quiet.flush_tracing);
        let forced = plan_shutdown(false, true);
        assert!(!forced.finish && !forced.on_stop && !forced.flush_tracing && !forced.ack);
        assert_eq!(forced.exit_code, None);
        assert!(!forced.renew_continues);
    }

    #[test]
    fn sleep_does_not_exhaust_the_budget_and_silence_exits_2() {
        let start = Instant::now();
        let mut watch = PingWatch::new(Duration::from_millis(30));
        let slept = observe_ping(&mut watch, true, start);
        assert!(budget_effect(slept).is_none());
        assert!(slept.renew_continues());
        let early = observe_ping(&mut watch, false, start);
        assert!(budget_effect(early).is_none());
        assert!(early.renew_continues());
        let expired = observe_ping(&mut watch, false, start + Duration::from_millis(30));
        let effect = budget_effect(expired).unwrap();
        assert_eq!(effect.exit_code, 2);
        assert!(!effect.on_stop && !effect.finish && !effect.flush_tracing);
        assert!(effect.stop_renew && effect.request_cancel);
        let mut gate = LoopGate::running();
        assert!(gate.allow_ping());
        gate.note_budget_exit();
        assert!(!gate.allow_ping());
        let mut draining = LoopGate::running();
        draining.begin_drain();
        assert!(!draining.allow_ping());
    }
}
