//! Loop ping. `asyncio.sleep` answers the ping. A stuck loop thread does not.
//!
//! One missed ping does not stop renew. Budget expiry stops renew, requests cancel,
//! and the child exits with code 2.

use std::time::{Duration, Instant};

/// Child exit when the loop has been silent for the whole ping budget.
pub const PING_BUDGET_EXIT_CODE: i32 = 2;

/// What the loop did during one ping stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopEvent {
    /// The loop ran the ping callback. Handler `asyncio.sleep` is this event.
    Responded,
    /// The stage timed out. A single miss does not stop renew.
    Missed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PingDecision {
    pub stop_renew: bool,
    pub request_cancel: bool,
    /// `Some(2)` when the silence budget is exhausted.
    pub exit_code: Option<i32>,
}

impl PingDecision {
    const fn continue_renew() -> Self {
        Self {
            stop_renew: false,
            request_cancel: false,
            exit_code: None,
        }
    }

    const fn budget_exhausted() -> Self {
        Self {
            stop_renew: true,
            request_cancel: true,
            exit_code: Some(PING_BUDGET_EXIT_CODE),
        }
    }

    pub const fn renew_continues(self) -> bool {
        !self.stop_renew
    }
}

/// Silence budget for the asyncio loop. The caller passes the monotonic clock.
#[derive(Debug, Clone)]
pub struct PingWatch {
    budget: Duration,
    failing_since: Option<Instant>,
}

impl PingWatch {
    pub fn new(budget: Duration) -> Self {
        Self {
            budget,
            failing_since: None,
        }
    }

    pub fn budget(&self) -> Duration {
        self.budget
    }

    /// `now` is a monotonic instant from the caller. This method does not read the clock.
    pub fn observe(&mut self, event: LoopEvent, now: Instant) -> PingDecision {
        match event {
            LoopEvent::Responded => {
                self.failing_since = None;
                PingDecision::continue_renew()
            }
            LoopEvent::Missed => {
                let started = *self.failing_since.get_or_insert(now);
                if now.saturating_duration_since(started) >= self.budget {
                    PingDecision::budget_exhausted()
                } else {
                    PingDecision::continue_renew()
                }
            }
        }
    }
}
