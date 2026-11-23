//! Pure readings of `EXEC` and the per-task `WATCH` budget.

/// `ARQ_WATCH_RETRIES` counts conflicts for one task, not updates of a shared ZSET.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchBudget {
    pub limit: u64,
    pub used: u64,
}

/// The caller gets this after the retry budget. It is not `StorageInconsistency`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionConflict {
    pub limit: u64,
}

impl std::fmt::Display for TransactionConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WATCH conflict budget of {} is exhausted", self.limit)
    }
}

impl std::error::Error for TransactionConflict {}

impl WatchBudget {
    pub fn new(limit: u64) -> Self {
        Self { limit, used: 0 }
    }

    /// Record one `WATCH` conflict. `Ok` means the read cycle may run again.
    /// `EXECABORT` is not a conflict and must not call this.
    pub fn on_conflict(&mut self) -> Result<(), TransactionConflict> {
        if self.used >= self.limit {
            return Err(TransactionConflict { limit: self.limit });
        }
        self.used += 1;
        Ok(())
    }
}

/// How the adapter should treat one `EXEC` reply. Postconditions are a separate step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecReply {
    /// Empty RESP2 array or RESP3 null caused by `WATCH`.
    WatchConflict,
    /// `EXECABORT`. No command was applied.
    Abort,
    /// The array length and a flag that every command succeeded.
    Applied { commands: usize, expected: usize, all_ok: bool },
    /// The connection dropped after `EXEC` was sent, before a full reply.
    Lost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecClass {
    /// Run the read cycle again. Abort does not count as storage failure.
    RetryCycle { charge_watch: bool },
    CheckPostconditions,
    StorageInconsistency,
    UnknownOutcome,
}

pub fn classify_exec(reply: ExecReply) -> ExecClass {
    match reply {
        ExecReply::WatchConflict => ExecClass::RetryCycle { charge_watch: true },
        ExecReply::Abort => ExecClass::RetryCycle { charge_watch: false },
        ExecReply::Applied {
            commands,
            expected,
            all_ok,
        } if all_ok && commands == expected => ExecClass::CheckPostconditions,
        ExecReply::Applied { .. } => ExecClass::StorageInconsistency,
        ExecReply::Lost => ExecClass::UnknownOutcome,
    }
}

/// Candidates taken from one poll. A lost race on one candidate does not shrink this.
pub const CLAIM_CANDIDATE_WINDOW: usize = 16;

/// Renew could not be confirmed, or ownership was already lost.
pub const OWNERSHIP_LOST_EXIT_CODE: i32 = 4;

/// Each claim candidate gets the full configured `WATCH` budget.
/// Losing the race for an earlier candidate does not consume it.
pub fn watch_limit_for_claim_candidate(configured_retries: u64) -> u64 {
    configured_retries
}

/// Concurrent enqueues that all saw `ZCARD < max_ready` can all commit.
/// The ready key is not watched. The excess is at most `concurrent_commits`.
pub fn soft_ready_ceiling(max_ready: u64, concurrent_commits: u64) -> u64 {
    max_ready.saturating_add(concurrent_commits)
}
