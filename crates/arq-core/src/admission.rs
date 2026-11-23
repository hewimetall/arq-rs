//! Admission gate. Drain blocks a new claim. An already-resolved claim is counted until inflight is settled.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainPhase {
    Accepting,
    /// New claims are not started. An in-flight claim may still become an attempt.
    AdmissionClosed,
    /// Every accepted attempt from the close race is registered.
    InflightSettled,
    TrackerClosed,
    WaitingTrackedWork,
    Hooks,
    ChildExit,
    Reaped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimAdmission {
    Accept,
    /// The claim resolved after close and must be tracked. It is not dropped.
    AcceptInflightAfterClose,
    Reject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionEvent {
    Close,
    /// A claim that already resolved in Redis.
    NoteClaimResolved,
    /// A new claim. Rejected for the whole drain.
    RejectLateClaim,
    MarkInflightSettled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionEffect {
    Closed,
    Decision(ClaimAdmission),
    Settled,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionGate {
    closed: bool,
    accepted_after_close: u64,
    rejected: u64,
    inflight_settled: bool,
}

impl AdmissionGate {
    pub fn close(&mut self) {
        self.closed = true;
    }

    pub fn is_closed(self) -> bool {
        self.closed
    }

    /// Drain has closed admission, so a new claim must not start.
    pub fn blocks_claim(self) -> bool {
        self.closed
    }

    pub fn note_claim_resolved(&mut self) -> ClaimAdmission {
        if self.inflight_settled {
            self.rejected = self.rejected.saturating_add(1);
            return ClaimAdmission::Reject;
        }
        if !self.closed {
            ClaimAdmission::Accept
        } else {
            self.accepted_after_close = self.accepted_after_close.saturating_add(1);
            ClaimAdmission::AcceptInflightAfterClose
        }
    }

    pub fn reject_late_claim(&mut self) -> ClaimAdmission {
        if !self.closed {
            ClaimAdmission::Accept
        } else {
            self.rejected = self.rejected.saturating_add(1);
            ClaimAdmission::Reject
        }
    }

    pub fn mark_inflight_settled(&mut self) {
        self.inflight_settled = true;
    }

    pub fn inflight_settled(self) -> bool {
        self.inflight_settled
    }

    pub fn accepted_after_close(self) -> u64 {
        self.accepted_after_close
    }

    pub fn rejected(self) -> u64 {
        self.rejected
    }

    /// Tracker close is safe only after admission is closed and the in-flight race is settled.
    pub fn ready_for_tracker_close(self) -> bool {
        self.closed && self.inflight_settled
    }

    pub fn apply(&mut self, event: AdmissionEvent) -> AdmissionEffect {
        match event {
            AdmissionEvent::Close => {
                self.close();
                AdmissionEffect::Closed
            }
            AdmissionEvent::NoteClaimResolved => {
                AdmissionEffect::Decision(self.note_claim_resolved())
            }
            AdmissionEvent::RejectLateClaim => AdmissionEffect::Decision(self.reject_late_claim()),
            AdmissionEvent::MarkInflightSettled => {
                self.mark_inflight_settled();
                AdmissionEffect::Settled
            }
        }
    }
}
