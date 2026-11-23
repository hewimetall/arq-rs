//! Execute one arq-core plan on the connection `drive` is holding.

use super::{inconsistency, Drive, RedisBackend, Step};
use crate::error::{transport, BackendError};
use crate::observe::{self, align_requeue_score, load_for_task, Loaded};
use crate::session::{Fail, Session};
use crate::types::{
    ClaimRequest, ClaimResult, ControlRequest, ControlResult, Delivery, EnqueueJob, EnqueueResult,
    FinishRequest, FinishResult, RenewRequest, RenewResult, RequeueRequest, RequeueResult,
};
use arq_core::{
    plan_cancel, plan_claim, plan_enqueue, plan_finish, plan_kill, plan_renew, plan_requeue, plan_settle,
    ClaimDecision, ClaimInput, ControlDecision, ControlInput, CycleId, EnqueueDecision, EnqueueInput, ExecClass,
    FinishDecision, FinishInput, MessageId, ObservedTask, OperationId, OperationPlan, OutcomeProposal, ParsedOutcome,
    QueueKeys, ReceiptStatus, RenewDecision, RenewInput, RequeueDecision, RequeueInput, SettleDecision, SettleInput,
    TaskId, TaskState, WatchBudget, CLAIM_CANDIDATE_WINDOW, OWNERSHIP_LOST_EXIT_CODE,
};
use std::thread;
use std::time::Duration;

enum Apply {
    Watch,
    Abort,
    Lost,
    Applied,
}

enum Candidate {
    Next,
    Stop(Step<ClaimResult>),
}

impl RedisBackend {
    pub async fn enqueue(&self, job: EnqueueJob) -> Result<EnqueueResult, BackendError> {
        let this = self.clone();
        super::block_on(move || this.enqueue_blocking(job)).await
    }

    pub async fn claim(&self, request: ClaimRequest) -> Result<ClaimResult, BackendError> {
        let this = self.clone();
        super::block_on(move || this.claim_blocking(request)).await
    }

    pub async fn renew(&self, request: RenewRequest) -> Result<RenewResult, BackendError> {
        let this = self.clone();
        super::block_on(move || this.renew_blocking(request)).await
    }

    pub async fn finish(&self, request: FinishRequest) -> Result<FinishResult, BackendError> {
        let this = self.clone();
        super::block_on(move || this.finish_blocking(request)).await
    }

    pub async fn requeue(&self, request: RequeueRequest) -> Result<RequeueResult, BackendError> {
        let this = self.clone();
        super::block_on(move || this.requeue_blocking(request)).await
    }

    pub async fn cancel(&self, request: ControlRequest) -> Result<ControlResult, BackendError> {
        let this = self.clone();
        super::block_on(move || this.control_blocking(request, true)).await
    }

    pub async fn kill(&self, request: ControlRequest) -> Result<ControlResult, BackendError> {
        let this = self.clone();
        super::block_on(move || this.control_blocking(request, false)).await
    }

    pub async fn settle(&self, request: ControlRequest) -> Result<ControlResult, BackendError> {
        let this = self.clone();
        super::block_on(move || this.settle_blocking(request)).await
    }

    fn enqueue_blocking(&self, job: EnqueueJob) -> Result<EnqueueResult, BackendError> {
        self.keys(&job.queue)?;
        match self.drive(&job.queue, |session| self.enqueue_once(session, &job))? {
            Drive::Done(result) => Ok(result),
            Drive::Lost => self.recover_enqueue(&job, true),
        }
    }

    fn claim_blocking(&self, request: ClaimRequest) -> Result<ClaimResult, BackendError> {
        self.keys(&request.queue)?;
        match self.drive(&request.queue, |session| self.claim_once(session, &request))? {
            Drive::Done(result) => Ok(result),
            Drive::Lost => self.recover_claim(&request),
        }
    }

    fn renew_blocking(&self, request: RenewRequest) -> Result<RenewResult, BackendError> {
        self.keys(&request.queue)?;
        match self.drive(&request.queue, |session| self.renew_once(session, &request))? {
            Drive::Done(result) => Ok(result),
            Drive::Lost => self.recover_renew(&request),
        }
    }

    fn finish_blocking(&self, request: FinishRequest) -> Result<FinishResult, BackendError> {
        self.keys(&request.queue)?;
        let captured = std::sync::atomic::AtomicU64::new(0);
        match self.drive(&request.queue, |session| {
            let (step, now) = self.finish_once(session, &request)?;
            captured.store(now, std::sync::atomic::Ordering::Relaxed);
            Ok(step)
        })? {
            Drive::Done(result) => Ok(result),
            Drive::Lost => self.recover_finish(&request, captured.load(std::sync::atomic::Ordering::Relaxed)),
        }
    }

    fn requeue_blocking(&self, request: RequeueRequest) -> Result<RequeueResult, BackendError> {
        self.keys(&request.queue)?;
        let captured = std::sync::atomic::AtomicU64::new(0);
        match self.drive(&request.queue, |session| {
            let (step, now) = self.requeue_once(session, &request)?;
            captured.store(now, std::sync::atomic::Ordering::Relaxed);
            Ok(step)
        })? {
            Drive::Done(result) => Ok(result),
            Drive::Lost => self.recover_requeue(&request, captured.load(std::sync::atomic::Ordering::Relaxed)),
        }
    }

    fn control_blocking(&self, request: ControlRequest, cancel: bool) -> Result<ControlResult, BackendError> {
        self.keys(&request.queue)?;
        match self.drive(&request.queue, |session| self.control_once(session, &request, cancel))? {
            Drive::Done(result) => Ok(result),
            Drive::Lost => self.recover_control(&request, cancel),
        }
    }

    fn settle_blocking(&self, request: ControlRequest) -> Result<ControlResult, BackendError> {
        self.keys(&request.queue)?;
        match self.drive(&request.queue, |session| self.settle_once(session, &request))? {
            Drive::Done(result) => Ok(result),
            Drive::Lost => self.recover_settle(&request),
        }
    }

    fn enqueue_once(&self, session: &mut Session, job: &EnqueueJob) -> Result<Step<EnqueueResult>, Fail> {
        let keys = queue_keys(&job.queue)?;
        let now = session.time_ms()?;
        let ready_zcard = session.zcard(&keys.ready())?;
        self.after_zcard();
        let watched = self.watch_task(session, &keys, job.operation_id, job.identity.task_id, job.unique_key.as_deref(), true)?;
        session.expect_type(&keys.ready())?;
        let loaded = load_for_task(session, &keys, job.identity.task_id, job.unique_key.as_deref(), Some(job.operation_id))?;
        let decision = plan_enqueue(&enqueue_input(self, job, now, ready_zcard, &loaded));
        self.finish_enqueue(session, job, now, ready_zcard, decision, &watched)
    }

    fn finish_enqueue(
        &self,
        session: &mut Session,
        job: &EnqueueJob,
        now: u64,
        ready_zcard: u64,
        decision: EnqueueDecision,
        watched: &[String],
    ) -> Result<Step<EnqueueResult>, Fail> {
        match decision {
            EnqueueDecision::Execute(plan) | EnqueueDecision::Duplicate { record: plan, .. } => {
                let duplicate = matches!(plan.receipt.status, ReceiptStatus::Duplicate);
                match self.apply(session, &job.queue, &plan, watched)? {
                    Apply::Watch => Ok(Step::Watch),
                    Apply::Abort => Ok(Step::Abort),
                    Apply::Lost => Ok(Step::Lost),
                    Apply::Applied => {
                        let loaded = load_for_task(session, &queue_keys(&job.queue)?, job.identity.task_id, job.unique_key.as_deref(), Some(job.operation_id))?;
                        let again = plan_enqueue(&enqueue_input(self, job, now, ready_zcard, &loaded));
                        match again {
                            EnqueueDecision::Replay(replay) if replay.status == ReceiptStatus::Enqueued => {
                                Ok(Step::Done(EnqueueResult::Enqueued {
                                    task_id: job.identity.task_id,
                                    cycle_id: job.cycle_id,
                                    replayed: false,
                                }))
                            }
                            EnqueueDecision::Replay(replay) if replay.status == ReceiptStatus::Duplicate => {
                                Ok(Step::Done(EnqueueResult::Duplicate(duplicate_of(job, &loaded.observed))))
                            }
                            EnqueueDecision::Inconsistent(error) => Err(Fail::Backend(BackendError::Inconsistent(error))),
                            _ => Err(Fail::Backend(inconsistency(
                                &job.queue,
                                Some(job.operation_id),
                                if duplicate {
                                    "duplicate enqueue postconditions did not hold"
                                } else {
                                    "enqueue postconditions did not hold"
                                },
                            ))),
                        }
                    }
                }
            }
            EnqueueDecision::Replay(replay) => {
                let _ = session.unwatch();
                if replay.status == ReceiptStatus::Duplicate {
                    let loaded = load_for_task(session, &queue_keys(&job.queue)?, job.identity.task_id, job.unique_key.as_deref(), Some(job.operation_id))?;
                    Ok(Step::Done(EnqueueResult::Duplicate(duplicate_of(job, &loaded.observed))))
                } else {
                    Ok(Step::Done(EnqueueResult::Enqueued {
                        task_id: job.identity.task_id,
                        cycle_id: job.cycle_id,
                        replayed: true,
                    }))
                }
            }
            EnqueueDecision::Full(error) => Err(Fail::Backend(BackendError::Full(error))),
            EnqueueDecision::Conflict(error) => Err(Fail::Backend(BackendError::Conflict(error))),
            EnqueueDecision::Inconsistent(error) => Err(Fail::Backend(BackendError::Inconsistent(error))),
            EnqueueDecision::Indeterminate(error) => Err(Fail::Backend(BackendError::Indeterminate(error))),
            EnqueueDecision::Rejected(error) => Err(Fail::Backend(BackendError::Input(error))),
        }
    }

    fn claim_once(&self, session: &mut Session, request: &ClaimRequest) -> Result<Step<ClaimResult>, Fail> {
        let keys = queue_keys(&request.queue)?;
        if request.worker_id.is_empty() {
            return Err(Fail::Backend(BackendError::Input(arq_core::InputError::WorkerId)));
        }
        let now = session.time_ms()?;
        if let Some(receipt) = observe::read_receipt(session, &keys, request.operation_id)? {
            return match self.claim_candidate(session, request, receipt.task_id)? {
                Candidate::Stop(step) => Ok(step),
                Candidate::Next => Ok(Step::Done(ClaimResult::Idle)),
            };
        }
        let inflight = session.zrange_by_score(&keys.inflight(), "-inf", &now.to_string(), CLAIM_CANDIDATE_WINDOW)?;
        // One poll considers one window. Overdue inflight is read first, including when ready
        // is non-empty. Ready is read only when that inflight window is empty.
        let ready_limit = ready_candidate_limit(inflight.len());
        let ready = if ready_limit == 0 {
            Vec::new()
        } else {
            session.zrange_by_score(&keys.ready(), "-inf", &now.to_string(), ready_limit)?
        };
        let mut seen = Vec::new();
        for member in inflight.into_iter().chain(ready) {
            if seen.iter().any(|seen: &String| seen == &member) {
                continue;
            }
            seen.push(member.clone());
            let Ok(task_id) = TaskId::parse(&member) else {
                continue;
            };
            match self.claim_candidate(session, request, task_id)? {
                Candidate::Next => continue,
                Candidate::Stop(step) => return Ok(step),
            }
        }
        let _ = session.unwatch();
        Ok(Step::Done(ClaimResult::Idle))
    }

    fn claim_candidate(&self, session: &mut Session, request: &ClaimRequest, task_id: TaskId) -> Result<Candidate, Fail> {
        let mut budget = WatchBudget::new(self.config().watch_retries);
        loop {
            session.discard_and_unwatch();
            let now = session.time_ms()?;
            let keys = queue_keys(&request.queue)?;
            let watched = self.watch_claim(session, &keys, request.operation_id, task_id)?;
            let loaded = load_for_task(session, &keys, task_id, None, Some(request.operation_id))?;
            let decision = plan_claim(&claim_input(self, request, now, &loaded));
            match self.dispatch_claim(session, request, now, task_id, decision, &watched)? {
                ClaimFlow::Retry => {
                    budget.on_conflict().map_err(|error| Fail::Backend(BackendError::Transaction(error)))?;
                }
                ClaimFlow::Skip => return Ok(Candidate::Next),
                ClaimFlow::Stop(step) => return Ok(Candidate::Stop(step)),
            }
        }
    }

    fn dispatch_claim(
        &self,
        session: &mut Session,
        request: &ClaimRequest,
        _now: u64,
        task_id: TaskId,
        decision: ClaimDecision,
        watched: &[String],
    ) -> Result<ClaimFlow, Fail> {
        match decision {
            ClaimDecision::Execute(plan) | ClaimDecision::DeadLetter(plan) => match self.apply(session, &request.queue, &plan, watched)? {
                Apply::Watch => self.claim_after_conflict(session, request, task_id),
                Apply::Abort => Ok(ClaimFlow::Stop(Step::Abort)),
                Apply::Lost => Ok(ClaimFlow::Stop(Step::Lost)),
                Apply::Applied => self.prove_claim(session, request, task_id, false).map(ClaimFlow::Stop),
            },
            ClaimDecision::Replay(_) => self.prove_claim(session, request, task_id, true).map(ClaimFlow::Stop),
            ClaimDecision::Blocked | ClaimDecision::NotDue | ClaimDecision::LiveLease => Ok(ClaimFlow::Skip),
            ClaimDecision::Conflict(error) => Err(Fail::Backend(BackendError::Conflict(error))),
            ClaimDecision::Inconsistent(error) => Err(Fail::Backend(BackendError::Inconsistent(error))),
            ClaimDecision::Indeterminate(error) => Err(Fail::Backend(BackendError::Indeterminate(error))),
            ClaimDecision::Rejected(error) => Err(Fail::Backend(BackendError::Input(error))),
        }
    }

    fn claim_after_conflict(&self, session: &mut Session, request: &ClaimRequest, task_id: TaskId) -> Result<ClaimFlow, Fail> {
        let now = session.time_ms()?;
        let keys = queue_keys(&request.queue)?;
        let loaded = load_for_task(session, &keys, task_id, None, Some(request.operation_id))?;
        match plan_claim(&claim_input(self, request, now, &loaded)) {
            ClaimDecision::Execute(_) | ClaimDecision::DeadLetter(_) => Ok(ClaimFlow::Retry),
            ClaimDecision::Replay(_) => self.prove_claim(session, request, task_id, true).map(ClaimFlow::Stop),
            ClaimDecision::Blocked | ClaimDecision::NotDue | ClaimDecision::LiveLease => Ok(ClaimFlow::Skip),
            ClaimDecision::Conflict(error) => Err(Fail::Backend(BackendError::Conflict(error))),
            ClaimDecision::Inconsistent(error) => Err(Fail::Backend(BackendError::Inconsistent(error))),
            ClaimDecision::Indeterminate(error) => Err(Fail::Backend(BackendError::Indeterminate(error))),
            ClaimDecision::Rejected(error) => Err(Fail::Backend(BackendError::Input(error))),
        }
    }

    fn prove_claim(
        &self,
        session: &mut Session,
        request: &ClaimRequest,
        task_id: TaskId,
        replayed: bool,
    ) -> Result<Step<ClaimResult>, Fail> {
        let keys = queue_keys(&request.queue)?;
        let loaded = load_for_task(session, &keys, task_id, None, Some(request.operation_id))?;
        let now = session.time_ms()?;
        let decision = plan_claim(&claim_input(self, request, now, &loaded));
        match decision {
            ClaimDecision::Replay(replay) => {
                let later = session.time_ms()?;
                Ok(Step::Done(claim_result(&loaded.observed, replay.status, replayed, later, request.operation_id, &request.queue)?))
            }
            ClaimDecision::Inconsistent(error) => Err(Fail::Backend(BackendError::Inconsistent(error))),
            _ => Err(Fail::Backend(inconsistency(
                &request.queue,
                Some(request.operation_id),
                "claim postconditions did not prove this delivery",
            ))),
        }
    }

    fn renew_once(&self, session: &mut Session, request: &RenewRequest) -> Result<Step<RenewResult>, Fail> {
        let keys = queue_keys(&request.queue)?;
        let now = session.time_ms()?;
        let watched = self.watch_task(session, &keys, request.operation_id, request.task_id, None, false)?;
        let loaded = load_for_task(session, &keys, request.task_id, None, Some(request.operation_id))?;
        let decision = plan_renew(&renew_input(self, request, now, &loaded));
        match decision {
            RenewDecision::Execute(plan) => match self.apply(session, &request.queue, &plan, &watched)? {
                Apply::Watch => Ok(Step::Watch),
                Apply::Abort => Ok(Step::Abort),
                Apply::Lost => Ok(Step::Lost),
                Apply::Applied => self.prove_renew(session, request, false),
            },
            RenewDecision::Replay(_) => {
                let _ = session.unwatch();
                self.prove_renew(session, request, true)
            }
            RenewDecision::NotHeld { exit_code } => {
                let _ = session.unwatch();
                Ok(Step::Done(RenewResult::NotHeld { exit_code }))
            }
            RenewDecision::Conflict(error) => Err(Fail::Backend(BackendError::Conflict(error))),
            RenewDecision::Inconsistent(error) => Err(Fail::Backend(BackendError::Inconsistent(error))),
            RenewDecision::Indeterminate(error) => Err(Fail::Backend(BackendError::Indeterminate(error))),
            RenewDecision::Rejected(error) => Err(Fail::Backend(BackendError::Input(error))),
        }
    }

    fn prove_renew(&self, session: &mut Session, request: &RenewRequest, replayed: bool) -> Result<Step<RenewResult>, Fail> {
        let keys = queue_keys(&request.queue)?;
        let now = session.time_ms()?;
        let loaded = load_for_task(session, &keys, request.task_id, None, Some(request.operation_id))?;
        match plan_renew(&renew_input(self, request, now, &loaded)) {
            RenewDecision::Replay(_) => Ok(Step::Done(RenewResult::Renewed {
                lease_deadline_ms: loaded.observed.lease_deadline_ms.unwrap_or(0),
                started_at_ms: loaded.observed.started_at_ms,
                replayed,
            })),
            RenewDecision::Inconsistent(error) => Err(Fail::Backend(BackendError::Inconsistent(error))),
            _ => Err(Fail::Backend(inconsistency(&request.queue, Some(request.operation_id), "renew postconditions did not hold"))),
        }
    }

    fn finish_once(&self, session: &mut Session, request: &FinishRequest) -> Result<(Step<FinishResult>, u64), Fail> {
        let keys = queue_keys(&request.queue)?;
        let now = session.time_ms()?;
        let watched = self.watch_claim(session, &keys, request.operation_id, request.task_id)?;
        let mut loaded = load_for_task(session, &keys, request.task_id, None, Some(request.operation_id))?;
        let (plan_now, view) = align_finish(now, &loaded, &request.proposal);
        loaded.observed = view;
        let decision = plan_finish(&finish_input(self, request, plan_now, &loaded));
        let step = match decision {
            FinishDecision::Execute(plan) => match self.apply(session, &request.queue, &plan, &watched)? {
                Apply::Watch => Step::Watch,
                Apply::Abort => Step::Abort,
                Apply::Lost => Step::Lost,
                Apply::Applied => return Ok((self.prove_finish(session, request, plan_now, false)?, plan_now)),
            },
            FinishDecision::Replay(_) => {
                let _ = session.unwatch();
                return Ok((self.prove_finish(session, request, plan_now, true)?, plan_now));
            }
            FinishDecision::Rejected => {
                let _ = session.unwatch();
                Step::Done(FinishResult::Rejected)
            }
            FinishDecision::Conflict(error) => return Err(Fail::Backend(BackendError::Conflict(error))),
            FinishDecision::Inconsistent(error) => return Err(Fail::Backend(BackendError::Inconsistent(error))),
            FinishDecision::Indeterminate(error) => return Err(Fail::Backend(BackendError::Indeterminate(error))),
            FinishDecision::RejectedInput(error) => return Err(Fail::Backend(BackendError::Input(error))),
        };
        Ok((step, plan_now))
    }

    fn prove_finish(&self, session: &mut Session, request: &FinishRequest, now: u64, replayed: bool) -> Result<Step<FinishResult>, Fail> {
        let keys = queue_keys(&request.queue)?;
        let mut loaded = load_for_task(session, &keys, request.task_id, None, Some(request.operation_id))?;
        let (plan_now, view) = align_finish(now, &loaded, &request.proposal);
        loaded.observed = view;
        match plan_finish(&finish_input(self, request, plan_now, &loaded)) {
            FinishDecision::Replay(_) => Ok(Step::Done(FinishResult::Finished { replayed })),
            FinishDecision::Inconsistent(error) => Err(Fail::Backend(BackendError::Inconsistent(error))),
            _ => Err(Fail::Backend(inconsistency(&request.queue, Some(request.operation_id), "finish postconditions did not hold"))),
        }
    }

    fn requeue_once(&self, session: &mut Session, request: &RequeueRequest) -> Result<(Step<RequeueResult>, u64), Fail> {
        let keys = queue_keys(&request.queue)?;
        let now = session.time_ms()?;
        let watched = self.watch_claim(session, &keys, request.operation_id, request.task_id)?;
        let mut loaded = load_for_task(session, &keys, request.task_id, None, Some(request.operation_id))?;
        if let Some(receipt) = loaded.receipt.as_mut() {
            align_requeue_score(receipt, &loaded.observed);
        }
        let history = observe::read_history(session, &keys, request.task_id)?;
        let decision = plan_requeue(&requeue_input(self, request, now, &history, &loaded));
        let step = self.finish_requeue(session, request, now, &history, decision, &watched, &loaded)?;
        Ok((step, now))
    }

    fn finish_requeue(
        &self,
        session: &mut Session,
        request: &RequeueRequest,
        now: u64,
        history: &arq_core::Json,
        decision: RequeueDecision,
        watched: &[String],
        loaded: &Loaded,
    ) -> Result<Step<RequeueResult>, Fail> {
        match decision {
            RequeueDecision::Execute(plan) | RequeueDecision::NotFound(plan) | RequeueDecision::NotDead(plan) => {
                match self.apply(session, &request.queue, &plan, watched)? {
                    Apply::Watch => Ok(Step::Watch),
                    Apply::Abort => Ok(Step::Abort),
                    Apply::Lost => Ok(Step::Lost),
                    Apply::Applied => self.prove_requeue(session, request, now, history, false),
                }
            }
            RequeueDecision::Replay(replay) => {
                let _ = session.unwatch();
                Ok(Step::Done(requeue_from_status(replay.status, request.new_cycle_id, true)))
            }
            RequeueDecision::Duplicate(error) => {
                let _ = session.unwatch();
                Ok(Step::Done(RequeueResult::Duplicate(error)))
            }
            RequeueDecision::Conflict(error) => Err(Fail::Backend(BackendError::Conflict(error))),
            RequeueDecision::Inconsistent(error) => {
                status_or_inconsistent(requeue_override(loaded, request.new_cycle_id).map(Step::Done), error)
            }
            RequeueDecision::Indeterminate(error) => Err(Fail::Backend(BackendError::Indeterminate(error))),
            RequeueDecision::Rejected(error) => Err(Fail::Backend(BackendError::Input(error))),
        }
    }

    fn prove_requeue(
        &self,
        session: &mut Session,
        request: &RequeueRequest,
        now: u64,
        history: &arq_core::Json,
        replayed: bool,
    ) -> Result<Step<RequeueResult>, Fail> {
        let keys = queue_keys(&request.queue)?;
        let mut loaded = load_for_task(session, &keys, request.task_id, None, Some(request.operation_id))?;
        if let Some(receipt) = loaded.receipt.as_mut() {
            align_requeue_score(receipt, &loaded.observed);
        }
        match plan_requeue(&requeue_input(self, request, now, history, &loaded)) {
            RequeueDecision::Replay(replay) => Ok(Step::Done(requeue_from_status(
                replay.status,
                loaded.observed.cycle_id.unwrap_or(request.new_cycle_id),
                replayed,
            ))),
            RequeueDecision::Inconsistent(error) => {
                status_or_inconsistent(requeue_override(&loaded, request.new_cycle_id).map(Step::Done), error)
            }
            _ => Err(Fail::Backend(inconsistency(&request.queue, Some(request.operation_id), "requeue postconditions did not hold"))),
        }
    }

    fn control_once(&self, session: &mut Session, request: &ControlRequest, cancel: bool) -> Result<Step<ControlResult>, Fail> {
        let keys = queue_keys(&request.queue)?;
        let now = session.time_ms()?;
        let watched = self.watch_task(session, &keys, request.operation_id, request.task_id, None, false)?;
        let loaded = load_for_task(session, &keys, request.task_id, None, Some(request.operation_id))?;
        let input = control_input(self, request, now, &loaded);
        let decision = if cancel { plan_cancel(&input) } else { plan_kill(&input) };
        match decision {
            ControlDecision::Execute(plan) => match self.apply(session, &request.queue, &plan, &watched)? {
                Apply::Watch => Ok(Step::Watch),
                Apply::Abort => Ok(Step::Abort),
                Apply::Lost => Ok(Step::Lost),
                Apply::Applied => self.prove_control(session, request, cancel, false),
            },
            ControlDecision::Replay(replay) => {
                let _ = session.unwatch();
                Ok(Step::Done(control_from_status(replay.status, loaded.observed.cycle_id, true)))
            }
            ControlDecision::Conflict(error) => Err(Fail::Backend(BackendError::Conflict(error))),
            ControlDecision::Inconsistent(error) => status_or_inconsistent(control_override(&loaded).map(Step::Done), error),
            ControlDecision::Indeterminate(error) => Err(Fail::Backend(BackendError::Indeterminate(error))),
            ControlDecision::Rejected(error) => Err(Fail::Backend(BackendError::Input(error))),
        }
    }

    fn prove_control(&self, session: &mut Session, request: &ControlRequest, cancel: bool, replayed: bool) -> Result<Step<ControlResult>, Fail> {
        let keys = queue_keys(&request.queue)?;
        let now = session.time_ms()?;
        let loaded = load_for_task(session, &keys, request.task_id, None, Some(request.operation_id))?;
        let input = control_input(self, request, now, &loaded);
        let decision = if cancel { plan_cancel(&input) } else { plan_kill(&input) };
        match decision {
            ControlDecision::Replay(replay) => Ok(Step::Done(control_from_status(replay.status, loaded.observed.cycle_id, replayed))),
            ControlDecision::Inconsistent(error) => status_or_inconsistent(control_override(&loaded).map(Step::Done), error),
            _ => Err(Fail::Backend(inconsistency(&request.queue, Some(request.operation_id), "control postconditions did not hold"))),
        }
    }

    fn settle_once(&self, session: &mut Session, request: &ControlRequest) -> Result<Step<ControlResult>, Fail> {
        let keys = queue_keys(&request.queue)?;
        let now = session.time_ms()?;
        let watched = self.watch_task(session, &keys, request.operation_id, request.task_id, None, false)?;
        let loaded = load_for_task(session, &keys, request.task_id, None, Some(request.operation_id))?;
        match plan_settle(&settle_input(self, request, now, &loaded)) {
            SettleDecision::Accepted(plan) | SettleDecision::AlreadyTerminal(plan) | SettleDecision::NotFound(plan) => {
                match self.apply(session, &request.queue, &plan, &watched)? {
                    Apply::Watch => Ok(Step::Watch),
                    Apply::Abort => Ok(Step::Abort),
                    Apply::Lost => Ok(Step::Lost),
                    Apply::Applied => self.prove_settle(session, request, false),
                }
            }
            SettleDecision::StillOwned => {
                let _ = session.unwatch();
                Ok(Step::Done(ControlResult::StillOwned))
            }
            SettleDecision::NotBlocked => {
                let _ = session.unwatch();
                Ok(Step::Done(ControlResult::NotBlocked))
            }
            SettleDecision::Replay(replay) => {
                let _ = session.unwatch();
                Ok(Step::Done(control_from_status(replay.status, loaded.observed.cycle_id, true)))
            }
            SettleDecision::Conflict(error) => Err(Fail::Backend(BackendError::Conflict(error))),
            SettleDecision::Inconsistent(error) => Err(Fail::Backend(BackendError::Inconsistent(error))),
            SettleDecision::Indeterminate(error) => Err(Fail::Backend(BackendError::Indeterminate(error))),
            SettleDecision::Rejected(error) => Err(Fail::Backend(BackendError::Input(error))),
        }
    }

    fn prove_settle(&self, session: &mut Session, request: &ControlRequest, replayed: bool) -> Result<Step<ControlResult>, Fail> {
        let keys = queue_keys(&request.queue)?;
        let now = session.time_ms()?;
        let loaded = load_for_task(session, &keys, request.task_id, None, Some(request.operation_id))?;
        match plan_settle(&settle_input(self, request, now, &loaded)) {
            SettleDecision::Replay(replay) => Ok(Step::Done(control_from_status(replay.status, loaded.observed.cycle_id, replayed))),
            SettleDecision::Inconsistent(error) => Err(Fail::Backend(BackendError::Inconsistent(error))),
            _ => Err(Fail::Backend(inconsistency(&request.queue, Some(request.operation_id), "settle postconditions did not hold"))),
        }
    }

    fn apply(&self, session: &mut Session, queue: &str, plan: &OperationPlan, watched: &[String]) -> Result<Apply, Fail> {
        if !plan.receipt_is_last() {
            return Err(Fail::Backend(inconsistency(queue, Some(plan.operation_id), "receipt is not the last command")));
        }
        let hook = self.before_hook();
        let fault = self.take_exec_fault();
        let executed = session.exec_plan(&plan.commands, watched, fault, hook.as_deref())?;
        match executed.class {
            ExecClass::RetryCycle { charge_watch: true } => Ok(Apply::Watch),
            ExecClass::RetryCycle { charge_watch: false } => Ok(Apply::Abort),
            ExecClass::UnknownOutcome => Ok(Apply::Lost),
            ExecClass::CheckPostconditions => Ok(Apply::Applied),
            ExecClass::StorageInconsistency => Err(Fail::Backend(inconsistency(
                queue,
                Some(plan.operation_id),
                "EXEC array was the wrong length or contained a command error",
            ))),
        }
    }

    fn watch_task(
        &self,
        session: &mut Session,
        keys: &QueueKeys,
        operation: OperationId,
        task: TaskId,
        unique: Option<&str>,
        include_uniq: bool,
    ) -> Result<Vec<String>, Fail> {
        let mut watched = vec![keys.receipt(&operation), keys.job(&task)];
        if include_uniq {
            if let Some(unique) = unique {
                watched.push(keys.uniq(unique).map_err(|error| Fail::Backend(BackendError::Input(error)))?);
            }
        }
        self.watch_with_extra(session, &mut watched)?;
        Ok(watched)
    }

    fn watch_claim(&self, session: &mut Session, keys: &QueueKeys, operation: OperationId, task: TaskId) -> Result<Vec<String>, Fail> {
        let mut watched = vec![keys.receipt(&operation), keys.job(&task)];
        self.watch_with_extra(session, &mut watched)?;
        let fields = session.hgetall(&keys.job(&task))?;
        if let Some(unique) = fields.iter().find(|(key, _)| key == "unique_key").map(|(_, value)| value.as_str()).filter(|text| !text.is_empty()) {
            let uniq = keys.uniq(unique).map_err(|error| Fail::Backend(BackendError::Input(error)))?;
            session.watch(&[uniq.clone()])?;
            watched.push(uniq);
        }
        session.expect_types(&watched)?;
        Ok(watched)
    }

    fn watch_with_extra(&self, session: &mut Session, watched: &mut Vec<String>) -> Result<(), Fail> {
        if let Some(extra) = self.cross_slot() {
            watched.push(extra);
        }
        session.watch(watched)?;
        session.expect_types(watched)?;
        Ok(())
    }

    fn recover_enqueue(&self, job: &EnqueueJob, allow_rewrite: bool) -> Result<EnqueueResult, BackendError> {
        let (now, loaded) = self.poll_loaded(&job.queue, job.identity.task_id, job.unique_key.as_deref(), job.operation_id)?;
        let ready_zcard = self.with_session(&job.queue, |session| {
            let keys = queue_keys(&job.queue)?;
            session.zcard(&keys.ready())
        })?;
        let decision = plan_enqueue(&enqueue_input(self, job, now, ready_zcard, &loaded));
        match decision {
            EnqueueDecision::Replay(replay) if replay.status == ReceiptStatus::Enqueued => Ok(EnqueueResult::Enqueued {
                task_id: job.identity.task_id,
                cycle_id: loaded.observed.cycle_id.unwrap_or(job.cycle_id),
                replayed: true,
            }),
            EnqueueDecision::Replay(replay) if replay.status == ReceiptStatus::Duplicate => {
                Ok(EnqueueResult::Duplicate(duplicate_of(job, &loaded.observed)))
            }
            EnqueueDecision::Duplicate { error, .. } => Ok(EnqueueResult::Duplicate(error)),
            EnqueueDecision::Execute(_) if allow_rewrite && loaded.observed.is_clean_slate() => {
                match self.drive(&job.queue, |session| self.enqueue_once(session, job))? {
                    Drive::Done(result) => Ok(result),
                    Drive::Lost => self.recover_enqueue(job, false),
                }
            }
            EnqueueDecision::Full(error) => Err(BackendError::Full(error)),
            EnqueueDecision::Conflict(error) => Err(BackendError::Conflict(error)),
            EnqueueDecision::Rejected(error) => Err(BackendError::Input(error)),
            EnqueueDecision::Inconsistent(error) => Err(self.note_inconsistent(error)),
            EnqueueDecision::Indeterminate(error) => Err(BackendError::Indeterminate(error)),
            EnqueueDecision::Execute(_) | EnqueueDecision::Replay(_) => Err(lost(job.operation_id, job.identity.message_id)),
        }
    }

    fn recover_claim(&self, request: &ClaimRequest) -> Result<ClaimResult, BackendError> {
        let task = self.find_claim_task(request)?;
        let Some(task_id) = task else {
            return Err(lost(request.operation_id, request.message_id));
        };
        let (now, loaded) = self.poll_loaded(&request.queue, task_id, None, request.operation_id)?;
        match plan_claim(&claim_input(self, request, now, &loaded)) {
            ClaimDecision::Replay(replay) => {
                let later = self.with_session(&request.queue, |session| session.time_ms())?;
                claim_result(&loaded.observed, replay.status, true, later, request.operation_id, &request.queue)
                    .map_err(|fail| self.fail_to_error(&request.queue, fail))
            }
            ClaimDecision::Inconsistent(error) => Err(self.note_inconsistent(error)),
            ClaimDecision::Conflict(error) => Err(BackendError::Conflict(error)),
            ClaimDecision::Rejected(error) => Err(BackendError::Input(error)),
            ClaimDecision::Indeterminate(error) => Err(BackendError::Indeterminate(error)),
            _ => Err(lost(request.operation_id, request.message_id)),
        }
    }

    fn find_claim_task(&self, request: &ClaimRequest) -> Result<Option<TaskId>, BackendError> {
        let mut found = None;
        for attempt in 0..20 {
            found = self.with_session(&request.queue, |session| {
                let keys = queue_keys(&request.queue)?;
                Ok(observe::read_receipt(session, &keys, request.operation_id)?.map(|receipt| receipt.task_id))
            })?;
            if found.is_some() || attempt == 19 {
                break;
            }
            thread::sleep(Duration::from_millis(2));
        }
        Ok(found)
    }

    fn recover_renew(&self, request: &RenewRequest) -> Result<RenewResult, BackendError> {
        let (now, loaded) = self.poll_loaded(&request.queue, request.task_id, None, request.operation_id)?;
        match plan_renew(&renew_input(self, request, now, &loaded)) {
            RenewDecision::Replay(_) => Ok(RenewResult::Renewed {
                lease_deadline_ms: loaded.observed.lease_deadline_ms.unwrap_or(0),
                started_at_ms: loaded.observed.started_at_ms,
                replayed: true,
            }),
            RenewDecision::NotHeld { exit_code } if loaded.receipt.is_none() && loaded.observed.lease_deadline_ms.map(|deadline| deadline <= now).unwrap_or(true) => {
                Ok(RenewResult::NotHeld { exit_code })
            }
            RenewDecision::Inconsistent(error) => Err(self.note_inconsistent(error)),
            RenewDecision::Conflict(error) => Err(BackendError::Conflict(error)),
            RenewDecision::Rejected(error) => Err(BackendError::Input(error)),
            RenewDecision::Indeterminate(error) => Err(BackendError::Indeterminate(error)),
            _ => Err(lost(request.operation_id, request.message_id)),
        }
    }

    fn recover_finish(&self, request: &FinishRequest, now: u64) -> Result<FinishResult, BackendError> {
        let (_, mut loaded) = self.poll_loaded(&request.queue, request.task_id, None, request.operation_id)?;
        let (plan_now, view) = align_finish(now, &loaded, &request.proposal);
        loaded.observed = view;
        match plan_finish(&finish_input(self, request, plan_now, &loaded)) {
            FinishDecision::Replay(_) => Ok(FinishResult::Finished { replayed: true }),
            FinishDecision::Rejected if loaded.receipt.is_none() => Ok(FinishResult::Rejected),
            FinishDecision::Inconsistent(error) => Err(self.note_inconsistent(error)),
            FinishDecision::Conflict(error) => Err(BackendError::Conflict(error)),
            FinishDecision::RejectedInput(error) => Err(BackendError::Input(error)),
            FinishDecision::Indeterminate(error) => Err(BackendError::Indeterminate(error)),
            _ => Err(lost(request.operation_id, request.message_id)),
        }
    }

    fn recover_requeue(&self, request: &RequeueRequest, now: u64) -> Result<RequeueResult, BackendError> {
        let (_, mut loaded) = self.poll_loaded(&request.queue, request.task_id, None, request.operation_id)?;
        if let Some(receipt) = loaded.receipt.as_mut() {
            align_requeue_score(receipt, &loaded.observed);
        }
        let history = self.with_session(&request.queue, |session| {
            observe::read_history(session, &queue_keys(&request.queue)?, request.task_id)
        })?;
        match plan_requeue(&requeue_input(self, request, now, &history, &loaded)) {
            RequeueDecision::Replay(replay) => Ok(requeue_from_status(
                replay.status,
                loaded.observed.cycle_id.unwrap_or(request.new_cycle_id),
                true,
            )),
            RequeueDecision::Duplicate(error) => Ok(RequeueResult::Duplicate(error)),
            RequeueDecision::Inconsistent(error) => match requeue_override(&loaded, request.new_cycle_id) {
                Some(result) => Ok(result),
                None => Err(self.note_inconsistent(error)),
            },
            RequeueDecision::Conflict(error) => Err(BackendError::Conflict(error)),
            RequeueDecision::Rejected(error) => Err(BackendError::Input(error)),
            RequeueDecision::Indeterminate(error) => Err(BackendError::Indeterminate(error)),
            _ => Err(lost(request.operation_id, message_for(request.task_id))),
        }
    }

    fn recover_control(&self, request: &ControlRequest, cancel: bool) -> Result<ControlResult, BackendError> {
        let (now, loaded) = self.poll_loaded(&request.queue, request.task_id, None, request.operation_id)?;
        let input = control_input(self, request, now, &loaded);
        let decision = if cancel { plan_cancel(&input) } else { plan_kill(&input) };
        match decision {
            ControlDecision::Replay(replay) => Ok(control_from_status(replay.status, loaded.observed.cycle_id, true)),
            ControlDecision::Inconsistent(error) => match control_override(&loaded) {
                Some(result) => Ok(result),
                None => Err(self.note_inconsistent(error)),
            },
            ControlDecision::Conflict(error) => Err(BackendError::Conflict(error)),
            ControlDecision::Rejected(error) => Err(BackendError::Input(error)),
            ControlDecision::Indeterminate(error) => Err(BackendError::Indeterminate(error)),
            ControlDecision::Execute(_) => Err(lost(request.operation_id, message_for(request.task_id))),
        }
    }

    fn recover_settle(&self, request: &ControlRequest) -> Result<ControlResult, BackendError> {
        let (now, loaded) = self.poll_loaded(&request.queue, request.task_id, None, request.operation_id)?;
        match plan_settle(&settle_input(self, request, now, &loaded)) {
            SettleDecision::Replay(replay) => Ok(control_from_status(replay.status, loaded.observed.cycle_id, true)),
            SettleDecision::StillOwned => Ok(ControlResult::StillOwned),
            SettleDecision::NotBlocked => Ok(ControlResult::NotBlocked),
            SettleDecision::Inconsistent(error) => Err(self.note_inconsistent(error)),
            SettleDecision::Conflict(error) => Err(BackendError::Conflict(error)),
            SettleDecision::Rejected(error) => Err(BackendError::Input(error)),
            SettleDecision::Indeterminate(error) => Err(BackendError::Indeterminate(error)),
            SettleDecision::Accepted(_) | SettleDecision::AlreadyTerminal(_) | SettleDecision::NotFound(_) => {
                Err(lost(request.operation_id, message_for(request.task_id)))
            }
        }
    }

    fn poll_loaded(
        &self,
        queue: &str,
        task: TaskId,
        unique: Option<&str>,
        operation: OperationId,
    ) -> Result<(u64, Loaded), BackendError> {
        let mut latest = None;
        for attempt in 0..20 {
            let snapshot = self.read_loaded(queue, task, unique, operation)?;
            let settled = snapshot.1.receipt.is_some();
            latest = Some(snapshot);
            if settled || attempt == 19 {
                break;
            }
            thread::sleep(Duration::from_millis(2));
        }
        latest.ok_or_else(|| transport("recovery read produced no snapshot"))
    }

    fn read_loaded(
        &self,
        queue: &str,
        task: TaskId,
        unique: Option<&str>,
        operation: OperationId,
    ) -> Result<(u64, Loaded), BackendError> {
        self.with_session(queue, |session| {
            let now = session.time_ms()?;
            let keys = queue_keys(queue)?;
            let loaded = load_for_task(session, &keys, task, unique, Some(operation))?;
            Ok((now, loaded))
        })
    }
}

enum ClaimFlow {
    Retry,
    Skip,
    Stop(Step<ClaimResult>),
}

/// Ready members included in this poll. A non-empty overdue inflight window uses the whole budget.
fn ready_candidate_limit(inflight_len: usize) -> usize {
    if inflight_len == 0 {
        CLAIM_CANDIDATE_WINDOW
    } else {
        0
    }
}

fn queue_keys(queue: &str) -> Result<QueueKeys, Fail> {
    QueueKeys::new(queue).map_err(|error| Fail::Backend(BackendError::Input(error)))
}

fn enqueue_input<'a>(
    backend: &'a RedisBackend,
    job: &'a EnqueueJob,
    now: u64,
    ready_zcard: u64,
    loaded: &'a Loaded,
) -> EnqueueInput<'a> {
    EnqueueInput {
        config: backend.config_ptr(),
        operation_id: job.operation_id,
        identity: job.identity,
        cycle_id: job.cycle_id,
        actor_name: &job.actor_name,
        queue: &job.queue,
        kwargs: &job.kwargs,
        headers: &job.headers,
        tag: job.tag.as_deref(),
        unique_key: job.unique_key.as_deref(),
        delay_ms: job.delay_ms,
        now_ms: now,
        max_retries: job.max_retries,
        max_deliveries: job.max_deliveries,
        time_limit_ms: job.time_limit_ms,
        min_backoff_ms: job.min_backoff_ms,
        max_backoff_ms: job.max_backoff_ms,
        ready_zcard,
        receipt: loaded.receipt.as_ref(),
        observed: &loaded.observed,
    }
}

fn claim_input<'a>(backend: &'a RedisBackend, request: &'a ClaimRequest, now: u64, loaded: &'a Loaded) -> ClaimInput<'a> {
    ClaimInput {
        config: backend.config_ptr(),
        operation_id: request.operation_id,
        message_id: request.message_id,
        queue: &request.queue,
        worker_id: &request.worker_id,
        proposed_token: request.proposed_token,
        now_ms: now,
        lease_ms: request.lease_ms,
        receipt: loaded.receipt.as_ref(),
        observed: &loaded.observed,
    }
}

fn renew_input<'a>(backend: &'a RedisBackend, request: &'a RenewRequest, now: u64, loaded: &'a Loaded) -> RenewInput<'a> {
    RenewInput {
        config: backend.config_ptr(),
        operation_id: request.operation_id,
        message_id: request.message_id,
        queue: &request.queue,
        task_id: request.task_id,
        cycle_id: request.cycle_id,
        delivery_token: request.delivery_token,
        now_ms: now,
        lease_ms: request.lease_ms,
        receipt: loaded.receipt.as_ref(),
        observed: &loaded.observed,
    }
}

fn finish_input<'a>(backend: &'a RedisBackend, request: &'a FinishRequest, now: u64, loaded: &'a Loaded) -> FinishInput<'a> {
    FinishInput {
        config: backend.config_ptr(),
        operation_id: request.operation_id,
        message_id: request.message_id,
        queue: &request.queue,
        task_id: request.task_id,
        cycle_id: request.cycle_id,
        delivery_token: request.delivery_token,
        now_ms: now,
        proposal: &request.proposal,
        receipt: loaded.receipt.as_ref(),
        observed: &loaded.observed,
    }
}

fn requeue_input<'a>(
    backend: &'a RedisBackend,
    request: &'a RequeueRequest,
    now: u64,
    history: &'a arq_core::Json,
    loaded: &'a Loaded,
) -> RequeueInput<'a> {
    RequeueInput {
        config: backend.config_ptr(),
        operation_id: request.operation_id,
        message_id: message_for(request.task_id),
        queue: &request.queue,
        task_id: request.task_id,
        new_cycle_id: request.new_cycle_id,
        now_ms: now,
        history,
        receipt: loaded.receipt.as_ref(),
        observed: &loaded.observed,
    }
}

fn control_input<'a>(backend: &'a RedisBackend, request: &'a ControlRequest, now: u64, loaded: &'a Loaded) -> ControlInput<'a> {
    ControlInput {
        config: backend.config_ptr(),
        operation_id: request.operation_id,
        message_id: message_for(request.task_id),
        queue: &request.queue,
        task_id: request.task_id,
        now_ms: now,
        receipt: loaded.receipt.as_ref(),
        observed: &loaded.observed,
    }
}

fn settle_input<'a>(backend: &'a RedisBackend, request: &'a ControlRequest, now: u64, loaded: &'a Loaded) -> SettleInput<'a> {
    SettleInput {
        config: backend.config_ptr(),
        operation_id: request.operation_id,
        message_id: message_for(request.task_id),
        queue: &request.queue,
        task_id: request.task_id,
        now_ms: now,
        receipt: loaded.receipt.as_ref(),
        observed: &loaded.observed,
    }
}

impl RedisBackend {
    fn config_ptr(&self) -> &arq_core::Config {
        &self.inner.config
    }
}

fn align_finish(now: u64, loaded: &Loaded, proposal: &ParsedOutcome) -> (u64, ObservedTask) {
    let observed = &loaded.observed;
    let view = observed.clone();
    let receipt = loaded.receipt.is_some();
    if let ParsedOutcome::Proposal(OutcomeProposal::Retry { delay_ms }) = proposal {
        if observed.state == Some(TaskState::Ready) && observed.delivery_token.is_none() {
            if let Some(score) = observed.ready_score {
                if receipt || now.saturating_add(*delay_ms) == score {
                    return (score.saturating_sub(*delay_ms), view);
                }
            }
        }
    } else if matches!(observed.state, Some(TaskState::Done | TaskState::Dead)) {
        if let Some(finished) = observed.finished_at_ms {
            if receipt || finished == now {
                return (finished, view);
            }
        }
    }
    (now, view)
}

fn duplicate_of(job: &EnqueueJob, observed: &ObservedTask) -> arq_core::DuplicateTask {
    arq_core::DuplicateTask {
        unique_key: job.unique_key.clone().unwrap_or_default(),
        task_id: job.identity.task_id,
        owner_task_id: observed.uniq_owner.unwrap_or(job.identity.task_id),
    }
}

fn claim_result(
    observed: &ObservedTask,
    status: ReceiptStatus,
    replayed: bool,
    now: u64,
    operation: OperationId,
    queue: &str,
) -> Result<ClaimResult, Fail> {
    let task_id = observed.task_id.ok_or_else(|| {
        Fail::Backend(inconsistency(queue, Some(operation), "proved claim has no task_id"))
    })?;
    if status == ReceiptStatus::Dead || observed.state == Some(TaskState::Dead) {
        let kwargs = if observed.kwargs_canonical.is_empty() {
            "{}".to_string()
        } else {
            observed.kwargs_canonical.clone()
        };
        return Ok(ClaimResult::DeadLetter {
            task_id,
            reason: observed.reason.clone(),
            replayed,
            actor_name: observed.actor_name.clone(),
            kwargs_canonical: kwargs,
            message_id: observed.message_id.map(|id| id.to_string()).unwrap_or_default(),
            retries: observed.retries,
            deliveries: observed.deliveries,
            error: observed.error.clone(),
        });
    }
    let token = observed.delivery_token.ok_or_else(|| {
        Fail::Backend(inconsistency(queue, Some(operation), "proved claim has no delivery token"))
    })?;
    let deadline = observed.lease_deadline_ms.ok_or_else(|| {
        Fail::Backend(inconsistency(queue, Some(operation), "proved claim has no lease deadline"))
    })?;
    if now >= deadline {
        return Ok(ClaimResult::OwnershipLost {
            exit_code: OWNERSHIP_LOST_EXIT_CODE,
            task_id,
            delivery_token: token,
        });
    }
    Ok(ClaimResult::Delivered(Delivery { observed: observed.clone(), replayed }))
}

fn requeue_from_status(status: ReceiptStatus, cycle: CycleId, replayed: bool) -> RequeueResult {
    match status {
        ReceiptStatus::NotFound => RequeueResult::NotFound,
        ReceiptStatus::NotDead => RequeueResult::NotDead,
        ReceiptStatus::Duplicate => RequeueResult::NotDead,
        _ => RequeueResult::Requeued { cycle_id: cycle, replayed },
    }
}

fn control_from_status(status: ReceiptStatus, cycle: Option<CycleId>, replayed: bool) -> ControlResult {
    match status {
        ReceiptStatus::AlreadyTerminal => ControlResult::AlreadyTerminal,
        ReceiptStatus::NotFound => ControlResult::NotFound,
        ReceiptStatus::NotDead => ControlResult::NotBlocked,
        _ => ControlResult::Accepted { cycle_id: cycle, replayed },
    }
}

fn message_for(task: TaskId) -> MessageId {
    MessageId::from_uuid(task.as_uuid())
}

fn lost(operation: OperationId, message: MessageId) -> BackendError {
    BackendError::Indeterminate(arq_core::IndeterminateOperation { operation_id: operation, message_id: message })
}

/// `not_found` and `not_dead` receipts are not success postconditions. The core reports
/// them as a contradiction on the next read. Keep the stored status when the task still
/// matches it, and halt when the receipt and the task disagree.
fn status_or_inconsistent<T>(stored: Option<T>, error: arq_core::StorageInconsistency) -> Result<T, Fail> {
    stored.ok_or_else(|| Fail::Backend(BackendError::Inconsistent(error)))
}

fn control_override(loaded: &Loaded) -> Option<ControlResult> {
    let receipt = loaded.receipt.as_ref()?;
    if receipt.status == ReceiptStatus::NotFound && !loaded.observed.present && !loaded.observed.indexes_contain() {
        return Some(ControlResult::NotFound);
    }
    None
}

fn requeue_override(loaded: &Loaded, cycle: CycleId) -> Option<RequeueResult> {
    let receipt = loaded.receipt.as_ref()?;
    let observed = &loaded.observed;
    match receipt.status {
        ReceiptStatus::NotFound if !observed.present && !observed.indexes_contain() => Some(RequeueResult::NotFound),
        ReceiptStatus::NotDead
            if observed.present && arq_core::classify_requeue(observed) == arq_core::RequeueEligibility::NotDead =>
        {
            Some(requeue_from_status(ReceiptStatus::NotDead, observed.cycle_id.unwrap_or(cycle), true))
        }
        _ => None,
    }
}

#[cfg(test)]
mod decision_tests {
    use super::{
        align_finish, claim_result, control_from_status, control_override, duplicate_of, ready_candidate_limit, requeue_from_status,
        requeue_override, status_or_inconsistent,
    };
    use crate::error::BackendError;
    use crate::observe::Loaded;
    use crate::session::Fail;
    use crate::types::{ClaimResult, ControlResult, EnqueueJob, RequeueResult};
    use arq_core::ObservedTask;
    use arq_core::{
        CanonicalUuid, CycleId, DeliveryToken, MessageIdentity, OperationId, OperationKind, OutcomeProposal, ParsedOutcome,
        ReceiptRecord, ReceiptStatus, StorageInconsistency, TaskId, TaskState, CLAIM_CANDIDATE_WINDOW,
    };

    fn uuid(byte: u8) -> CanonicalUuid {
        CanonicalUuid::from_random_v4([byte; 16])
    }

    fn task(byte: u8) -> TaskId {
        TaskId::from_uuid(uuid(byte))
    }

    fn operation(byte: u8) -> OperationId {
        OperationId::from_uuid(uuid(byte))
    }

    fn cycle(byte: u8) -> CycleId {
        CycleId::from_uuid(uuid(byte))
    }

    fn receipt(status: ReceiptStatus) -> ReceiptRecord {
        ReceiptRecord {
            operation_id: operation(1),
            op: OperationKind::Cancel,
            cycle_id: None,
            task_id: task(2),
            request_canonical: "{}".into(),
            result_canonical: "{}".into(),
            status,
            delivery_token: None,
            lease_deadline_ms: None,
            started_at_ms: None,
            deliveries: None,
            ready_score: None,
        }
    }

    fn job() -> EnqueueJob {
        EnqueueJob {
            operation_id: operation(3),
            identity: MessageIdentity::new(uuid(4)),
            cycle_id: cycle(5),
            actor_name: "process_item".into(),
            queue: "default".into(),
            kwargs: arq_core::Json::object(vec![]),
            headers: arq_core::Json::object(vec![]),
            tag: None,
            unique_key: None,
            delay_ms: 0,
            max_retries: 1,
            max_deliveries: 2,
            time_limit_ms: 1,
            min_backoff_ms: 1,
            max_backoff_ms: 1,
        }
    }

    #[test]
    fn one_poll_reads_ready_only_when_the_inflight_window_is_empty() {
        assert_eq!(ready_candidate_limit(0), CLAIM_CANDIDATE_WINDOW);
        assert_eq!(ready_candidate_limit(1), 0);
        assert_eq!(ready_candidate_limit(CLAIM_CANDIDATE_WINDOW), 0);
    }

    #[test]
    fn pure_decision_helpers_cover_each_status() {
        let mut observed = ObservedTask::absent();
        let error = claim_result(&observed, ReceiptStatus::Claimed, false, 10, operation(3), "default").unwrap_err();
        assert!(matches!(error, Fail::Backend(BackendError::Inconsistent(_))));
        observed.task_id = Some(task(4));
        let error = claim_result(&observed, ReceiptStatus::Claimed, false, 10, operation(3), "default").unwrap_err();
        assert!(matches!(error, Fail::Backend(BackendError::Inconsistent(_))));
        observed.delivery_token = Some(DeliveryToken::from_uuid(uuid(6)));
        let error = claim_result(&observed, ReceiptStatus::Claimed, false, 10, operation(3), "default").unwrap_err();
        assert!(matches!(error, Fail::Backend(BackendError::Inconsistent(_))));
        observed.lease_deadline_ms = Some(10);
        assert!(matches!(
            claim_result(&observed, ReceiptStatus::Claimed, false, 10, operation(3), "default").unwrap(),
            ClaimResult::OwnershipLost { .. }
        ));
        observed.lease_deadline_ms = Some(11);
        observed.state = Some(TaskState::Dead);
        assert!(matches!(
            claim_result(&observed, ReceiptStatus::Claimed, true, 10, operation(3), "default").unwrap(),
            ClaimResult::DeadLetter { .. }
        ));

        assert!(matches!(requeue_from_status(ReceiptStatus::NotFound, cycle(5), false), RequeueResult::NotFound));
        assert!(matches!(requeue_from_status(ReceiptStatus::NotDead, cycle(5), false), RequeueResult::NotDead));
        assert!(matches!(requeue_from_status(ReceiptStatus::Duplicate, cycle(5), false), RequeueResult::NotDead));
        assert!(matches!(requeue_from_status(ReceiptStatus::Requeued, cycle(5), true), RequeueResult::Requeued { replayed: true, .. }));
        assert!(matches!(control_from_status(ReceiptStatus::NotFound, None, false), ControlResult::NotFound));
        assert!(matches!(control_from_status(ReceiptStatus::NotDead, None, false), ControlResult::NotBlocked));
        assert!(matches!(control_from_status(ReceiptStatus::AlreadyTerminal, None, false), ControlResult::AlreadyTerminal));
        assert!(matches!(control_from_status(ReceiptStatus::Accepted, Some(cycle(5)), false), ControlResult::Accepted { .. }));

        let inconsistency = StorageInconsistency { queue: "default".into(), operation_id: None, detail: "x".into() };
        assert!(status_or_inconsistent::<u8>(None, inconsistency.clone()).is_err());
        assert_eq!(status_or_inconsistent(Some(7), inconsistency).unwrap(), 7);

        let absent = Loaded { receipt: None, observed: ObservedTask::absent() };
        assert!(control_override(&absent).is_none());
        let not_found = Loaded { receipt: Some(receipt(ReceiptStatus::NotFound)), observed: ObservedTask::absent() };
        assert!(matches!(control_override(&not_found), Some(ControlResult::NotFound)));
        let other = Loaded { receipt: Some(receipt(ReceiptStatus::Accepted)), observed: ObservedTask::absent() };
        assert!(control_override(&other).is_none());
        assert!(matches!(requeue_override(&not_found, cycle(5)), Some(RequeueResult::NotFound)));
        let mut ready = ObservedTask::absent();
        ready.present = true;
        ready.state = Some(TaskState::Ready);
        let not_dead = Loaded { receipt: Some(receipt(ReceiptStatus::NotDead)), observed: ready };
        assert!(matches!(requeue_override(&not_dead, cycle(5)), Some(RequeueResult::NotDead)));
        assert!(requeue_override(&other, cycle(5)).is_none());

        let queued = job();
        let mut owned = ObservedTask::absent();
        owned.uniq_owner = Some(task(9));
        let duplicate = duplicate_of(&queued, &owned);
        assert_eq!(duplicate.owner_task_id, task(9));
        assert!(duplicate_of(&queued, &ObservedTask::absent()).unique_key.is_empty());

        let mut observed = ObservedTask::absent();
        observed.state = Some(TaskState::Ready);
        observed.ready_score = Some(50);
        let loaded = Loaded { receipt: None, observed: observed.clone() };
        let retry = ParsedOutcome::Proposal(OutcomeProposal::Retry { delay_ms: 5 });
        assert_eq!(align_finish(10, &loaded, &retry).0, 10);
        let loaded = Loaded { receipt: Some(receipt(ReceiptStatus::Enqueued)), observed: observed.clone() };
        assert_eq!(align_finish(10, &loaded, &retry).0, 45);
        observed.state = Some(TaskState::Done);
        observed.finished_at_ms = Some(3);
        let loaded = Loaded { receipt: None, observed: observed.clone() };
        let ok = ParsedOutcome::Proposal(OutcomeProposal::Ok);
        assert_eq!(align_finish(10, &loaded, &ok).0, 10);
        let loaded = Loaded { receipt: Some(receipt(ReceiptStatus::Finished)), observed };
        assert_eq!(align_finish(10, &loaded, &ok).0, 3);
    }
}
