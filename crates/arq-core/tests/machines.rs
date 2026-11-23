#[path = "common/mod.rs"]
mod common;
use arq_core::*;
use common::*;
use std::time::{Duration, Instant};

fn step(life: AttemptLifecycle, event: AttemptEvent) -> AttemptLifecycle {
    life.transition(event).unwrap_or_else(|err| panic!("{err}"))
}

#[test]
fn attempt_keeps_lost_ownership_through_handler_return() {
    let life = step(AttemptLifecycle::claimed(), AttemptEvent::StartHandler);
    assert_eq!(life.execution, ExecutionState::Running);
    assert!(!life.ownership_lost());
    let cancel = step(life, AttemptEvent::RequestCancel);
    let again = step(cancel, AttemptEvent::RequestCancel);
    assert_eq!(again.execution, ExecutionState::CancelRequested);
    let lost = step(again, AttemptEvent::LoseOwnership);
    assert!(lost.ownership_lost());
    assert_eq!(lost.execution, ExecutionState::CancelRequested);
    let returned = step(lost, AttemptEvent::HandlerReturned);
    assert_eq!(returned.execution, ExecutionState::HandlerFinished);
    assert!(returned.ownership_lost());
    let finishing = step(returned, AttemptEvent::BeginFinish);
    assert!(!finishing.allowed_success_ack());
    let done = step(finishing, AttemptEvent::FinishConfirmed);
    assert!(done.is_terminal());
    let err = done.transition(AttemptEvent::LoseOwnership).unwrap_err();
    assert!(err.to_string().contains("illegal attempt"));
    touch_error(&err);
}

#[test]
fn attempt_stop_from_running_requires_lost_ownership() {
    let running = step(AttemptLifecycle::claimed(), AttemptEvent::StartHandler);
    assert!(running.transition(AttemptEvent::ConfirmExecutionStopped).is_err());
    let lost = step(running, AttemptEvent::LoseOwnership);
    let stopped = step(lost, AttemptEvent::ConfirmExecutionStopped);
    assert_eq!(stopped.execution, ExecutionState::Stopped);
    let finishing = step(stopped, AttemptEvent::BeginFinish);
    assert!(!finishing.allowed_success_ack());
    let rejected = step(finishing, AttemptEvent::FinishRejectedStale);
    assert_eq!(rejected.finish, FinishState::RejectedStale);
    let held = step(AttemptLifecycle::claimed(), AttemptEvent::StartHandler);
    let asked = step(held, AttemptEvent::RequestCancel);
    let stopped = step(asked, AttemptEvent::ConfirmExecutionStopped);
    assert_eq!(stopped.ownership, OwnershipState::Held);
    let finishing = step(stopped, AttemptEvent::BeginFinish);
    assert!(finishing.allowed_success_ack());
    let unknown = step(finishing, AttemptEvent::FinishUnconfirmed);
    assert_eq!(unknown.finish, FinishState::Unconfirmed);
}

#[test]
fn attempt_rejects_events_from_the_wrong_axis() {
    let claimed = AttemptLifecycle::claimed();
    for event in [
        AttemptEvent::RequestCancel,
        AttemptEvent::ConfirmExecutionStopped,
        AttemptEvent::HandlerReturned,
        AttemptEvent::BeginFinish,
        AttemptEvent::FinishConfirmed,
        AttemptEvent::FinishRejectedStale,
        AttemptEvent::FinishUnconfirmed,
        AttemptEvent::StartHandler,
    ] {
        if event == AttemptEvent::StartHandler {
            continue;
        }
        assert!(claimed.transition(event).is_err(), "{event:?}");
    }
    let running = step(claimed, AttemptEvent::StartHandler);
    assert!(running.transition(AttemptEvent::StartHandler).is_err());
    assert!(running.transition(AttemptEvent::BeginFinish).is_err());
    assert!(running.transition(AttemptEvent::FinishConfirmed).is_err());
    let finished = step(running, AttemptEvent::HandlerReturned);
    assert!(finished.transition(AttemptEvent::HandlerReturned).is_err());
    assert!(finished.transition(AttemptEvent::ConfirmExecutionStopped).is_err());
    assert!(finished.transition(AttemptEvent::FinishConfirmed).is_err());
    let inflight = step(finished, AttemptEvent::BeginFinish);
    assert!(inflight.transition(AttemptEvent::BeginFinish).is_err());
    assert!(inflight.transition(AttemptEvent::StartHandler).is_err());
    assert!(inflight.transition(AttemptEvent::RequestCancel).is_err());
}

#[test]
fn admission_blocks_claim_during_drain_and_keeps_resolved_claims() {
    let mut gate = AdmissionGate::default();
    assert!(!gate.blocks_claim());
    assert!(!gate.is_closed());
    assert_eq!(gate.reject_late_claim(), ClaimAdmission::Accept);
    assert_eq!(gate.rejected(), 0);
    assert_eq!(gate.apply(AdmissionEvent::Close), AdmissionEffect::Closed);
    assert!(gate.blocks_claim());
    assert!(!gate.ready_for_tracker_close());
    assert_eq!(gate.apply(AdmissionEvent::RejectLateClaim), AdmissionEffect::Decision(ClaimAdmission::Reject));
    assert_eq!(gate.rejected(), 1);
    assert_eq!(
        gate.apply(AdmissionEvent::NoteClaimResolved),
        AdmissionEffect::Decision(ClaimAdmission::AcceptInflightAfterClose)
    );
    assert_eq!(gate.accepted_after_close(), 1);
    assert_eq!(gate.note_claim_resolved(), ClaimAdmission::AcceptInflightAfterClose);
    assert_eq!(gate.apply(AdmissionEvent::MarkInflightSettled), AdmissionEffect::Settled);
    assert!(gate.inflight_settled());
    assert!(gate.ready_for_tracker_close());
    assert_eq!(gate.note_claim_resolved(), ClaimAdmission::Reject);
    assert_eq!(gate.reject_late_claim(), ClaimAdmission::Reject);
    let mut open = AdmissionGate::default();
    open.mark_inflight_settled();
    assert!(!open.ready_for_tracker_close());
    assert_eq!(open.note_claim_resolved(), ClaimAdmission::Reject);
    open.close();
    assert!(open.ready_for_tracker_close());
    assert_eq!(open.note_claim_resolved(), ClaimAdmission::Reject);
}

#[test]
fn slot_ready_drain_reaped_and_illegal_reap() {
    let slot = SlotLifecycle::starting();
    assert_eq!(slot.phase(), ProcessPhase::Starting);
    let running = slot.transition(ProcessEvent::Ready).unwrap();
    let draining = running.transition(ProcessEvent::Drain).unwrap();
    let still = draining.transition(ProcessEvent::Drain).unwrap();
    assert_eq!(still.phase(), ProcessPhase::Draining);
    let stopping = still.transition(ProcessEvent::Stop).unwrap();
    assert_eq!(stopping.transition(ProcessEvent::Stop).unwrap().phase(), ProcessPhase::Stopping);
    assert_eq!(stopping.transition(ProcessEvent::Reaped).unwrap().phase(), ProcessPhase::Exited);
    let err = running.transition(ProcessEvent::Reaped).unwrap_err();
    assert!(err.to_string().contains("illegal slot"));
    touch_error(&err);
    let failed = SlotLifecycle::starting().transition(ProcessEvent::StartFailed).unwrap();
    assert_eq!(failed.transition(ProcessEvent::Reaped).unwrap().phase(), ProcessPhase::Exited);
    assert_eq!(running.transition(ProcessEvent::Crash).unwrap().phase(), ProcessPhase::Failed);
    assert_eq!(draining.transition(ProcessEvent::Crash).unwrap().phase(), ProcessPhase::Failed);
    assert_eq!(running.transition(ProcessEvent::Stop).unwrap().phase(), ProcessPhase::Stopping);
    assert!(slot.transition(ProcessEvent::Drain).is_err());
    assert!(slot.transition(ProcessEvent::Reaped).is_err());
    let exited = stopping.transition(ProcessEvent::Reaped).unwrap();
    assert!(exited.transition(ProcessEvent::Reaped).is_err());
    let owner = SlotOwner::starting();
    assert_eq!(owner.apply(ProcessEvent::Ready).unwrap(), ProcessPhase::Running);
    assert_eq!(owner.apply(ProcessEvent::Drain).unwrap(), ProcessPhase::Draining);
    assert_eq!(owner.phase(), ProcessPhase::Draining);
}

#[test]
fn ping_sleep_does_not_burn_the_budget_and_expiry_exits_2() {
    let start = Instant::now();
    let mut watch = PingWatch::new(Duration::from_secs(30));
    assert_eq!(watch.budget(), Duration::from_secs(30));
    let slept = watch.observe(LoopEvent::Responded, start + Duration::from_secs(10_000));
    assert!(slept.renew_continues());
    assert!(!slept.stop_renew);
    assert_eq!(slept.exit_code, None);
    let miss = watch.observe(LoopEvent::Missed, start);
    assert!(miss.renew_continues());
    assert!(!miss.request_cancel);
    let still = watch.observe(LoopEvent::Missed, start + Duration::from_secs(29));
    assert!(still.renew_continues());
    let edge = watch.observe(LoopEvent::Missed, start + Duration::from_secs(30));
    assert!(edge.stop_renew);
    assert!(edge.request_cancel);
    assert_eq!(edge.exit_code, Some(PING_BUDGET_EXIT_CODE));
    assert_eq!(PING_BUDGET_EXIT_CODE, 2);
    let mut reset = PingWatch::new(Duration::from_millis(5));
    assert!(reset.observe(LoopEvent::Missed, start).renew_continues());
    assert!(reset.observe(LoopEvent::Responded, start + Duration::from_millis(4)).renew_continues());
    let later = start + Duration::from_secs(10);
    assert!(reset.observe(LoopEvent::Missed, later).renew_continues());
    assert_eq!(
        reset.observe(LoopEvent::Missed, later + Duration::from_millis(5)).exit_code,
        Some(2)
    );
}

#[test]
fn watch_budget_and_exec_classifier() {
    let mut budget = WatchBudget::new(0);
    let err = budget.on_conflict().unwrap_err();
    assert!(err.to_string().contains("exhausted"));
    touch_error(&err);
    let mut budget = WatchBudget::new(1);
    budget.on_conflict().unwrap();
    assert!(budget.on_conflict().is_err());
    assert_eq!(classify_exec(ExecReply::WatchConflict), ExecClass::RetryCycle { charge_watch: true });
    assert_eq!(classify_exec(ExecReply::Abort), ExecClass::RetryCycle { charge_watch: false });
    assert_eq!(
        classify_exec(ExecReply::Applied { commands: 2, expected: 2, all_ok: true }),
        ExecClass::CheckPostconditions
    );
    assert_eq!(
        classify_exec(ExecReply::Applied { commands: 1, expected: 2, all_ok: true }),
        ExecClass::StorageInconsistency
    );
    assert_eq!(
        classify_exec(ExecReply::Applied { commands: 2, expected: 2, all_ok: false }),
        ExecClass::StorageInconsistency
    );
    assert_eq!(classify_exec(ExecReply::Lost), ExecClass::UnknownOutcome);
    assert_eq!(CLAIM_CANDIDATE_WINDOW, 16);
    assert_eq!(watch_limit_for_claim_candidate(8), 8);
    assert_eq!(soft_ready_ceiling(100_000, 2), 100_002);
    assert_eq!(soft_ready_ceiling(u64::MAX, 1), u64::MAX);
    assert_eq!(OWNERSHIP_LOST_EXIT_CODE, 4);
    assert!(!watches_uniq(OperationKind::Renew, true));
    assert!(!watches_uniq(OperationKind::Cancel, true));
    assert!(!watches_uniq(OperationKind::Kill, true));
    assert!(!watches_uniq(OperationKind::Settle, true));
    assert!(watches_uniq(OperationKind::Enqueue, true));
    assert!(watches_uniq(OperationKind::Claim, true));
    assert!(watches_uniq(OperationKind::Finish, true));
    assert!(watches_uniq(OperationKind::Requeue, true));
    assert!(!watches_uniq(OperationKind::Enqueue, false));
}

#[test]
fn key_names_and_named_errors() {
    let keys = QueueKeys::new("default").unwrap();
    assert_eq!(keys.queue(), "default");
    assert_eq!(keys.ready(), "q:{default}:ready");
    assert_eq!(keys.inflight(), "q:{default}:inflight");
    assert_eq!(keys.done(), "q:{default}:done");
    assert_eq!(keys.dead(), "q:{default}:dead");
    assert_eq!(keys.dead_tag("batch-a").unwrap(), "q:{default}:dead:tag:batch-a");
    assert_eq!(keys.job(&task()), format!("q:{{default}}:job:{TASK}"));
    assert_eq!(keys.uniq("item:1001").unwrap(), "q:{default}:uniq:item:1001");
    assert_eq!(keys.receipt(&op()), format!("q:{{default}}:receipt:{OP}"));
    assert!(is_shared_index(&keys.ready()));
    assert!(!is_shared_index(&keys.job(&task())));
    assert!(!is_shared_index(&keys.uniq("ready").unwrap()));
    assert_eq!(key_kind(&keys.uniq("ready").unwrap()), Some("uniq"));
    assert_eq!(key_kind("no-tag"), None);
    assert!(QueueKeys::new("Default").is_err());
    assert!(QueueKeys::new("").is_err());
    assert!(QueueKeys::new("a-b").is_err());
    assert!(validate_queue("abc123").is_ok());
    assert!(validate_opaque("tag", "batch-b").is_ok());
    assert!(validate_opaque("unique_key", &"a".repeat(256)).is_ok());
    for bad in ["", &"a".repeat(257), "has space", "a{b", "a}b", "a\nb"] {
        let err = validate_opaque("unique_key", bad).unwrap_err();
        assert!(matches!(err, InputError::Opaque { field: "unique_key" }));
    }
    assert!(keys.dead_tag("bad tag").is_err());
    assert!(keys.uniq("bad key").is_err());
    let errors: Vec<Box<dyn std::error::Error>> = vec![
        Box::new(StorageInconsistency { queue: "default".into(), operation_id: Some(op()), detail: "split".into() }),
        Box::new(IndeterminateOperation { operation_id: op(), message_id: message() }),
        Box::new(OperationConflict {
            operation_id: op(),
            existing_op: "enqueue".into(),
            existing_request: "{}".into(),
            attempted_op: "claim".into(),
            attempted_request: "{}".into(),
        }),
        Box::new(QueueFull { queue: "default".into(), max_ready: 1, ready_zcard: 1 }),
        Box::new(DuplicateTask { unique_key: "item:1001".into(), task_id: task(), owner_task_id: other() }),
        Box::new(InputError::Queue),
        Box::new(InputError::Opaque { field: "tag" }),
        Box::new(InputError::Headers),
        Box::new(InputError::Kwargs),
        Box::new(InputError::Identity),
        Box::new(InputError::Backoff),
        Box::new(InputError::TimeLimit),
        Box::new(InputError::MaxDeliveries),
        Box::new(InputError::WorkerId),
        Box::new(InputError::History),
        Box::new(InputError::Payload { field: "kwargs" }),
        Box::new(InputError::Overflow { field: "delay_ms" }),
    ];
    for err in errors {
        assert!(!err.to_string().is_empty());
    }
    assert_eq!(TaskState::Ready.as_str(), "ready");
    assert_eq!(TaskState::Inflight.as_str(), "inflight");
    assert_eq!(TaskState::Done.as_str(), "done");
    assert_eq!(TaskState::Dead.as_str(), "dead");
    assert_eq!(ControlOp::parse("cancel"), Some(ControlOp::Cancel));
    assert_eq!(ControlOp::parse("kill"), Some(ControlOp::Kill));
    assert_eq!(ControlOp::parse(""), Some(ControlOp::Empty));
    assert_eq!(ControlOp::parse("nope"), None);
    assert!(ControlOp::Kill.is_blocking());
    assert!(!ControlOp::Empty.is_blocking());
    assert_eq!(ControlOp::Kill.terminal_reason(), Some("killed"));
    assert_eq!(ControlOp::Empty.terminal_reason(), None);
    assert_eq!(StopEvidence::ObservedExit.as_str(), "observed_exit");
    assert_eq!(StopEvidence::LeaseExpired.as_str(), "lease_expired");
    assert_eq!(StopEvidence::Empty.as_str(), "");
    assert_eq!(TaskResult::Ok.as_str(), "ok");
    assert_eq!(TaskResult::Skipped.as_str(), "skipped");
    for kind in [
        OperationKind::Enqueue,
        OperationKind::Claim,
        OperationKind::Renew,
        OperationKind::Finish,
        OperationKind::Requeue,
        OperationKind::Cancel,
        OperationKind::Kill,
        OperationKind::Settle,
    ] {
        assert!(!kind.as_str().is_empty());
    }
    for status in [
        ReceiptStatus::Enqueued,
        ReceiptStatus::Duplicate,
        ReceiptStatus::Claimed,
        ReceiptStatus::Dead,
        ReceiptStatus::Renewed,
        ReceiptStatus::Finished,
        ReceiptStatus::Requeued,
        ReceiptStatus::Accepted,
        ReceiptStatus::AlreadyTerminal,
        ReceiptStatus::NotFound,
        ReceiptStatus::NotDead,
    ] {
        assert!(!status.as_str().is_empty());
    }
    let absent = ObservedTask::absent();
    assert!(absent.is_clean_slate());
    assert!(!absent.indexes_contain());
}
