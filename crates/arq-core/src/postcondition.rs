//! Success postconditions. A partial match is not success.

use crate::ids::{CycleId, DeliveryToken, MessageId, TaskId};
use crate::state::{ControlOp, ObservedTask, StopEvidence, TaskResult, TaskState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostconditionMismatch {
    pub checks: Vec<&'static str>,
}

impl PostconditionMismatch {
    pub fn contains(&self, name: &str) -> bool {
        self.checks.iter().any(|check| *check == name)
    }
}

fn done(failed: Vec<&'static str>) -> Result<(), PostconditionMismatch> {
    if failed.is_empty() {
        Ok(())
    } else {
        Err(PostconditionMismatch { checks: failed })
    }
}

fn req(ok: bool, name: &'static str, failed: &mut Vec<&'static str>) {
    if !ok {
        failed.push(name);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnqueueProof {
    pub task_id: TaskId,
    pub message_id: MessageId,
    pub cycle_id: CycleId,
    pub actor_name: String,
    pub queue: String,
    pub kwargs_canonical: String,
    pub headers_canonical: String,
    pub tag: Option<String>,
    pub unique_key: Option<String>,
    /// `Some` when this operation recorded the score. `None` checks that the hash and the ZSET agree.
    pub available_at_ms: Option<u64>,
}

pub fn check_enqueue(expect: &EnqueueProof, observed: &ObservedTask) -> Result<(), PostconditionMismatch> {
    let mut failed = Vec::new();
    req(observed.present, "hash_exists", &mut failed);
    req(observed.state == Some(TaskState::Ready), "state", &mut failed);
    req(observed.cycle_id == Some(expect.cycle_id), "cycle", &mut failed);
    req(observed.task_id == Some(expect.task_id), "task_id", &mut failed);
    req(observed.message_id == Some(expect.message_id), "message_id", &mut failed);
    req(observed.actor_name == expect.actor_name, "actor", &mut failed);
    req(observed.queue == expect.queue, "queue", &mut failed);
    req(observed.kwargs_canonical == expect.kwargs_canonical, "kwargs", &mut failed);
    req(observed.headers_canonical == expect.headers_canonical, "headers", &mut failed);
    req(observed.tag == expect.tag, "tag", &mut failed);
    req(observed.unique_key == expect.unique_key, "unique_key", &mut failed);
    match expect.available_at_ms {
        Some(score) => {
            req(observed.ready_score == Some(score), "ready_score", &mut failed);
            req(observed.available_at_ms == Some(score), "available_at", &mut failed);
        }
        None => {
            let agrees = observed.ready_score.is_some() && observed.ready_score == observed.available_at_ms;
            req(agrees, "ready_score", &mut failed);
        }
    }
    req(observed.deliveries == 0, "deliveries", &mut failed);
    req(observed.retries == 0, "retries", &mut failed);
    if expect.unique_key.is_some() {
        req(observed.uniq_owner == Some(expect.task_id), "uniq", &mut failed);
    }
    req(!observed.in_inflight && !observed.in_done && !observed.in_dead, "indexes", &mut failed);
    done(failed)
}

/// Duplicate enqueue created no hash for this task.
pub fn duplicate_holds(observed: &ObservedTask) -> bool {
    !observed.present
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimDeliveryProof {
    pub delivery_token: DeliveryToken,
    /// Exact counter after the claim when the receipt or the plan recorded it.
    /// `None` accepts any positive counter, used when the pre-claim value was not retained.
    pub deliveries: Option<u64>,
    pub cycle_id: CycleId,
}

pub fn check_claim_delivery(expect: &ClaimDeliveryProof, observed: &ObservedTask) -> Result<(), PostconditionMismatch> {
    let mut failed = Vec::new();
    req(observed.present, "hash_exists", &mut failed);
    req(observed.state == Some(TaskState::Inflight), "state", &mut failed);
    req(observed.delivery_token == Some(expect.delivery_token), "delivery_token", &mut failed);
    let deliveries_ok = match expect.deliveries {
        Some(count) => observed.deliveries == count,
        None => observed.deliveries >= 1,
    };
    req(deliveries_ok, "deliveries", &mut failed);
    req(observed.in_inflight, "inflight_member", &mut failed);
    req(observed.ready_score.is_none(), "ready_absent", &mut failed);
    req(observed.cycle_id == Some(expect.cycle_id), "cycle", &mut failed);
    done(failed)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimDlqProof {
    pub uniq_owned_before: bool,
    pub tag: Option<String>,
}

pub fn check_claim_dlq(expect: &ClaimDlqProof, observed: &ObservedTask) -> Result<(), PostconditionMismatch> {
    let mut failed = Vec::new();
    req(observed.present, "hash_exists", &mut failed);
    req(observed.state == Some(TaskState::Dead), "state", &mut failed);
    req(observed.reason == "max_deliveries", "reason", &mut failed);
    req(observed.in_dead, "dead_member", &mut failed);
    req(!observed.in_inflight && observed.ready_score.is_none(), "indexes", &mut failed);
    if expect.uniq_owned_before {
        req(observed.uniq_owner.is_none(), "uniq_cleared", &mut failed);
    }
    req(tag_index(expect.tag.as_deref(), observed), "dead_tag", &mut failed);
    done(failed)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenewProof {
    pub delivery_token: DeliveryToken,
    pub cycle_id: CycleId,
    pub lease_deadline_ms: u64,
    pub started_at_ms: Option<u64>,
    pub deliveries: u64,
}

pub fn check_renew(expect: &RenewProof, observed: &ObservedTask) -> Result<(), PostconditionMismatch> {
    let mut failed = Vec::new();
    req(observed.present, "hash_exists", &mut failed);
    req(observed.state == Some(TaskState::Inflight), "state", &mut failed);
    req(observed.delivery_token == Some(expect.delivery_token), "delivery_token", &mut failed);
    req(observed.lease_deadline_ms == Some(expect.lease_deadline_ms), "lease_deadline", &mut failed);
    req(observed.started_at_ms == expect.started_at_ms, "started_at", &mut failed);
    req(observed.cycle_id == Some(expect.cycle_id), "cycle", &mut failed);
    req(observed.deliveries == expect.deliveries, "deliveries", &mut failed);
    done(failed)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinishShape {
    Ok,
    Skipped,
    Retry { retries_after: u64 },
    Dead { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinishProof {
    pub task_id: TaskId,
    pub cycle_id: CycleId,
    pub shape: FinishShape,
    pub uniq_owned_before: bool,
    pub ready_score: u64,
    pub finished_at_ms: u64,
    pub tag: Option<String>,
    pub error: String,
}

pub fn check_finish(expect: &FinishProof, observed: &ObservedTask) -> Result<(), PostconditionMismatch> {
    let mut failed = Vec::new();
    req(observed.present, "hash_exists", &mut failed);
    req(observed.cycle_id == Some(expect.cycle_id), "cycle", &mut failed);
    match &expect.shape {
        FinishShape::Ok => check_done_result(TaskResult::Ok, expect, observed, &mut failed),
        FinishShape::Skipped => check_done_result(TaskResult::Skipped, expect, observed, &mut failed),
        FinishShape::Retry { retries_after } => {
            req(observed.state == Some(TaskState::Ready), "state", &mut failed);
            req(observed.retries == *retries_after, "retries", &mut failed);
            req(observed.delivery_token.is_none(), "delivery_cleared", &mut failed);
            req(!observed.in_inflight, "inflight_absent", &mut failed);
            req(observed.ready_score == Some(expect.ready_score), "ready_score", &mut failed);
            req(!observed.in_done, "done_absent", &mut failed);
            if expect.uniq_owned_before {
                req(observed.uniq_owner == Some(expect.task_id), "uniq", &mut failed);
            } else {
                req(observed.uniq_owner.is_none(), "uniq", &mut failed);
            }
        }
        FinishShape::Dead { reason } => {
            req(observed.state == Some(TaskState::Dead), "state", &mut failed);
            req(&observed.reason == reason, "reason", &mut failed);
            req(observed.error == expect.error, "error", &mut failed);
            req(!observed.in_inflight, "inflight_absent", &mut failed);
            req(!observed.in_done, "done_absent", &mut failed);
            req(observed.in_dead, "dead_member", &mut failed);
            req(tag_index(expect.tag.as_deref(), observed), "dead_tag", &mut failed);
            uniq_cleared(expect.uniq_owned_before, expect.task_id, observed, &mut failed);
        }
    }
    done(failed)
}

fn check_done_result(result: TaskResult, expect: &FinishProof, observed: &ObservedTask, failed: &mut Vec<&'static str>) {
    req(observed.state == Some(TaskState::Done), "state", failed);
    req(observed.result == Some(result), "result", failed);
    req(observed.in_done, "done_member", failed);
    req(!observed.in_inflight && observed.ready_score.is_none(), "indexes", failed);
    req(observed.finished_at_ms == Some(expect.finished_at_ms), "finished_at", failed);
    uniq_cleared(expect.uniq_owned_before, expect.task_id, observed, failed);
}

fn uniq_cleared(owned_before: bool, task_id: TaskId, observed: &ObservedTask, failed: &mut Vec<&'static str>) {
    if owned_before {
        req(observed.uniq_owner.is_none(), "uniq_cleared", failed);
    } else {
        req(observed.uniq_owner != Some(task_id), "uniq_cleared", failed);
    }
}

fn tag_index(tag: Option<&str>, observed: &ObservedTask) -> bool {
    match tag {
        Some(_) => observed.in_dead_tag,
        None => !observed.in_dead_tag,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequeueProof {
    pub task_id: TaskId,
    pub cycle_id: CycleId,
    pub ready_score: u64,
    pub has_unique_key: bool,
}

pub fn check_requeue(expect: &RequeueProof, observed: &ObservedTask) -> Result<(), PostconditionMismatch> {
    let mut failed = Vec::new();
    req(observed.present, "hash_exists", &mut failed);
    req(observed.state == Some(TaskState::Ready), "state", &mut failed);
    req(observed.cycle_id == Some(expect.cycle_id), "cycle", &mut failed);
    req(observed.retries == 0 && observed.deliveries == 0, "counters", &mut failed);
    req(observed.history_contains_prior, "history", &mut failed);
    req(observed.ready_score == Some(expect.ready_score), "ready_score", &mut failed);
    req(!observed.in_dead && !observed.in_dead_tag, "dead_absent", &mut failed);
    req(observed.control_op == ControlOp::Empty, "control", &mut failed);
    req(observed.delivery_token.is_none(), "delivery_cleared", &mut failed);
    if expect.has_unique_key {
        req(observed.uniq_owner == Some(expect.task_id), "uniq", &mut failed);
    }
    done(failed)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlShape {
    Terminal { reason: String, uniq_owned_before: bool },
    Inflight { cycle_id: CycleId, token: DeliveryToken, op: ControlOp },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlProof {
    pub task_id: TaskId,
    pub shape: ControlShape,
}

pub fn check_control(expect: &ControlProof, observed: &ObservedTask) -> Result<(), PostconditionMismatch> {
    let mut failed = Vec::new();
    match &expect.shape {
        ControlShape::Terminal { reason, uniq_owned_before } => {
            req(observed.present, "hash_exists", &mut failed);
            req(observed.state == Some(TaskState::Dead), "state", &mut failed);
            req(&observed.reason == reason, "reason", &mut failed);
            req(observed.ready_score.is_none(), "ready_absent", &mut failed);
            req(!observed.in_done, "done_absent", &mut failed);
            req(observed.in_dead, "dead_member", &mut failed);
            req(observed.control_op == ControlOp::Empty, "control", &mut failed);
            uniq_cleared(*uniq_owned_before, expect.task_id, observed, &mut failed);
        }
        ControlShape::Inflight { cycle_id, token, op } => {
            req(observed.present, "hash_exists", &mut failed);
            req(observed.state == Some(TaskState::Inflight), "state", &mut failed);
            req(observed.control_op == *op, "control", &mut failed);
            req(observed.control_cycle_id == Some(*cycle_id), "control_cycle", &mut failed);
            req(observed.control_delivery_token == Some(*token), "control_token", &mut failed);
            req(observed.in_inflight, "inflight_member", &mut failed);
            req(!observed.in_dead, "dead_absent", &mut failed);
        }
    }
    done(failed)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettleProof {
    pub task_id: TaskId,
    pub reason: String,
    pub uniq_owned_before: bool,
    pub tag: Option<String>,
}

pub fn check_settle(expect: &SettleProof, observed: &ObservedTask) -> Result<(), PostconditionMismatch> {
    let mut failed = Vec::new();
    req(observed.present, "hash_exists", &mut failed);
    req(observed.state == Some(TaskState::Dead), "state", &mut failed);
    req(observed.reason == expect.reason, "reason", &mut failed);
    req(observed.stop_evidence == StopEvidence::LeaseExpired, "stop_evidence", &mut failed);
    req(observed.control_op == ControlOp::Empty, "control", &mut failed);
    req(observed.in_dead, "dead_member", &mut failed);
    req(!observed.in_inflight, "inflight_absent", &mut failed);
    req(observed.ready_score.is_none(), "ready_absent", &mut failed);
    req(tag_index(expect.tag.as_deref(), observed), "dead_tag", &mut failed);
    uniq_cleared(expect.uniq_owned_before, expect.task_id, observed, &mut failed);
    done(failed)
}

pub fn settle_success(observed: &ObservedTask) -> bool {
    let reason_ok = observed.reason == "cancelled" || observed.reason == "killed";
    let tag_ok = match observed.tag.as_deref() {
        Some(_) => observed.in_dead_tag,
        None => !observed.in_dead_tag,
    };
    let uniq_ok = match observed.unique_key.as_deref() {
        Some(_) => observed.uniq_owner.is_none(),
        None => true,
    };
    observed.present
        && observed.state == Some(TaskState::Dead)
        && reason_ok
        && observed.stop_evidence == StopEvidence::LeaseExpired
        && observed.control_op == ControlOp::Empty
        && observed.in_dead
        && !observed.in_inflight
        && observed.ready_score.is_none()
        && tag_ok
        && uniq_ok
}

pub fn is_terminal_state(observed: &ObservedTask) -> bool {
    observed.present && matches!(observed.state, Some(TaskState::Done | TaskState::Dead))
}
