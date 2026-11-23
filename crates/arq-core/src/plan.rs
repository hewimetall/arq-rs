//! Operation plans and the decisions that choose them.
//!
//! Plans name the keys to `WATCH` and the commands to put in `MULTI`.
//! Shared ZSETs are never watched. The caller supplies time.

use crate::config::Config;
use crate::error::{
    DuplicateTask, IndeterminateOperation, InputError, OperationConflict, QueueFull, StorageInconsistency,
};
use crate::ids::{CycleId, DeliveryToken, MessageId, MessageIdentity, OperationId, TaskId};
use crate::json::Json;
use crate::keys::{key_kind, validate_opaque, QueueKeys};
use crate::outcome::{OutcomeProposal, ParsedOutcome};
use crate::postcondition::{
    check_claim_delivery, check_claim_dlq, check_control, check_enqueue, check_finish, check_renew, check_requeue,
    duplicate_holds, is_terminal_state, settle_success, ClaimDeliveryProof, ClaimDlqProof, ControlProof,
    ControlShape, EnqueueProof, FinishProof, FinishShape, RenewProof, RequeueProof,
};
use crate::protocol::OWNERSHIP_LOST_EXIT_CODE;
use crate::state::{
    ControlOp, ObservedTask, OperationKind, ReceiptRecord, ReceiptStatus, StopEvidence, TaskState,
};
use crate::text::truncate_utf8;

/// `WATCH` covers the receipt, the job hash, and uniq when the decision reads uniq.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchSet {
    pub receipt: String,
    pub job: String,
    pub uniq: Option<String>,
}

impl WatchSet {
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.receipt.as_str())
            .chain(std::iter::once(self.job.as_str()))
            .chain(self.uniq.as_deref())
    }

    pub fn is_task_local(&self) -> bool {
        self.keys().all(|key| matches!(key_kind(key), Some("receipt" | "job" | "uniq")))
    }
}

pub fn watches_uniq(kind: OperationKind, has_unique_key: bool) -> bool {
    has_unique_key
        && matches!(
            kind,
            OperationKind::Enqueue | OperationKind::Claim | OperationKind::Finish | OperationKind::Requeue
        )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannedCommand {
    Hset { key: String, fields: Vec<(String, String)> },
    Zadd { key: String, member: String, score: u64 },
    Zrem { key: String, member: String },
    Set { key: String, value: String },
    Del { key: String },
    SetEx { key: String, value: String, ttl_secs: u64 },
}

impl PlannedCommand {
    pub fn key(&self) -> &str {
        match self {
            Self::Hset { key, .. } | Self::Zadd { key, .. } | Self::Zrem { key, .. } | Self::Set { key, .. } | Self::Del { key } | Self::SetEx { key, .. } => key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationPlan {
    pub kind: OperationKind,
    pub operation_id: OperationId,
    pub watch: WatchSet,
    pub commands: Vec<PlannedCommand>,
    pub receipt: ReceiptRecord,
}

impl OperationPlan {
    pub fn watch_is_task_local(&self) -> bool {
        self.watch.is_task_local()
    }

    pub fn receipt_is_last(&self) -> bool {
        matches!(self.commands.last(), Some(PlannedCommand::SetEx { .. }))
    }

    pub fn hset_field(&self, name: &str) -> Option<&str> {
        self.commands.iter().find_map(|command| {
            let PlannedCommand::Hset { fields, .. } = command else {
                return None;
            };
            fields.iter().find(|(key, _)| key == name).map(|(_, value)| value.as_str())
        })
    }

    pub fn command_kinds(&self) -> Vec<&str> {
        self.commands.iter().filter_map(|command| key_kind(command.key())).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replay {
    pub status: ReceiptStatus,
    pub proved_without_receipt: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnqueueDecision {
    Execute(OperationPlan),
    Replay(Replay),
    Duplicate { error: DuplicateTask, record: OperationPlan },
    Full(QueueFull),
    Conflict(OperationConflict),
    Inconsistent(StorageInconsistency),
    Indeterminate(IndeterminateOperation),
    Rejected(InputError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimDecision {
    Execute(OperationPlan),
    DeadLetter(OperationPlan),
    Replay(Replay),
    Blocked,
    NotDue,
    LiveLease,
    Conflict(OperationConflict),
    Inconsistent(StorageInconsistency),
    Indeterminate(IndeterminateOperation),
    Rejected(InputError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenewDecision {
    Execute(OperationPlan),
    Replay(Replay),
    NotHeld { exit_code: i32 },
    Conflict(OperationConflict),
    Inconsistent(StorageInconsistency),
    Indeterminate(IndeterminateOperation),
    Rejected(InputError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinishDecision {
    Execute(OperationPlan),
    Replay(Replay),
    Rejected,
    Conflict(OperationConflict),
    Inconsistent(StorageInconsistency),
    Indeterminate(IndeterminateOperation),
    RejectedInput(InputError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequeueDecision {
    Execute(OperationPlan),
    Replay(Replay),
    NotFound(OperationPlan),
    NotDead(OperationPlan),
    Duplicate(DuplicateTask),
    Conflict(OperationConflict),
    Inconsistent(StorageInconsistency),
    Indeterminate(IndeterminateOperation),
    Rejected(InputError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlDecision {
    Execute(OperationPlan),
    Replay(Replay),
    Conflict(OperationConflict),
    Inconsistent(StorageInconsistency),
    Indeterminate(IndeterminateOperation),
    Rejected(InputError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettleDecision {
    Accepted(OperationPlan),
    StillOwned,
    NotBlocked,
    AlreadyTerminal(OperationPlan),
    NotFound(OperationPlan),
    Replay(Replay),
    Conflict(OperationConflict),
    Inconsistent(StorageInconsistency),
    Indeterminate(IndeterminateOperation),
    Rejected(InputError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimEligibility {
    Deliver,
    DeadLetter,
    Blocked,
    NotDue,
    LiveLease,
    Absent,
    Inconsistent(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenewEligibility {
    Ok,
    NotHeld,
    Inconsistent(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinishEligibility {
    Ok,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequeueEligibility {
    Ok,
    NotFound,
    NotDead,
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlEligibility {
    NotFound,
    AlreadyTerminal,
    ReadyDead,
    InflightAccept,
    Inconsistent(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettleView {
    NotFound,
    AlreadyTerminal,
    StillOwned,
    NotBlocked,
    Accepted { reason: &'static str },
    Inconsistent(&'static str),
}

pub struct EnqueueInput<'a> {
    pub config: &'a Config,
    pub operation_id: OperationId,
    pub identity: MessageIdentity,
    pub cycle_id: CycleId,
    pub actor_name: &'a str,
    pub queue: &'a str,
    pub kwargs: &'a Json,
    pub headers: &'a Json,
    pub tag: Option<&'a str>,
    pub unique_key: Option<&'a str>,
    pub delay_ms: u64,
    pub now_ms: u64,
    pub max_retries: u64,
    pub max_deliveries: u64,
    pub time_limit_ms: u64,
    pub min_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub ready_zcard: u64,
    pub receipt: Option<&'a ReceiptRecord>,
    pub observed: &'a ObservedTask,
}

pub struct ClaimInput<'a> {
    pub config: &'a Config,
    pub operation_id: OperationId,
    pub message_id: MessageId,
    pub queue: &'a str,
    pub worker_id: &'a str,
    pub proposed_token: DeliveryToken,
    pub now_ms: u64,
    pub lease_ms: u64,
    pub receipt: Option<&'a ReceiptRecord>,
    pub observed: &'a ObservedTask,
}

pub struct RenewInput<'a> {
    pub config: &'a Config,
    pub operation_id: OperationId,
    pub message_id: MessageId,
    pub queue: &'a str,
    pub task_id: TaskId,
    pub cycle_id: CycleId,
    pub delivery_token: DeliveryToken,
    pub now_ms: u64,
    pub lease_ms: u64,
    pub receipt: Option<&'a ReceiptRecord>,
    pub observed: &'a ObservedTask,
}

pub struct FinishInput<'a> {
    pub config: &'a Config,
    pub operation_id: OperationId,
    pub message_id: MessageId,
    pub queue: &'a str,
    pub task_id: TaskId,
    pub cycle_id: CycleId,
    pub delivery_token: DeliveryToken,
    pub now_ms: u64,
    pub proposal: &'a ParsedOutcome,
    pub receipt: Option<&'a ReceiptRecord>,
    pub observed: &'a ObservedTask,
}

pub struct RequeueInput<'a> {
    pub config: &'a Config,
    pub operation_id: OperationId,
    pub message_id: MessageId,
    pub queue: &'a str,
    pub task_id: TaskId,
    pub new_cycle_id: CycleId,
    pub now_ms: u64,
    pub history: &'a Json,
    pub receipt: Option<&'a ReceiptRecord>,
    pub observed: &'a ObservedTask,
}

pub struct ControlInput<'a> {
    pub config: &'a Config,
    pub operation_id: OperationId,
    pub message_id: MessageId,
    pub queue: &'a str,
    pub task_id: TaskId,
    pub now_ms: u64,
    pub receipt: Option<&'a ReceiptRecord>,
    pub observed: &'a ObservedTask,
}

pub struct SettleInput<'a> {
    pub config: &'a Config,
    pub operation_id: OperationId,
    pub message_id: MessageId,
    pub queue: &'a str,
    pub task_id: TaskId,
    pub now_ms: u64,
    pub receipt: Option<&'a ReceiptRecord>,
    pub observed: &'a ObservedTask,
}

pub fn header_bytes(headers: &Json) -> Result<usize, InputError> {
    let Some(pairs) = headers.as_object_pairs() else {
        return Err(InputError::Headers);
    };
    let mut total = 0usize;
    for (key, value) in pairs {
        let Json::String(text) = value else {
            return Err(InputError::Headers);
        };
        total = total.saturating_add(key.len()).saturating_add(text.len());
    }
    Ok(total)
}

pub fn classify_claim(job: &ObservedTask, now_ms: u64) -> ClaimEligibility {
    if !job.present {
        return ClaimEligibility::Absent;
    }
    if job.control_op.is_blocking() {
        return ClaimEligibility::Blocked;
    }
    match job.state {
        Some(TaskState::Ready) => match job.available_at_ms {
            Some(at) if at <= now_ms => {
                if job.deliveries < job.max_deliveries {
                    ClaimEligibility::Deliver
                } else {
                    ClaimEligibility::DeadLetter
                }
            }
            Some(_) => ClaimEligibility::NotDue,
            None => ClaimEligibility::Inconsistent("ready without available_at_ms"),
        },
        Some(TaskState::Inflight) => match job.lease_deadline_ms {
            Some(deadline) if deadline > now_ms => ClaimEligibility::LiveLease,
            Some(_) => {
                if job.deliveries < job.max_deliveries {
                    ClaimEligibility::Deliver
                } else {
                    ClaimEligibility::DeadLetter
                }
            }
            None => ClaimEligibility::Inconsistent("inflight without lease"),
        },
        Some(TaskState::Done | TaskState::Dead) => ClaimEligibility::NotDue,
        None => ClaimEligibility::Inconsistent("present without state"),
    }
}

pub fn classify_renew(job: &ObservedTask, cycle: CycleId, token: DeliveryToken, now_ms: u64) -> RenewEligibility {
    if !job.present || job.state != Some(TaskState::Inflight) {
        return RenewEligibility::NotHeld;
    }
    if job.cycle_id != Some(cycle) || job.delivery_token != Some(token) {
        return RenewEligibility::NotHeld;
    }
    match job.lease_deadline_ms {
        Some(deadline) if deadline > now_ms => RenewEligibility::Ok,
        Some(_) => RenewEligibility::NotHeld,
        None => RenewEligibility::Inconsistent("inflight without lease"),
    }
}

pub fn classify_finish(job: &ObservedTask, cycle: CycleId, token: DeliveryToken, now_ms: u64) -> FinishEligibility {
    if !job.present || job.state != Some(TaskState::Inflight) {
        return FinishEligibility::Rejected;
    }
    if job.cycle_id != Some(cycle) || job.delivery_token != Some(token) || job.control_op.is_blocking() {
        return FinishEligibility::Rejected;
    }
    match job.lease_deadline_ms {
        Some(deadline) if deadline > now_ms => FinishEligibility::Ok,
        _ => FinishEligibility::Rejected,
    }
}

pub fn classify_requeue(job: &ObservedTask) -> RequeueEligibility {
    if !job.present {
        return RequeueEligibility::NotFound;
    }
    if job.state != Some(TaskState::Dead) || job.control_op != ControlOp::Empty {
        return RequeueEligibility::NotDead;
    }
    if job.unique_key.is_some() {
        if let Some(owner) = job.uniq_owner {
            if job.task_id != Some(owner) {
                return RequeueEligibility::Duplicate;
            }
        }
    }
    RequeueEligibility::Ok
}

pub fn classify_control(job: &ObservedTask) -> ControlEligibility {
    if !job.present {
        return ControlEligibility::NotFound;
    }
    match job.state {
        Some(TaskState::Ready) => ControlEligibility::ReadyDead,
        Some(TaskState::Inflight) => {
            if job.cycle_id.is_none() || job.delivery_token.is_none() {
                ControlEligibility::Inconsistent("inflight control without cycle or token")
            } else {
                ControlEligibility::InflightAccept
            }
        }
        Some(TaskState::Done | TaskState::Dead) => ControlEligibility::AlreadyTerminal,
        None => ControlEligibility::Inconsistent("present without state"),
    }
}

pub fn classify_settle(job: &ObservedTask, now_ms: u64) -> SettleView {
    if !job.present {
        return SettleView::NotFound;
    }
    match job.state {
        Some(TaskState::Done | TaskState::Dead) => SettleView::AlreadyTerminal,
        Some(TaskState::Inflight) => match job.control_op {
            ControlOp::Cancel | ControlOp::Kill => match job.lease_deadline_ms {
                Some(deadline) if deadline > now_ms => SettleView::StillOwned,
                Some(_) => SettleView::Accepted {
                    reason: job.control_op.terminal_reason().expect("blocking control has a reason"),
                },
                None => SettleView::Inconsistent("inflight control without lease_deadline_ms"),
            },
            ControlOp::Empty => SettleView::NotBlocked,
        },
        Some(TaskState::Ready) | None => SettleView::NotBlocked,
    }
}

pub fn plan_enqueue(input: &EnqueueInput<'_>) -> EnqueueDecision {
    if !input.identity.same_value() {
        return EnqueueDecision::Rejected(InputError::Identity);
    }
    if input.max_deliveries < 1 {
        return EnqueueDecision::Rejected(InputError::MaxDeliveries);
    }
    if input.time_limit_ms == 0 {
        return EnqueueDecision::Rejected(InputError::TimeLimit);
    }
    if input.min_backoff_ms == 0 || input.min_backoff_ms > input.max_backoff_ms {
        return EnqueueDecision::Rejected(InputError::Backoff);
    }
    let Ok(keys) = QueueKeys::new(input.queue) else {
        return EnqueueDecision::Rejected(InputError::Queue);
    };
    if let Some(tag) = input.tag {
        if validate_opaque("tag", tag).is_err() {
            return EnqueueDecision::Rejected(InputError::Opaque { field: "tag" });
        }
    }
    if let Some(unique_key) = input.unique_key {
        if validate_opaque("unique_key", unique_key).is_err() {
            return EnqueueDecision::Rejected(InputError::Opaque { field: "unique_key" });
        }
    }
    if !matches!(input.kwargs, Json::Object(_)) {
        return EnqueueDecision::Rejected(InputError::Kwargs);
    }
    let Ok(headers_len) = header_bytes(input.headers) else {
        return EnqueueDecision::Rejected(InputError::Headers);
    };
    let request = enqueue_request(input);
    let proof = enqueue_proof(input, None);
    match gate(OperationKind::Enqueue, &request.canonical(), input.operation_id, input.queue, input.receipt) {
        Gate::Corrupt(error) => return EnqueueDecision::Inconsistent(error),
        Gate::Conflict(error) => return EnqueueDecision::Conflict(error),
        Gate::Same(receipt) => {
            return match receipt.status {
                ReceiptStatus::Enqueued if check_enqueue(&enqueue_proof(input, receipt.ready_score), input.observed).is_ok() => {
                    EnqueueDecision::Replay(Replay { status: ReceiptStatus::Enqueued, proved_without_receipt: false })
                }
                ReceiptStatus::Enqueued => EnqueueDecision::Inconsistent(bad(input.queue, input.operation_id, "enqueue receipt contradicts postconditions")),
                ReceiptStatus::Duplicate if duplicate_holds(input.observed) => {
                    EnqueueDecision::Replay(Replay { status: ReceiptStatus::Duplicate, proved_without_receipt: false })
                }
                ReceiptStatus::Duplicate => EnqueueDecision::Inconsistent(bad(input.queue, input.operation_id, "duplicate receipt contradicts postconditions")),
                _ => EnqueueDecision::Inconsistent(bad(input.queue, input.operation_id, "enqueue receipt has an unexpected status")),
            };
        }
        Gate::Absent => {}
    }
    if check_enqueue(&proof, input.observed).is_ok() {
        return EnqueueDecision::Replay(Replay { status: ReceiptStatus::Enqueued, proved_without_receipt: true });
    }
    if input.observed.is_clean_slate() {
        let kwargs_len = input.kwargs.canonical().len();
        if let Err(error) = input.config.check_payload(kwargs_len, headers_len) {
            let field = match error {
                crate::config::ConfigError::Invalid { name: "ARQ_MAX_HEADERS_BYTES", .. } => "headers",
                _ => "kwargs",
            };
            return EnqueueDecision::Rejected(InputError::Payload { field });
        }
        if input.ready_zcard >= input.config.max_ready {
            return EnqueueDecision::Full(QueueFull {
                queue: input.queue.to_string(),
                max_ready: input.config.max_ready,
                ready_zcard: input.ready_zcard,
            });
        }
        let Some(available) = input.now_ms.checked_add(input.delay_ms) else {
            return EnqueueDecision::Rejected(InputError::Overflow { field: "delay_ms" });
        };
        let plan = match enqueue_plan(input, &keys, &request, available) {
            Ok(plan) => plan,
            Err(error) => return EnqueueDecision::Rejected(error),
        };
        return EnqueueDecision::Execute(plan);
    }
    if !input.observed.present && !input.observed.indexes_contain() {
        if let Some(owner) = input.observed.uniq_owner {
            if owner != input.identity.task_id {
                let error = DuplicateTask {
                    unique_key: input.unique_key.unwrap_or("").to_string(),
                    task_id: input.identity.task_id,
                    owner_task_id: owner,
                };
                let record = match duplicate_plan(input, &keys, &request, owner) {
                    Ok(plan) => plan,
                    Err(rejected) => return EnqueueDecision::Rejected(rejected),
                };
                return EnqueueDecision::Duplicate { error, record };
            }
        }
    }
    EnqueueDecision::Indeterminate(lost(input.operation_id, input.identity.message_id))
}

pub fn plan_claim(input: &ClaimInput<'_>) -> ClaimDecision {
    if input.worker_id.is_empty() {
        return ClaimDecision::Rejected(InputError::WorkerId);
    }
    if input.lease_ms == 0 {
        return ClaimDecision::Rejected(InputError::TimeLimit);
    }
    let Some(task_id) = input.observed.task_id else {
        return if input.observed.is_clean_slate() {
            ClaimDecision::NotDue
        } else if !input.observed.present {
            ClaimDecision::Indeterminate(lost(input.operation_id, input.message_id))
        } else {
            ClaimDecision::Inconsistent(bad(input.queue, input.operation_id, "present task without task_id"))
        };
    };
    let Ok(keys) = QueueKeys::new(input.queue) else {
        return ClaimDecision::Rejected(InputError::Queue);
    };
    let request = Json::object(vec![
        ("task_id", Json::String(task_id.to_string())),
        ("worker_id", Json::String(input.worker_id.to_string())),
    ]);
    match gate(OperationKind::Claim, &request.canonical(), input.operation_id, input.queue, input.receipt) {
        Gate::Corrupt(error) => return ClaimDecision::Inconsistent(error),
        Gate::Conflict(error) => return ClaimDecision::Conflict(error),
        Gate::Same(receipt) => {
            return match receipt.status {
                ReceiptStatus::Claimed => replay_claim(input, receipt),
                ReceiptStatus::Dead => {
                    let proof = ClaimDlqProof {
                        uniq_owned_before: false,
                        tag: input.observed.tag.clone(),
                    };
                    if check_claim_dlq(&proof, input.observed).is_ok() || input.observed.reason == "max_deliveries" && input.observed.state == Some(TaskState::Dead) && input.observed.in_dead {
                        ClaimDecision::Replay(Replay { status: ReceiptStatus::Dead, proved_without_receipt: false })
                    } else {
                        ClaimDecision::Inconsistent(bad(input.queue, input.operation_id, "claim dlq receipt contradicts postconditions"))
                    }
                }
                _ => ClaimDecision::Inconsistent(bad(input.queue, input.operation_id, "claim receipt has an unexpected status")),
            };
        }
        Gate::Absent => {}
    }
    if let Some(cycle_id) = input.observed.cycle_id {
        let proof = ClaimDeliveryProof {
            delivery_token: input.proposed_token,
            deliveries: None,
            cycle_id,
        };
        if check_claim_delivery(&proof, input.observed).is_ok() {
            return ClaimDecision::Replay(Replay { status: ReceiptStatus::Claimed, proved_without_receipt: true });
        }
    }
    match classify_claim(input.observed, input.now_ms) {
        ClaimEligibility::Deliver => match claim_plan(input, &keys, &request, task_id, false) {
            Ok(plan) => ClaimDecision::Execute(plan),
            Err(error) => ClaimDecision::Rejected(error),
        },
        ClaimEligibility::DeadLetter => match claim_plan(input, &keys, &request, task_id, true) {
            Ok(plan) => ClaimDecision::DeadLetter(plan),
            Err(error) => ClaimDecision::Rejected(error),
        },
        ClaimEligibility::Blocked => ClaimDecision::Blocked,
        ClaimEligibility::NotDue | ClaimEligibility::Absent => ClaimDecision::NotDue,
        ClaimEligibility::LiveLease => ClaimDecision::LiveLease,
        ClaimEligibility::Inconsistent(detail) => ClaimDecision::Inconsistent(bad(input.queue, input.operation_id, detail)),
    }
}

pub fn plan_renew(input: &RenewInput<'_>) -> RenewDecision {
    if input.lease_ms == 0 {
        return RenewDecision::Rejected(InputError::TimeLimit);
    }
    let Ok(keys) = QueueKeys::new(input.queue) else {
        return RenewDecision::Rejected(InputError::Queue);
    };
    let request = Json::object(vec![
        ("cycle_id", Json::String(input.cycle_id.to_string())),
        ("delivery_token", Json::String(input.delivery_token.to_string())),
        ("task_id", Json::String(input.task_id.to_string())),
    ]);
    match gate(OperationKind::Renew, &request.canonical(), input.operation_id, input.queue, input.receipt) {
        Gate::Corrupt(error) => return RenewDecision::Inconsistent(error),
        Gate::Conflict(error) => return RenewDecision::Conflict(error),
        Gate::Same(receipt) => {
            let proof = RenewProof {
                delivery_token: input.delivery_token,
                cycle_id: input.cycle_id,
                lease_deadline_ms: receipt.lease_deadline_ms.unwrap_or(0),
                started_at_ms: receipt.started_at_ms.or(input.observed.started_at_ms),
                deliveries: receipt.deliveries.unwrap_or(input.observed.deliveries),
            };
            return if receipt.lease_deadline_ms.is_some() && check_renew(&proof, input.observed).is_ok() {
                RenewDecision::Replay(Replay { status: ReceiptStatus::Renewed, proved_without_receipt: false })
            } else {
                RenewDecision::Inconsistent(bad(input.queue, input.operation_id, "renew receipt contradicts postconditions"))
            };
        }
        Gate::Absent => {}
    }
    match classify_renew(input.observed, input.cycle_id, input.delivery_token, input.now_ms) {
        RenewEligibility::Ok => match renew_plan(input, &keys, &request) {
            Ok(plan) => RenewDecision::Execute(plan),
            Err(error) => RenewDecision::Rejected(error),
        },
        RenewEligibility::NotHeld => RenewDecision::NotHeld { exit_code: OWNERSHIP_LOST_EXIT_CODE },
        RenewEligibility::Inconsistent(detail) => {
            RenewDecision::Inconsistent(bad(input.queue, input.operation_id, detail))
        }
    }
}

pub fn plan_finish(input: &FinishInput<'_>) -> FinishDecision {
    let Ok(keys) = QueueKeys::new(input.queue) else {
        return FinishDecision::RejectedInput(InputError::Queue);
    };
    let applied = applied_finish(input.proposal, input.observed.retries, input.config.max_error_bytes as usize);
    let request = finish_request(input, &applied);
    let proof = finish_proof(input, &applied, None);
    match gate(OperationKind::Finish, &request.canonical(), input.operation_id, input.queue, input.receipt) {
        Gate::Corrupt(error) => return FinishDecision::Inconsistent(error),
        Gate::Conflict(error) => return FinishDecision::Conflict(error),
        Gate::Same(_) => {
            return if check_finish(&proof, input.observed).is_ok() {
                FinishDecision::Replay(Replay { status: ReceiptStatus::Finished, proved_without_receipt: false })
            } else {
                FinishDecision::Inconsistent(bad(input.queue, input.operation_id, "finish receipt contradicts postconditions"))
            };
        }
        Gate::Absent => {}
    }
    if check_finish(&proof, input.observed).is_ok() {
        return FinishDecision::Replay(Replay { status: ReceiptStatus::Finished, proved_without_receipt: true });
    }
    if classify_finish(input.observed, input.cycle_id, input.delivery_token, input.now_ms) == FinishEligibility::Ok {
        return match finish_plan(input, &keys, &request, &applied) {
            Ok(plan) => FinishDecision::Execute(plan),
            Err(error) => FinishDecision::RejectedInput(error),
        };
    }
    if finish_target_reached(&applied.shape, input.observed) {
        FinishDecision::Indeterminate(lost(input.operation_id, input.message_id))
    } else {
        FinishDecision::Rejected
    }
}

pub fn plan_requeue(input: &RequeueInput<'_>) -> RequeueDecision {
    let Ok(keys) = QueueKeys::new(input.queue) else {
        return RequeueDecision::Rejected(InputError::Queue);
    };
    let request = Json::object(vec![
        ("new_cycle_id", Json::String(input.new_cycle_id.to_string())),
        ("task_id", Json::String(input.task_id.to_string())),
    ]);
    let proof = RequeueProof {
        task_id: input.task_id,
        cycle_id: input.receipt.and_then(|receipt| receipt.cycle_id).unwrap_or(input.new_cycle_id),
        ready_score: input.receipt.and_then(|receipt| receipt.ready_score).unwrap_or(input.now_ms),
        has_unique_key: input.observed.unique_key.is_some(),
    };
    match gate(OperationKind::Requeue, &request.canonical(), input.operation_id, input.queue, input.receipt) {
        Gate::Corrupt(error) => return RequeueDecision::Inconsistent(error),
        Gate::Conflict(error) => return RequeueDecision::Conflict(error),
        Gate::Same(receipt) => {
            let proved = RequeueProof {
                cycle_id: receipt.cycle_id.unwrap_or(input.new_cycle_id),
                ready_score: receipt.ready_score.unwrap_or(input.now_ms),
                ..proof
            };
            return if check_requeue(&proved, input.observed).is_ok() {
                RequeueDecision::Replay(Replay { status: receipt.status, proved_without_receipt: false })
            } else {
                RequeueDecision::Inconsistent(bad(input.queue, input.operation_id, "requeue receipt contradicts postconditions"))
            };
        }
        Gate::Absent => {}
    }
    let fresh = RequeueProof {
        cycle_id: input.new_cycle_id,
        ready_score: input.now_ms,
        ..proof
    };
    if check_requeue(&fresh, input.observed).is_ok() {
        return RequeueDecision::Replay(Replay { status: ReceiptStatus::Requeued, proved_without_receipt: true });
    }
    if !input.observed.present && !input.observed.is_clean_slate() {
        return RequeueDecision::Indeterminate(lost(input.operation_id, input.message_id));
    }
    match classify_requeue(input.observed) {
        RequeueEligibility::Ok => match requeue_plan(input, &keys, &request) {
            Ok(plan) => RequeueDecision::Execute(plan),
            Err(error) => RequeueDecision::Rejected(error),
        },
        RequeueEligibility::NotFound => match status_only(input.config, OperationKind::Requeue, input.operation_id, input.queue, input.task_id, None, &request, ReceiptStatus::NotFound, &keys, input.observed) {
            Ok(plan) => RequeueDecision::NotFound(plan),
            Err(error) => RequeueDecision::Rejected(error),
        },
        RequeueEligibility::NotDead => match status_only(input.config, OperationKind::Requeue, input.operation_id, input.queue, input.task_id, input.observed.cycle_id, &request, ReceiptStatus::NotDead, &keys, input.observed) {
            Ok(plan) => RequeueDecision::NotDead(plan),
            Err(error) => RequeueDecision::Rejected(error),
        },
        RequeueEligibility::Duplicate => {
            let owner = input.observed.uniq_owner.unwrap_or(input.task_id);
            RequeueDecision::Duplicate(DuplicateTask {
                unique_key: input.observed.unique_key.clone().unwrap_or_default(),
                task_id: input.task_id,
                owner_task_id: owner,
            })
        }
    }
}

pub fn plan_cancel(input: &ControlInput<'_>) -> ControlDecision {
    plan_control(input, ControlOp::Cancel, OperationKind::Cancel)
}

pub fn plan_kill(input: &ControlInput<'_>) -> ControlDecision {
    plan_control(input, ControlOp::Kill, OperationKind::Kill)
}

pub fn plan_settle(input: &SettleInput<'_>) -> SettleDecision {
    let Ok(keys) = QueueKeys::new(input.queue) else {
        return SettleDecision::Rejected(InputError::Queue);
    };
    let request = Json::object(vec![("task_id", Json::String(input.task_id.to_string()))]);
    match gate(OperationKind::Settle, &request.canonical(), input.operation_id, input.queue, input.receipt) {
        Gate::Corrupt(error) => return SettleDecision::Inconsistent(error),
        Gate::Conflict(error) => return SettleDecision::Conflict(error),
        Gate::Same(receipt) => return replay_settle(input, receipt),
        Gate::Absent => {}
    }
    if settle_success(input.observed) {
        return SettleDecision::Replay(Replay { status: ReceiptStatus::Accepted, proved_without_receipt: true });
    }
    if !input.observed.present && !input.observed.is_clean_slate() {
        return SettleDecision::Indeterminate(lost(input.operation_id, input.message_id));
    }
    match classify_settle(input.observed, input.now_ms) {
        SettleView::StillOwned => SettleDecision::StillOwned,
        SettleView::NotBlocked => SettleDecision::NotBlocked,
        SettleView::Accepted { reason } => match settle_plan(input, &keys, &request, reason) {
            Ok(plan) => SettleDecision::Accepted(plan),
            Err(error) => SettleDecision::Rejected(error),
        },
        SettleView::AlreadyTerminal => match status_only_settle(input, &keys, &request, ReceiptStatus::AlreadyTerminal) {
            Ok(plan) => SettleDecision::AlreadyTerminal(plan),
            Err(error) => SettleDecision::Rejected(error),
        },
        SettleView::NotFound => match status_only_settle(input, &keys, &request, ReceiptStatus::NotFound) {
            Ok(plan) => SettleDecision::NotFound(plan),
            Err(error) => SettleDecision::Rejected(error),
        },
        SettleView::Inconsistent(detail) => SettleDecision::Inconsistent(bad(input.queue, input.operation_id, detail)),
    }
}

fn plan_control(input: &ControlInput<'_>, op: ControlOp, kind: OperationKind) -> ControlDecision {
    let Ok(keys) = QueueKeys::new(input.queue) else {
        return ControlDecision::Rejected(InputError::Queue);
    };
    let request = Json::object(vec![("task_id", Json::String(input.task_id.to_string()))]);
    match gate(kind, &request.canonical(), input.operation_id, input.queue, input.receipt) {
        Gate::Corrupt(error) => return ControlDecision::Inconsistent(error),
        Gate::Conflict(error) => return ControlDecision::Conflict(error),
        Gate::Same(_) => {
            let proof = control_proof(input, op);
            return if check_control(&proof, input.observed).is_ok() || is_terminal_state(input.observed) {
                let status = if input.observed.state == Some(TaskState::Inflight) && input.observed.control_op == op {
                    ReceiptStatus::Accepted
                } else if is_terminal_state(input.observed) && input.observed.reason != op.terminal_reason().unwrap_or("") {
                    ReceiptStatus::AlreadyTerminal
                } else {
                    ReceiptStatus::Accepted
                };
                ControlDecision::Replay(Replay { status, proved_without_receipt: false })
            } else {
                ControlDecision::Inconsistent(bad(input.queue, input.operation_id, "control receipt contradicts postconditions"))
            };
        }
        Gate::Absent => {}
    }
    let proved = control_proof(input, op);
    if check_control(&proved, input.observed).is_ok() {
        return ControlDecision::Replay(Replay { status: ReceiptStatus::Accepted, proved_without_receipt: true });
    }
    if !input.observed.present && !input.observed.is_clean_slate() {
        return ControlDecision::Indeterminate(lost(input.operation_id, input.message_id));
    }
    match classify_control(input.observed) {
        ControlEligibility::ReadyDead => match control_ready_plan(input, &keys, &request, op, kind) {
            Ok(plan) => ControlDecision::Execute(plan),
            Err(error) => ControlDecision::Rejected(error),
        },
        ControlEligibility::InflightAccept => match control_inflight_plan(input, &keys, &request, op, kind) {
            Ok(plan) => ControlDecision::Execute(plan),
            Err(error) => ControlDecision::Rejected(error),
        },
        ControlEligibility::AlreadyTerminal => match status_only(input.config, kind, input.operation_id, input.queue, input.task_id, input.observed.cycle_id, &request, ReceiptStatus::AlreadyTerminal, &keys, input.observed) {
            Ok(plan) => ControlDecision::Execute(plan),
            Err(error) => ControlDecision::Rejected(error),
        },
        ControlEligibility::NotFound => match status_only(input.config, kind, input.operation_id, input.queue, input.task_id, None, &request, ReceiptStatus::NotFound, &keys, input.observed) {
            Ok(plan) => ControlDecision::Execute(plan),
            Err(error) => ControlDecision::Rejected(error),
        },
        ControlEligibility::Inconsistent(detail) => ControlDecision::Inconsistent(bad(input.queue, input.operation_id, detail)),
    }
}

fn replay_claim(input: &ClaimInput<'_>, receipt: &ReceiptRecord) -> ClaimDecision {
    let Some(cycle_id) = receipt.cycle_id.or(input.observed.cycle_id) else {
        return ClaimDecision::Inconsistent(bad(input.queue, input.operation_id, "claim receipt missing cycle"));
    };
    let proof = ClaimDeliveryProof {
        delivery_token: receipt.delivery_token.unwrap_or(input.proposed_token),
        deliveries: receipt.deliveries,
        cycle_id,
    };
    if check_claim_delivery(&proof, input.observed).is_ok() {
        ClaimDecision::Replay(Replay { status: ReceiptStatus::Claimed, proved_without_receipt: false })
    } else {
        ClaimDecision::Inconsistent(bad(input.queue, input.operation_id, "claim receipt contradicts postconditions"))
    }
}

fn replay_settle(input: &SettleInput<'_>, receipt: &ReceiptRecord) -> SettleDecision {
    match receipt.status {
        ReceiptStatus::Accepted if settle_success(input.observed) => {
            SettleDecision::Replay(Replay { status: ReceiptStatus::Accepted, proved_without_receipt: false })
        }
        ReceiptStatus::Accepted => SettleDecision::Inconsistent(bad(input.queue, input.operation_id, "settle receipt contradicts postconditions")),
        ReceiptStatus::AlreadyTerminal if is_terminal_state(input.observed) => {
            SettleDecision::Replay(Replay { status: ReceiptStatus::AlreadyTerminal, proved_without_receipt: false })
        }
        ReceiptStatus::AlreadyTerminal => SettleDecision::Inconsistent(bad(input.queue, input.operation_id, "already_terminal receipt contradicts postconditions")),
        ReceiptStatus::NotFound if !input.observed.present && !input.observed.indexes_contain() => {
            SettleDecision::Replay(Replay { status: ReceiptStatus::NotFound, proved_without_receipt: false })
        }
        ReceiptStatus::NotFound => SettleDecision::Inconsistent(bad(input.queue, input.operation_id, "not_found receipt contradicts postconditions")),
        _ => SettleDecision::Inconsistent(bad(input.queue, input.operation_id, "settle receipt has an unexpected status")),
    }
}

enum Gate<'a> {
    Absent,
    Same(&'a ReceiptRecord),
    Conflict(OperationConflict),
    Corrupt(StorageInconsistency),
}

fn gate<'a>(op: OperationKind, request: &str, operation_id: OperationId, queue: &str, receipt: Option<&'a ReceiptRecord>) -> Gate<'a> {
    let Some(receipt) = receipt else {
        return Gate::Absent;
    };
    if receipt.operation_id != operation_id {
        return Gate::Corrupt(bad(queue, operation_id, "receipt operation_id does not match the key"));
    }
    if receipt.op != op || receipt.request_canonical != request {
        return Gate::Conflict(OperationConflict {
            operation_id,
            existing_op: receipt.op.as_str().to_string(),
            existing_request: receipt.request_canonical.clone(),
            attempted_op: op.as_str().to_string(),
            attempted_request: request.to_string(),
        });
    }
    Gate::Same(receipt)
}

fn lost(operation_id: OperationId, message_id: MessageId) -> IndeterminateOperation {
    IndeterminateOperation { operation_id, message_id }
}

fn bad(queue: &str, operation_id: OperationId, detail: &str) -> StorageInconsistency {
    StorageInconsistency {
        queue: queue.to_string(),
        operation_id: Some(operation_id),
        detail: detail.to_string(),
    }
}

fn enqueue_request(input: &EnqueueInput<'_>) -> Json {
    Json::object(vec![
        ("actor", Json::String(input.actor_name.to_string())),
        ("delay", Json::Number(input.delay_ms.to_string())),
        ("headers", input.headers.clone()),
        ("kwargs", input.kwargs.clone()),
        ("message_id", Json::String(input.identity.message_id.to_string())),
        ("queue", Json::String(input.queue.to_string())),
        ("tag", optional_string(input.tag)),
        ("unique_key", optional_string(input.unique_key)),
    ])
}

fn optional_string(value: Option<&str>) -> Json {
    match value {
        Some(text) => Json::String(text.to_string()),
        None => Json::Null,
    }
}

fn enqueue_proof(input: &EnqueueInput<'_>, available_at_ms: Option<u64>) -> EnqueueProof {
    EnqueueProof {
        task_id: input.identity.task_id,
        message_id: input.identity.message_id,
        cycle_id: input.cycle_id,
        actor_name: input.actor_name.to_string(),
        queue: input.queue.to_string(),
        kwargs_canonical: input.kwargs.canonical(),
        headers_canonical: input.headers.canonical(),
        tag: input.tag.map(str::to_string),
        unique_key: input.unique_key.map(str::to_string),
        available_at_ms,
    }
}

fn enqueue_plan(input: &EnqueueInput<'_>, keys: &QueueKeys, request: &Json, available: u64) -> Result<OperationPlan, InputError> {
    let mut fields = vec![
        ("version", "1".to_string()),
        ("task_id", input.identity.task_id.to_string()),
        ("message_id", input.identity.message_id.to_string()),
        ("cycle_id", input.cycle_id.to_string()),
        ("actor_name", input.actor_name.to_string()),
        ("queue", input.queue.to_string()),
        ("kwargs", input.kwargs.canonical()),
        ("headers", input.headers.canonical()),
        ("state", TaskState::Ready.as_str().to_string()),
        ("created_at_ms", input.now_ms.to_string()),
        ("available_at_ms", available.to_string()),
        ("deliveries", "0".to_string()),
        ("retries", "0".to_string()),
        ("max_deliveries", input.max_deliveries.to_string()),
        ("max_retries", input.max_retries.to_string()),
        ("time_limit_ms", input.time_limit_ms.to_string()),
        ("min_backoff_ms", input.min_backoff_ms.to_string()),
        ("max_backoff_ms", input.max_backoff_ms.to_string()),
        ("result", String::new()),
        ("reason", String::new()),
        ("error", String::new()),
        ("stop_evidence", String::new()),
        ("control_op", String::new()),
        ("history", "[]".to_string()),
    ];
    if let Some(tag) = input.tag {
        fields.push(("tag", tag.to_string()));
    }
    if let Some(unique_key) = input.unique_key {
        fields.push(("unique_key", unique_key.to_string()));
    }
    let mut commands = vec![
        PlannedCommand::Hset { key: keys.job(&input.identity.task_id), fields: owned_fields(fields) },
        PlannedCommand::Zadd { key: keys.ready(), member: input.identity.task_id.to_string(), score: available },
    ];
    if let Some(unique_key) = input.unique_key {
        commands.insert(1, PlannedCommand::Set { key: keys.uniq(unique_key)?, value: input.identity.task_id.to_string() });
    }
    let mut extra = vec![("available_at_ms", Json::Number(available.to_string()))];
    extra.push(("task_id", Json::String(input.identity.task_id.to_string())));
    finish_plan_receipt(
        OperationKind::Enqueue,
        input.operation_id,
        Some(input.cycle_id),
        input.identity.task_id,
        request,
        ReceiptStatus::Enqueued,
        extra,
        None,
        None,
        None,
        Some(0),
        Some(available),
        input.config.receipt_ttl_secs,
        keys,
        input.unique_key,
        commands,
    )
}

fn duplicate_plan(input: &EnqueueInput<'_>, keys: &QueueKeys, request: &Json, owner: TaskId) -> Result<OperationPlan, InputError> {
    let extra = vec![
        ("owner_task_id", Json::String(owner.to_string())),
        ("task_id", Json::String(input.identity.task_id.to_string())),
    ];
    finish_plan_receipt(
        OperationKind::Enqueue,
        input.operation_id,
        Some(input.cycle_id),
        input.identity.task_id,
        request,
        ReceiptStatus::Duplicate,
        extra,
        None,
        None,
        None,
        None,
        None,
        input.config.receipt_ttl_secs,
        keys,
        input.unique_key,
        Vec::new(),
    )
}

fn claim_plan(input: &ClaimInput<'_>, keys: &QueueKeys, request: &Json, task_id: TaskId, dead: bool) -> Result<OperationPlan, InputError> {
    let cycle = input.observed.cycle_id.ok_or(InputError::History)?;
    let unique_key = input.observed.unique_key.as_deref();
    if dead {
        let mut fields = vec![
            ("state", TaskState::Dead.as_str().to_string()),
            ("reason", "max_deliveries".to_string()),
            ("error", String::new()),
            ("finished_at_ms", input.now_ms.to_string()),
            ("delivery_token", String::new()),
            ("worker_id", String::new()),
            ("lease_deadline_ms", String::new()),
            ("result", String::new()),
            ("control_op", String::new()),
        ];
        let _ = &mut fields;
        let mut commands = vec![
            PlannedCommand::Hset { key: keys.job(&task_id), fields: owned_fields(fields) },
            PlannedCommand::Zrem { key: keys.ready(), member: task_id.to_string() },
            PlannedCommand::Zrem { key: keys.inflight(), member: task_id.to_string() },
            PlannedCommand::Zadd { key: keys.dead(), member: task_id.to_string(), score: input.now_ms },
        ];
        if let Some(tag) = input.observed.tag.as_deref() {
            commands.push(PlannedCommand::Zadd { key: keys.dead_tag(tag)?, member: task_id.to_string(), score: input.now_ms });
        }
        if input.observed.uniq_owner == Some(task_id) {
            if let Some(unique_key) = unique_key {
                commands.push(PlannedCommand::Del { key: keys.uniq(unique_key)? });
            }
        }
        return finish_plan_receipt(
            OperationKind::Claim,
            input.operation_id,
            Some(cycle),
            task_id,
            request,
            ReceiptStatus::Dead,
            vec![("reason", Json::String("max_deliveries".to_string()))],
            None,
            None,
            None,
            None,
            None,
            input.config.receipt_ttl_secs,
            keys,
            unique_key,
            commands,
        );
    }
    let deliveries = input.observed.deliveries.checked_add(1).ok_or(InputError::Overflow { field: "deliveries" })?;
    let deadline = input.now_ms.checked_add(input.lease_ms).ok_or(InputError::Overflow { field: "lease_ms" })?;
    let fields = vec![
        ("state", TaskState::Inflight.as_str().to_string()),
        ("delivery_token", input.proposed_token.to_string()),
        ("worker_id", input.worker_id.to_string()),
        ("lease_deadline_ms", deadline.to_string()),
        ("started_at_ms", input.now_ms.to_string()),
        ("deliveries", deliveries.to_string()),
    ];
    let commands = vec![
        PlannedCommand::Hset { key: keys.job(&task_id), fields: owned_fields(fields) },
        PlannedCommand::Zrem { key: keys.ready(), member: task_id.to_string() },
        PlannedCommand::Zadd { key: keys.inflight(), member: task_id.to_string(), score: deadline },
    ];
    finish_plan_receipt(
        OperationKind::Claim,
        input.operation_id,
        Some(cycle),
        task_id,
        request,
        ReceiptStatus::Claimed,
        vec![
            ("deliveries", Json::Number(deliveries.to_string())),
            ("delivery_token", Json::String(input.proposed_token.to_string())),
            ("lease_deadline_ms", Json::Number(deadline.to_string())),
        ],
        Some(input.proposed_token),
        Some(deadline),
        Some(input.now_ms),
        Some(deliveries),
        None,
        input.config.receipt_ttl_secs,
        keys,
        unique_key,
        commands,
    )
}

fn renew_plan(input: &RenewInput<'_>, keys: &QueueKeys, request: &Json) -> Result<OperationPlan, InputError> {
    let deadline = input.now_ms.checked_add(input.lease_ms).ok_or(InputError::Overflow { field: "lease_ms" })?;
    let fields = vec![("lease_deadline_ms", deadline.to_string())];
    let commands = vec![PlannedCommand::Hset { key: keys.job(&input.task_id), fields: owned_fields(fields) }];
    finish_plan_receipt(
        OperationKind::Renew,
        input.operation_id,
        Some(input.cycle_id),
        input.task_id,
        request,
        ReceiptStatus::Renewed,
        vec![
            ("lease_deadline_ms", Json::Number(deadline.to_string())),
            ("started_at_ms", match input.observed.started_at_ms {
                Some(started) => Json::Number(started.to_string()),
                None => Json::Null,
            }),
        ],
        Some(input.delivery_token),
        Some(deadline),
        input.observed.started_at_ms,
        Some(input.observed.deliveries),
        None,
        input.config.receipt_ttl_secs,
        keys,
        None,
        commands,
    )
}

struct AppliedFinish {
    shape: FinishShape,
    error: String,
    delay_ms: u64,
}

fn applied_finish(proposal: &ParsedOutcome, retries_before: u64, max_error_bytes: usize) -> AppliedFinish {
    match proposal {
        ParsedOutcome::Proposal(OutcomeProposal::Ok) => AppliedFinish {
            shape: FinishShape::Ok,
            error: String::new(),
            delay_ms: 0,
        },
        ParsedOutcome::Proposal(OutcomeProposal::Skipped) => AppliedFinish {
            shape: FinishShape::Skipped,
            error: String::new(),
            delay_ms: 0,
        },
        ParsedOutcome::Proposal(OutcomeProposal::Retry { delay_ms }) => AppliedFinish {
            shape: FinishShape::Retry { retries_after: retries_before.saturating_add(1) },
            error: String::new(),
            delay_ms: *delay_ms,
        },
        ParsedOutcome::Proposal(OutcomeProposal::Dead { reason, error }) => AppliedFinish {
            shape: FinishShape::Dead { reason: reason.clone() },
            error: truncate_utf8(error.as_deref().unwrap_or(""), max_error_bytes),
            delay_ms: 0,
        },
        ParsedOutcome::Invalid(fault) => AppliedFinish {
            shape: FinishShape::Dead { reason: fault.reason().to_string() },
            error: String::new(),
            delay_ms: 0,
        },
    }
}

fn finish_request(input: &FinishInput<'_>, applied: &AppliedFinish) -> Json {
    let mut pairs = vec![
        ("cycle_id", Json::String(input.cycle_id.to_string())),
        ("delivery_token", Json::String(input.delivery_token.to_string())),
        ("task_id", Json::String(input.task_id.to_string())),
    ];
    match &applied.shape {
        FinishShape::Ok => pairs.push(("outcome", Json::String("ok".to_string()))),
        FinishShape::Skipped => pairs.push(("outcome", Json::String("skipped".to_string()))),
        FinishShape::Retry { .. } => {
            pairs.push(("delay_ms", Json::Number(applied.delay_ms.to_string())));
            pairs.push(("outcome", Json::String("retry".to_string())));
        }
        FinishShape::Dead { reason } => {
            pairs.push(("outcome", Json::String("dead".to_string())));
            pairs.push(("reason", Json::String(reason.clone())));
        }
    }
    Json::Object(pairs.into_iter().map(|(key, value)| (key.to_string(), value)).collect())
}

fn finish_proof(input: &FinishInput<'_>, applied: &AppliedFinish, ready_score: Option<u64>) -> FinishProof {
    let ready_score = ready_score.unwrap_or_else(|| match &applied.shape {
        FinishShape::Retry { .. } => input.now_ms.saturating_add(applied.delay_ms),
        _ => input.now_ms,
    });
    FinishProof {
        task_id: input.task_id,
        cycle_id: input.cycle_id,
        shape: applied.shape.clone(),
        uniq_owned_before: input.observed.uniq_owner == Some(input.task_id),
        ready_score,
        finished_at_ms: input.now_ms,
        tag: input.observed.tag.clone(),
        error: applied.error.clone(),
    }
}

fn finish_target_reached(shape: &FinishShape, observed: &ObservedTask) -> bool {
    match shape {
        FinishShape::Ok | FinishShape::Skipped => observed.state == Some(TaskState::Done),
        FinishShape::Retry { .. } => observed.state == Some(TaskState::Ready) && observed.delivery_token.is_none(),
        FinishShape::Dead { reason } => observed.state == Some(TaskState::Dead) && observed.reason == *reason,
    }
}

fn finish_plan(input: &FinishInput<'_>, keys: &QueueKeys, request: &Json, applied: &AppliedFinish) -> Result<OperationPlan, InputError> {
    let task_id = input.task_id;
    let unique_key = input.observed.unique_key.as_deref();
    let owned = input.observed.uniq_owner == Some(task_id);
    let (commands, status_extra, ready_score) = match &applied.shape {
        FinishShape::Ok | FinishShape::Skipped => {
            let result = if matches!(applied.shape, FinishShape::Ok) { "ok" } else { "skipped" };
            let fields = vec![
                ("state", TaskState::Done.as_str().to_string()),
                ("result", result.to_string()),
                ("finished_at_ms", input.now_ms.to_string()),
                ("delivery_token", String::new()),
                ("worker_id", String::new()),
                ("lease_deadline_ms", String::new()),
                ("started_at_ms", String::new()),
            ];
            let mut commands = vec![
                PlannedCommand::Hset { key: keys.job(&task_id), fields: owned_fields(fields) },
                PlannedCommand::Zrem { key: keys.inflight(), member: task_id.to_string() },
                PlannedCommand::Zadd { key: keys.done(), member: task_id.to_string(), score: input.now_ms },
            ];
            if owned {
                if let Some(unique_key) = unique_key {
                    commands.push(PlannedCommand::Del { key: keys.uniq(unique_key)? });
                }
            }
            (commands, vec![("result", Json::String(result.to_string()))], None)
        }
        FinishShape::Retry { retries_after } => {
            let delay = applied.delay_ms;
            let score = input.now_ms.checked_add(delay).ok_or(InputError::Overflow { field: "delay_ms" })?;
            let fields = vec![
                ("state", TaskState::Ready.as_str().to_string()),
                ("retries", retries_after.to_string()),
                ("delivery_token", String::new()),
                ("worker_id", String::new()),
                ("lease_deadline_ms", String::new()),
                ("started_at_ms", String::new()),
                ("finished_at_ms", String::new()),
                ("available_at_ms", score.to_string()),
            ];
            let commands = vec![
                PlannedCommand::Hset { key: keys.job(&task_id), fields: owned_fields(fields) },
                PlannedCommand::Zrem { key: keys.inflight(), member: task_id.to_string() },
                PlannedCommand::Zadd { key: keys.ready(), member: task_id.to_string(), score },
            ];
            (commands, vec![("delay_ms", Json::Number(delay.to_string()))], Some(score))
        }
        FinishShape::Dead { reason } => {
            let mut commands = vec![
                PlannedCommand::Hset {
                    key: keys.job(&task_id),
                    fields: owned_fields(vec![
                        ("state", TaskState::Dead.as_str().to_string()),
                        ("reason", reason.clone()),
                        ("error", applied.error.clone()),
                        ("finished_at_ms", input.now_ms.to_string()),
                        ("result", String::new()),
                        ("delivery_token", String::new()),
                        ("worker_id", String::new()),
                        ("lease_deadline_ms", String::new()),
                        ("started_at_ms", String::new()),
                    ]),
                },
                PlannedCommand::Zrem { key: keys.inflight(), member: task_id.to_string() },
                PlannedCommand::Zrem { key: keys.done(), member: task_id.to_string() },
                PlannedCommand::Zadd { key: keys.dead(), member: task_id.to_string(), score: input.now_ms },
            ];
            if let Some(tag) = input.observed.tag.as_deref() {
                commands.push(PlannedCommand::Zadd { key: keys.dead_tag(tag)?, member: task_id.to_string(), score: input.now_ms });
            }
            if owned {
                if let Some(unique_key) = unique_key {
                    commands.push(PlannedCommand::Del { key: keys.uniq(unique_key)? });
                }
            }
            (commands, vec![("reason", Json::String(reason.clone()))], None)
        }
    };
    finish_plan_receipt(
        OperationKind::Finish,
        input.operation_id,
        Some(input.cycle_id),
        task_id,
        request,
        ReceiptStatus::Finished,
        status_extra,
        Some(input.delivery_token),
        None,
        None,
        None,
        ready_score,
        input.config.receipt_ttl_secs,
        keys,
        unique_key,
        commands,
    )
}

fn requeue_plan(input: &RequeueInput<'_>, keys: &QueueKeys, request: &Json) -> Result<OperationPlan, InputError> {
    let entry = history_entry(input.observed)?;
    let history = append_history(input.history, entry)?;
    let task_id = input.task_id;
    let fields = vec![
        ("cycle_id", input.new_cycle_id.to_string()),
        ("state", TaskState::Ready.as_str().to_string()),
        ("retries", "0".to_string()),
        ("deliveries", "0".to_string()),
        ("delivery_token", String::new()),
        ("worker_id", String::new()),
        ("lease_deadline_ms", String::new()),
        ("started_at_ms", String::new()),
        ("finished_at_ms", String::new()),
        ("reason", String::new()),
        ("error", String::new()),
        ("result", String::new()),
        ("stop_evidence", String::new()),
        ("control_op", String::new()),
        ("control_operation_id", String::new()),
        ("control_cycle_id", String::new()),
        ("control_delivery_token", String::new()),
        ("control_accepted_at_ms", String::new()),
        ("available_at_ms", input.now_ms.to_string()),
        ("history", history.canonical()),
    ];
    let mut commands = vec![
        PlannedCommand::Hset { key: keys.job(&task_id), fields: owned_fields(fields) },
        PlannedCommand::Zrem { key: keys.dead(), member: task_id.to_string() },
        PlannedCommand::Zadd { key: keys.ready(), member: task_id.to_string(), score: input.now_ms },
    ];
    if let Some(tag) = input.observed.tag.as_deref() {
        commands.push(PlannedCommand::Zrem { key: keys.dead_tag(tag)?, member: task_id.to_string() });
    }
    let unique_key = input.observed.unique_key.as_deref();
    if let Some(unique_key) = unique_key {
        commands.push(PlannedCommand::Set { key: keys.uniq(unique_key)?, value: task_id.to_string() });
    }
    finish_plan_receipt(
        OperationKind::Requeue,
        input.operation_id,
        Some(input.new_cycle_id),
        task_id,
        request,
        ReceiptStatus::Requeued,
        vec![("cycle_id", Json::String(input.new_cycle_id.to_string()))],
        None,
        None,
        None,
        Some(0),
        Some(input.now_ms),
        input.config.receipt_ttl_secs,
        keys,
        unique_key,
        commands,
    )
}

fn control_proof(input: &ControlInput<'_>, op: ControlOp) -> ControlProof {
    let shape = match (input.observed.state, input.observed.cycle_id, input.observed.delivery_token) {
        (Some(TaskState::Inflight), Some(cycle_id), Some(token)) => ControlShape::Inflight { cycle_id, token, op },
        _ => ControlShape::Terminal {
            reason: op.terminal_reason().unwrap_or("").to_string(),
            uniq_owned_before: input.observed.uniq_owner == Some(input.task_id),
        },
    };
    ControlProof { task_id: input.task_id, shape }
}

fn control_ready_plan(input: &ControlInput<'_>, keys: &QueueKeys, request: &Json, op: ControlOp, kind: OperationKind) -> Result<OperationPlan, InputError> {
    let reason = op.terminal_reason().unwrap_or("").to_string();
    let task_id = input.task_id;
    let mut commands = vec![
        PlannedCommand::Hset {
            key: keys.job(&task_id),
            fields: owned_fields(vec![
                ("state", TaskState::Dead.as_str().to_string()),
                ("reason", reason.clone()),
                ("finished_at_ms", input.now_ms.to_string()),
                ("control_op", String::new()),
                ("result", String::new()),
                ("delivery_token", String::new()),
                ("worker_id", String::new()),
                ("lease_deadline_ms", String::new()),
            ]),
        },
        PlannedCommand::Zrem { key: keys.ready(), member: task_id.to_string() },
        PlannedCommand::Zrem { key: keys.done(), member: task_id.to_string() },
        PlannedCommand::Zadd { key: keys.dead(), member: task_id.to_string(), score: input.now_ms },
    ];
    if let Some(tag) = input.observed.tag.as_deref() {
        commands.push(PlannedCommand::Zadd { key: keys.dead_tag(tag)?, member: task_id.to_string(), score: input.now_ms });
    }
    let unique_key = input.observed.unique_key.as_deref();
    if input.observed.uniq_owner == Some(task_id) {
        if let Some(unique_key) = unique_key {
            commands.push(PlannedCommand::Del { key: keys.uniq(unique_key)? });
        }
    }
    finish_plan_receipt(kind, input.operation_id, input.observed.cycle_id, task_id, request, ReceiptStatus::Accepted, vec![("reason", Json::String(reason))], None, None, None, None, None, input.config.receipt_ttl_secs, keys, None, commands)
}

fn control_inflight_plan(input: &ControlInput<'_>, keys: &QueueKeys, request: &Json, op: ControlOp, kind: OperationKind) -> Result<OperationPlan, InputError> {
    let cycle = input.observed.cycle_id.ok_or(InputError::History)?;
    let token = input.observed.delivery_token.ok_or(InputError::History)?;
    let fields = vec![
        ("control_op", op.as_str().to_string()),
        ("control_operation_id", input.operation_id.to_string()),
        ("control_cycle_id", cycle.to_string()),
        ("control_delivery_token", token.to_string()),
        ("control_accepted_at_ms", input.now_ms.to_string()),
    ];
    let commands = vec![PlannedCommand::Hset { key: keys.job(&input.task_id), fields: owned_fields(fields) }];
    finish_plan_receipt(kind, input.operation_id, Some(cycle), input.task_id, request, ReceiptStatus::Accepted, vec![("control_op", Json::String(op.as_str().to_string()))], Some(token), None, None, None, None, input.config.receipt_ttl_secs, keys, None, commands)
}

fn settle_plan(input: &SettleInput<'_>, keys: &QueueKeys, request: &Json, reason: &str) -> Result<OperationPlan, InputError> {
    let task_id = input.task_id;
    let mut commands = vec![
        PlannedCommand::Hset {
            key: keys.job(&task_id),
            fields: owned_fields(vec![
                ("state", TaskState::Dead.as_str().to_string()),
                ("reason", reason.to_string()),
                ("stop_evidence", StopEvidence::LeaseExpired.as_str().to_string()),
                ("control_op", String::new()),
                ("finished_at_ms", input.now_ms.to_string()),
                ("result", String::new()),
            ]),
        },
        PlannedCommand::Zrem { key: keys.inflight(), member: task_id.to_string() },
        PlannedCommand::Zadd { key: keys.dead(), member: task_id.to_string(), score: input.now_ms },
    ];
    if let Some(tag) = input.observed.tag.as_deref() {
        commands.push(PlannedCommand::Zadd { key: keys.dead_tag(tag)?, member: task_id.to_string(), score: input.now_ms });
    }
    let unique_key = input.observed.unique_key.as_deref();
    if input.observed.uniq_owner == Some(task_id) {
        if let Some(unique_key) = unique_key {
            commands.push(PlannedCommand::Del { key: keys.uniq(unique_key)? });
        }
    }
    finish_plan_receipt(
        OperationKind::Settle,
        input.operation_id,
        input.observed.cycle_id,
        task_id,
        request,
        ReceiptStatus::Accepted,
        vec![
            ("reason", Json::String(reason.to_string())),
            ("stop_evidence", Json::String(StopEvidence::LeaseExpired.as_str().to_string())),
        ],
        None,
        None,
        None,
        None,
        None,
        input.config.receipt_ttl_secs,
        keys,
        None,
        commands,
    )
}

fn status_only(config: &Config, kind: OperationKind, operation_id: OperationId, queue: &str, task_id: TaskId, cycle_id: Option<CycleId>, request: &Json, status: ReceiptStatus, keys: &QueueKeys, _observed: &ObservedTask) -> Result<OperationPlan, InputError> {
    let _ = queue;
    finish_plan_receipt(kind, operation_id, cycle_id, task_id, request, status, Vec::new(), None, None, None, None, None, config.receipt_ttl_secs, keys, None, Vec::new())
}

fn status_only_settle(input: &SettleInput<'_>, keys: &QueueKeys, request: &Json, status: ReceiptStatus) -> Result<OperationPlan, InputError> {
    status_only(input.config, OperationKind::Settle, input.operation_id, input.queue, input.task_id, input.observed.cycle_id, request, status, keys, input.observed)
}

fn history_entry(job: &ObservedTask) -> Result<Json, InputError> {
    let cycle = job.cycle_id.ok_or(InputError::History)?;
    let state = job.state.ok_or(InputError::History)?;
    let finished = job.finished_at_ms.ok_or(InputError::History)?;
    Ok(Json::object(vec![
        ("control_op", Json::String(job.control_op.as_str().to_string())),
        ("cycle_id", Json::String(cycle.to_string())),
        ("deliveries", Json::Number(job.deliveries.to_string())),
        ("error", Json::String(job.error.clone())),
        ("finished_at_ms", Json::Number(finished.to_string())),
        ("reason", Json::String(job.reason.clone())),
        ("retries", Json::Number(job.retries.to_string())),
        ("state", Json::String(state.as_str().to_string())),
        ("stop_evidence", Json::String(job.stop_evidence.as_str().to_string())),
    ]))
}

fn append_history(prior: &Json, entry: Json) -> Result<Json, InputError> {
    let Some(items) = prior.as_array() else {
        return Err(InputError::History);
    };
    let mut items = items.to_vec();
    items.push(entry);
    Ok(Json::Array(items))
}

fn owned_fields(fields: Vec<(&str, String)>) -> Vec<(String, String)> {
    fields.into_iter().map(|(key, value)| (key.to_string(), value)).collect()
}

fn finish_plan_receipt(
    kind: OperationKind,
    operation_id: OperationId,
    cycle_id: Option<CycleId>,
    task_id: TaskId,
    request: &Json,
    status: ReceiptStatus,
    extra: Vec<(&str, Json)>,
    delivery_token: Option<DeliveryToken>,
    lease_deadline_ms: Option<u64>,
    started_at_ms: Option<u64>,
    deliveries: Option<u64>,
    ready_score: Option<u64>,
    ttl_secs: u64,
    keys: &QueueKeys,
    unique_key: Option<&str>,
    mut commands: Vec<PlannedCommand>,
) -> Result<OperationPlan, InputError> {
    let mut pairs = vec![("status".to_string(), Json::String(status.as_str().to_string()))];
    for (key, value) in extra {
        pairs.push((key.to_string(), value));
    }
    let result = Json::Object(pairs);
    let body = Json::object(vec![
        ("cycle_id", match &cycle_id {
            Some(id) => Json::String(id.to_string()),
            None => Json::Null,
        }),
        ("op", Json::String(kind.as_str().to_string())),
        ("operation_id", Json::String(operation_id.to_string())),
        ("request", request.clone()),
        ("result", result.clone()),
        ("task_id", Json::String(task_id.to_string())),
    ]);
    let record = ReceiptRecord {
        operation_id,
        op: kind,
        cycle_id,
        task_id,
        request_canonical: request.canonical(),
        result_canonical: result.canonical(),
        status,
        delivery_token,
        lease_deadline_ms,
        started_at_ms,
        deliveries,
        ready_score,
    };
    commands.push(PlannedCommand::SetEx {
        key: keys.receipt(&operation_id),
        value: body.canonical(),
        ttl_secs,
    });
    let watch = WatchSet {
        receipt: keys.receipt(&operation_id),
        job: keys.job(&task_id),
        uniq: match unique_key {
            Some(unique_key) if watches_uniq(kind, true) => Some(keys.uniq(unique_key)?),
            _ => None,
        },
    };
    Ok(OperationPlan { kind, operation_id, watch, commands, receipt: record })
}
