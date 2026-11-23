//! Sans-IO core of arq-rs.
//!
//! Identifiers, `ARQ_*` configuration, outcome parsing, lifecycle state machines,
//! operation plans, and postcondition checks. There is no network, no Tokio, no
//! PyO3, and no filesystem. Callers pass time and randomness in.

#![forbid(unsafe_code)]

pub mod admission;
pub mod attempt;
pub mod config;
pub mod error;
pub mod ids;
pub mod json;
pub mod keys;
pub mod outcome;
pub mod ping;
pub mod plan;
pub mod postcondition;
pub mod protocol;
pub mod slot;
pub mod state;
pub mod text;

pub use admission::{AdmissionEffect, AdmissionEvent, AdmissionGate, ClaimAdmission, DrainPhase};
pub use attempt::{
    AttemptEvent, AttemptLifecycle, AttemptTransitionError, ExecutionState, FinishState, OwnershipState,
};
pub use config::{Config, ConfigError, CONFIG_EXIT_CODE};
pub use error::{
    DuplicateTask, IndeterminateOperation, InputError, OperationConflict, QueueFull, StorageInconsistency,
};
pub use ids::{
    CanonicalUuid, CycleId, DeliveryToken, FillBytes, InvalidIdentifier, MessageId, MessageIdentity, OperationId,
    SliceBytes, TaskId,
};
pub use json::{Json, JsonError};
pub use keys::{is_shared_index, key_kind, validate_opaque, validate_queue, QueueKeys};
pub use outcome::{dispatch_exception, parse_outcome, OutcomeFault, OutcomeProposal, ParsedOutcome};
pub use ping::{LoopEvent, PingDecision, PingWatch, PING_BUDGET_EXIT_CODE};
pub use plan::{
    classify_claim, classify_control, classify_finish, classify_renew, classify_requeue, classify_settle, header_bytes,
    plan_cancel, plan_claim, plan_enqueue, plan_finish, plan_kill, plan_renew, plan_requeue, plan_settle,
    watches_uniq, ClaimDecision, ClaimEligibility, ClaimInput, ControlDecision, ControlEligibility, ControlInput,
    EnqueueDecision, EnqueueInput, FinishDecision, FinishEligibility, FinishInput, OperationPlan, PlannedCommand,
    RenewDecision, RenewEligibility, RenewInput, Replay, RequeueDecision, RequeueEligibility, RequeueInput,
    SettleDecision, SettleInput, SettleView, WatchSet,
};
pub use postcondition::{
    check_claim_delivery, check_claim_dlq, check_control, check_enqueue, check_finish, check_renew, check_requeue,
    check_settle, duplicate_holds, is_terminal_state, settle_success, ClaimDeliveryProof, ClaimDlqProof, ControlProof,
    ControlShape, EnqueueProof,
    FinishProof, FinishShape, PostconditionMismatch, RenewProof, RequeueProof, SettleProof,
};
pub use protocol::{
    classify_exec, soft_ready_ceiling, watch_limit_for_claim_candidate, ExecClass, ExecReply, TransactionConflict,
    WatchBudget, CLAIM_CANDIDATE_WINDOW, OWNERSHIP_LOST_EXIT_CODE,
};
pub use slot::{ProcessEvent, ProcessPhase, ProcessTransitionError, SlotLifecycle, SlotOwner};
pub use state::{
    ControlOp, ObservedTask, OperationKind, ReceiptRecord, ReceiptStatus, StopEvidence, TaskResult, TaskState,
};
pub use text::truncate_utf8;
