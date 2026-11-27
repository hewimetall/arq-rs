//! Concrete Redis adapter.
//!
//! `RedisBackend` executes the plans from `arq-core`. It is a struct, not a trait.
//! There is no `QueuePort` and no storage trait.

#![forbid(unsafe_code)]

mod backend;
mod error;
mod observe;
mod resp;
mod session;
mod types;

#[cfg(test)]
mod scripted;

pub use backend::{halts_the_queue, QueueAdmin, RedisBackend};
pub use error::BackendError;
pub use types::{
    ChildRegistration, ChildView, ClaimRequest, ClaimResult, ControlRequest, ControlResult, Delivery, EnqueueJob,
    EnqueueResult, ExecFault, Fault, FinishRequest, FinishResult, MaintenanceReport, PeekState, QueueDepth,
    RenewRequest, RenewResult, RequeueRequest, RequeueResult, Snapshot, StoredReceipt, SupervisorRegistration,
    SupervisorView, TaskInfo, TaskPage,
};
