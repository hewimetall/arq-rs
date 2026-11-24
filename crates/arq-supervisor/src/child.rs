//! Child delivery loop without CPython.
//!
//! `AdmissionGate` closes on drain and on parent exit. `PingWatch` decides exit 2.
//! A scripted feed is used when the test must force a `ClaimResult` the clock cannot.
//! An empty script does not call Redis. Exit 2 does not finish and does not call `on_stop`.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arq_core::{
    parse_outcome, validate_opaque, validate_queue, AdmissionGate, CanonicalUuid, ClaimAdmission, CycleId,
    DeliveryToken, Json, LoopEvent, MessageId, OperationId, ParsedOutcome, PingWatch, TaskId, CONFIG_EXIT_CODE,
    PING_BUDGET_EXIT_CODE,
};
use arq_redis::{
    BackendError, ClaimRequest, ClaimResult, Delivery, FinishRequest, FinishResult, RedisBackend, RenewRequest,
    RenewResult,
};

use crate::pidfd::ParentWatch;
use crate::policy::{exit_for_storage, max_tasks_reached};

#[derive(Debug, Clone)]
pub struct ChildParams {
    pub queue: String,
    pub worker_id: String,
    pub lease_ms: u64,
    pub renew_every: Duration,
    pub tick: Duration,
    pub shutdown_timeout: Duration,
    pub ping_budget: Duration,
    pub max_tasks: u64,
    pub watch_retries: u64,
}

impl ChildParams {
    pub fn test_fast() -> Self {
        Self {
            queue: "default".into(),
            worker_id: "worker-1".into(),
            lease_ms: 15_000,
            renew_every: Duration::from_millis(5),
            tick: Duration::from_millis(5),
            shutdown_timeout: Duration::from_millis(40),
            ping_budget: Duration::from_secs(30),
            max_tasks: 10,
            watch_retries: 1,
        }
    }
}

#[derive(Clone)]
pub enum DispatchMode {
    Immediate(ParsedOutcome),
    UntilDrain(ParsedOutcome),
    Hang,
    Blocked,
    Misses(u32),
}

impl DispatchMode {
    pub fn ok_now() -> Self {
        Self::Immediate(ok_outcome())
    }
}

#[derive(Default)]
pub struct Scripts {
    pub claims: VecDeque<Result<ClaimResult, BackendError>>,
    pub renews: VecDeque<Result<RenewResult, BackendError>>,
    pub finishes: VecDeque<Result<FinishResult, BackendError>>,
}

pub enum Feed {
    Script(Scripts),
    Redis(RedisBackend),
}

#[derive(Clone, Default)]
pub struct SessionControl {
    drain: Arc<AtomicBool>,
}

impl SessionControl {
    pub fn request_drain(&self) {
        self.drain.store(true, Ordering::SeqCst);
    }

    pub fn drain_requested(&self) -> bool {
        self.drain.load(Ordering::SeqCst)
    }
}

#[derive(Default)]
pub struct Live {
    pub claims: AtomicU64,
    pub renews: AtomicU64,
    pub finished: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildReport {
    pub exit_code: i32,
    pub finished: u64,
    pub on_stop: u64,
    pub claims: u64,
    pub renews: u64,
    pub rejected: u64,
    pub dead_letters: u64,
    pub timed_out: bool,
    pub tasks_done: u64,
    pub drained: bool,
    pub accepted_after_close: u64,
    pub peak_inflight: u64,
}

pub struct ChildSession {
    pub params: ChildParams,
    pub feed: Feed,
    pub watch: ParentWatch,
    pub control: SessionControl,
    pub dispatch: DispatchMode,
    pub live: Arc<Live>,
    pub on_stop: Arc<dyn Fn() + Send + Sync>,
    pub close_before_note: bool,
    pub max_ticks: Option<u64>,
}

impl ChildSession {
    pub fn scripted(params: ChildParams, scripts: Scripts, dispatch: DispatchMode) -> Self {
        Self {
            params,
            feed: Feed::Script(scripts),
            watch: ParentWatch::detached(),
            control: SessionControl::default(),
            dispatch,
            live: Arc::new(Live::default()),
            on_stop: Arc::new(|| {}),
            close_before_note: false,
            max_ticks: None,
        }
    }
}

/// Parent exit and `SIGUSR1` share this gate. A live parent is not a drain.
pub fn parent_or_signal_drain(parent_exited: bool, signal: bool, gate: &mut AdmissionGate, draining: &mut bool) -> bool {
    if !(parent_exited || signal) {
        return false;
    }
    if !*draining {
        gate.close();
        *draining = true;
    }
    true
}

pub fn ok_outcome() -> ParsedOutcome {
    parse_outcome(&Json::parse(r#"{"outcome":"ok"}"#).expect("outcome"))
}

pub async fn run_child_session(session: ChildSession) -> ChildReport {
    let ChildSession { params, mut feed, watch, control, dispatch, live, on_stop, close_before_note, max_ticks } =
        session;
    let mut report = ChildReport {
        exit_code: 0,
        finished: 0,
        on_stop: 0,
        claims: 0,
        renews: 0,
        rejected: 0,
        dead_letters: 0,
        timed_out: false,
        tasks_done: 0,
        drained: false,
        accepted_after_close: 0,
        peak_inflight: 0,
    };
    if watch.failed().is_some() || params.max_tasks == 0 || params.worker_id.is_empty() {
        report.exit_code = CONFIG_EXIT_CODE;
        return report;
    }
    if validate_queue(&params.queue).is_err() || validate_opaque("worker_id", &params.worker_id).is_err() {
        report.exit_code = CONFIG_EXIT_CODE;
        return report;
    }
    let mut gate = AdmissionGate::default();
    let mut ping = PingWatch::new(params.ping_budget);
    let mut draining = false;
    let mut drain_started: Option<Instant> = None;
    let mut held: Option<Held> = None;
    let mut inflight = 0u64;
    let mut misses = 0u32;
    let mut transport_used = 0u64;
    let mut ticks = 0u64;
    let mut interval = tokio::time::interval(params.tick.max(Duration::from_millis(1)));
    loop {
        interval.tick().await;
        ticks = ticks.saturating_add(1);
        let parent_exited = watch.exited();
        let signal = control.drain_requested();
        if parent_or_signal_drain(parent_exited, signal, &mut gate, &mut draining) && drain_started.is_none() {
            drain_started = Some(Instant::now());
            report.drained = true;
        }
        if let Some(started) = drain_started {
            if held.is_some() && started.elapsed() >= params.shutdown_timeout {
                report.timed_out = true;
                report.exit_code = 0;
                report.rejected = gate.rejected();
                report.accepted_after_close = gate.accepted_after_close();
                return report;
            }
        }
        if let Some(current) = held.as_mut() {
            let event = match &dispatch {
                DispatchMode::Blocked => LoopEvent::Missed,
                DispatchMode::Misses(limit) if misses < *limit => {
                    misses = misses.saturating_add(1);
                    LoopEvent::Missed
                }
                DispatchMode::Hang | DispatchMode::Immediate(_) | DispatchMode::UntilDrain(_) | DispatchMode::Misses(_) => {
                    LoopEvent::Responded
                }
            };
            let decision = ping.observe(event, Instant::now());
            if decision.exit_code == Some(PING_BUDGET_EXIT_CODE) {
                report.exit_code = PING_BUDGET_EXIT_CODE;
                report.rejected = gate.rejected();
                return report;
            }
            if decision.renew_continues() && Instant::now() >= current.renew_at {
                current.renew_at = Instant::now() + params.renew_every;
                match renew_held(&mut feed, &params, current).await {
                    Ok(RenewResult::Renewed { .. }) => {
                        report.renews = report.renews.saturating_add(1);
                        live.renews.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(RenewResult::NotHeld { .. }) => {
                        return stop_code(&mut report, &gate, &on_stop, 4);
                    }
                    Err(error) => {
                        if let Some(code) = exit_for_storage(&error, &mut transport_used, params.watch_retries) {
                            report.exit_code = code;
                            return report;
                        }
                    }
                }
            }
            if handler_done(&dispatch, draining, misses) {
                match finish_held(&mut feed, &params, current).await {
                    Ok(FinishResult::Finished { .. }) => {
                        report.finished = report.finished.saturating_add(1);
                        report.tasks_done = report.tasks_done.saturating_add(1);
                        live.finished.fetch_add(1, Ordering::SeqCst);
                        inflight = inflight.saturating_sub(1);
                        held = None;
                        if max_tasks_reached(report.tasks_done, params.max_tasks) || draining {
                            return graceful(&mut report, &mut gate, &on_stop);
                        }
                    }
                    Ok(FinishResult::Rejected) => {
                        return stop_code(&mut report, &gate, &on_stop, 4);
                    }
                    Err(error) => {
                        if let Some(code) = exit_for_storage(&error, &mut transport_used, params.watch_retries) {
                            report.exit_code = code;
                            return report;
                        }
                    }
                }
            }
        }
        if held.is_none() {
            if gate.blocks_claim() {
                let _ = gate.reject_late_claim();
                return graceful(&mut report, &mut gate, &on_stop);
            }
            match claim_next(&mut feed, &params).await {
                Ok(ClaimResult::Idle) => {}
                Ok(ClaimResult::DeadLetter { .. }) => {
                    report.dead_letters = report.dead_letters.saturating_add(1);
                    report.claims = report.claims.saturating_add(1);
                    live.claims.fetch_add(1, Ordering::SeqCst);
                }
                Ok(ClaimResult::OwnershipLost { .. }) => {
                    report.claims = report.claims.saturating_add(1);
                    live.claims.fetch_add(1, Ordering::SeqCst);
                    return stop_code(&mut report, &gate, &on_stop, 4);
                }
                Ok(ClaimResult::Delivered(delivery)) => {
                    if close_before_note && !gate.is_closed() {
                        gate.close();
                    }
                    let admission = gate.note_claim_resolved();
                    if matches!(admission, ClaimAdmission::AcceptInflightAfterClose) {
                        gate.mark_inflight_settled();
                    }
                    match admission {
                        ClaimAdmission::Reject => {
                            report.claims = report.claims.saturating_add(1);
                        }
                        ClaimAdmission::Accept | ClaimAdmission::AcceptInflightAfterClose => {
                            report.claims = report.claims.saturating_add(1);
                            live.claims.fetch_add(1, Ordering::SeqCst);
                            inflight = inflight.saturating_add(1);
                            report.peak_inflight = report.peak_inflight.max(inflight);
                            match Held::from_delivery(&delivery, params.renew_every) {
                                Some(next) => held = Some(next),
                                None => {
                                    report.exit_code = CONFIG_EXIT_CODE;
                                    return report;
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    if let Some(code) = exit_for_storage(&error, &mut transport_used, params.watch_retries) {
                        report.exit_code = code;
                        return report;
                    }
                }
            }
        }
        if let Some(limit) = max_ticks {
            if ticks >= limit && held.is_none() {
                report.rejected = gate.rejected();
                report.accepted_after_close = gate.accepted_after_close();
                report.exit_code = 0;
                return report;
            }
        }
    }
}

fn handler_done(dispatch: &DispatchMode, draining: bool, misses: u32) -> bool {
    match dispatch {
        DispatchMode::Immediate(_) => true,
        DispatchMode::UntilDrain(_) => draining,
        DispatchMode::Hang | DispatchMode::Blocked => false,
        DispatchMode::Misses(limit) => misses >= *limit,
    }
}

fn graceful(report: &mut ChildReport, gate: &mut AdmissionGate, on_stop: &Arc<dyn Fn() + Send + Sync>) -> ChildReport {
    let _ = gate.reject_late_claim();
    if report.on_stop == 0 {
        on_stop();
        report.on_stop = 1;
    }
    report.rejected = gate.rejected();
    report.accepted_after_close = gate.accepted_after_close();
    report.exit_code = 0;
    report.drained = gate.is_closed() || report.drained;
    report.clone()
}

fn stop_code(report: &mut ChildReport, gate: &AdmissionGate, on_stop: &Arc<dyn Fn() + Send + Sync>, code: i32) -> ChildReport {
    let effects = crate::policy::exit_effects(Some(code));
    report.exit_code = code;
    if effects.on_stop && report.on_stop == 0 {
        on_stop();
        report.on_stop = 1;
    }
    report.rejected = gate.rejected();
    report.accepted_after_close = gate.accepted_after_close();
    report.clone()
}

struct Held {
    task_id: TaskId,
    message_id: MessageId,
    cycle_id: CycleId,
    token: DeliveryToken,
    renew_at: Instant,
}

impl Held {
    fn from_delivery(delivery: &Delivery, renew_every: Duration) -> Option<Self> {
        Some(Self {
            task_id: delivery.observed.task_id?,
            message_id: delivery.observed.message_id?,
            cycle_id: delivery.observed.cycle_id?,
            token: delivery.observed.delivery_token?,
            renew_at: Instant::now() + renew_every,
        })
    }
}

async fn claim_next(feed: &mut Feed, params: &ChildParams) -> Result<ClaimResult, BackendError> {
    match feed {
        Feed::Script(scripts) => Ok(scripts.claims.pop_front().unwrap_or(Ok(ClaimResult::Idle))?),
        Feed::Redis(backend) => {
            backend
                .claim(ClaimRequest {
                    operation_id: OperationId::from_uuid(fresh_id()),
                    message_id: MessageId::from_uuid(fresh_id()),
                    queue: params.queue.clone(),
                    worker_id: params.worker_id.clone(),
                    proposed_token: DeliveryToken::from_uuid(fresh_id()),
                    lease_ms: params.lease_ms,
                })
                .await
        }
    }
}

async fn renew_held(feed: &mut Feed, params: &ChildParams, held: &Held) -> Result<RenewResult, BackendError> {
    match feed {
        Feed::Script(scripts) => Ok(scripts.renews.pop_front().unwrap_or(Ok(RenewResult::Renewed {
            lease_deadline_ms: 1,
            started_at_ms: Some(1),
            replayed: false,
        }))?),
        Feed::Redis(backend) => {
            backend
                .renew(RenewRequest {
                    operation_id: OperationId::from_uuid(fresh_id()),
                    message_id: held.message_id,
                    queue: params.queue.clone(),
                    task_id: held.task_id,
                    cycle_id: held.cycle_id,
                    delivery_token: held.token,
                    lease_ms: params.lease_ms,
                })
                .await
        }
    }
}

async fn finish_held(feed: &mut Feed, params: &ChildParams, held: &Held) -> Result<FinishResult, BackendError> {
    match feed {
        Feed::Script(scripts) => Ok(scripts.finishes.pop_front().unwrap_or(Ok(FinishResult::Finished { replayed: false }))?),
        Feed::Redis(backend) => {
            backend
                .finish(FinishRequest {
                    operation_id: OperationId::from_uuid(fresh_id()),
                    message_id: held.message_id,
                    queue: params.queue.clone(),
                    task_id: held.task_id,
                    cycle_id: held.cycle_id,
                    delivery_token: held.token,
                    proposal: ok_outcome(),
                })
                .await
        }
    }
}

fn fresh_id() -> CanonicalUuid {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&n.to_be_bytes());
    bytes[8..].copy_from_slice(&(std::process::id() as u64).to_be_bytes());
    CanonicalUuid::from_random_v4(bytes)
}

pub fn scripted_delivery(n: u128) -> Delivery {
    let id = CanonicalUuid::from_random_v4(n.to_be_bytes());
    let mut observed = arq_core::ObservedTask::absent();
    observed.present = true;
    observed.state = Some(arq_core::TaskState::Inflight);
    observed.task_id = Some(TaskId::from_uuid(id));
    observed.message_id = Some(MessageId::from_uuid(id));
    observed.cycle_id = Some(CycleId::from_uuid(id));
    observed.delivery_token = Some(DeliveryToken::from_uuid(id));
    observed.actor_name = "process_item".into();
    observed.queue = "default".into();
    observed.unique_key = Some(format!("item:{n}"));
    Delivery { observed, replayed: false }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::{exit_effects, ChildExit};
    use std::sync::atomic::AtomicU64;

    fn delivered() -> ClaimResult {
        ClaimResult::Delivered(scripted_delivery(9))
    }

    #[test]
    fn parent_exit_and_sigusr1_close_the_same_gate() {
        let mut gate = AdmissionGate::default();
        let mut draining = false;
        assert!(!parent_or_signal_drain(false, false, &mut gate, &mut draining));
        assert!(!gate.is_closed());
        assert!(parent_or_signal_drain(true, false, &mut gate, &mut draining));
        assert!(gate.is_closed());
        assert!(parent_or_signal_drain(false, true, &mut gate, &mut draining));
        let mut gate = AdmissionGate::default();
        let mut draining = false;
        assert!(parent_or_signal_drain(false, true, &mut gate, &mut draining));
        assert!(gate.blocks_claim());
    }

    #[tokio::test]
    async fn exit_2_does_not_finish_or_call_on_stop() {
        let stops = Arc::new(AtomicU64::new(0));
        let stops_hook = Arc::clone(&stops);
        let mut params = ChildParams::test_fast();
        params.ping_budget = Duration::ZERO;
        let mut scripts = Scripts::default();
        scripts.claims.push_back(Ok(delivered()));
        let mut session = ChildSession::scripted(params, scripts, DispatchMode::Blocked);
        session.on_stop = Arc::new(move || {
            stops_hook.fetch_add(1, Ordering::SeqCst);
        });
        let report = run_child_session(session).await;
        assert_eq!(report.exit_code, 2);
        assert_eq!(report.finished, 0);
        assert_eq!(report.on_stop, 0);
        assert_eq!(stops.load(Ordering::SeqCst), 0);
        let effects = exit_effects(Some(2));
        assert_eq!(effects.class, ChildExit::Restart);
        assert!(!effects.finish && !effects.on_stop);
    }

    #[tokio::test]
    async fn one_miss_keeps_renew_and_a_responding_handler_is_not_exit_2() {
        let mut scripts = Scripts::default();
        scripts.claims.push_back(Ok(delivered()));
        scripts.renews.push_back(Ok(RenewResult::Renewed { lease_deadline_ms: 50, started_at_ms: Some(10), replayed: false }));
        scripts.finishes.push_back(Ok(FinishResult::Finished { replayed: false }));
        let mut params = ChildParams::test_fast();
        params.max_tasks = 1;
        params.renew_every = Duration::ZERO;
        let report = run_child_session(ChildSession::scripted(params, scripts, DispatchMode::Misses(1))).await;
        assert_eq!(report.exit_code, 0);
        assert!(report.renews >= 1);
        assert_eq!(report.finished, 1);
        assert_eq!(report.on_stop, 1);

        let mut scripts = Scripts::default();
        scripts.claims.push_back(Ok(delivered()));
        let mut params = ChildParams::test_fast();
        params.ping_budget = Duration::ZERO;
        params.shutdown_timeout = Duration::from_millis(30);
        let session = ChildSession::scripted(params, scripts, DispatchMode::Hang);
        let control = session.control.clone();
        let live = Arc::clone(&session.live);
        let task = tokio::spawn(run_child_session(session));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while live.claims.load(Ordering::SeqCst) == 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        control.request_drain();
        let report = task.await.unwrap();
        assert_ne!(report.exit_code, 2);
        assert!(report.timed_out);
        assert_eq!(report.finished, 0);
        assert_eq!(report.on_stop, 0);
    }

    #[tokio::test]
    async fn ownership_loss_dead_letter_and_storage_errors() {
        let mut lost = Scripts::default();
        lost.claims.push_back(Ok(ClaimResult::OwnershipLost {
            exit_code: 4,
            task_id: TaskId::from_uuid(CanonicalUuid::from_random_v4(4u128.to_be_bytes())),
            delivery_token: DeliveryToken::from_uuid(CanonicalUuid::from_random_v4(5u128.to_be_bytes())),
        }));
        let report = run_child_session(ChildSession::scripted(ChildParams::test_fast(), lost, DispatchMode::ok_now())).await;
        assert_eq!(report.exit_code, 4);
        assert_eq!(report.finished, 0);
        assert_eq!(report.on_stop, 1);

        let mut not_held = Scripts::default();
        not_held.claims.push_back(Ok(delivered()));
        not_held.renews.push_back(Ok(RenewResult::NotHeld { exit_code: 4 }));
        let mut params = ChildParams::test_fast();
        params.renew_every = Duration::ZERO;
        let report = run_child_session(ChildSession::scripted(params, not_held, DispatchMode::ok_now())).await;
        assert_eq!(report.exit_code, 4);
        assert_eq!(report.finished, 0);
        assert_eq!(report.on_stop, 1);

        let mut rejected = Scripts::default();
        rejected.claims.push_back(Ok(delivered()));
        rejected.finishes.push_back(Ok(FinishResult::Rejected));
        let mut params = ChildParams::test_fast();
        params.renew_every = Duration::from_secs(30);
        let report = run_child_session(ChildSession::scripted(params, rejected, DispatchMode::ok_now())).await;
        assert_eq!(report.exit_code, 4);
        assert_eq!(report.finished, 0);

        let mut dead = Scripts::default();
        dead.claims.push_back(Ok(ClaimResult::dead_letter(
            TaskId::from_uuid(CanonicalUuid::from_random_v4(6u128.to_be_bytes())),
            "max_deliveries",
            false,
        )));
        let session = ChildSession::scripted(ChildParams::test_fast(), dead, DispatchMode::ok_now());
        let control = session.control.clone();
        control.request_drain();
        let report = run_child_session(session).await;
        assert_eq!(report.exit_code, 0);
        assert_eq!(report.dead_letters, 0, "drain closes admission before a new claim");
        assert_eq!(report.on_stop, 1);

        let mut dead = Scripts::default();
        dead.claims.push_back(Ok(ClaimResult::dead_letter(
            TaskId::from_uuid(CanonicalUuid::from_random_v4(7u128.to_be_bytes())),
            "max_deliveries",
            false,
        )));
        let session = ChildSession::scripted(ChildParams::test_fast(), dead, DispatchMode::ok_now());
        session.control.request_drain();
        let control = session.control.clone();
        let task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(8)).await;
            control.request_drain();
        });
        // The request above races the first tick. Drive it explicitly instead.
        drop(task);
        let mut dead = Scripts::default();
        dead.claims.push_back(Ok(ClaimResult::dead_letter(
            TaskId::from_uuid(CanonicalUuid::from_random_v4(8u128.to_be_bytes())),
            "max_deliveries",
            false,
        )));
        let mut session = ChildSession::scripted(ChildParams::test_fast(), dead, DispatchMode::ok_now());
        session.max_ticks = Some(3);
        let control = session.control.clone();
        let live = Arc::clone(&session.live);
        let running = tokio::spawn(run_child_session(session));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while live.claims.load(Ordering::SeqCst) == 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        control.request_drain();
        let report = running.await.unwrap();
        assert_eq!(report.dead_letters, 1);
        assert_eq!(report.exit_code, 0);
        assert_eq!(report.on_stop, 1);

        let mut broken = Scripts::default();
        broken.claims.push_back(Err(BackendError::Input(arq_core::InputError::Queue)));
        let report = run_child_session(ChildSession::scripted(ChildParams::test_fast(), broken, DispatchMode::ok_now())).await;
        assert_eq!(report.exit_code, 3);
        assert_eq!(report.on_stop, 0);

        let mut transport = Scripts::default();
        transport.claims.push_back(Err(BackendError::Transport { detail: "closed".into() }));
        transport.claims.push_back(Err(BackendError::Transport { detail: "closed".into() }));
        let mut session = ChildSession::scripted(ChildParams::test_fast(), transport, DispatchMode::ok_now());
        session.max_ticks = Some(6);
        let report = run_child_session(session).await;
        assert_eq!(report.exit_code, 0);
        assert_eq!(report.finished, 0);
        assert_eq!(report.on_stop, 0);

        let mut txn = Scripts::default();
        txn.claims.push_back(Err(BackendError::Transaction(arq_core::TransactionConflict { limit: 1 })));
        let report = run_child_session(ChildSession::scripted(ChildParams::test_fast(), txn, DispatchMode::ok_now())).await;
        assert_eq!(report.exit_code, 5);
    }

    #[tokio::test]
    async fn max_tasks_replaces_with_exit_0_and_one_task_at_a_time() {
        let mut scripts = Scripts::default();
        scripts.claims.push_back(Ok(delivered()));
        scripts.claims.push_back(Ok(ClaimResult::Delivered(scripted_delivery(10))));
        scripts.finishes.push_back(Ok(FinishResult::Finished { replayed: false }));
        let mut params = ChildParams::test_fast();
        params.max_tasks = 1;
        params.renew_every = Duration::from_secs(30);
        let report = run_child_session(ChildSession::scripted(params, scripts, DispatchMode::ok_now())).await;
        assert_eq!(report.exit_code, 0);
        assert_eq!(report.tasks_done, 1);
        assert_eq!(report.claims, 1);
        assert_eq!(report.peak_inflight, 1);
        assert_eq!(report.on_stop, 1);

        let mut params = ChildParams::test_fast();
        params.max_tasks = 0;
        let report = run_child_session(ChildSession::scripted(params, Scripts::default(), DispatchMode::ok_now())).await;
        assert_eq!(report.exit_code, 3);
        params = ChildParams::test_fast();
        params.queue = "Default".into();
        let report = run_child_session(ChildSession::scripted(params, Scripts::default(), DispatchMode::ok_now())).await;
        assert_eq!(report.exit_code, 3);
        params = ChildParams::test_fast();
        params.worker_id.clear();
        let report = run_child_session(ChildSession::scripted(params, Scripts::default(), DispatchMode::ok_now())).await;
        assert_eq!(report.exit_code, 3);
        let mut session = ChildSession::scripted(ChildParams::test_fast(), Scripts::default(), DispatchMode::ok_now());
        session.watch = ParentWatch::broken("pidfd");
        let report = run_child_session(session).await;
        assert_eq!(report.exit_code, 3);
    }

    #[tokio::test]
    async fn a_claim_that_resolves_after_close_is_tracked_then_rejected() {
        let mut scripts = Scripts::default();
        scripts.claims.push_back(Ok(delivered()));
        scripts.finishes.push_back(Ok(FinishResult::Finished { replayed: false }));
        let mut session = ChildSession::scripted(ChildParams::test_fast(), scripts, DispatchMode::ok_now());
        session.close_before_note = true;
        session.params.renew_every = Duration::from_secs(30);
        session.params.max_tasks = 1;
        let report = run_child_session(session).await;
        assert_eq!(report.accepted_after_close, 1);
        assert_eq!(report.finished, 1);
        assert!(report.rejected >= 1);
    }

    #[tokio::test]
    async fn a_dead_parent_drains_and_a_live_parent_does_not() {
        let mut child = std::process::Command::new("/bin/sleep").arg("30").spawn().unwrap();
        let fd = crate::pidfd::open_pidfd(child.id()).unwrap();
        assert!(crate::osutil::signal_pid(child.id(), libc::SIGKILL));
        let _ = child.wait();
        let watch = ParentWatch::serve(fd).await.unwrap();
        assert!(watch.exited());
        let mut scripts = Scripts::default();
        scripts.claims.push_back(Ok(delivered()));
        let mut session = ChildSession::scripted(ChildParams::test_fast(), scripts, DispatchMode::ok_now());
        session.watch = watch;
        let report = run_child_session(session).await;
        assert!(report.drained);
        assert_eq!(report.claims, 0);
        assert_eq!(report.exit_code, 0);
        assert_eq!(report.on_stop, 1);

        let mut child = std::process::Command::new("/bin/sleep").arg("30").spawn().unwrap();
        let fd = crate::pidfd::open_pidfd(child.id()).unwrap();
        let watch = ParentWatch::serve(fd).await.unwrap();
        let mut scripts = Scripts::default();
        scripts.claims.push_back(Ok(delivered()));
        scripts.finishes.push_back(Ok(FinishResult::Finished { replayed: false }));
        let mut session = ChildSession::scripted(ChildParams::test_fast(), scripts, DispatchMode::ok_now());
        session.watch = watch;
        session.max_ticks = Some(2);
        session.params.renew_every = Duration::from_secs(30);
        session.params.max_tasks = 5;
        let report = run_child_session(session).await;
        assert!(!report.drained);
        assert!(report.claims >= 1);
        assert_eq!(report.on_stop, 0);
        assert!(crate::osutil::signal_pid(child.id(), libc::SIGKILL));
        let _ = child.wait();
    }

    #[tokio::test]
    async fn storage_errors_on_renew_and_finish_use_the_exit_map() {
        let mut kept = Scripts::default();
        kept.claims.push_back(Ok(delivered()));
        kept.renews.push_back(Err(BackendError::Transport { detail: "closed".into() }));
        kept.finishes.push_back(Ok(FinishResult::Finished { replayed: false }));
        let mut params = ChildParams::test_fast();
        params.renew_every = Duration::ZERO;
        params.max_tasks = 1;
        params.watch_retries = 2;
        let report = run_child_session(ChildSession::scripted(params, kept, DispatchMode::ok_now())).await;
        assert_eq!(report.exit_code, 0);
        assert_eq!(report.finished, 1);

        let mut spent = Scripts::default();
        spent.claims.push_back(Ok(delivered()));
        spent.renews.push_back(Err(BackendError::Transport { detail: "closed".into() }));
        let mut params = ChildParams::test_fast();
        params.renew_every = Duration::ZERO;
        params.watch_retries = 0;
        params.max_tasks = 1;
        let report = run_child_session(ChildSession::scripted(params, spent, DispatchMode::ok_now())).await;
        assert_eq!(report.exit_code, 0);
        assert_eq!(report.finished, 1);
        assert_eq!(report.on_stop, 1);

        let mut halted = Scripts::default();
        halted.claims.push_back(Ok(delivered()));
        halted.finishes.push_back(Err(BackendError::Halted { queue: "default".into() }));
        let mut params = ChildParams::test_fast();
        params.renew_every = Duration::from_secs(30);
        let report = run_child_session(ChildSession::scripted(params, halted, DispatchMode::ok_now())).await;
        assert_eq!(report.exit_code, 5);
        assert_eq!(report.finished, 0);
        assert_eq!(report.on_stop, 0);

        let mut dropped = Scripts::default();
        dropped.claims.push_back(Ok(delivered()));
        for _ in 0..40 {
            dropped.renews.push_back(Err(BackendError::Transport { detail: "closed".into() }));
        }
        let mut params = ChildParams::test_fast();
        params.renew_every = Duration::ZERO;
        params.watch_retries = 0;
        params.shutdown_timeout = Duration::from_millis(80);
        let session = ChildSession::scripted(params, dropped, DispatchMode::Hang);
        let control = session.control.clone();
        let running = tokio::spawn(run_child_session(session));
        tokio::time::sleep(Duration::from_millis(30)).await;
        control.request_drain();
        let report = tokio::time::timeout(Duration::from_secs(2), running).await.unwrap().unwrap();
        assert_eq!(report.exit_code, 0);
        assert_eq!(report.finished, 0);
        assert_eq!(report.renews, 0);
        assert!(report.drained);
        assert!(report.timed_out);

        let mut input = Scripts::default();
        input.claims.push_back(Ok(delivered()));
        input.finishes.push_back(Err(BackendError::Input(arq_core::InputError::Queue)));
        let mut params = ChildParams::test_fast();
        params.renew_every = Duration::from_secs(30);
        let report = run_child_session(ChildSession::scripted(params, input, DispatchMode::ok_now())).await;
        assert_eq!(report.exit_code, 3);
        assert_eq!(report.on_stop, 0);

        let mut broken = scripted_delivery(11);
        broken.observed.task_id = None;
        let mut scripts = Scripts::default();
        scripts.claims.push_back(Ok(ClaimResult::Delivered(broken)));
        let report = run_child_session(ChildSession::scripted(ChildParams::test_fast(), scripts, DispatchMode::ok_now())).await;
        assert_eq!(report.exit_code, 3);
        assert_eq!(report.finished, 0);
    }

    #[test]
    fn a_real_redis_drain_finishes_one_task_and_a_timeout_leaves_the_lease() {
        let _guard = crate::testutil::test_lock();
        crate::testutil::flush();
        crate::testutil::block_on(async {
            let backend = RedisBackend::connect(crate::testutil::URL, arq_core::Config::default()).unwrap();
            let first = crate::testutil::enqueue_unique(&backend, 71).await;
            let second = crate::testutil::enqueue_unique(&backend, 72).await;
            let mut params = ChildParams::test_fast();
            params.renew_every = Duration::ZERO;
            params.max_tasks = 10;
            let mut session = ChildSession::scripted(params, Scripts::default(), DispatchMode::UntilDrain(ok_outcome()));
            session.feed = Feed::Redis(backend.clone());
            let control = session.control.clone();
            let live = Arc::clone(&session.live);
            let running = tokio::spawn(run_child_session(session));
            wait_until(|| live.renews.load(Ordering::SeqCst) >= 1).await;
            let started = crate::testutil::hget(&crate::testutil::job_key(&first), "started_at_ms");
            tokio::time::sleep(Duration::from_millis(15)).await;
            assert_eq!(crate::testutil::hget(&crate::testutil::job_key(&first), "started_at_ms"), started);
            control.request_drain();
            let report = tokio::time::timeout(Duration::from_secs(3), running).await.expect("drain").unwrap();
            assert_eq!(report.exit_code, 0);
            assert_eq!(report.finished, 1);
            assert_eq!(report.claims, 1);
            assert!(report.renews >= 1);
            assert_eq!(report.on_stop, 1);
            assert!(report.rejected >= 1);
            assert!(report.drained);
            assert_eq!(crate::testutil::hget(&crate::testutil::job_key(&first), "state").as_deref(), Some("done"));
            assert!(!crate::testutil::exists(&crate::testutil::uniq_key(71)));
            assert_eq!(crate::testutil::hget(&crate::testutil::job_key(&second), "state").as_deref(), Some("ready"));
            assert_eq!(crate::testutil::get(&crate::testutil::uniq_key(72)).as_deref(), Some(second.as_str()));

            crate::testutil::flush();
            let hung = crate::testutil::enqueue_unique(&backend, 73).await;
            let mut params = ChildParams::test_fast();
            params.renew_every = Duration::ZERO;
            params.shutdown_timeout = Duration::from_millis(40);
            let mut session = ChildSession::scripted(params, Scripts::default(), DispatchMode::Hang);
            session.feed = Feed::Redis(backend);
            let control = session.control.clone();
            let live = Arc::clone(&session.live);
            let running = tokio::spawn(run_child_session(session));
            wait_until(|| live.renews.load(Ordering::SeqCst) >= 1).await;
            let started = crate::testutil::hget(&crate::testutil::job_key(&hung), "started_at_ms");
            control.request_drain();
            let report = tokio::time::timeout(Duration::from_secs(3), running).await.expect("timeout").unwrap();
            assert!(report.timed_out);
            assert_ne!(report.exit_code, 2);
            assert_eq!(report.finished, 0);
            assert_eq!(report.on_stop, 0);
            assert!(report.renews >= 1);
            assert_eq!(crate::testutil::hget(&crate::testutil::job_key(&hung), "state").as_deref(), Some("inflight"));
            assert_eq!(crate::testutil::hget(&crate::testutil::job_key(&hung), "started_at_ms"), started);
            assert_eq!(crate::testutil::get(&crate::testutil::uniq_key(73)).as_deref(), Some(hung.as_str()));
        });
    }

    async fn wait_until(mut ready: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !ready() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(ready());
    }
}
