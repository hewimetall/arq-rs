//! Production delivery loop. One child runs one task at a time.
//!
//! `SIGTERM`, `SIGINT`, `SIGUSR1`, and parent exit close the local admission gate.
//! An active `dispatch` is renewed and then finished. Exit 2 does not finish and
//! does not call `shutdown`. A committed retry or dead letter is recorded through
//! `arq_rs.metrics`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use pyo3::prelude::*;

use arq_core::{
    AdmissionGate, CanonicalUuid, ClaimAdmission, CycleId, DeliveryToken, Json, LoopEvent,
    MessageId, ObservedTask, OperationId, OutcomeProposal, ParsedOutcome, PingWatch, TaskId,
    CONFIG_EXIT_CODE,
};
use arq_pybridge::{
    action_for, dispatch_json, invoke_note_dead, note_dead_after_commit, ping_running_loop,
    record_outcome, saw_drain, saw_int, saw_term, spawn_event_loop, stop_event_loop, CommitAction,
    DispatchRaw,
};
use arq_redis::{
    BackendError, ClaimRequest, ClaimResult, FinishRequest, FinishResult, RedisBackend,
    RenewRequest, RenewResult,
};
use arq_supervisor::{
    exit_effects, exit_for_storage, max_tasks_reached, parent_or_signal_drain, ParentWatch,
};

use crate::cli::ChildPlan;

const DISPATCH_WAIT_SECS: f64 = 86_400.0 * 365.0;
const DEFAULT_TIME_LIMIT_MS: u64 = 1_800_000;

struct Held {
    queue: String,
    actor: String,
    task_id: TaskId,
    message_id: MessageId,
    cycle_id: CycleId,
    token: DeliveryToken,
    renew_at: Instant,
    last_ping: Instant,
    ping: PingWatch,
    renew_stopped: bool,
    ping_task: Option<tokio::task::JoinHandle<bool>>,
    dispatch: Option<JoinHandle<DispatchRaw>>,
    outcome: Option<ParsedOutcome>,
    /// `dispatch` ended in `CancelGraceExceeded`. Finish is not written.
    abandoned: bool,
}

pub(crate) async fn run(plan: &ChildPlan, watch: ParentWatch, app: Py<PyAny>) -> i32 {
    let running = match spawn_event_loop() {
        Ok(running) => running,
        Err(error) => {
            eprintln!("arq-worker: event loop: {error}");
            return CONFIG_EXIT_CODE;
        }
    };
    let wait_secs = lifecycle_wait_secs(plan.config.shutdown_timeout_ms);
    if let Err(error) = call_lifecycle(&running.event_loop, &app, "startup", wait_secs) {
        eprintln!("arq-worker: startup: {error}");
        let _ = stop_event_loop(&running.event_loop);
        let _ = running.handle.join();
        return CONFIG_EXIT_CODE;
    }
    let code = deliver(plan, &watch, &app, &running.event_loop).await;
    let _ = stop_event_loop(&running.event_loop);
    let _ = running.handle.join();
    code
}

async fn deliver(
    plan: &ChildPlan,
    watch: &ParentWatch,
    app: &Py<PyAny>,
    event_loop: &Py<PyAny>,
) -> i32 {
    if plan.queues.is_empty() {
        eprintln!("arq-worker: at least one queue is required");
        return CONFIG_EXIT_CODE;
    }
    let worker = worker_id();
    if arq_core::validate_opaque("worker_id", &worker).is_err() {
        eprintln!("arq-worker: worker id is not a valid opaque token");
        return CONFIG_EXIT_CODE;
    }
    let backend = match RedisBackend::connect(&plan.redis_url, plan.config) {
        Ok(backend) => backend,
        Err(error) => {
            eprintln!("arq-worker: redis: {error}");
            return if stop_requested(watch) {
                leave(0, true, event_loop, app, plan)
            } else {
                CONFIG_EXIT_CODE
            };
        }
    };

    let mut gate = AdmissionGate::default();
    let mut draining = false;
    let mut drain_started: Option<Instant> = None;
    let mut held: Option<Held> = None;
    let mut tasks_done = 0u64;
    let mut transport_used = 0u64;
    let mut cursor = 0usize;
    let mut idle_queues = 0usize;
    let mut next_claim = Instant::now();
    let tick = Duration::from_millis(plan.config.lifecycle_interval_ms.max(1));
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;
        if note_drain(watch, &mut gate, &mut draining) && drain_started.is_none() {
            drain_started = Some(Instant::now());
        }
        if let (Some(started), Some(_)) = (drain_started, held.as_ref()) {
            if started.elapsed() >= Duration::from_millis(plan.config.shutdown_timeout_ms) {
                return leave(0, false, event_loop, app, plan);
            }
        }
        if let Some(code) = poll_held(
            plan,
            watch,
            &backend,
            event_loop,
            &mut held,
            &mut gate,
            draining,
            &mut tasks_done,
            &mut transport_used,
        )
        .await
        {
            return leave(
                code,
                exit_effects(Some(code)).on_stop,
                event_loop,
                app,
                plan,
            );
        }
        if held.is_none()
            && (gate.blocks_claim()
                || max_tasks_reached(tasks_done, plan.config.max_tasks_per_child))
        {
            return leave(0, true, event_loop, app, plan);
        }
        if held.is_none() && Instant::now() >= next_claim {
            match claim_one(
                plan,
                watch,
                &backend,
                app,
                event_loop,
                &worker,
                &mut cursor,
                &mut gate,
                &mut draining,
                &mut drain_started,
                &mut transport_used,
            )
            .await
            {
                ClaimStep::Hold(next) => {
                    idle_queues = 0;
                    held = Some(next);
                }
                ClaimStep::Idle => {
                    idle_queues = idle_queues.saturating_add(1);
                    if idle_queues >= plan.queues.len() {
                        idle_queues = 0;
                        next_claim = Instant::now()
                            + Duration::from_millis(plan.config.poll_interval_ms.max(1));
                    }
                }
                ClaimStep::Again => idle_queues = 0,
                ClaimStep::Stop(code) => {
                    return leave(
                        code,
                        exit_effects(Some(code)).on_stop,
                        event_loop,
                        app,
                        plan,
                    );
                }
            }
        }
    }
}

enum ClaimStep {
    Hold(Held),
    Idle,
    Again,
    Stop(i32),
}

#[allow(clippy::too_many_arguments)]
async fn claim_one(
    plan: &ChildPlan,
    watch: &ParentWatch,
    backend: &RedisBackend,
    app: &Py<PyAny>,
    event_loop: &Py<PyAny>,
    worker: &str,
    cursor: &mut usize,
    gate: &mut AdmissionGate,
    draining: &mut bool,
    drain_started: &mut Option<Instant>,
    transport_used: &mut u64,
) -> ClaimStep {
    if gate.blocks_claim() {
        return ClaimStep::Stop(0);
    }
    let queue = plan.queues[*cursor % plan.queues.len()].clone();
    *cursor = cursor.saturating_add(1);
    let claimed = backend
        .claim(ClaimRequest {
            operation_id: OperationId::from_uuid(fresh_id()),
            message_id: MessageId::from_uuid(fresh_id()),
            queue: queue.clone(),
            worker_id: worker.to_string(),
            proposed_token: DeliveryToken::from_uuid(fresh_id()),
            lease_ms: plan.config.lease_ms,
        })
        .await;
    if note_drain(watch, gate, draining) && drain_started.is_none() {
        *drain_started = Some(Instant::now());
    }
    if stop_requested(watch)
        && !matches!(
            claimed,
            Ok(ClaimResult::Delivered(_)) | Ok(ClaimResult::DeadLetter { .. })
        )
    {
        return ClaimStep::Stop(0);
    }
    match claimed {
        Ok(ClaimResult::Idle) => ClaimStep::Idle,
        Ok(ClaimResult::DeadLetter {
            reason,
            actor_name,
            kwargs_canonical,
            message_id,
            retries,
            deliveries,
            error,
            ..
        }) => {
            note_dead_letter(
                app,
                &queue,
                &actor_name,
                &kwargs_canonical,
                &reason,
                &message_id,
                retries,
                deliveries,
                &error,
            );
            ClaimStep::Again
        }
        Ok(ClaimResult::OwnershipLost { exit_code, .. }) => ClaimStep::Stop(exit_code),
        Ok(ClaimResult::Delivered(delivery)) => match gate.note_claim_resolved() {
            ClaimAdmission::Reject => ClaimStep::Again,
            admission => {
                if matches!(admission, ClaimAdmission::AcceptInflightAfterClose) {
                    gate.mark_inflight_settled();
                }
                match hold_delivery(plan, event_loop, app, &queue, worker, &delivery.observed) {
                    Ok(held) => ClaimStep::Hold(held),
                    Err(error) => {
                        eprintln!("arq-worker: {error}");
                        ClaimStep::Stop(CONFIG_EXIT_CODE)
                    }
                }
            }
        },
        Err(error) => {
            if stop_requested(watch) {
                ClaimStep::Stop(0)
            } else if let Some(code) =
                exit_for_storage(&error, transport_used, plan.config.watch_retries)
            {
                eprintln!("arq-worker: claim: {error}");
                ClaimStep::Stop(code)
            } else {
                ClaimStep::Again
            }
        }
    }
}

fn hold_delivery(
    plan: &ChildPlan,
    event_loop: &Py<PyAny>,
    app: &Py<PyAny>,
    queue: &str,
    worker: &str,
    observed: &ObservedTask,
) -> Result<Held, String> {
    let task_id = observed.task_id.ok_or("claimed delivery has no task_id")?;
    let message_id = observed
        .message_id
        .ok_or("claimed delivery has no message_id")?;
    let cycle_id = observed
        .cycle_id
        .ok_or("claimed delivery has no cycle_id")?;
    let token = observed
        .delivery_token
        .ok_or("claimed delivery has no delivery_token")?;
    let message = message_json(observed, queue, worker, plan.config.cancel_grace_ms)
        .ok_or("claimed delivery is missing an id")?;
    let limit_ms = if observed.time_limit_ms == 0 {
        DEFAULT_TIME_LIMIT_MS
    } else {
        observed.time_limit_ms
    };
    Ok(Held {
        queue: queue.to_string(),
        actor: observed.actor_name.clone(),
        task_id,
        message_id,
        cycle_id,
        token,
        renew_at: Instant::now() + Duration::from_millis(plan.config.renew_interval_ms.max(1)),
        last_ping: Instant::now(),
        ping: PingWatch::new(
            plan.config
                .effective_ping_budget(Duration::from_millis(limit_ms)),
        ),
        renew_stopped: false,
        ping_task: None,
        dispatch: Some(start_dispatch(app, event_loop, message)?),
        outcome: None,
        abandoned: false,
    })
}

#[allow(clippy::too_many_arguments)]
async fn poll_held(
    plan: &ChildPlan,
    watch: &ParentWatch,
    backend: &RedisBackend,
    event_loop: &Py<PyAny>,
    held: &mut Option<Held>,
    gate: &mut AdmissionGate,
    draining: bool,
    tasks_done: &mut u64,
    transport_used: &mut u64,
) -> Option<i32> {
    if !draining {
        if let Some(code) = poll_ping(plan, event_loop, held).await {
            return Some(code);
        }
    }
    if let Some(code) = renew_if_due(plan, watch, backend, held, transport_used).await {
        return Some(code);
    }
    take_dispatch_result(held);
    finish_if_ready(plan, watch, backend, held, gate, tasks_done, transport_used).await
}

async fn poll_ping(
    plan: &ChildPlan,
    event_loop: &Py<PyAny>,
    held: &mut Option<Held>,
) -> Option<i32> {
    let Some(current) = held.as_mut() else {
        return None;
    };
    if current.outcome.is_some() || current.renew_stopped {
        return None;
    }
    let ping_every = Duration::from_millis(plan.config.ping_interval_ms.max(1));
    if current.ping_task.is_none() && Instant::now() >= current.last_ping + ping_every {
        current.last_ping = Instant::now();
        let watched = Python::attach(|py| event_loop.clone_ref(py));
        let timeout = Duration::from_millis(plan.config.ping_timeout_ms.max(1));
        current.ping_task = Some(tokio::task::spawn_blocking(move || {
            ping_running_loop(&watched, timeout)
        }));
    }
    let finished = current
        .ping_task
        .as_ref()
        .is_some_and(|task| task.is_finished());
    if !finished {
        return None;
    }
    let task = current.ping_task.take()?;
    let responded = task.await.unwrap_or(false);
    let decision = current.ping.observe(
        if responded {
            LoopEvent::Responded
        } else {
            LoopEvent::Missed
        },
        Instant::now(),
    );
    current.renew_stopped = decision.stop_renew;
    decision.exit_code
}

async fn renew_if_due(
    plan: &ChildPlan,
    watch: &ParentWatch,
    backend: &RedisBackend,
    held: &mut Option<Held>,
    transport_used: &mut u64,
) -> Option<i32> {
    let due = held.as_ref().is_some_and(|current| {
        !current.renew_stopped && !current.abandoned && Instant::now() >= current.renew_at
    });
    if !due {
        return None;
    }
    let request = {
        let current = held.as_mut().expect("due renew has a delivery");
        current.renew_at =
            Instant::now() + Duration::from_millis(plan.config.renew_interval_ms.max(1));
        RenewRequest {
            operation_id: OperationId::from_uuid(fresh_id()),
            message_id: current.message_id,
            queue: current.queue.clone(),
            task_id: current.task_id,
            cycle_id: current.cycle_id,
            delivery_token: current.token,
            lease_ms: plan.config.lease_ms,
        }
    };
    let confirm = Duration::from_millis(plan.config.renew_confirm_ms.max(1));
    match tokio::time::timeout(confirm, backend.renew(request)).await {
        Ok(Ok(RenewResult::Renewed { .. })) => None,
        Ok(Ok(RenewResult::NotHeld { exit_code })) => Some(exit_code),
        Ok(Err(error)) => storage_stop(
            watch,
            &error,
            transport_used,
            plan.config.watch_retries,
            "renew",
        ),
        Err(_) => Some(arq_core::OWNERSHIP_LOST_EXIT_CODE),
    }
}

fn take_dispatch_result(held: &mut Option<Held>) {
    let ready = held.as_ref().is_some_and(|current| {
        current.outcome.is_none()
            && current
                .dispatch
                .as_ref()
                .is_some_and(|handle| handle.is_finished())
    });
    if !ready {
        return;
    }
    let Some(current) = held.as_mut() else {
        return;
    };
    let Some(handle) = current.dispatch.take() else {
        return;
    };
    let raw = handle.join().unwrap_or(DispatchRaw::Raised);
    match action_for(&raw) {
        CommitAction::Abandon => {
            current.abandoned = true;
            current.renew_stopped = true;
        }
        CommitAction::Finish(outcome) => current.outcome = Some(outcome),
    }
}

async fn finish_if_ready(
    plan: &ChildPlan,
    watch: &ParentWatch,
    backend: &RedisBackend,
    held: &mut Option<Held>,
    gate: &AdmissionGate,
    tasks_done: &mut u64,
    transport_used: &mut u64,
) -> Option<i32> {
    if held.as_ref().is_some_and(|current| current.abandoned) {
        return Some(arq_core::OWNERSHIP_LOST_EXIT_CODE);
    }
    let Some(outcome) = held.as_ref().and_then(|current| current.outcome.clone()) else {
        return None;
    };
    let request = {
        let current = held.as_ref().expect("outcome belongs to a delivery");
        FinishRequest {
            operation_id: OperationId::from_uuid(fresh_id()),
            message_id: current.message_id,
            queue: current.queue.clone(),
            task_id: current.task_id,
            cycle_id: current.cycle_id,
            delivery_token: current.token,
            proposal: outcome.clone(),
        }
    };
    let (queue, actor) = {
        let current = held.as_ref().expect("outcome belongs to a delivery");
        (current.queue.clone(), current.actor.clone())
    };
    match backend.finish(request).await {
        Ok(FinishResult::Finished { .. }) => {
            record(&queue, &actor, &outcome);
            *held = None;
            *tasks_done = tasks_done.saturating_add(1);
            if gate.blocks_claim()
                || max_tasks_reached(*tasks_done, plan.config.max_tasks_per_child)
            {
                Some(0)
            } else {
                None
            }
        }
        Ok(FinishResult::Rejected) => Some(arq_core::OWNERSHIP_LOST_EXIT_CODE),
        Err(error) => storage_stop(
            watch,
            &error,
            transport_used,
            plan.config.watch_retries,
            "finish",
        ),
    }
}

fn storage_stop(
    watch: &ParentWatch,
    error: &BackendError,
    used: &mut u64,
    limit: u64,
    op: &str,
) -> Option<i32> {
    if stop_requested(watch) {
        return Some(0);
    }
    match exit_for_storage(error, used, limit) {
        Some(code) => {
            eprintln!("arq-worker: {op}: {error}");
            Some(code)
        }
        None => None,
    }
}

fn note_dead_letter(
    app: &Py<PyAny>,
    queue: &str,
    actor: &str,
    kwargs: &str,
    reason: &str,
    message_id: &str,
    retries: u64,
    deliveries: u64,
    error: &str,
) {
    if note_dead_after_commit(reason) {
        let body = if Json::parse(kwargs).is_ok() {
            kwargs
        } else {
            "{}"
        };
        let detail = if error.is_empty() { None } else { Some(error) };
        if let Err(failure) = invoke_note_dead(
            app,
            actor,
            body,
            reason,
            message_id,
            i64::try_from(retries).unwrap_or(i64::MAX),
            i64::try_from(deliveries).unwrap_or(i64::MAX),
            detail,
        ) {
            eprintln!("arq-worker: note_dead: {failure}");
        }
    }
    record(
        queue,
        actor,
        &ParsedOutcome::Proposal(OutcomeProposal::Dead {
            reason: reason.to_string(),
            error: if error.is_empty() {
                None
            } else {
                Some(error.to_string())
            },
        }),
    );
}

fn record(queue: &str, actor: &str, outcome: &ParsedOutcome) {
    if let Err(error) = Python::attach(|py| record_outcome(py, queue, actor, outcome)) {
        eprintln!("arq-worker: metrics: {error}");
    }
}

fn start_dispatch(
    app: &Py<PyAny>,
    event_loop: &Py<PyAny>,
    message: String,
) -> Result<JoinHandle<DispatchRaw>, String> {
    let (app, event_loop) = Python::attach(|py| (app.clone_ref(py), event_loop.clone_ref(py)));
    std::thread::Builder::new()
        .name("arq-dispatch".into())
        .spawn(move || {
            dispatch_json(&event_loop, &app, &message, DISPATCH_WAIT_SECS)
                .unwrap_or(DispatchRaw::Raised)
        })
        .map_err(|error| error.to_string())
}

fn leave(code: i32, hooks: bool, event_loop: &Py<PyAny>, app: &Py<PyAny>, plan: &ChildPlan) -> i32 {
    if hooks {
        let wait_secs = lifecycle_wait_secs(plan.config.shutdown_timeout_ms);
        if let Err(error) = call_lifecycle(event_loop, app, "shutdown", wait_secs) {
            eprintln!("arq-worker: shutdown: {error}");
        }
    }
    code
}

pub(crate) fn call_lifecycle(
    event_loop: &Py<PyAny>,
    app: &Py<PyAny>,
    name: &str,
    wait_secs: f64,
) -> Result<(), String> {
    Python::attach(|py| {
        let bound = app.bind(py);
        if bound.getattr(name).is_err() {
            return Ok(());
        }
        let result = bound
            .call_method0(name)
            .map_err(|error| error.to_string())?;
        if !result.hasattr("__await__").unwrap_or(false) {
            return Ok(());
        }
        let asyncio = py.import("asyncio").map_err(|error| error.to_string())?;
        let future = asyncio
            .call_method1("run_coroutine_threadsafe", (result, event_loop.bind(py)))
            .map_err(|error| error.to_string())?;
        future
            .call_method1("result", (wait_secs,))
            .map_err(|error| error.to_string())?;
        Ok(())
    })
}

fn lifecycle_wait_secs(timeout_ms: u64) -> f64 {
    (timeout_ms.max(1) as f64) / 1000.0
}

fn note_drain(watch: &ParentWatch, gate: &mut AdmissionGate, draining: &mut bool) -> bool {
    parent_or_signal_drain(
        watch.exited(),
        saw_term() || saw_int() || saw_drain(),
        gate,
        draining,
    )
}

fn stop_requested(watch: &ParentWatch) -> bool {
    crate::child::child_should_stop(saw_term(), saw_int(), watch.exited()) || saw_drain()
}

pub(crate) fn worker_id() -> String {
    let slot = std::env::var("ARQRS_SLOT").unwrap_or_else(|_| "0".into());
    let id = format!("w{}s{slot}", std::process::id());
    if arq_core::validate_opaque("worker_id", &id).is_ok() {
        id
    } else {
        format!("w{}", std::process::id())
    }
}

fn fresh_id() -> CanonicalUuid {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let tick = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or(0);
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&n.to_be_bytes());
    bytes[8..].copy_from_slice(&tick.to_be_bytes());
    CanonicalUuid::from_random_v4(bytes)
}

pub(crate) fn message_json(
    observed: &ObservedTask,
    queue: &str,
    worker: &str,
    cancel_grace_ms: u64,
) -> Option<String> {
    let message_id = observed.message_id?.to_string();
    let task_id = observed.task_id?.to_string();
    let cycle_id = observed.cycle_id?.to_string();
    let kwargs = Json::parse(&observed.kwargs_canonical).unwrap_or_else(|_| Json::object(vec![]));
    let headers = Json::parse(&observed.headers_canonical).unwrap_or_else(|_| Json::object(vec![]));
    let queue_name = if observed.queue.is_empty() {
        queue
    } else {
        observed.queue.as_str()
    };
    let mut pairs = vec![
        (
            "actor_name".to_string(),
            Json::String(observed.actor_name.clone()),
        ),
        (
            "cancel_grace_ms".to_string(),
            Json::Number(cancel_grace_ms.to_string()),
        ),
        ("cycle_id".to_string(), Json::String(cycle_id)),
        (
            "deliveries".to_string(),
            Json::Number(observed.deliveries.to_string()),
        ),
        ("headers".to_string(), headers),
        ("kwargs".to_string(), kwargs),
        (
            "max_deliveries".to_string(),
            Json::Number(observed.max_deliveries.to_string()),
        ),
        (
            "max_retries".to_string(),
            Json::Number(observed.max_retries.to_string()),
        ),
        ("message_id".to_string(), Json::String(message_id)),
        ("queue".to_string(), Json::String(queue_name.to_string())),
        (
            "retries".to_string(),
            Json::Number(observed.retries.to_string()),
        ),
        ("task_id".to_string(), Json::String(task_id)),
        ("worker_name".to_string(), Json::String(worker.to_string())),
    ];
    if observed.time_limit_ms > 0 {
        pairs.push((
            "time_limit_ms".to_string(),
            Json::Number(observed.time_limit_ms.to_string()),
        ));
    }
    if observed.min_backoff_ms > 0 {
        pairs.push((
            "min_backoff_ms".to_string(),
            Json::Number(observed.min_backoff_ms.to_string()),
        ));
    }
    if observed.max_backoff_ms > 0 {
        pairs.push((
            "max_backoff_ms".to_string(),
            Json::Number(observed.max_backoff_ms.to_string()),
        ));
    }
    if let Some(tag) = &observed.tag {
        pairs.push(("tag".to_string(), Json::String(tag.clone())));
    }
    Some(Json::Object(pairs).canonical())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arq_core::{CycleId, MessageId, TaskId};

    fn uuid(byte: u8) -> CanonicalUuid {
        CanonicalUuid::from_random_v4([byte; 16])
    }

    #[test]
    fn the_dispatch_message_carries_the_stored_attempt() {
        let mut observed = ObservedTask::absent();
        observed.actor_name = "process_item".into();
        observed.queue = "default".into();
        observed.kwargs_canonical = r#"{"item_id":"1"}"#.into();
        observed.headers_canonical = r#"{"source":"test"}"#.into();
        observed.message_id = Some(MessageId::from_uuid(uuid(1)));
        observed.task_id = Some(TaskId::from_uuid(uuid(1)));
        observed.cycle_id = Some(CycleId::from_uuid(uuid(2)));
        observed.retries = 1;
        observed.max_retries = 3;
        observed.deliveries = 2;
        observed.max_deliveries = 5;
        observed.time_limit_ms = 1000;
        observed.tag = Some("batch-a".into());
        let text = message_json(&observed, "other", "w1s0", 25).expect("message");
        let parsed = Json::parse(&text).unwrap();
        assert_eq!(
            parsed.get("actor_name").and_then(Json::as_str),
            Some("process_item")
        );
        assert_eq!(parsed.get("queue").and_then(Json::as_str), Some("default"));
        assert_eq!(
            parsed.get("worker_name").and_then(Json::as_str),
            Some("w1s0")
        );
        assert_eq!(parsed.get("retries").and_then(Json::as_u64), Some(1));
        assert_eq!(
            parsed.get("time_limit_ms").and_then(Json::as_u64),
            Some(1000)
        );
        assert_eq!(
            parsed.get("cancel_grace_ms").and_then(Json::as_u64),
            Some(25)
        );
        assert_eq!(parsed.get("tag").and_then(Json::as_str), Some("batch-a"));
        observed.queue.clear();
        observed.time_limit_ms = 0;
        observed.min_backoff_ms = 5;
        observed.max_backoff_ms = 9;
        observed.tag = None;
        observed.kwargs_canonical = "not-json".into();
        observed.headers_canonical = "[".into();
        let fallback = message_json(&observed, "other", "w1s0", 25).expect("message");
        let parsed = Json::parse(&fallback).unwrap();
        assert_eq!(parsed.get("queue").and_then(Json::as_str), Some("other"));
        assert_eq!(parsed.get("min_backoff_ms").and_then(Json::as_u64), Some(5));
        assert_eq!(parsed.get("max_backoff_ms").and_then(Json::as_u64), Some(9));
        assert!(parsed.get("time_limit_ms").is_none());
        assert!(parsed.get("tag").is_none());
        assert!(message_json(&ObservedTask::absent(), "default", "w1s0", 25).is_none());
        let previous = std::env::var("ARQRS_SLOT").ok();
        std::env::set_var("ARQRS_SLOT", "bad slot");
        assert_eq!(worker_id(), format!("w{}", std::process::id()));
        match previous {
            Some(value) => std::env::set_var("ARQRS_SLOT", value),
            None => std::env::remove_var("ARQRS_SLOT"),
        }
    }
}
