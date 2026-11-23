//! Parent loop. Signals are armed before slots. The parent never initializes CPython.

use std::collections::{HashMap, HashSet};
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arq_core::{validate_queue, AdmissionGate, ClaimAdmission, ProcessEvent, ProcessPhase, SlotLifecycle};
use arq_redis::RedisBackend;
use tokio::task::JoinSet;

use crate::memory::{self, PlannedSignal, PressureCounters};
use crate::metrics::MetricsProcess;
use crate::osutil::signal_group;
use crate::pidfd::open_supervisor_pidfd;
use crate::policy::{backoff_step, classify_child_exit, BackoffStep, ChildExit, BACKOFF_MIN};
use crate::registry::{self, record_child, RegistryBody, RegistryChild};
use crate::signals::{stop_requested, Shutdown, StopSignals};
use crate::spawn::{spawn_slot, CommandSpec};

#[derive(Debug)]
pub struct SupervisorError {
    pub message: String,
}

impl std::fmt::Display for SupervisorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SupervisorError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupervisorExit {
    pub code: i32,
    pub started_slots: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PidfdMode {
    Require,
    /// Test seam. The kernel may have `pidfd_open`; the supervisor still exits 3.
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PauseMode {
    Real,
    Record,
}

#[derive(Debug, Clone, Default)]
pub struct PressureOverride {
    pub enabled: bool,
    pub rss_by_slot: HashMap<u32, u64>,
    pub occupied: Option<u64>,
    pub limit: Option<u64>,
    pub soft_bytes: Option<u64>,
    pub hard_bytes: Option<u64>,
}

pub struct Trace {
    pub phases: Mutex<Vec<(u32, ProcessPhase)>>,
    pub pauses: Mutex<Vec<Duration>>,
    pub backoffs: Mutex<Vec<Duration>>,
    pub spawns: AtomicU32,
    pub exits: AtomicU32,
    pub admission_closed: AtomicBool,
    pub admission: Mutex<Option<ClaimAdmission>>,
    pub kills: AtomicU64,
    pub drains: AtomicU64,
    pub recycled: AtomicU64,
    pub restarts: AtomicU64,
    pub digits: Mutex<Option<Arc<crate::busy::SlotDigits>>>,
    pub metrics_pid: AtomicU32,
}

impl Trace {
    pub fn new() -> Self {
        Self {
            phases: Mutex::new(Vec::new()),
            pauses: Mutex::new(Vec::new()),
            backoffs: Mutex::new(Vec::new()),
            spawns: AtomicU32::new(0),
            exits: AtomicU32::new(0),
            admission_closed: AtomicBool::new(false),
            admission: Mutex::new(None),
            kills: AtomicU64::new(0),
            drains: AtomicU64::new(0),
            recycled: AtomicU64::new(0),
            restarts: AtomicU64::new(0),
            digits: Mutex::new(None),
            metrics_pid: AtomicU32::new(0),
        }
    }
}

impl Default for Trace {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct SupervisorSpec {
    pub config: arq_core::Config,
    pub url: String,
    pub queues: Vec<String>,
    pub processes: u32,
    pub concurrency: u32,
    pub host: String,
    pub pid: u32,
    pub version: String,
    pub slot: CommandSpec,
    pub metrics: CommandSpec,
    pub shutdown: Shutdown,
    pub pidfd: PidfdMode,
    pub pressure: PressureOverride,
    pub pause: PauseMode,
    pub memory_tick: Duration,
    pub heartbeat_every: Duration,
    pub stop_after_exits: Option<u32>,
    pub before_slots: Option<Arc<dyn Fn() + Send + Sync>>,
    pub on_spawn: Option<Arc<dyn Fn(u32) + Send + Sync>>,
    pub trace: Arc<Trace>,
}

impl SupervisorSpec {
    pub fn new(slot: CommandSpec) -> Self {
        let config = arq_core::Config::default();
        Self {
            heartbeat_every: Duration::from_secs(config.heartbeat_secs),
            memory_tick: Duration::from_secs(1),
            config,
            url: test_url(),
            queues: vec!["default".into()],
            processes: 1,
            concurrency: 1,
            host: "arq-test".into(),
            pid: std::process::id(),
            version: "arq-rs".into(),
            slot,
            metrics: CommandSpec::new("/bin/sleep").arg("30"),
            shutdown: Shutdown::new(),
            pidfd: PidfdMode::Require,
            pressure: PressureOverride::default(),
            pause: PauseMode::Real,
            stop_after_exits: None,
            before_slots: None,
            on_spawn: None,
            trace: Arc::new(Trace::new()),
        }
    }
}

fn test_url() -> String {
    "redis://redis:6379/0".into()
}

#[derive(Clone, Copy, Default)]
struct Counters {
    recycled: u64,
    drains: u64,
    kills: u64,
    restarts: u64,
}

struct Shared {
    spec: SupervisorSpec,
    backend: Option<RedisBackend>,
    parent_fd: Option<i32>,
    digits: Arc<crate::busy::SlotDigits>,
    pids: Mutex<HashMap<u32, u32>>,
    drained: Mutex<HashSet<u32>>,
    counters: Mutex<Counters>,
    gate: Mutex<AdmissionGate>,
}

enum SlotStop {
    Fatal,
    Done,
}

pub async fn run(spec: SupervisorSpec) -> Result<SupervisorExit, SupervisorError> {
    let mut signals = StopSignals::arm().map_err(|error| SupervisorError { message: error.to_string() })?;
    if invalid(&spec) {
        return Ok(SupervisorExit { code: 3, started_slots: false });
    }
    let backend = match RedisBackend::connect(&spec.url, spec.config) {
        Ok(backend) => Some(backend),
        Err(_) => None,
    };
    let mut registered = false;
    if let Some(backend) = &backend {
        let body = body(&spec, Counters::default());
        if registry::register_supervisor(backend, &body).await.is_ok() {
            registered = true;
        }
    }
    if let Some(hook) = &spec.before_slots {
        hook();
    }
    tokio::task::yield_now().await;
    tokio::select! {
        biased;
        _ = stop_requested(&spec.shutdown, &mut signals) => {
            release(&backend, &spec, registered).await;
            return Ok(SupervisorExit { code: 0, started_slots: false });
        }
        _ = tokio::time::sleep(Duration::from_millis(30)) => {}
    }
    let parent = match spec.pidfd {
        PidfdMode::Unavailable => {
            release(&backend, &spec, registered).await;
            return Ok(SupervisorExit { code: 3, started_slots: false });
        }
        PidfdMode::Require => match open_supervisor_pidfd() {
            Ok(fd) => fd,
            Err(error) => {
                release(&backend, &spec, registered).await;
                return Err(SupervisorError { message: error.to_string() });
            }
        },
    };
    let metrics = match MetricsProcess::spawn(&spec.metrics.program, &spec.metrics.args).await {
        Ok(metrics) => metrics,
        Err(_) => {
            release(&backend, &spec, registered).await;
            return Ok(SupervisorExit { code: 3, started_slots: false });
        }
    };
    spec.trace.metrics_pid.store(metrics.id(), Ordering::SeqCst);
    let digits = match crate::busy::SlotDigits::create(spec.processes.max(1)) {
        Ok(digits) => Arc::new(digits),
        Err(_) => {
            let _ = metrics.stop().await;
            release(&backend, &spec, registered).await;
            return Ok(SupervisorExit { code: 3, started_slots: false });
        }
    };
    *spec.trace.digits.lock().unwrap_or_else(|err| err.into_inner()) = Some(Arc::clone(&digits));
    let shared = Arc::new(Shared {
        spec: spec.clone(),
        backend: backend.clone(),
        parent_fd: Some(parent.as_raw_fd()),
        digits,
        pids: Mutex::new(HashMap::new()),
        drained: Mutex::new(HashSet::new()),
        counters: Mutex::new(Counters::default()),
        gate: Mutex::new(AdmissionGate::default()),
    });
    let heartbeat = {
        let backend = backend.clone();
        let spec_beat = spec.clone();
        tokio::spawn(async move {
            let start = tokio::time::Instant::now() + spec_beat.heartbeat_every;
            let mut interval = tokio::time::interval_at(start, spec_beat.heartbeat_every.max(Duration::from_millis(1)));
            loop {
                interval.tick().await;
                if spec_beat.shutdown.is_requested() {
                    break;
                }
                if let Some(backend) = &backend {
                    let _ = registry::heartbeat_supervisor(backend, &spec_beat.host, spec_beat.pid).await;
                }
            }
        })
    };
    let signal_task = {
        let shutdown = spec.shutdown.clone();
        tokio::spawn(async move {
            let _ = stop_requested(&shutdown, &mut signals).await;
            shutdown.request();
        })
    };
    let mut slots = JoinSet::new();
    for id in 0..spec.processes {
        let shared = Arc::clone(&shared);
        slots.spawn(async move { supervise_slot(shared, id).await });
    }
    let mut code = 0;
    let start = tokio::time::Instant::now() + spec.memory_tick;
    let mut memory = tokio::time::interval_at(start, spec.memory_tick.max(Duration::from_millis(1)));
    loop {
        tokio::select! {
            biased;
            joined = slots.join_next() => {
                match joined {
                    Some(Ok(SlotStop::Fatal)) => {
                        code = 3;
                        spec.shutdown.request();
                        close_admission(&shared);
                        break;
                    }
                    Some(Ok(SlotStop::Done)) => {
                        if spec.shutdown.is_requested() && slots.is_empty() {
                            close_admission(&shared);
                            break;
                        }
                    }
                    Some(Err(_)) => {
                        code = 3;
                        spec.shutdown.request();
                        break;
                    }
                    None => break,
                }
            }
            _ = memory.tick() => memory_pass(&shared).await,
            _ = wait_shutdown(&spec.shutdown) => {
                close_admission(&shared);
                break;
            }
        }
    }
    spec.shutdown.request();
    close_admission(&shared);
    let deadline = tokio::time::Instant::now() + Duration::from_millis(spec.config.shutdown_timeout_ms.saturating_add(1_000));
    loop {
        if slots.is_empty() {
            break;
        }
        match tokio::time::timeout_at(deadline, slots.join_next()).await {
            Ok(Some(_)) => {}
            _ => {
                slots.abort_all();
                break;
            }
        }
    }
    signal_task.abort();
    heartbeat.abort();
    let _ = metrics.stop().await;
    release(&backend, &spec, registered).await;
    drop(parent);
    Ok(SupervisorExit { code, started_slots: true })
}

fn invalid(spec: &SupervisorSpec) -> bool {
    spec.processes == 0
        || spec.concurrency == 0
        || spec.config.max_tasks_per_child == 0
        || spec.queues.is_empty()
        || spec.queues.iter().any(|queue| validate_queue(queue).is_err())
}

fn body(spec: &SupervisorSpec, counters: Counters) -> RegistryBody {
    RegistryBody {
        host: spec.host.clone(),
        pid: spec.pid,
        started_at_ms: registry::now_ms(),
        version: spec.version.clone(),
        queues: spec.queues.clone(),
        processes: u64::from(spec.processes),
        concurrency: u64::from(spec.concurrency),
        max_tasks: spec.config.max_tasks_per_child,
        config: spec.config,
        cgroup: registry::cgroup_label(),
        recycled: counters.recycled,
        drains: counters.drains,
        kills: counters.kills,
        restarts: counters.restarts,
    }
}

async fn release(backend: &Option<RedisBackend>, spec: &SupervisorSpec, registered: bool) {
    if !registered {
        return;
    }
    if let Some(backend) = backend {
        let _ = registry::release_supervisor(backend, &spec.host, spec.pid).await;
    }
}

fn close_admission(shared: &Shared) {
    let mut gate = shared.gate.lock().unwrap_or_else(|err| err.into_inner());
    if !gate.is_closed() {
        gate.close();
        shared.spec.trace.admission_closed.store(true, Ordering::SeqCst);
    }
    let decision = gate.reject_late_claim();
    *shared.spec.trace.admission.lock().unwrap_or_else(|err| err.into_inner()) = Some(decision);
}

async fn wait_shutdown(shutdown: &Shutdown) {
    if shutdown.is_requested() {
        return;
    }
    let mut flag = shutdown.subscribe();
    loop {
        if *flag.borrow() {
            return;
        }
        if flag.changed().await.is_err() {
            return;
        }
    }
}

fn note(trace: &Trace, slot: u32, life: SlotLifecycle) {
    trace.phases.lock().unwrap_or_else(|err| err.into_inner()).push((slot, life.phase()));
}

fn event(trace: &Trace, slot: u32, life: SlotLifecycle, happened: ProcessEvent) -> SlotLifecycle {
    match life.transition(happened) {
        Ok(next) => {
            note(trace, slot, next);
            next
        }
        Err(_) => life,
    }
}

async fn supervise_slot(shared: Arc<Shared>, slot: u32) -> SlotStop {
    let mut life = SlotLifecycle::starting();
    note(&shared.spec.trace, slot, life);
    let mut backoff = BACKOFF_MIN;
    loop {
        if shared.spec.shutdown.is_requested() {
            if life.phase() == ProcessPhase::Running {
                life = event(&shared.spec.trace, slot, life, ProcessEvent::Drain);
                life = event(&shared.spec.trace, slot, life, ProcessEvent::Stop);
                let _ = event(&shared.spec.trace, slot, life, ProcessEvent::Reaped);
            }
            return SlotStop::Done;
        }
        if matches!(life.phase(), ProcessPhase::Exited | ProcessPhase::Failed) {
            life = SlotLifecycle::starting();
            note(&shared.spec.trace, slot, life);
        }
        let spawned = spawn_slot(
            shared.spec.slot.clone(),
            shared.parent_fd,
            Some(shared.digits.raw_fd()),
            slot,
        )
        .await;
        let child = match spawned {
            Ok(child) => child,
            Err(_) => {
                life = event(&shared.spec.trace, slot, life, ProcessEvent::StartFailed);
                let _ = event(&shared.spec.trace, slot, life, ProcessEvent::Reaped);
                return SlotStop::Fatal;
            }
        };
        life = event(&shared.spec.trace, slot, life, ProcessEvent::Ready);
        let pid = child.id();
        shared.pids.lock().unwrap_or_else(|err| err.into_inner()).insert(slot, pid);
        shared.spec.trace.spawns.fetch_add(1, Ordering::SeqCst);
        if let Some(hook) = &shared.spec.on_spawn {
            hook(pid);
        }
        remember_child(&shared, slot, pid, true, None).await;
        let mut waiting = tokio::spawn(async move { child.wait().await });
        let code = loop {
            if shared.spec.shutdown.is_requested() {
                break drain_child(&shared, slot, pid, &mut life, &mut waiting).await;
            }
            tokio::select! {
                biased;
                ended = &mut waiting => break flatten(ended),
                _ = wait_shutdown(&shared.spec.shutdown) => {}
            }
        };
        shared.pids.lock().unwrap_or_else(|err| err.into_inner()).remove(&slot);
        let _ = waiting.abort();
        let stop = finish_exit(&shared, slot, &mut life, code, &mut backoff).await;
        if let Some(stop) = stop {
            return stop;
        }
    }
}

async fn drain_child(
    shared: &Shared,
    slot: u32,
    pid: u32,
    life: &mut SlotLifecycle,
    waiting: &mut tokio::task::JoinHandle<std::io::Result<Option<i32>>>,
) -> Option<i32> {
    close_admission(shared);
    if life.phase() == ProcessPhase::Running {
        *life = event(&shared.spec.trace, slot, *life, ProcessEvent::Drain);
    }
    if signal_group(pid, libc::SIGUSR1) {
        bump(shared, |counters| {
            counters.drains = counters.drains.saturating_add(1);
            shared.spec.trace.drains.store(counters.drains, Ordering::SeqCst);
        });
    }
    let timeout = Duration::from_millis(shared.spec.config.shutdown_timeout_ms);
    match tokio::time::timeout(timeout, &mut *waiting).await {
        Ok(ended) => flatten(ended),
        Err(_) => {
            let _ = signal_group(pid, libc::SIGKILL);
            waiting.abort();
            None
        }
    }
}

async fn finish_exit(
    shared: &Shared,
    slot: u32,
    life: &mut SlotLifecycle,
    code: Option<i32>,
    backoff: &mut Duration,
) -> Option<SlotStop> {
    let drained = matches!(life.phase(), ProcessPhase::Draining);
    if drained {
        *life = event(&shared.spec.trace, slot, *life, ProcessEvent::Stop);
        *life = event(&shared.spec.trace, slot, *life, ProcessEvent::Reaped);
    } else {
        match classify_child_exit(code) {
            ChildExit::Recycle => {
                *life = event(&shared.spec.trace, slot, *life, ProcessEvent::Stop);
                *life = event(&shared.spec.trace, slot, *life, ProcessEvent::Reaped);
            }
            ChildExit::Restart => {
                *life = event(&shared.spec.trace, slot, *life, ProcessEvent::Crash);
                *life = event(&shared.spec.trace, slot, *life, ProcessEvent::Reaped);
            }
            ChildExit::Fatal => {
                *life = event(&shared.spec.trace, slot, *life, ProcessEvent::Crash);
                *life = event(&shared.spec.trace, slot, *life, ProcessEvent::Reaped);
            }
        }
    }
    remember_child(shared, slot, 0, false, code).await;
    let class = classify_child_exit(code);
    let exit_n = shared.spec.trace.exits.fetch_add(1, Ordering::SeqCst).saturating_add(1);
    let reached = shared.spec.stop_after_exits.is_some_and(|limit| exit_n >= limit);
    match class {
        ChildExit::Fatal => return Some(SlotStop::Fatal),
        ChildExit::Recycle => {
            bump(shared, |counters| {
                if !drained {
                    counters.recycled = counters.recycled.saturating_add(1);
                    shared.spec.trace.recycled.store(counters.recycled, Ordering::SeqCst);
                }
            });
            *backoff = BACKOFF_MIN;
            shared.spec.trace.backoffs.lock().unwrap_or_else(|err| err.into_inner()).push(*backoff);
            if reached || shared.spec.shutdown.is_requested() {
                shared.spec.shutdown.request();
                return Some(SlotStop::Done);
            }
        }
        ChildExit::Restart => {
            bump(shared, |counters| {
                counters.restarts = counters.restarts.saturating_add(1);
                shared.spec.trace.restarts.store(counters.restarts, Ordering::SeqCst);
            });
            if reached || shared.spec.shutdown.is_requested() {
                shared.spec.shutdown.request();
                return Some(SlotStop::Done);
            }
            let (step, next) = backoff_step(code, *backoff);
            *backoff = next;
            if let BackoffStep::Wait(duration) = step {
                shared.spec.trace.backoffs.lock().unwrap_or_else(|err| err.into_inner()).push(duration);
                if pause(&shared.spec, duration).await {
                    return Some(SlotStop::Done);
                }
            }
        }
    }
    None
}

fn bump(shared: &Shared, update: impl FnOnce(&mut Counters)) {
    let mut counters = shared.counters.lock().unwrap_or_else(|err| err.into_inner());
    update(&mut counters);
}

async fn pause(spec: &SupervisorSpec, duration: Duration) -> bool {
    if spec.shutdown.is_requested() {
        return true;
    }
    spec.trace.pauses.lock().unwrap_or_else(|err| err.into_inner()).push(duration);
    if spec.pause == PauseMode::Record {
        return false;
    }
    let mut flag = spec.shutdown.subscribe();
    tokio::select! {
        biased;
        _ = tokio::time::sleep(duration) => false,
        changed = flag.changed() => changed.is_ok() && spec.shutdown.is_requested(),
    }
}

fn flatten(result: Result<std::io::Result<Option<i32>>, tokio::task::JoinError>) -> Option<i32> {
    result.ok().and_then(|inner| inner.ok()).flatten()
}

async fn remember_child(shared: &Shared, slot: u32, pid: u32, alive: bool, exit_code: Option<i32>) {
    let Some(backend) = &shared.backend else {
        return;
    };
    let row = RegistryChild {
        slot,
        pid,
        state: if alive { "running".into() } else { "exited".into() },
        reason: if alive { String::new() } else { "exit".into() },
        exit_code: if alive { None } else { exit_code.or(Some(0)) },
        rss_bytes: memory::resident_bytes(pid).unwrap_or(0),
        started_at_ms: registry::now_ms(),
        ended_at_ms: if alive { None } else { Some(registry::now_ms()) },
        tasks_done: 0,
        max_tasks: shared.spec.config.max_tasks_per_child,
        alive,
    };
    let _ = record_child(backend, &shared.spec.host, shared.spec.pid, &row).await;
    let counters = *shared.counters.lock().unwrap_or_else(|err| err.into_inner());
    let _ = registry::register_supervisor(backend, &body(&shared.spec, counters)).await;
}

async fn memory_pass(shared: &Shared) {
    let pids: Vec<(u32, u32)> = shared.pids.lock().unwrap_or_else(|err| err.into_inner()).iter().map(|(slot, pid)| (*slot, *pid)).collect();
    if pids.is_empty() {
        return;
    }
    let pressure = &shared.spec.pressure;
    let mut children = Vec::new();
    let mut busy = Vec::new();
    for (slot, pid) in pids {
        let rss = if pressure.enabled {
            pressure.rss_by_slot.get(&slot).copied().unwrap_or(0)
        } else {
            memory::resident_bytes(pid).unwrap_or(0)
        };
        children.push((pid, rss));
        if shared.digits.is_busy(slot) {
            busy.push(pid);
        }
    }
    let (occupied, limit, soft, hard) = if pressure.enabled {
        (pressure.occupied, pressure.limit, pressure.soft_bytes, pressure.hard_bytes)
    } else {
        let (occupied, limit) = memory::container_memory().map(|(usage, max)| (Some(usage), Some(max))).unwrap_or((None, None));
        (occupied, limit, shared.spec.config.child_memory_soft, shared.spec.config.child_memory_hard)
    };
    let already: Vec<u32> = shared.drained.lock().unwrap_or_else(|err| err.into_inner()).iter().copied().collect();
    let planned = memory::plan_memory_signals(
        &children,
        soft,
        hard,
        occupied,
        limit,
        shared.spec.config.memory_soft_ratio,
        shared.spec.config.memory_hard_ratio,
        &already,
        &busy,
    );
    let pressure_counters = {
        let mut drained = shared.drained.lock().unwrap_or_else(|err| err.into_inner());
        let mut pressure_counters = PressureCounters::default();
        memory::apply_planned(&planned, &mut drained, |pid, signal| signal_group(pid, signal), &mut pressure_counters);
        pressure_counters
    };
    if pressure_counters.drains > 0 || pressure_counters.kills > 0 {
        bump(shared, |counters| {
            counters.drains = counters.drains.saturating_add(pressure_counters.drains);
            counters.kills = counters.kills.saturating_add(pressure_counters.kills);
            shared.spec.trace.drains.store(counters.drains, Ordering::SeqCst);
            shared.spec.trace.kills.store(counters.kills, Ordering::SeqCst);
        });
    }
    let _ = PlannedSignal::Drain(0, memory::DrainCause::Shutdown);
    if let Some(backend) = &shared.backend {
        let bindings = Vec::new();
        let _ = crate::control::poll_kills_default(&backend.admin(&shared.spec.queues.first().cloned().unwrap_or_else(|| "default".into())), &bindings).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{self, enqueue_unique, exists, hget, job_key, scan, ttl, uniq_key, zscore};
    use arq_core::Config;
    use std::sync::atomic::AtomicU32;
    use std::time::Instant;

    fn dead(command: &str) -> SupervisorSpec {
        let mut spec = SupervisorSpec::new(CommandSpec::new("/bin/sh").args(["-c", command]));
        spec.url = "redis://127.0.0.1:1".into();
        spec.config = Config::parse([("ARQ_WATCH_RETRIES", "1")]).unwrap();
        spec.config.shutdown_timeout_ms = 200;
        spec.pause = PauseMode::Record;
        spec.memory_tick = Duration::from_secs(3_600);
        spec.heartbeat_every = Duration::from_secs(3_600);
        spec.metrics = CommandSpec::new("/bin/sleep").arg("30");
        spec
    }

    #[test]
    fn exit_0_replaces_immediately_and_resets_backoff() {
        let _guard = testutil::test_lock();
        let note = std::env::temp_dir().join(format!("arq-fd-{}-{}", std::process::id(), line!()));
        let _ = std::fs::remove_file(&note);
        let script = format!(
            "if [ -e \"/proc/self/fd/$ARQRS_PARENT_PIDFD\" ]; then echo open > \"{}\"; else echo closed > \"{}\"; fi; exit 0",
            note.display(),
            note.display()
        );
        let mut spec = dead(&script);
        spec.stop_after_exits = Some(3);
        let trace = Arc::clone(&spec.trace);
        let exit = testutil::block_on(run(spec)).unwrap();
        assert_eq!(exit.code, 0);
        assert!(exit.started_slots);
        assert_eq!(trace.spawns.load(Ordering::SeqCst), 3);
        assert!(trace.pauses.lock().unwrap().is_empty());
        let backoffs = trace.backoffs.lock().unwrap().clone();
        assert!(backoffs.iter().all(|pause| *pause == BACKOFF_MIN), "{backoffs:?}");
        assert!(phases_of(&trace).contains(&ProcessPhase::Running));
        assert!(phases_of(&trace).contains(&ProcessPhase::Exited));
        let text = std::fs::read_to_string(&note).unwrap_or_default();
        assert!(text.contains("open"), "{text}");
        let _ = std::fs::remove_file(&note);
    }

    fn phases_of(trace: &Trace) -> Vec<ProcessPhase> {
        trace.phases.lock().unwrap().iter().map(|(_, phase)| *phase).collect()
    }

    #[test]
    fn exit_2_4_and_5_back_off_until_thirty_seconds_would_cap() {
        let _guard = testutil::test_lock();
        for code in [2, 4, 5] {
            let mut spec = dead(&format!("exit {code}"));
            spec.stop_after_exits = Some(4);
            let trace = Arc::clone(&spec.trace);
            let exit = testutil::block_on(run(spec)).unwrap();
            assert_eq!(exit.code, 0, "code {code}");
            assert_eq!(trace.pauses.lock().unwrap().clone(), vec![
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4),
            ]);
        }
    }

    #[test]
    fn exit_3_stops_the_supervisor_and_does_not_restart_the_slot() {
        let _guard = testutil::test_lock();
        let mut spec = dead("exit 3");
        spec.processes = 2;
        let trace = Arc::clone(&spec.trace);
        let exit = testutil::block_on(run(spec)).unwrap();
        assert_eq!(exit.code, 3);
        assert!(exit.started_slots);
        assert_eq!(trace.spawns.load(Ordering::SeqCst), 2);
        assert!(phases_of(&trace).contains(&ProcessPhase::Failed) || phases_of(&trace).contains(&ProcessPhase::Exited));
    }

    #[test]
    fn an_accepted_stop_is_not_delayed_by_backoff() {
        let _guard = testutil::test_lock();
        let mut spec = dead("exit 2");
        let shutdown = spec.shutdown.clone();
        spec.on_spawn = Some(Arc::new(move |_| shutdown.request()));
        let trace = Arc::clone(&spec.trace);
        let started = Instant::now();
        let exit = testutil::block_on(run(spec)).unwrap();
        assert_eq!(exit.code, 0);
        assert!(trace.pauses.lock().unwrap().is_empty());
        assert!(trace.spawns.load(Ordering::SeqCst) >= 1);
        assert!(started.elapsed() < Duration::from_millis(800));
    }

    #[test]
    fn a_real_backoff_pause_stops_when_shutdown_is_accepted() {
        let _guard = testutil::test_lock();
        let mut spec = dead("exit 2");
        spec.pause = PauseMode::Real;
        let shutdown = spec.shutdown.clone();
        spec.on_spawn = Some(Arc::new(move |_| {
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(30)).await;
                shutdown.request();
            });
        }));
        let trace = Arc::clone(&spec.trace);
        let started = Instant::now();
        let exit = testutil::block_on(run(spec)).unwrap();
        assert_eq!(exit.code, 0);
        assert_eq!(trace.pauses.lock().unwrap().clone(), vec![Duration::from_secs(1)]);
        assert!(started.elapsed() < Duration::from_millis(800), "{:?}", started.elapsed());
    }

    #[test]
    fn sigterm_and_sigint_before_slots_delete_the_registry_and_leave_tasks() {
        let _guard = testutil::test_lock();
        testutil::flush();
        testutil::block_on(async {
            let backend = RedisBackend::connect(testutil::URL, Config::default()).unwrap();
            let task = enqueue_unique(&backend, 41).await;
            for signal in [libc::SIGTERM, libc::SIGINT] {
                testutil::flush();
                let task = enqueue_unique(&backend, 41).await;
                let file = std::env::temp_dir().join(format!("arq-early-{}-{}", signal, std::process::id()));
                let _ = std::fs::remove_file(&file);
                let mut spec = SupervisorSpec::new(CommandSpec::new("/bin/sh").arg("-c").arg(format!("touch {}", file.display())));
                spec.url = testutil::URL.into();
                spec.pid = std::process::id().wrapping_add(signal as u32);
                spec.before_slots = Some(Arc::new(move || {
                    assert!(crate::osutil::signal_pid(std::process::id(), signal));
                }));
                let exit = run(spec.clone()).await.unwrap();
                assert_eq!(exit.code, 0);
                assert!(!exit.started_slots);
                assert!(!file.exists());
                let key = registry::supervisor_key(&spec.host, spec.pid);
                assert!(!exists(&key));
                assert!(zscore("arq:supervisors", &registry::member(&spec.host, spec.pid)).is_none());
                assert_eq!(hget(&job_key(&task), "state").as_deref(), Some("ready"));
                assert_eq!(testutil::get(&uniq_key(41)).as_deref(), Some(task.as_str()));
            }
            let _ = task;
        });
    }

    #[test]
    fn shutdown_before_slots_deletes_the_registry() {
        let _guard = testutil::test_lock();
        testutil::flush();
        testutil::block_on(async {
            let backend = RedisBackend::connect(testutil::URL, Config::default()).unwrap();
            let task = enqueue_unique(&backend, 42).await;
            let file = std::env::temp_dir().join(format!("arq-manual-{}", std::process::id()));
            let _ = std::fs::remove_file(&file);
            let mut spec = SupervisorSpec::new(CommandSpec::new("/bin/sh").arg("-c").arg(format!("touch {}", file.display())));
            spec.url = testutil::URL.into();
            spec.shutdown.request();
            let exit = run(spec.clone()).await.unwrap();
            assert_eq!(exit.code, 0);
            assert!(!exit.started_slots);
            assert!(!file.exists());
            assert!(!exists(&registry::supervisor_key(&spec.host, spec.pid)));
            assert_eq!(hget(&job_key(&task), "state").as_deref(), Some("ready"));
        });
    }

    #[test]
    fn a_registry_left_behind_expires_by_ttl_and_does_not_touch_the_queue() {
        let _guard = testutil::test_lock();
        testutil::flush();
        testutil::block_on(async {
            let backend = RedisBackend::connect(testutil::URL, Config::default()).unwrap();
            let task = enqueue_unique(&backend, 43).await;
            let before = scan("q:*");
            let spec = SupervisorSpec::new(CommandSpec::new("/bin/true"));
            let body = body(&spec, Counters::default());
            registry::register_supervisor(&backend, &body).await.unwrap();
            let key = registry::supervisor_key(&spec.host, spec.pid);
            let ttl = ttl(&key);
            assert!(ttl > 0 && ttl <= 122, "{ttl}");
            assert!(exists(&key));
            assert!(zscore("arq:supervisors", &registry::member(&spec.host, spec.pid)).is_some());
            assert_eq!(scan("q:*"), before);
            assert_eq!(hget(&job_key(&task), "state").as_deref(), Some("ready"));
            registry::release_supervisor(&backend, &spec.host, spec.pid).await.unwrap();
        });
    }

    #[test]
    fn linux_without_pidfd_exits_3_and_does_not_start_slots() {
        let _guard = testutil::test_lock();
        testutil::flush();
        testutil::block_on(async {
            let backend = RedisBackend::connect(testutil::URL, Config::default()).unwrap();
            let task = enqueue_unique(&backend, 44).await;
            let file = std::env::temp_dir().join(format!("arq-nopidfd-{}", std::process::id()));
            let _ = std::fs::remove_file(&file);
            let mut spec = SupervisorSpec::new(CommandSpec::new("/bin/sh").arg("-c").arg(format!("touch {}", file.display())));
            spec.url = testutil::URL.into();
            spec.pidfd = PidfdMode::Unavailable;
            let exit = run(spec.clone()).await.unwrap();
            assert_eq!(exit.code, 3);
            assert!(!exit.started_slots);
            assert!(!file.exists());
            assert!(!exists(&registry::supervisor_key(&spec.host, spec.pid)));
            assert_eq!(hget(&job_key(&task), "state").as_deref(), Some("ready"));
            assert_eq!(testutil::get(&uniq_key(44)).as_deref(), Some(task.as_str()));
        });
    }

    #[test]
    fn a_missing_metrics_helper_exits_3_before_slots() {
        let _guard = testutil::test_lock();
        let file = std::env::temp_dir().join(format!("arq-nometrics-{}", std::process::id()));
        let _ = std::fs::remove_file(&file);
        let mut spec = dead(&format!("touch {}", file.display()));
        spec.metrics = CommandSpec::new("/no/such/arq-metrics");
        let exit = testutil::block_on(run(spec)).unwrap();
        assert_eq!(exit.code, 3);
        assert!(!exit.started_slots);
        assert!(!file.exists());
    }

    #[test]
    fn a_missing_slot_program_exits_3_after_the_slot_is_attempted() {
        let _guard = testutil::test_lock();
        let mut spec = dead("exit 0");
        spec.slot = CommandSpec::new("/no/such/arq-slot");
        let trace = Arc::clone(&spec.trace);
        let exit = testutil::block_on(run(spec)).unwrap();
        assert_eq!(exit.code, 3);
        assert!(exit.started_slots);
        assert!(phases_of(&trace).contains(&ProcessPhase::Failed));
        assert_eq!(trace.spawns.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a_dead_redis_does_not_stop_slot_start() {
        let _guard = testutil::test_lock();
        let mut spec = dead("exit 0");
        spec.stop_after_exits = Some(1);
        let started = Instant::now();
        let exit = testutil::block_on(run(spec)).unwrap();
        assert_eq!(exit.code, 0);
        assert!(exit.started_slots);
        assert!(started.elapsed() < Duration::from_secs(3), "{:?}", started.elapsed());
    }

    #[test]
    fn bad_config_exits_3_without_slots() {
        let _guard = testutil::test_lock();
        let cases = [
            ("processes", 0u32, 1u32, 1u64, "default", true),
            ("concurrency", 1, 0, 1, "default", true),
            ("tasks", 1, 1, 0, "default", true),
            ("queue", 1, 1, 1, "Default", true),
            ("empty", 1, 1, 1, "default", false),
        ];
        for (name, processes, concurrency, tasks, queue, keep) in cases {
            let mut spec = dead("touch /tmp/arq-should-not-exist-config");
            spec.processes = processes;
            spec.concurrency = concurrency;
            spec.config.max_tasks_per_child = tasks;
            spec.queues = if keep { vec![queue.into()] } else { Vec::new() };
            let exit = testutil::block_on(run(spec)).unwrap();
            assert_eq!(exit.code, 3, "{name}");
            assert!(!exit.started_slots, "{name}");
        }
        assert!(!std::path::Path::new("/tmp/arq-should-not-exist-config").exists());
    }

    #[test]
    fn heartbeat_updates_the_registry_and_does_not_touch_queue_keys() {
        let _guard = testutil::test_lock();
        testutil::flush();
        testutil::block_on(async {
            let backend = RedisBackend::connect(testutil::URL, Config::default()).unwrap();
            let task = enqueue_unique(&backend, 45).await;
            let claimed = backend.claim(arq_redis::ClaimRequest {
                operation_id: arq_core::OperationId::from_uuid(arq_core::CanonicalUuid::from_random_v4(4501u128.to_be_bytes())),
                message_id: arq_core::MessageId::from_uuid(arq_core::CanonicalUuid::from_random_v4(4502u128.to_be_bytes())),
                queue: "default".into(),
                worker_id: "worker-1".into(),
                proposed_token: arq_core::DeliveryToken::from_uuid(arq_core::CanonicalUuid::from_random_v4(4503u128.to_be_bytes())),
                lease_ms: 15_000,
            }).await.unwrap();
            let arq_redis::ClaimResult::Delivered(delivery) = claimed else { panic!("{claimed:?}") };
            let before = scan("q:*");
            let deadline = hget(&job_key(&task), "lease_deadline_ms");
            let spec = SupervisorSpec::new(CommandSpec::new("/bin/true"));
            registry::register_supervisor(&backend, &body(&spec, Counters::default())).await.unwrap();
            registry::heartbeat_supervisor(&backend, &spec.host, spec.pid).await.unwrap();
            assert_eq!(scan("q:*"), before);
            assert_eq!(hget(&job_key(&task), "lease_deadline_ms"), deadline);
            assert_eq!(hget(&job_key(&task), "state").as_deref(), Some("inflight"));
            assert_eq!(testutil::get(&uniq_key(45)).as_deref(), Some(task.as_str()));
            tokio::time::sleep(Duration::from_millis(40)).await;
            assert_eq!(hget(&job_key(&task), "state").as_deref(), Some("inflight"));
            assert_eq!(testutil::get(&uniq_key(45)).as_deref(), Some(task.as_str()));
            assert!(exists(&registry::supervisor_key(&spec.host, spec.pid)));
            let _ = delivery;
            registry::release_supervisor(&backend, &spec.host, spec.pid).await.unwrap();

            let mut running = spec.clone();
            running.url = testutil::URL.into();
            running.heartbeat_every = Duration::from_millis(15);
            running.pause = PauseMode::Record;
            running.memory_tick = Duration::from_secs(3_600);
            running.stop_after_exits = Some(1);
            running.slot = CommandSpec::new("/bin/sh").arg("-c").arg("exit 0");
            let exit = run(running).await.unwrap();
            assert_eq!(exit.code, 0);
            assert_eq!(hget(&job_key(&task), "state").as_deref(), Some("inflight"));
            assert_eq!(scan("q:*"), before);
        });
    }

    #[test]
    fn child_records_drive_the_in_progress_gauge() {
        let _guard = testutil::test_lock();
        testutil::flush();
        testutil::block_on(async {
            let backend = RedisBackend::connect(testutil::URL, Config::default()).unwrap();
            let spec = SupervisorSpec::new(CommandSpec::new("/bin/true"));
            registry::register_supervisor(&backend, &body(&spec, Counters::default())).await.unwrap();
            let running = RegistryChild {
                slot: 0,
                pid: 4242,
                state: "running".into(),
                reason: String::new(),
                exit_code: None,
                rss_bytes: 10,
                started_at_ms: 1,
                ended_at_ms: None,
                tasks_done: 0,
                max_tasks: 2,
                alive: true,
            };
            record_child(&backend, &spec.host, spec.pid, &running).await.unwrap();
            let snap = backend.admin("default").snapshot(Some(&spec.host)).await.unwrap();
            assert_eq!(snap.supervisors[0].in_progress, 1);
            let exited = RegistryChild {
                state: "exited".into(),
                exit_code: Some(0),
                alive: false,
                ended_at_ms: Some(2),
                ..running
            };
            record_child(&backend, &spec.host, spec.pid, &exited).await.unwrap();
            let snap = backend.admin("default").snapshot(Some(&spec.host)).await.unwrap();
            assert_eq!(snap.supervisors[0].in_progress, 0);
            assert_eq!(snap.supervisors[0].children[0].exit_code, Some(0));
            registry::release_supervisor(&backend, &spec.host, spec.pid).await.unwrap();
        });
    }

    #[test]
    fn soft_limit_does_not_kill_a_busy_child_and_hard_limit_does() {
        let _guard = testutil::test_lock();
        let mut spec = dead("sleep 30");
        spec.memory_tick = Duration::from_millis(20);
        spec.pressure.enabled = true;
        spec.pressure.rss_by_slot.insert(0, 50_000);
        spec.pressure.soft_bytes = Some(10);
        spec.pressure.hard_bytes = Some(1_000_000);
        let trace = Arc::clone(&spec.trace);
        let first = Arc::new(AtomicU32::new(0));
        let first_hook = Arc::clone(&first);
        let shutdown = spec.shutdown.clone();
        spec.on_spawn = Some(Arc::new(move |pid| {
            if first_hook.swap(pid, Ordering::SeqCst) == 0 {
                if let Some(digits) = trace.digits.lock().unwrap().clone() {
                    digits.enter(0);
                }
                let shutdown = shutdown.clone();
                let recorded = pid;
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(160)).await;
                    assert!(crate::osutil::process_alive(recorded), "soft signal killed a busy child");
                    shutdown.request();
                });
            }
        }));
        let trace = Arc::clone(&spec.trace);
        let exit = testutil::block_on(run(spec)).unwrap();
        assert_eq!(exit.code, 0);
        assert_eq!(trace.kills.load(Ordering::SeqCst), 0);
        assert!(first.load(Ordering::SeqCst) > 1);

        let mut spec = dead("sleep 30");
        spec.memory_tick = Duration::from_millis(20);
        spec.pause = PauseMode::Record;
        spec.pressure.enabled = true;
        spec.pressure.rss_by_slot.insert(0, 50_000);
        spec.pressure.soft_bytes = Some(10);
        spec.pressure.hard_bytes = Some(100);
        let trace = Arc::clone(&spec.trace);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_hook = Arc::clone(&seen);
        let shutdown = spec.shutdown.clone();
        spec.on_spawn = Some(Arc::new(move |pid| {
            if let Some(digits) = trace.digits.lock().unwrap().clone() {
                digits.enter(0);
            }
            let mut seen = seen_hook.lock().unwrap();
            seen.push(pid);
            if seen.len() > 1 {
                shutdown.request();
            }
        }));
        let trace = Arc::clone(&spec.trace);
        let exit = testutil::block_on(run(spec)).unwrap();
        assert_eq!(exit.code, 0);
        assert!(trace.kills.load(Ordering::SeqCst) >= 1);
        let first_pid = seen.lock().unwrap()[0];
        assert!(!crate::osutil::process_alive(first_pid));
    }

    #[test]
    fn a_live_memory_tick_and_graceful_drain_reap_the_slot() {
        let _guard = testutil::test_lock();
        testutil::flush();
        let mut spec = SupervisorSpec::new(CommandSpec::new("/bin/sleep").arg("30"));
        spec.url = testutil::URL.into();
        spec.config.shutdown_timeout_ms = 80;
        spec.memory_tick = Duration::from_millis(20);
        spec.heartbeat_every = Duration::from_secs(3_600);
        spec.pause = PauseMode::Record;
        let shutdown = spec.shutdown.clone();
        let metrics = Arc::clone(&spec.trace);
        spec.on_spawn = Some(Arc::new(move |pid| {
            assert!(crate::osutil::process_alive(pid));
            assert!(crate::osutil::process_alive(metrics.metrics_pid.load(Ordering::SeqCst)));
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                shutdown.request();
            });
        }));
        let trace = Arc::clone(&spec.trace);
        let exit = testutil::block_on(run(spec)).unwrap();
        assert_eq!(exit.code, 0);
        assert!(phases_of(&trace).contains(&ProcessPhase::Draining));
        assert!(phases_of(&trace).contains(&ProcessPhase::Exited));
        assert!(!crate::osutil::process_alive(trace.metrics_pid.load(Ordering::SeqCst)));
        assert!(trace.admission_closed.load(Ordering::SeqCst));
        let decision = *trace.admission.lock().unwrap();
        assert_eq!(decision, Some(ClaimAdmission::Reject));
    }
}
