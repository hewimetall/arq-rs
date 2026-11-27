//! A broken queue member is quarantined. A systemic halt is stored in Redis.
//!
//! Each case re-executes this binary so Python initializes once.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arq_core::{CanonicalUuid, Config, Json, MessageIdentity};
use arq_redis::{EnqueueJob, EnqueueResult, RedisBackend};
use arq_worker::{prepare, production_arq_metric, production_halt_log_count, run_child, Action};

const URL: &str = "redis://redis:6379/1";

#[test]
fn quarantine_does_not_stop_the_queue() {
    if std::env::var_os("ARQ_QUARANTINE_CASE").is_some() {
        run_case();
        return;
    }
    for case in ["missing", "window", "finish", "backoff"] {
        let mut child = Command::new(std::env::current_exe().unwrap());
        child
            .arg("quarantine_does_not_stop_the_queue")
            .arg("--exact")
            .arg("--test-threads=1")
            .env_remove("ARQRS_PARENT_PIDFD")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (key, _) in std::env::vars() {
            if key.starts_with("ARQ_") || key.starts_with("SIMPLE_Q_") || key == "REDIS_URL" {
                child.env_remove(&key);
            }
        }
        child.env("ARQ_QUARANTINE_CASE", case);
        let done = child.spawn().unwrap().wait_with_output().unwrap();
        assert!(
            done.status.success(),
            "{case} {:?}\n{}{}",
            done.status,
            String::from_utf8_lossy(&done.stdout),
            String::from_utf8_lossy(&done.stderr)
        );
    }
}

fn run_case() {
    thread::spawn(|| {
        thread::sleep(Duration::from_secs(25));
        std::process::exit(98);
    });
    match std::env::var("ARQ_QUARANTINE_CASE").unwrap().as_str() {
        "missing" => missing_hash_takes_the_next_task(),
        "window" => blocked_inflight_does_not_hide_ready(),
        "finish" => finish_records_while_halted(),
        "backoff" => halted_claim_waits_and_logs_once(),
        other => panic!("unknown case {other}"),
    }
}

fn missing_hash_takes_the_next_task() {
    let fx = Fixture::new("missing", OK_APP);
    let broken = fx.enqueue();
    let healthy = fx.enqueue();
    let ready = format!("q:{{{}}}:ready", fx.queue);
    let job = format!("q:{{{}}}:job:{broken}", fx.queue);
    redis(&["ZADD", &ready, "1", &broken]);
    redis(&["DEL", &job]);
    let code = fx.child(&[("ARQ_MAX_TASKS_PER_CHILD", "1")]);
    assert_eq!(code, 0);
    assert_eq!(fx.task_state(&healthy), "done");
    let reason = format!("q:{{{}}}:qreason", fx.queue);
    assert_eq!(redis_get(&["HGET", &reason, &broken]), "missing_hash");
    assert_eq!(redis_get(&["ZSCORE", &ready, &broken]), "");
    assert!(!fx.halted());
    assert!(production_arq_metric("arq_quarantine_total", &fx.queue, "missing_hash") >= 1.0);
}

fn blocked_inflight_does_not_hide_ready() {
    let fx = Fixture::new("window", OK_APP);
    for _ in 0..17 {
        let task = fx.enqueue();
        let job = format!("q:{{{}}}:job:{task}", fx.queue);
        let ready = format!("q:{{{}}}:ready", fx.queue);
        let inflight = format!("q:{{{}}}:inflight", fx.queue);
        redis(&[
            "HSET",
            &job,
            "control_op",
            "cancel",
            "state",
            "inflight",
            "lease_deadline_ms",
            "1",
        ]);
        redis(&["ZREM", &ready, &task]);
        redis(&["ZADD", &inflight, "1", &task]);
    }
    let healthy = fx.enqueue();
    let code = fx.child(&[("ARQ_MAX_TASKS_PER_CHILD", "1")]);
    assert_eq!(code, 0);
    assert_eq!(fx.task_state(&healthy), "done");
    let inflight = format!("q:{{{}}}:inflight", fx.queue);
    assert_eq!(redis_get(&["ZCARD", &inflight]), "17");
    assert!(!fx.halted());
}

fn finish_records_while_halted() {
    let fx = Fixture::new("finish", SLEEP_APP);
    let task = fx.enqueue();
    halt_when(fx.marker(), &fx.queue);
    let code = fx.child(&[
        ("ARQ_MAX_TASKS_PER_CHILD", "1"),
        ("ARQ_RENEW_INTERVAL_MS", "50"),
        ("ARQ_LEASE_MS", "10000"),
        ("ARQ_LIFECYCLE_INTERVAL_MS", "40"),
    ]);
    assert_eq!(code, 0);
    assert_eq!(fx.task_state(&task), "done");
    let job = format!("q:{{{}}}:job:{task}", fx.queue);
    let halted = format!("q:{{{}}}:halted", fx.queue);
    assert_eq!(redis_get(&["HGET", &job, "result"]), "ok");
    assert_eq!(redis_get(&["GET", &halted]), "during-handler");
    assert!(fx.halted());
}

fn halted_claim_waits_and_logs_once() {
    let fx = Fixture::new("backoff", OK_APP);
    let halted = format!("q:{{{}}}:halted", fx.queue);
    redis(&["SET", &halted, "persisted"]);
    let queue = fx.queue.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(1500));
        let _ = Command::new("kill")
            .args(["-TERM", &std::process::id().to_string()])
            .status();
        let _ = queue;
    });
    let code = fx.child(&[
        ("ARQ_POLL_INTERVAL_MS", "200"),
        ("ARQ_LIFECYCLE_INTERVAL_MS", "40"),
    ]);
    assert_eq!(code, 0);
    assert_eq!(production_halt_log_count(), 1);
    assert_eq!(
        production_arq_metric("arq_storage_inconsistent", &fx.queue, ""),
        1.0
    );
    assert!(fx.halted());
}

const OK_APP: &str = "\
class App:
    def __init__(self):
        self.client = object()

    async def dispatch(self, message):
        return {'outcome': 'ok'}

    def shutdown(self):
        return None
";

const SLEEP_APP: &str = "\
class App:
    def __init__(self):
        self.client = object()

    async def dispatch(self, message):
        from pathlib import Path
        import os
        import time
        Path(os.environ['CASE_MARKER']).write_text('ran')
        time.sleep(0.8)
        return {'outcome': 'ok'}

    def shutdown(self):
        return None
";

struct Fixture {
    queue: String,
    marker_path: PathBuf,
}

impl Fixture {
    fn new(name: &str, app: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "arq-quarantine-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos())
                .unwrap_or(0)
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("appmod.py"), format!("{app}\napp = App()\n")).unwrap();
        let queue = format!("q{}{name}", std::process::id());
        std::env::set_var("PYTHONPATH", &dir);
        std::env::set_var("CASE_MARKER", dir.join("marker"));
        Self {
            queue,
            marker_path: dir.join("marker"),
        }
    }

    fn marker(&self) -> &Path {
        &self.marker_path
    }

    fn enqueue(&self) -> String {
        let backend =
            RedisBackend::connect(URL, Config::parse([] as [(&str, &str); 0]).unwrap()).unwrap();
        let identity = MessageIdentity::new(CanonicalUuid::from_random_v4(material()));
        let task_id = identity.task_id.to_string();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let enqueued = runtime.block_on(backend.enqueue(EnqueueJob {
            operation_id: arq_core::OperationId::from_uuid(CanonicalUuid::from_random_v4(
                material(),
            )),
            identity,
            cycle_id: arq_core::CycleId::from_uuid(CanonicalUuid::from_random_v4(material())),
            actor_name: "process_item".into(),
            queue: self.queue.clone(),
            kwargs: Json::parse(r#"{"item_id":"1"}"#).unwrap(),
            headers: Json::object(vec![]),
            tag: None,
            unique_key: None,
            delay_ms: 0,
            max_retries: 3,
            max_deliveries: 5,
            time_limit_ms: 5_000,
            min_backoff_ms: 10,
            max_backoff_ms: 20,
        }));
        match enqueued.expect("enqueue") {
            EnqueueResult::Enqueued { .. } => task_id,
            EnqueueResult::Duplicate(error) => panic!("duplicate {error:?}"),
        }
    }

    fn task_state(&self, task_id: &str) -> String {
        let backend =
            RedisBackend::connect(URL, Config::parse([] as [(&str, &str); 0]).unwrap()).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime
            .block_on(backend.admin(&self.queue).get(task_id))
            .expect("admin")
            .expect("task")
            .state
    }

    fn halted(&self) -> bool {
        let backend =
            RedisBackend::connect(URL, Config::parse([] as [(&str, &str); 0]).unwrap()).unwrap();
        backend.is_halted(&self.queue)
    }

    fn child(&self, env: &[(&str, &str)]) -> i32 {
        let mut pairs = vec![
            ("REDIS_URL", URL),
            ("ARQ_POLL_INTERVAL_MS", "40"),
            ("ARQ_LIFECYCLE_INTERVAL_MS", "40"),
        ];
        for (key, value) in env {
            if let Some(found) = pairs.iter_mut().find(|(name, _)| name == key) {
                found.1 = value;
            } else {
                pairs.push((key, value));
            }
        }
        let plan = match prepare(
            [
                "--child",
                "--app",
                "appmod:app",
                "--queues",
                self.queue.as_str(),
            ],
            pairs,
        )
        .unwrap()
        {
            Action::Child(plan) => plan,
            other => panic!("expected child, got {other:?}"),
        };
        run_child(plan)
    }
}

fn redis(args: &[&str]) {
    let _ = redis_get(args);
}

fn redis_get(args: &[&str]) -> String {
    let output = Command::new("/usr/local/bin/redis-cli")
        .arg("-u")
        .arg(URL)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if text == "(nil)" {
        String::new()
    } else {
        text
    }
}

fn halt_when(marker: &Path, queue: &str) {
    let marker = marker.to_path_buf();
    let key = format!("q:{{{queue}}}:halted");
    thread::spawn(move || {
        for _ in 0..400 {
            if marker.exists() {
                redis(&["SET", &key, "during-handler"]);
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
    });
}

fn material() -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[0] = 0x31;
    bytes[1..5].copy_from_slice(&std::process::id().to_be_bytes());
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or(0);
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    bytes[5..8].copy_from_slice(&n.to_be_bytes()[1..]);
    bytes[8..].copy_from_slice(&nanos.to_be_bytes());
    bytes
}
