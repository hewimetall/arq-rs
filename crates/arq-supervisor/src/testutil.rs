//! Shared locks for signal handlers, the process environment, and Redis DB 0.

use std::future::Future;
use std::process::Command;
use std::sync::{Mutex, MutexGuard, OnceLock};

use arq_core::{CanonicalUuid, CycleId, Json, MessageIdentity, OperationId};
use arq_redis::{EnqueueJob, EnqueueResult, RedisBackend};

pub const URL: &str = "redis://redis:6379/0";

static TEST_LOCK: Mutex<()> = Mutex::new(());
static ENV_LOCK: Mutex<()> = Mutex::new(());
static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

pub fn test_lock() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|err| err.into_inner())
}

pub fn env_lock() -> MutexGuard<'static, ()> {
    ENV_LOCK.lock().unwrap_or_else(|err| err.into_inner())
}

pub fn block_on<T>(future: impl Future<Output = T> + Send) -> T {
    runtime().block_on(future)
}

fn runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread().worker_threads(8).enable_all().build().expect("runtime")
    })
}

pub fn flush() {
    let status = Command::new("redis-cli").args(["-n", "0", "FLUSHDB"]).status().expect("redis-cli");
    assert!(status.success(), "FLUSHDB");
}

pub fn redis_raw(args: &[&str]) -> String {
    let output = Command::new("redis-cli").arg("--raw").args(args).output().expect("redis-cli");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

pub fn scan(pattern: &str) -> Vec<String> {
    let mut keys: Vec<String> = redis_raw(&["--scan", "--pattern", pattern])
        .lines()
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect();
    keys.sort();
    keys
}

pub fn hget(key: &str, field: &str) -> Option<String> {
    let text = redis_raw(&["HGET", key, field]);
    if text.is_empty() { None } else { Some(text) }
}

pub fn get(key: &str) -> Option<String> {
    let text = redis_raw(&["GET", key]);
    if text.is_empty() { None } else { Some(text) }
}

pub fn exists(key: &str) -> bool {
    redis_raw(&["EXISTS", key]) == "1"
}

pub fn ttl(key: &str) -> i64 {
    redis_raw(&["TTL", key]).parse().unwrap_or(-2)
}

pub fn zscore(key: &str, member: &str) -> Option<String> {
    let text = redis_raw(&["ZSCORE", key, member]);
    if text.is_empty() { None } else { Some(text) }
}

pub async fn enqueue_unique(backend: &RedisBackend, n: u128) -> String {
    let id = CanonicalUuid::from_random_v4(n.to_be_bytes());
    let result = backend
        .enqueue(EnqueueJob {
            operation_id: OperationId::from_uuid(CanonicalUuid::from_random_v4((n + 10_000).to_be_bytes())),
            identity: MessageIdentity::new(id),
            cycle_id: CycleId::from_uuid(CanonicalUuid::from_random_v4((n + 20_000).to_be_bytes())),
            actor_name: "process_item".into(),
            queue: "default".into(),
            kwargs: Json::parse(&format!(r#"{{"item_id":{n}}}"#)).unwrap(),
            headers: Json::parse("{}").unwrap(),
            tag: None,
            unique_key: Some(format!("item:{n}")),
            delay_ms: 0,
            max_retries: 4,
            max_deliveries: 5,
            time_limit_ms: 1_000,
            min_backoff_ms: 10,
            max_backoff_ms: 20,
        })
        .await
        .expect("enqueue");
    match result {
        EnqueueResult::Enqueued { task_id, .. } => task_id.to_string(),
        EnqueueResult::Duplicate(duplicate) => duplicate.task_id.to_string(),
    }
}

pub fn job_key(task_id: &str) -> String {
    format!("q:{{default}}:job:{task_id}")
}

pub fn uniq_key(n: u128) -> String {
    format!("q:{{default}}:uniq:item:{n}")
}
