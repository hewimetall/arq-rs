//! Supervisor registry. Heartbeat updates this record only.
//!
//! It does not extend a lease, release uniq, requeue, or write a queue key.
//! The hash and the `arq:supervisors` member expire after two heartbeat windows
//! when the process is killed before it can delete them.

use arq_core::Config;
use arq_redis::{BackendError, ChildRegistration, RedisBackend, SupervisorRegistration};

#[derive(Debug, Clone)]
pub struct RegistryChild {
    pub slot: u32,
    pub pid: u32,
    pub state: String,
    pub reason: String,
    pub exit_code: Option<i32>,
    pub rss_bytes: u64,
    pub started_at_ms: u64,
    pub ended_at_ms: Option<u64>,
    pub tasks_done: u64,
    pub max_tasks: u64,
    pub alive: bool,
}

#[derive(Debug, Clone)]
pub struct RegistryBody {
    pub host: String,
    pub pid: u32,
    pub started_at_ms: u64,
    pub version: String,
    pub queues: Vec<String>,
    pub processes: u64,
    pub concurrency: u64,
    pub max_tasks: u64,
    pub config: Config,
    pub cgroup: String,
    pub recycled: u64,
    pub drains: u64,
    pub kills: u64,
    pub restarts: u64,
}

pub fn hostname() -> String {
    let env = std::env::var("HOSTNAME").ok();
    let file = std::fs::read_to_string("/etc/hostname").ok();
    hostname_from(env, file)
}

pub fn hostname_from(env: Option<String>, file: Option<String>) -> String {
    for raw in [env, file].into_iter().flatten() {
        let text = raw.trim();
        if valid_host(text) {
            return text.to_string();
        }
    }
    "localhost".to_string()
}

pub fn valid_host(text: &str) -> bool {
    (1..=253).contains(&text.len())
        && text.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

pub fn cgroup_label() -> String {
    std::fs::read_to_string("/proc/self/cgroup").ok().and_then(|text| {
        text.lines().find_map(|line| line.trim().strip_prefix("0::")).map(|rest| rest.to_string())
    }).filter(|text| !text.is_empty()).unwrap_or_else(|| "unknown".into())
}

pub fn member(host: &str, pid: u32) -> String {
    format!("{host}.{pid}")
}

pub fn supervisor_key(host: &str, pid: u32) -> String {
    format!("arq:supervisor:{}", member(host, pid))
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn registration(body: &RegistryBody) -> SupervisorRegistration {
    SupervisorRegistration {
        host: body.host.clone(),
        pid: body.pid,
        started_at_ms: body.started_at_ms,
        version: body.version.clone(),
        queues: body.queues.clone(),
        processes: body.processes,
        concurrency: body.concurrency,
        max_tasks: body.max_tasks,
        memory_soft_ratio: body.config.memory_soft_ratio,
        memory_hard_ratio: body.config.memory_hard_ratio,
        child_memory_soft: body.config.child_memory_soft,
        child_memory_hard: body.config.child_memory_hard,
        cgroup: body.cgroup.clone(),
        recycled: body.recycled,
        drains: body.drains,
        kills: body.kills,
        restarts: body.restarts,
    }
}

pub async fn register_supervisor(backend: &RedisBackend, body: &RegistryBody) -> Result<(), BackendError> {
    backend.register_supervisor(registration(body)).await?;
    Ok(())
}

/// Refresh `host`, `pid`, `last_seen_ms`, and the zset score. Queue keys are not written.
pub async fn heartbeat_supervisor(backend: &RedisBackend, host: &str, pid: u32) -> Result<(), BackendError> {
    backend.heartbeat(host, pid).await?;
    Ok(())
}

pub async fn record_child(backend: &RedisBackend, host: &str, parent_pid: u32, child: &RegistryChild) -> Result<(), BackendError> {
    backend.upsert_child(
        host,
        parent_pid,
        ChildRegistration {
            slot: child.slot,
            pid: child.pid,
            ppid: parent_pid,
            state: child.state.clone(),
            reason: child.reason.clone(),
            exit_code: child.exit_code,
            rss_bytes: child.rss_bytes,
            started_at_ms: child.started_at_ms,
            ended_at_ms: child.ended_at_ms,
            tasks_done: child.tasks_done,
            max_tasks: child.max_tasks,
            alive: child.alive,
        },
    )
    .await
}

pub async fn release_supervisor(backend: &RedisBackend, host: &str, pid: u32) -> Result<(), BackendError> {
    backend.remove_supervisor(host, pid).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hostname_keeps_a_safe_label_and_falls_back() {
        assert_eq!(hostname_from(Some("worker-a".into()), None), "worker-a");
        assert_eq!(hostname_from(Some("bad host".into()), Some("file.host".into())), "file.host");
        assert_eq!(hostname_from(Some("".into()), Some("***".into())), "localhost");
        assert!(!valid_host(""));
        assert!(!valid_host(&"a".repeat(254)));
        assert!(valid_host("A_1.b-c"));
        assert!(!member("h", 7).is_empty());
        assert_eq!(supervisor_key("h", 7), "arq:supervisor:h.7");
        assert!(now_ms() > 0);
        let live = hostname();
        assert!(valid_host(&live));
        assert!(!cgroup_label().is_empty());
    }
}
