//! Supervisor registry. These keys sit outside the queue hash tag and outside claim/finish.

use super::RedisBackend;
use crate::error::BackendError;
use crate::session::{Fail, Session};
use crate::types::{ChildRegistration, ChildView, SupervisorRegistration, SupervisorView};
use arq_core::{Config, InputError};
use redis::cmd;

const SUPERVISORS: &str = "arq:supervisors";
const ROUTE: &str = "arq-registry";

impl RedisBackend {
    pub async fn register_supervisor(&self, registration: SupervisorRegistration) -> Result<u64, BackendError> {
        let this = self.clone();
        super::block_on(move || this.register_blocking(registration)).await
    }

    pub async fn heartbeat(&self, host: &str, pid: u32) -> Result<u64, BackendError> {
        let this = self.clone();
        let host = host.to_string();
        super::block_on(move || this.heartbeat_blocking(&host, pid)).await
    }

    pub async fn upsert_child(&self, host: &str, parent_pid: u32, child: ChildRegistration) -> Result<(), BackendError> {
        let this = self.clone();
        let host = host.to_string();
        super::block_on(move || this.child_blocking(&host, parent_pid, child)).await
    }

    pub async fn remove_supervisor(&self, host: &str, pid: u32) -> Result<(), BackendError> {
        let this = self.clone();
        let host = host.to_string();
        super::block_on(move || this.remove_blocking(&host, pid)).await
    }

    fn register_blocking(&self, registration: SupervisorRegistration) -> Result<u64, BackendError> {
        check_host(&registration.host)?;
        let member = member(&registration.host, registration.pid);
        let key = supervisor_key(&member);
        let ttl = registry_ttl(&self.config());
        self.with_session(ROUTE, |session| {
            let now = session.time_ms()?;
            let mut fields = vec![
                ("host", registration.host.clone()),
                ("pid", registration.pid.to_string()),
                ("started_at_ms", registration.started_at_ms.to_string()),
                ("version", registration.version.clone()),
                ("queues", registration.queues.join(",")),
                ("processes", registration.processes.to_string()),
                ("concurrency", registration.concurrency.to_string()),
                ("max_tasks", registration.max_tasks.to_string()),
                ("memory_soft_ratio", registration.memory_soft_ratio.to_string()),
                ("memory_hard_ratio", registration.memory_hard_ratio.to_string()),
                ("child_memory_soft", registration.child_memory_soft.map(|value| value.to_string()).unwrap_or_default()),
                ("child_memory_hard", registration.child_memory_hard.map(|value| value.to_string()).unwrap_or_default()),
                ("cgroup", registration.cgroup.clone()),
                ("recycled", registration.recycled.to_string()),
                ("drains", registration.drains.to_string()),
                ("kills", registration.kills.to_string()),
                ("restarts", registration.restarts.to_string()),
                ("last_seen_ms", now.to_string()),
            ];
            hset(session, &key, &fields)?;
            let _ = &mut fields;
            zadd(session, SUPERVISORS, &member, now)?;
            expire(session, &key, ttl)?;
            Ok(now)
        })
    }

    fn heartbeat_blocking(&self, host: &str, pid: u32) -> Result<u64, BackendError> {
        check_host(host)?;
        let member = member(host, pid);
        let key = supervisor_key(&member);
        let ttl = registry_ttl(&self.config());
        self.with_session(ROUTE, |session| {
            let now = session.time_ms()?;
            hset(session, &key, &[("host", host.to_string()), ("pid", pid.to_string()), ("last_seen_ms", now.to_string())])?;
            zadd(session, SUPERVISORS, &member, now)?;
            expire(session, &key, ttl)?;
            Ok(now)
        })
    }

    fn child_blocking(&self, host: &str, parent_pid: u32, child: ChildRegistration) -> Result<(), BackendError> {
        check_host(host)?;
        let member = member(host, parent_pid);
        let key = child_key(&member, child.slot);
        let ttl = registry_ttl(&self.config());
        let window = ttl.saturating_mul(1000);
        self.with_session(ROUTE, |session| {
            hset(
                session,
                &key,
                &[
                    ("host", host.to_string()),
                    ("parent_pid", parent_pid.to_string()),
                    ("slot", child.slot.to_string()),
                    ("pid", child.pid.to_string()),
                    ("ppid", child.ppid.to_string()),
                    ("state", child.state.clone()),
                    ("reason", child.reason.clone()),
                    ("exit_code", child.exit_code.map(|code| code.to_string()).unwrap_or_default()),
                    ("rss_bytes", child.rss_bytes.to_string()),
                    ("started_at_ms", child.started_at_ms.to_string()),
                    ("ended_at_ms", child.ended_at_ms.map(|value| value.to_string()).unwrap_or_default()),
                    ("tasks_done", child.tasks_done.to_string()),
                    ("max_tasks", child.max_tasks.to_string()),
                    ("alive", if child.alive { "1" } else { "0" }.to_string()),
                    ("alive_window_ms", window.to_string()),
                ],
            )?;
            expire(session, &key, ttl)?;
            Ok(())
        })
    }

    fn remove_blocking(&self, host: &str, pid: u32) -> Result<(), BackendError> {
        check_host(host)?;
        let member = member(host, pid);
        self.with_session(ROUTE, |session| {
            let children = session.scan(&format!("arq:supervisor:{member}:child:*"))?;
            for key in children {
                del(session, &key)?;
            }
            del(session, &supervisor_key(&member))?;
            zrem(session, SUPERVISORS, &member)?;
            Ok(())
        })
    }

    pub(crate) fn read_registry(&self, host: Option<&str>) -> Result<(Vec<SupervisorView>, Vec<ChildView>), BackendError> {
        let host_filter = host.map(str::to_string);
        self.with_session(ROUTE, |session| {
            let members = session.zrange_with_scores(SUPERVISORS, "-inf", "+inf", false, 0, 10_000)?;
            let mut present = Vec::new();
            let mut supervisors = Vec::new();
            for (member, score) in members {
                present.push(member.clone());
                let Some((host, pid)) = split_member(&member) else {
                    continue;
                };
                if host_filter.as_ref().is_some_and(|wanted| wanted != &host) {
                    continue;
                }
                let fields = session.hgetall(&supervisor_key(&member))?;
                if fields.is_empty() {
                    continue;
                }
                let mut view = supervisor_from(&fields, &host, pid, score);
                let child_keys = session.scan(&format!("arq:supervisor:{member}:child:*"))?;
                for key in child_keys {
                    let child_fields = session.hgetall(&key)?;
                    if child_fields.is_empty() {
                        continue;
                    }
                    let child = child_from(&child_fields, &host, pid);
                    view.children.push(child);
                }
                view.children.sort_by_key(|child| child.slot);
                view.in_progress = view.children.iter().filter(|child| child_in_progress(child)).count() as u64;
                supervisors.push(view);
            }
            let mut orphans = Vec::new();
            for key in session.scan("arq:supervisor:*:child:*")? {
                let Some((member, slot)) = parse_child_key(&key) else {
                    continue;
                };
                if present.iter().any(|known| known == &member) {
                    continue;
                }
                let Some((host, pid)) = split_member(&member) else {
                    continue;
                };
                if host_filter.as_ref().is_some_and(|wanted| wanted != &host) {
                    continue;
                }
                let fields = session.hgetall(&key)?;
                if fields.is_empty() {
                    continue;
                }
                let mut child = child_from(&fields, &host, pid);
                child.slot = slot;
                orphans.push(child);
            }
            orphans.sort_by(|left, right| left.host.cmp(&right.host).then(left.slot.cmp(&right.slot)));
            Ok((supervisors, orphans))
        })
    }
}

fn child_in_progress(child: &ChildView) -> bool {
    child.alive && child.exit_code.is_none() && child.state != "exited"
}

fn check_host(host: &str) -> Result<(), BackendError> {
    let ok = (1..=253).contains(&host.len())
        && host.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if ok {
        Ok(())
    } else {
        Err(BackendError::Input(InputError::Opaque { field: "hostname" }))
    }
}

fn member(host: &str, pid: u32) -> String {
    format!("{host}.{pid}")
}

fn supervisor_key(member: &str) -> String {
    format!("arq:supervisor:{member}")
}

fn child_key(member: &str, slot: u32) -> String {
    format!("arq:supervisor:{member}:child:{slot}")
}

fn split_member(member: &str) -> Option<(String, u32)> {
    let (host, pid) = member.rsplit_once('.')?;
    let pid = pid.parse().ok()?;
    if host.is_empty() {
        return None;
    }
    Some((host.to_string(), pid))
}

fn parse_child_key(key: &str) -> Option<(String, u32)> {
    let rest = key.strip_prefix("arq:supervisor:")?;
    let (member, slot) = rest.split_once(":child:")?;
    Some((member.to_string(), slot.parse().ok()?))
}

fn registry_ttl(config: &Config) -> u64 {
    config.heartbeat_secs.saturating_mul(config.missed_heartbeats).saturating_mul(2).max(1)
}

fn hset(session: &mut Session, key: &str, fields: &[(&str, String)]) -> Result<(), Fail> {
    if fields.is_empty() {
        return Ok(());
    }
    let mut command = cmd("HSET");
    command.arg(key);
    for (field, value) in fields {
        command.arg(*field).arg(value);
    }
    match session.call(&command)? {
        redis::Value::Int(_) | redis::Value::Okay => Ok(()),
        other => Err(Fail::io(format!("HSET reply {other:?}"))),
    }
}

fn zadd(session: &mut Session, key: &str, member: &str, score: u64) -> Result<(), Fail> {
    let mut command = cmd("ZADD");
    command.arg(key).arg(score).arg(member);
    match session.call(&command)? {
        redis::Value::Int(_) => Ok(()),
        other => Err(Fail::io(format!("ZADD reply {other:?}"))),
    }
}

fn zrem(session: &mut Session, key: &str, member: &str) -> Result<(), Fail> {
    let mut command = cmd("ZREM");
    command.arg(key).arg(member);
    match session.call(&command)? {
        redis::Value::Int(_) => Ok(()),
        other => Err(Fail::io(format!("ZREM reply {other:?}"))),
    }
}

fn del(session: &mut Session, key: &str) -> Result<(), Fail> {
    let mut command = cmd("DEL");
    command.arg(key);
    match session.call(&command)? {
        redis::Value::Int(_) => Ok(()),
        other => Err(Fail::io(format!("DEL reply {other:?}"))),
    }
}

fn expire(session: &mut Session, key: &str, ttl: u64) -> Result<(), Fail> {
    let mut command = cmd("EXPIRE");
    command.arg(key).arg(ttl);
    match session.call(&command)? {
        redis::Value::Int(_) => Ok(()),
        other => Err(Fail::io(format!("EXPIRE reply {other:?}"))),
    }
}

fn field<'a>(fields: &'a [(String, String)], name: &str) -> &'a str {
    fields.iter().find(|(key, _)| key == name).map(|(_, value)| value.as_str()).unwrap_or("")
}

fn field_u64(fields: &[(String, String)], name: &str) -> u64 {
    field(fields, name).parse().unwrap_or(0)
}

fn field_opt_u64(fields: &[(String, String)], name: &str) -> Option<u64> {
    let text = field(fields, name);
    if text.is_empty() {
        None
    } else {
        text.parse().ok()
    }
}

fn supervisor_from(fields: &[(String, String)], host: &str, pid: u32, score: u64) -> SupervisorView {
    let queues = field(fields, "queues");
    SupervisorView {
        host: field(fields, "host").to_string().is_empty().then(|| host.to_string()).unwrap_or_else(|| field(fields, "host").to_string()),
        pid: field(fields, "pid").parse().unwrap_or(pid),
        started_at_ms: field_u64(fields, "started_at_ms"),
        version: field(fields, "version").to_string(),
        queues: if queues.is_empty() { Vec::new() } else { queues.split(',').map(str::to_string).collect() },
        processes: field_u64(fields, "processes"),
        concurrency: field_u64(fields, "concurrency"),
        max_tasks: field_u64(fields, "max_tasks"),
        memory_soft_ratio: field(fields, "memory_soft_ratio").to_string(),
        memory_hard_ratio: field(fields, "memory_hard_ratio").to_string(),
        child_memory_soft: field_opt_u64(fields, "child_memory_soft"),
        child_memory_hard: field_opt_u64(fields, "child_memory_hard"),
        cgroup: field(fields, "cgroup").to_string(),
        recycled: field_u64(fields, "recycled"),
        drains: field_u64(fields, "drains"),
        kills: field_u64(fields, "kills"),
        restarts: field_u64(fields, "restarts"),
        last_seen_ms: field_u64(fields, "last_seen_ms").max(score),
        children: Vec::new(),
        in_progress: 0,
    }
}

fn child_from(fields: &[(String, String)], host: &str, parent_pid: u32) -> ChildView {
    let exit = field(fields, "exit_code");
    ChildView {
        host: {
            let stored = field(fields, "host");
            if stored.is_empty() { host.to_string() } else { stored.to_string() }
        },
        parent_pid: field(fields, "parent_pid").parse().unwrap_or(parent_pid),
        slot: field_u64(fields, "slot") as u32,
        pid: field_u64(fields, "pid") as u32,
        ppid: field_u64(fields, "ppid") as u32,
        state: field(fields, "state").to_string(),
        reason: field(fields, "reason").to_string(),
        exit_code: if exit.is_empty() { None } else { exit.parse().ok() },
        rss_bytes: field_u64(fields, "rss_bytes"),
        started_at_ms: field_u64(fields, "started_at_ms"),
        ended_at_ms: field_opt_u64(fields, "ended_at_ms"),
        tasks_done: field_u64(fields, "tasks_done"),
        max_tasks: field_u64(fields, "max_tasks"),
        alive: field(fields, "alive") == "1",
        alive_window_ms: field_u64(fields, "alive_window_ms"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scripted::{array, bulk, empty_array, integer, ok, time_reply};
    use crate::session::Session;
    use crate::types::ChildRegistration;
    use arq_core::Config;

    fn config() -> Config {
        Config::parse(std::iter::empty::<(&str, &str)>()).unwrap()
    }

    fn registration(host: &str) -> SupervisorRegistration {
        SupervisorRegistration {
            host: host.into(),
            pid: 7,
            started_at_ms: 1,
            version: "0".into(),
            queues: vec!["default".into()],
            processes: 1,
            concurrency: 1,
            max_tasks: 1,
            memory_soft_ratio: 0.5,
            memory_hard_ratio: 0.9,
            child_memory_soft: None,
            child_memory_hard: None,
            cgroup: String::new(),
            recycled: 0,
            drains: 0,
            kills: 0,
            restarts: 0,
        }
    }

    #[test]
    fn helpers_and_bad_replies() {
        assert!(split_member(".9").is_none());
        assert!(split_member("host").is_none());
        assert!(parse_child_key("arq:supervisor:nope").is_none());
        assert!(check_host("").is_err());
        assert!(check_host("ok.host").is_ok());
        assert!(!child_in_progress(&ChildView {
            host: "h".into(),
            parent_pid: 1,
            slot: 1,
            pid: 2,
            ppid: 1,
            state: "exited".into(),
            reason: String::new(),
            exit_code: None,
            rss_bytes: 0,
            started_at_ms: 0,
            ended_at_ms: None,
            tasks_done: 0,
            max_tasks: 1,
            alive: true,
            alive_window_ms: 1,
        }));
        let url = crate::scripted::serve(vec![]);
        let mut session = Session::open(&url, false).unwrap();
        assert!(hset(&mut session, "k", &[]).is_ok());

        let url = crate::scripted::serve(vec![time_reply(), ok()]);
        let backend = RedisBackend::connect(&url, config()).unwrap();
        assert!(backend.register_blocking(registration("worker.local")).is_err());
        let url = crate::scripted::serve(vec![crate::scripted::error("ERR hset")]);
        let backend = RedisBackend::connect(&url, config()).unwrap();
        let child = ChildRegistration {
            slot: 1,
            pid: 2,
            ppid: 1,
            state: "running".into(),
            reason: String::new(),
            exit_code: None,
            rss_bytes: 0,
            started_at_ms: 0,
            ended_at_ms: None,
            tasks_done: 0,
            max_tasks: 1,
            alive: true,
        };
        assert!(backend.child_blocking("worker.local", 7, child).is_err());

        let url = crate::scripted::serve(vec![time_reply(), integer(1), ok()]);
        let backend = RedisBackend::connect(&url, config()).unwrap();
        assert!(backend.register_blocking(registration("worker.local")).is_err());

        let url = crate::scripted::serve(vec![
            array(&[bulk("0"), empty_array()]),
            ok(),
        ]);
        let backend = RedisBackend::connect(&url, config()).unwrap();
        assert!(backend.remove_blocking("worker.local", 7).is_err());

        let url = crate::scripted::serve(vec![
            array(&[bulk("0"), empty_array()]),
            integer(1),
            ok(),
        ]);
        let backend = RedisBackend::connect(&url, config()).unwrap();
        assert!(backend.remove_blocking("worker.local", 7).is_err());

        let url = crate::scripted::serve(vec![time_reply(), integer(1), integer(1), ok()]);
        let backend = RedisBackend::connect(&url, config()).unwrap();
        assert!(backend.register_blocking(registration("worker.local")).is_err());

        let members = array(&[bulk("host.1"), integer(1), bulk("host.2"), integer(2)]);
        let child_scan = array(&[bulk("0"), array(&[bulk("arq:supervisor:host.2:child:1")])]);
        let orphan_scan = array(&[
            bulk("0"),
            array(&[bulk("arq:supervisor:.9:child:1"), bulk("arq:supervisor:host.3:child:1")]),
        ]);
        let url = crate::scripted::serve(vec![
            members,
            empty_array(),
            array(&[bulk("host"), bulk("host.2")]),
            child_scan,
            empty_array(),
            orphan_scan,
            empty_array(),
        ]);
        let backend = RedisBackend::connect(&url, config()).unwrap();
        let (supervisors, orphans) = backend.read_registry(None).unwrap();
        assert_eq!(supervisors.len(), 1);
        assert!(supervisors[0].children.is_empty());
        assert!(orphans.is_empty());
    }
}
