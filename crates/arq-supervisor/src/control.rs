//! Kill only a child this supervisor still has bound to the task.
//!
//! An empty binding set after restart does not signal and does not requeue.
//! `cancel` is not a signal. Pid 0 and pid 1 are never signaled.

use arq_redis::{BackendError, QueueAdmin, TaskInfo};

use crate::osutil::signal_group;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub slot: u32,
    pub pid: u32,
    pub task_id: String,
    pub token: Option<String>,
}

pub fn plan_kill(task: &TaskInfo, bindings: &[Binding], command_token: Option<&str>) -> Option<u32> {
    if task.control_op != "kill" {
        return None;
    }
    let binding = bindings.iter().find(|binding| binding.task_id == task.task_id && token_matches(binding, command_token))?;
    if binding.pid == 0 || binding.pid == 1 || binding.pid > i32::MAX as u32 {
        return None;
    }
    Some(binding.pid)
}

fn token_matches(binding: &Binding, command_token: Option<&str>) -> bool {
    match (binding.token.as_deref(), command_token) {
        (_, None) => true,
        (Some(have), Some(want)) => have == want,
        (None, Some(_)) => false,
    }
}

/// Signal bound children. This does not finish, renew, requeue, or settle.
pub async fn poll_kills(admin: &QueueAdmin, bindings: &[Binding], mut signal: impl FnMut(u32) -> bool) -> Result<u64, BackendError> {
    let page = admin.peek("inflight", 50, None).await?;
    let mut sent = 0u64;
    for task in &page.tasks {
        if let Some(pid) = plan_kill(task, bindings, None) {
            if signal(pid) {
                sent = sent.saturating_add(1);
            }
        }
    }
    Ok(sent)
}

pub async fn poll_kills_default(admin: &QueueAdmin, bindings: &[Binding]) -> Result<u64, BackendError> {
    poll_kills(admin, bindings, signal_group_kill).await
}

fn signal_group_kill(pid: u32) -> bool {
    signal_group(pid, libc::SIGKILL)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(op: &str, id: &str) -> TaskInfo {
        TaskInfo {
            task_id: id.into(),
            message_id: None,
            state: "inflight".into(),
            deliveries: 1,
            retries: 0,
            score: None,
            missing_job: false,
            tag: None,
            reason: String::new(),
            cycle_id: None,
            control_op: op.into(),
            actor_name: "process_item".into(),
            unique_key: Some("item:1".into()),
            lease_deadline_ms: None,
            started_at_ms: None,
            available_at_ms: None,
            error: String::new(),
            stop_evidence: String::new(),
        }
    }

    fn binding(pid: u32, token: Option<&str>) -> Binding {
        Binding { slot: 0, pid, task_id: "task-1".into(), token: token.map(str::to_string) }
    }

    #[test]
    fn kill_requires_a_live_binding_and_ignores_cancel() {
        let kill = task("kill", "task-1");
        assert_eq!(plan_kill(&kill, &[], None), None);
        assert_eq!(plan_kill(&task("cancel", "task-1"), &[binding(40, None)], None), None);
        assert_eq!(plan_kill(&task("", "task-1"), &[binding(40, None)], None), None);
        assert_eq!(plan_kill(&kill, &[binding(0, None)], None), None);
        assert_eq!(plan_kill(&kill, &[binding(1, None)], None), None);
        assert_eq!(plan_kill(&kill, &[binding(u32::MAX, None)], None), None);
        assert_eq!(plan_kill(&kill, &[binding(40, Some("aaa"))], Some("bbb")), None);
        assert_eq!(plan_kill(&kill, &[binding(40, None)], Some("bbb")), None);
        assert_eq!(plan_kill(&kill, &[binding(40, Some("aaa"))], Some("aaa")), Some(40));
        assert_eq!(plan_kill(&kill, &[binding(40, Some("aaa"))], None), Some(40));
        assert_eq!(plan_kill(&task("kill", "other"), &[binding(40, None)], None), None);
    }

    #[test]
    fn an_empty_binding_set_does_not_signal_or_requeue_and_a_match_kills() {
        let _guard = crate::testutil::test_lock();
        crate::testutil::flush();
        crate::testutil::block_on(async {
            let backend = arq_redis::RedisBackend::connect(crate::testutil::URL, arq_core::Config::default()).unwrap();
            let task = crate::testutil::enqueue_unique(&backend, 81).await;
            let claimed = backend
                .claim(arq_redis::ClaimRequest {
                    operation_id: arq_core::OperationId::from_uuid(arq_core::CanonicalUuid::from_random_v4(8101u128.to_be_bytes())),
                    message_id: arq_core::MessageId::from_uuid(arq_core::CanonicalUuid::from_random_v4(8102u128.to_be_bytes())),
                    queue: "default".into(),
                    worker_id: "worker-1".into(),
                    proposed_token: arq_core::DeliveryToken::from_uuid(arq_core::CanonicalUuid::from_random_v4(8103u128.to_be_bytes())),
                    lease_ms: 15_000,
                })
                .await
                .unwrap();
            let arq_redis::ClaimResult::Delivered(delivery) = claimed else { panic!("{claimed:?}") };
            let task_id = delivery.observed.task_id.expect("task");
            let killed = backend
                .kill(arq_redis::ControlRequest {
                    operation_id: arq_core::OperationId::from_uuid(arq_core::CanonicalUuid::from_random_v4(8104u128.to_be_bytes())),
                    queue: "default".into(),
                    task_id,
                })
                .await
                .unwrap();
            assert!(matches!(killed, arq_redis::ControlResult::Accepted { .. }));
            let mut child = std::process::Command::new("/bin/sleep").arg("30").spawn().unwrap();
            let pid = child.id();
            let admin = backend.admin("default");
            let before = crate::testutil::scan("q:*");
            let sent = poll_kills(&admin, &[], |_| panic!("empty bindings must not signal")).await.unwrap();
            assert_eq!(sent, 0);
            assert!(crate::osutil::process_alive(pid));
            assert_eq!(crate::testutil::scan("q:*"), before);
            assert_eq!(crate::testutil::hget(&crate::testutil::job_key(&task), "state").as_deref(), Some("inflight"));
            assert_eq!(crate::testutil::get(&crate::testutil::uniq_key(81)).as_deref(), Some(task.as_str()));
            let mut bound = binding(pid, None);
            bound.task_id = task_id.to_string();
            let sent = poll_kills_default(&admin, &[bound]).await.unwrap();
            assert_eq!(sent, 1);
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            while crate::osutil::process_alive(pid) && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            assert!(!crate::osutil::process_alive(pid));
            let _ = child.wait();
            assert_eq!(crate::testutil::hget(&crate::testutil::job_key(&task), "state").as_deref(), Some("inflight"));
            assert_eq!(crate::testutil::get(&crate::testutil::uniq_key(81)).as_deref(), Some(task.as_str()));
            assert_eq!(crate::testutil::scan("q:*"), before);

            let other = crate::testutil::enqueue_unique(&backend, 82).await;
            let claimed = backend
                .claim(arq_redis::ClaimRequest {
                    operation_id: arq_core::OperationId::from_uuid(arq_core::CanonicalUuid::from_random_v4(8201u128.to_be_bytes())),
                    message_id: arq_core::MessageId::from_uuid(arq_core::CanonicalUuid::from_random_v4(8202u128.to_be_bytes())),
                    queue: "default".into(),
                    worker_id: "worker-1".into(),
                    proposed_token: arq_core::DeliveryToken::from_uuid(arq_core::CanonicalUuid::from_random_v4(8203u128.to_be_bytes())),
                    lease_ms: 15_000,
                })
                .await
                .unwrap();
            let arq_redis::ClaimResult::Delivered(delivery) = claimed else { panic!("{claimed:?}") };
            let task_id = delivery.observed.task_id.expect("task");
            let cancelled = backend
                .cancel(arq_redis::ControlRequest {
                    operation_id: arq_core::OperationId::from_uuid(arq_core::CanonicalUuid::from_random_v4(8204u128.to_be_bytes())),
                    queue: "default".into(),
                    task_id,
                })
                .await
                .unwrap();
            assert!(matches!(cancelled, arq_redis::ControlResult::Accepted { .. }));
            let mut child = std::process::Command::new("/bin/sleep").arg("30").spawn().unwrap();
            let pid = child.id();
            let mut bound = binding(pid, None);
            bound.task_id = task_id.to_string();
            let sent = poll_kills(&admin, &[bound], |_| panic!("cancel is not a signal")).await.unwrap();
            assert_eq!(sent, 0);
            assert!(crate::osutil::process_alive(pid));
            assert_eq!(crate::testutil::hget(&crate::testutil::job_key(&other), "state").as_deref(), Some("inflight"));
            let _ = crate::osutil::signal_pid(pid, libc::SIGKILL);
            let _ = child.wait();
        });
    }
}
