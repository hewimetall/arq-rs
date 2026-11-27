use super::{
    claim_to, config, conn, cycle, delivered, enqueue, get, hget, ident, job, job_key, key_type, message, op, open_backend,
    outcome, token, zcard, URL,
};
use arq_core::Json;
use arq_redis::{
    BackendError, ClaimRequest, ClaimResult, ControlRequest, ControlResult, EnqueueResult, ExecFault, Fault, FinishRequest,
    FinishResult, RedisBackend, RenewRequest, RequeueRequest, RequeueResult,
};
use std::error::Error;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn touch_receipt_once(flag: &Arc<AtomicBool>, keys: &[String]) {
    if flag.swap(true, Ordering::SeqCst) {
        return;
    }
    let Some(receipt) = keys.iter().find(|key| key.contains(":receipt:")).cloned() else {
        return;
    };
    let mut con = conn();
    let _: () = redis::cmd("SET").arg(&receipt).arg("x").query(&mut con).unwrap();
    let _: i64 = redis::cmd("DEL").arg(&receipt).query(&mut con).unwrap();
}

fn dirty_job(keys: &[String]) {
    let Some(job) = keys.iter().find(|key| key.contains(":job:")).cloned() else {
        return;
    };
    let mut con = conn();
    let _: i64 = redis::cmd("HSET").arg(&job).arg("touched").arg("1").query(&mut con).unwrap();
    let _: i64 = redis::cmd("HDEL").arg(&job).arg("touched").query(&mut con).unwrap();
}

#[test]
fn idle_claim_pages_and_bad_reads_stay_available() {
    super::harness(async {
        let backend = open_backend(&[]);
        assert_eq!(backend.admin("default").queue(), "default");
        assert!(matches!(claim_to(&backend, 1, 1_000).await, ClaimResult::Idle));
        let bad = backend.admin("default").peek("paused", 1, None).await.unwrap_err();
        assert!(matches!(bad, BackendError::Page { .. }));
        assert!(bad.to_string().contains("page"));
        assert!(std::error::Error::source(&bad).is_none());
        let bad = backend.admin("default").command("not-a-uuid").await.unwrap_err();
        assert!(matches!(bad, BackendError::Page { .. }));
        let bad = backend.admin("default").peek("ready", 1, Some("v1:zz:00:1:task")).await.unwrap_err();
        assert!(matches!(bad, BackendError::Page { .. }));
        let foreign = format!("v1:{}:{}:1:{}", hex("other"), hex("ready"), ident(1).task_id);
        let bad = backend.admin("default").peek("ready", 1, Some(&foreign)).await.unwrap_err();
        assert!(matches!(bad, BackendError::Page { .. }));

        enqueue(&backend, job(301, Some("item:301"), 0)).await;
        enqueue(&backend, job(302, Some("item:302"), 0)).await;
        let mut con = conn();
        let _: i64 = redis::cmd("ZADD")
            .arg("q:{default}:ready")
            .arg(5)
            .arg(ident(301).task_id.to_string())
            .arg(5)
            .arg(ident(302).task_id.to_string())
            .query(&mut con)
            .unwrap();
        let _: i64 = redis::cmd("ZADD").arg("q:{default}:ready").arg(4).arg("not-a-uuid").query(&mut con).unwrap();
        let first = backend.admin("default").peek("ready", 1, None).await.unwrap();
        assert_eq!(first.tasks.len(), 1);
        assert!(first.tasks[0].missing_job || first.tasks[0].task_id == "not-a-uuid" || !first.tasks[0].task_id.is_empty());
        let second = backend.admin("default").peek("ready", 1, first.next_cursor.as_deref()).await.unwrap();
        assert_eq!(second.tasks.len(), 1);
        assert_ne!(second.tasks[0].task_id, first.tasks[0].task_id);

        let claimed = claim_to(&backend, 3010, 15_000).await;
        let _ = delivered(&claimed);
        let inflight = backend.admin("default").peek("inflight", 10, None).await.unwrap();
        assert_eq!(inflight.tasks.len(), 1);
        assert_eq!(inflight.tasks[0].state, "inflight");

        let ghost = ident(303).task_id.to_string();
        let _: () = redis::cmd("SET").arg(format!("q:{{default}}:job:{ghost}")).arg("x").query(&mut con).unwrap();
        let error = backend.admin("default").get(&ghost).await.unwrap_err();
        assert!(matches!(error, BackendError::Transport { .. }), "{error}");
        assert!(!backend.is_halted("default"));
        let _: i64 = redis::cmd("ZADD").arg("q:{default}:inflight").arg(9).arg(&ghost).query(&mut con).unwrap();
        let page = backend.admin("default").peek("inflight", 10, None).await.unwrap();
        assert!(page.tasks.iter().any(|task| task.task_id == ghost && task.state == "string"));
    });
}

fn hex(text: &str) -> String {
    text.bytes().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn status_receipts_replays_and_input_errors() {
    super::harness(async {
        let backend = open_backend(&[]);
        let mut headers = job(310, Some("item:310"), 0);
        headers.headers = Json::Array(vec![]);
        let error = backend.enqueue(headers).await.unwrap_err();
        assert!(matches!(error, BackendError::Input(_)));
        assert!(error.source().is_some());
        let mut kwargs = job(311, Some("item:311"), 0);
        kwargs.kwargs = Json::Array(vec![]);
        assert!(matches!(backend.enqueue(kwargs).await.unwrap_err(), BackendError::Input(_)));
        let mut overflow = job(312, Some("item:312"), u64::MAX);
        overflow.delay_ms = u64::MAX;
        assert!(matches!(backend.enqueue(overflow).await.unwrap_err(), BackendError::Input(_)));
        assert!(matches!(
            backend.cancel(ControlRequest { operation_id: op(313), queue: "NO".into(), task_id: ident(313).task_id }).await.unwrap_err(),
            BackendError::Input(_)
        ));

        let created = enqueue(&backend, job(314, Some("item:314"), 0)).await;
        let EnqueueResult::Enqueued { task_id, .. } = created else { panic!("{created:?}") };
        let other = job(315, Some("item:314"), 0);
        let duplicate = backend.enqueue(other.clone()).await.unwrap();
        assert!(matches!(duplicate, EnqueueResult::Duplicate(_)));
        let again = backend.enqueue(other).await.unwrap();
        assert!(matches!(again, EnqueueResult::Duplicate(_)));
        assert_eq!(zcard("q:{default}:ready"), 1);

        let missing = backend
            .cancel(ControlRequest { operation_id: op(316), queue: "default".into(), task_id: ident(316).task_id })
            .await
            .unwrap();
        assert!(matches!(missing, ControlResult::NotFound));
        let missing_again = backend
            .cancel(ControlRequest { operation_id: op(316), queue: "default".into(), task_id: ident(316).task_id })
            .await
            .unwrap();
        assert!(matches!(missing_again, ControlResult::NotFound));
        assert!(!backend.is_halted("default"));

        let claimed = claim_to(&backend, 314, 15_000).await;
        let _ = delivered(&claimed);
        backend
            .finish(FinishRequest {
                operation_id: op(317),
                message_id: message(314),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(314),
                delivery_token: token(314),
                proposal: outcome(r#"{"outcome":"ok"}"#),
            })
            .await
            .unwrap();
        let terminal = backend
            .cancel(ControlRequest { operation_id: op(318), queue: "default".into(), task_id })
            .await
            .unwrap();
        assert!(matches!(terminal, ControlResult::AlreadyTerminal), "{terminal:?}");
        let page = backend.admin("default").dead(1, None).await.unwrap();
        assert!(page.tasks.is_empty());
        let done = backend.admin("default").get(&task_id.to_string()).await.unwrap().unwrap();
        assert_eq!(done.state, "done");

        let mut con = conn();
        let receipt = format!("q:{{default}}:receipt:{}", op(314));
        let _: () = redis::cmd("SET").arg(&receipt).arg("{").query(&mut con).unwrap();
        let error = backend.enqueue(job(314, Some("item:314"), 0)).await.unwrap_err();
        assert!(matches!(error, BackendError::Inconsistent(_)), "{error}");
        assert!(!backend.is_halted("default"));
    });
}

#[test]
fn execabort_and_lost_reply_branches() {
    super::harness(async {
        let limited = open_backend(&[("ARQ_WATCH_RETRIES", "1")]);
        limited.set_fault(Fault { exec: ExecFault::AbortAlways, ..Fault::default() });
        let error = limited.enqueue(job(320, Some("item:320"), 0)).await.unwrap_err();
        assert!(matches!(error, BackendError::Transport { .. }), "{error}");
        assert!(error.to_string().contains("EXECABORT"));
        assert!(!limited.is_halted("default"));
        let backend = open_backend(&[]);
        enqueue(&backend, job(320, Some("item:320"), 0)).await;

        backend.set_fault(Fault { exec: ExecFault::AbortOnce, ..Fault::default() });
        let claimed = claim_to(&backend, 320, 15_000).await;
        let observed = delivered(&claimed);
        let task_id = observed.task_id.unwrap();
        backend.set_fault(Fault { exec: ExecFault::AbortOnce, ..Fault::default() });
        let renewed = backend
            .renew(arq_redis::RenewRequest {
                operation_id: op(321),
                message_id: message(320),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(320),
                delivery_token: token(320),
                lease_ms: 15_000,
            })
            .await
            .unwrap();
        assert!(matches!(renewed, arq_redis::RenewResult::Renewed { replayed: false, .. }));
        backend.set_fault(Fault { exec: ExecFault::AbortOnce, ..Fault::default() });
        let finished = backend
            .finish(FinishRequest {
                operation_id: op(322),
                message_id: message(320),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(320),
                delivery_token: token(320),
                proposal: outcome(r#"{"outcome":"retry","delay_ms":0}"#),
            })
            .await
            .unwrap();
        assert!(matches!(finished, FinishResult::Finished { replayed: false }));

        let once = Arc::new(AtomicBool::new(false));
        let flag = once.clone();
        backend.set_fault(Fault {
            exec: ExecFault::LoseReply,
            before_exec: Some(Arc::new(move |keys| touch_receipt_once(&flag, keys))),
            ..Fault::default()
        });
        let created = enqueue(&backend, job(323, Some("item:323"), 0)).await;
        assert!(matches!(created, EnqueueResult::Enqueued { .. }), "{created:?}");
        assert_eq!(hget(&job_key(323), "state").as_deref(), Some("ready"));

        backend.set_fault(Fault {
            exec: ExecFault::LoseReply,
            before_exec: Some(Arc::new(dirty_job)),
            ..Fault::default()
        });
        let lost = backend
            .claim(arq_redis::ClaimRequest {
                operation_id: op(324),
                message_id: message(324),
                queue: "default".into(),
                worker_id: "worker-324".into(),
                proposed_token: token(324),
                lease_ms: 15_000,
            })
            .await
            .unwrap_err();
        assert!(matches!(lost, BackendError::Indeterminate(_)), "{lost}");
        assert!(lost.to_string().contains("indeterminate"));
        assert_eq!(hget(&job_key(323), "state").as_deref(), Some("ready"));
        backend.set_fault(Fault::default());

        let claimed = claim_to(&backend, 323, 15_000).await;
        let task_id = delivered(&claimed).task_id.unwrap();
        backend.set_fault(Fault {
            exec: ExecFault::LoseReply,
            before_exec: Some(Arc::new(|keys| {
                let Some(job) = keys.iter().find(|key| key.contains(":job:")).cloned() else { return };
                let mut con = conn();
                let _: i64 = redis::cmd("HSET").arg(&job).arg("delivery_token").arg(token(399).to_string()).query(&mut con).unwrap();
            })),
            ..Fault::default()
        });
        let rejected = backend
            .finish(FinishRequest {
                operation_id: op(325),
                message_id: message(323),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(323),
                delivery_token: token(323),
                proposal: outcome(r#"{"outcome":"ok"}"#),
            })
            .await
            .unwrap();
        assert!(matches!(rejected, FinishResult::Rejected), "{rejected:?}");
    });
}

#[test]
fn wrong_receipt_type_is_not_a_queue_halt() {
    super::harness(async {
        let backend = open_backend(&[]);
        let receipt = format!("q:{{default}}:receipt:{}", op(330));
        let mut con = conn();
        let _: i64 = redis::cmd("RPUSH").arg(&receipt).arg("x").query(&mut con).unwrap();
        let error = backend.enqueue(job(330, Some("item:330"), 0)).await.unwrap_err();
        assert!(matches!(error, BackendError::Inconsistent(_)), "{error}");
        assert!(error.to_string().contains("type"));
        assert!(!backend.is_halted("default"));
        let fresh = open_backend(&[]);
        fresh.set_fault(Fault { cross_slot_watch: Some("outside".into()), ..Fault::default() });
        let created = enqueue(&fresh, job(331, Some("item:331"), 0)).await;
        assert!(matches!(created, EnqueueResult::Enqueued { .. }));
        assert!(!fresh.is_halted("default"));
    });
}

#[test]
fn maintenance_covers_dead_conflict_abort_and_lost_reply() {
    super::harness(async {
        let backend = open_backend(&[("ARQ_MAINTENANCE_BATCH", "4"), ("ARQ_WATCH_RETRIES", "1")]);
        let mut queued = job(340, Some("item:340"), 0);
        queued.tag = Some("batch-a".into());
        let EnqueueResult::Enqueued { task_id, .. } = enqueue(&backend, queued).await else { panic!("enqueue") };
        backend
            .cancel(ControlRequest { operation_id: op(3400), queue: "default".into(), task_id })
            .await
            .unwrap();
        let mut con = conn();
        let _: i64 = redis::cmd("ZADD").arg("q:{default}:dead").arg(0).arg(task_id.to_string()).query(&mut con).unwrap();
        let _: i64 = redis::cmd("ZADD").arg("q:{default}:dead").arg(0).arg("not-a-uuid").query(&mut con).unwrap();
        let _: i64 = redis::cmd("HDEL").arg(job_key(340)).arg("history").query(&mut con).unwrap();
        let report = backend.maintain("default").await.unwrap();
        assert_eq!(report.removed, 1);
        assert!(report.examined >= 1);
        assert!(hget(&job_key(340), "state").is_none());
        assert_eq!(key_type("q:{default}:dead:tag:batch-a"), "none");

        let EnqueueResult::Enqueued { task_id, .. } = enqueue(&backend, job(341, Some("item:341"), 0)).await else { panic!("enqueue") };
        let claimed = claim_to(&backend, 341, 15_000).await;
        let _ = delivered(&claimed);
        backend
            .finish(FinishRequest {
                operation_id: op(3410),
                message_id: message(341),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(341),
                delivery_token: token(341),
                proposal: outcome(r#"{"outcome":"skipped"}"#),
            })
            .await
            .unwrap();
        let _: i64 = redis::cmd("ZADD").arg("q:{default}:done").arg(0).arg(task_id.to_string()).query(&mut con).unwrap();
        backend.set_fault(Fault { before_exec: Some(Arc::new(dirty_job)), ..Fault::default() });
        let error = backend.maintain("default").await.unwrap_err();
        assert!(matches!(error, BackendError::Transaction(_)), "{error}");
        assert_eq!(hget(&job_key(341), "state").as_deref(), Some("done"));

        backend.set_fault(Fault { exec: ExecFault::AbortAlways, ..Fault::default() });
        let error = backend.maintain("default").await.unwrap_err();
        assert!(matches!(error, BackendError::Transport { .. }), "{error}");
        assert!(!backend.is_halted("default"));

        backend.set_fault(Fault { exec: ExecFault::LoseReply, ..Fault::default() });
        let report = backend.maintain("default").await.unwrap();
        assert_eq!(report.removed, 1);
        assert!(hget(&job_key(341), "state").is_none());

        let EnqueueResult::Enqueued { task_id, .. } = enqueue(&backend, job(342, Some("item:342"), 0)).await else { panic!("enqueue") };
        backend
            .cancel(ControlRequest { operation_id: op(3420), queue: "default".into(), task_id })
            .await
            .unwrap();
        let _: i64 = redis::cmd("ZADD").arg("q:{default}:dead").arg(0).arg(task_id.to_string()).query(&mut con).unwrap();
        backend.set_fault(Fault {
            before_exec: Some(Arc::new(|_| {
                let mut con = conn();
                let _: i64 = redis::cmd("DEL").arg("q:{default}:dead").query(&mut con).unwrap();
                let _: () = redis::cmd("SET").arg("q:{default}:dead").arg("string").query(&mut con).unwrap();
            })),
            ..Fault::default()
        });
        let error = backend.maintain("default").await.unwrap_err();
        assert!(matches!(error, BackendError::Inconsistent(_)), "{error}");
        assert!(backend.is_halted("default"));
    });
}

#[test]
fn kill_ready_settle_replay_and_registry_skips_garbage() {
    super::harness(async {
        let backend = open_backend(&[]);
        let EnqueueResult::Enqueued { task_id, .. } = enqueue(&backend, job(350, Some("item:350"), 0)).await else { panic!("enqueue") };
        let killed = backend
            .kill(ControlRequest { operation_id: op(3500), queue: "default".into(), task_id })
            .await
            .unwrap();
        assert!(matches!(killed, ControlResult::Accepted { .. }));
        assert_eq!(hget(&job_key(350), "reason").as_deref(), Some("killed"));
        assert_eq!(hget(&job_key(350), "state").as_deref(), Some("dead"));
        let mut second = job(354, Some("item:354"), 0);
        second.tag = Some("batch-b".into());
        let EnqueueResult::Enqueued { task_id: other, .. } = enqueue(&backend, second).await else { panic!("enqueue") };
        backend
            .kill(ControlRequest { operation_id: op(3540), queue: "default".into(), task_id: other })
            .await
            .unwrap();
        let dead = backend.admin("default").dead(1, None).await.unwrap();
        assert_eq!(dead.tasks.len(), 1);
        let cursor = dead.next_cursor.expect("dead page continues");
        let next = backend.admin("default").dead(1, Some(&cursor)).await.unwrap();
        assert_eq!(next.tasks.len(), 1);
        assert_ne!(next.tasks[0].task_id, dead.tasks[0].task_id);

        super::flush();
        let EnqueueResult::Enqueued { task_id, .. } = enqueue(&backend, job(351, Some("item:351"), 0)).await else { panic!("enqueue") };
        let claimed = claim_to(&backend, 351, 200).await;
        let _ = delivered(&claimed);
        backend
            .kill(ControlRequest { operation_id: op(3510), queue: "default".into(), task_id })
            .await
            .unwrap();
        std::thread::sleep(Duration::from_millis(250));
        backend.set_fault(Fault { exec: ExecFault::LoseReply, ..Fault::default() });
        let settled = backend
            .settle(ControlRequest { operation_id: op(352), queue: "default".into(), task_id })
            .await
            .unwrap();
        assert!(matches!(settled, ControlResult::Accepted { replayed: true, .. }), "{settled:?}");
        backend.set_fault(Fault::default());
        let replayed = backend
            .settle(ControlRequest { operation_id: op(352), queue: "default".into(), task_id })
            .await
            .unwrap();
        assert!(matches!(replayed, ControlResult::Accepted { replayed: true, .. }));
        let not_dead = backend
            .requeue(RequeueRequest {
                operation_id: op(353),
                queue: "default".into(),
                task_id: ident(399).task_id,
                new_cycle_id: cycle(399),
            })
            .await
            .unwrap();
        assert!(matches!(not_dead, RequeueResult::NotFound));

        let mut con = conn();
        let _: i64 = redis::cmd("ZADD").arg("arq:supervisors").arg(1).arg("noperiod").query(&mut con).unwrap();
        let _: i64 = redis::cmd("ZADD").arg("arq:supervisors").arg(1).arg("worker.local.8").query(&mut con).unwrap();
        let _: () = redis::cmd("SET").arg("arq:supervisor:worker.local.5:child:nope").arg("x").query(&mut con).unwrap();
        let snap = backend.admin("default").snapshot(Some("other-host")).await.unwrap();
        assert!(snap.supervisors.is_empty());
        assert!(snap.orphans.is_empty());
        let _ = get("q:{default}:uniq:item:351");
    });
}

fn cli(port: u16, args: &[&str]) -> String {
    let output = Command::new("redis-cli").arg("-p").arg(port.to_string()).args(args).output().expect("redis-cli");
    let mut text = String::from_utf8(output.stdout).unwrap_or_default();
    text.push_str(&String::from_utf8(output.stderr).unwrap_or_default());
    text
}

fn ensure_cluster() {
    for port in [7000_u16, 7001, 7002] {
        if cli(port, &["PING"]).trim() == "PONG" {
            continue;
        }
        let dir = format!("/tmp/rc/{port}");
        let _ = std::fs::create_dir_all(&dir);
        let status = Command::new("redis-server")
            .args([
                "--port",
                &port.to_string(),
                "--bind",
                "127.0.0.1",
                "--protected-mode",
                "no",
                "--cluster-enabled",
                "yes",
                "--cluster-config-file",
                &format!("{dir}/nodes.conf"),
                "--cluster-node-timeout",
                "3000",
                "--cluster-announce-ip",
                "127.0.0.1",
                "--cluster-announce-port",
                &port.to_string(),
                "--appendonly",
                "no",
                "--save",
                "",
                "--daemonize",
                "yes",
                "--dir",
                &dir,
                "--dbfilename",
                "dump.rdb",
                "--logfile",
                &format!("{dir}/redis.log"),
                "--pidfile",
                &format!("{dir}/redis.pid"),
            ])
            .status()
            .expect("redis-server");
        assert!(status.success());
    }
    if !cli(7000, &["CLUSTER", "INFO"]).contains("cluster_state:ok") {
        let status = Command::new("redis-cli")
            .args(["--cluster", "create", "127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002", "--cluster-replicas", "0", "--cluster-yes"])
            .status()
            .expect("cluster create");
        assert!(status.success(), "redis cluster did not form");
    }
}

fn force_stable(slot: u16) {
    for _ in 0..5 {
        for port in [7000_u16, 7001, 7002] {
            cli(port, &["CLUSTER", "SETSLOT", &slot.to_string(), "STABLE"]);
            cli(port, &["FLUSHALL"]);
        }
        let owner = slot_owner(slot);
        let reply = cli(owner, &["ZCARD", "q:{default}:ready"]);
        if !reply.contains("ASK") && !reply.contains("MOVED") {
            return;
        }
    }
    panic!("slot {slot} stayed migrating:\n{}", cli(7001, &["CLUSTER", "NODES"]));
}

fn slot_owner(slot: u16) -> u16 {
    for port in [7000_u16, 7001, 7002] {
        let nodes = cli(port, &["CLUSTER", "NODES"]);
        let myself = nodes.lines().find(|line| line.contains("myself")).unwrap_or("");
        if myself.split_whitespace().any(|part| range_has(part, slot)) {
            return port;
        }
    }
    panic!("no cluster node owns slot {slot}");
}

fn range_has(part: &str, slot: u16) -> bool {
    if let Some((start, end)) = part.split_once('-') {
        let Ok(start) = start.parse::<u16>() else { return false };
        let Ok(end) = end.parse::<u16>() else { return false };
        return (start..=end).contains(&slot);
    }
    part.parse::<u16>().ok() == Some(slot)
}

fn node_id(port: u16) -> String {
    cli(port, &["CLUSTER", "NODES"])
        .lines()
        .find(|line| line.contains("myself"))
        .and_then(|line| line.split_whitespace().next())
        .unwrap_or("")
        .to_string()
}

fn exists_on(port: u16, key: &str) -> bool {
    cli(port, &["EXISTS", key]).trim() == "1"
}

#[test]
fn cluster_follows_moved_ask_and_crossslot() {
    super::harness(async {
        ensure_cluster();
        let slot: u16 = cli(7000, &["CLUSTER", "KEYSLOT", "q:{default}:ready"]).trim().parse().expect("slot");
        force_stable(slot);
        let owner = slot_owner(slot);
        let other = [7000_u16, 7001, 7002].into_iter().find(|port| *port != owner).unwrap();
        let backend = RedisBackend::connect(&format!("redis://redis:{other}"), config(&[])).unwrap();
        let created = backend.enqueue(job(360, Some("item:360"), 0)).await.expect("moved enqueue");
        let EnqueueResult::Enqueued { task_id, .. } = created else { panic!("{created:?}") };
        let key = format!("q:{{default}}:job:{task_id}");
        assert!(exists_on(owner, &key), "job should land on the slot owner {owner}");
        assert!(!exists_on(other, &key), "redirect must not leave the job on {other}");

        for port in [7000_u16, 7001, 7002] {
            cli(port, &["FLUSHALL"]);
            cli(port, &["CLUSTER", "SETSLOT", &slot.to_string(), "STABLE"]);
        }
        let importer = other;
        let migrating = cli(owner, &["CLUSTER", "SETSLOT", &slot.to_string(), "MIGRATING", &node_id(importer)]);
        let importing = cli(importer, &["CLUSTER", "SETSLOT", &slot.to_string(), "IMPORTING", &node_id(owner)]);
        assert!(migrating.contains("OK") && importing.contains("OK"), "migrating={migrating} importing={importing}");
        let backend = RedisBackend::connect(&format!("redis://redis:{owner}"), config(&[])).unwrap();
        let missing = ident(361).task_id.to_string();
        let found = backend.admin("default").get(&missing).await.expect("ask read");
        assert!(found.is_none(), "ASK should answer a missing job from the importing node");
        let writing = RedisBackend::connect(&format!("redis://redis:{owner}"), config(&[("ARQ_WATCH_RETRIES", "1")])).unwrap();
        let error = writing.enqueue(job(361, Some("item:361"), 0)).await.unwrap_err();
        let detail = error.to_string();
        assert!(
            detail.contains("TryAgain") || detail.contains("rehashing"),
            "multi-key WATCH during slot migration is TRYAGAIN, got {detail}"
        );
        assert!(!writing.is_halted("default"), "TRYAGAIN must not halt the queue");
        for port in [7000_u16, 7001, 7002] {
            cli(port, &["CLUSTER", "SETSLOT", &slot.to_string(), "STABLE"]);
            cli(port, &["FLUSHALL"]);
        }

        let backend = RedisBackend::connect(&format!("redis://redis:{owner}"), config(&[])).unwrap();
        backend.set_fault(Fault { cross_slot_watch: Some("outside".into()), ..Fault::default() });
        let error = backend.enqueue(job(362, Some("item:362"), 0)).await.unwrap_err();
        assert!(matches!(error, BackendError::Inconsistent(_)), "{error}");
        assert!(error.to_string().contains("CROSSSLOT"));
        assert!(backend.is_halted("default"));
        let _ = URL;
    });
}

#[test]
fn decision_arms_pages_and_lost_reply_classification() {
    super::harness(async {
        let backend = open_backend(&[]);
        let mut rejected = job(370, None, 0);
        rejected.time_limit_ms = 0;
        assert!(matches!(backend.enqueue(rejected).await.unwrap_err(), BackendError::Input(_)));

        let mut con = conn();
        let seeded = job_key(371);
        let _: i64 = redis::cmd("HSET")
            .arg(&seeded)
            .arg("state")
            .arg("ready")
            .arg("actor_name")
            .arg("other")
            .arg("task_id")
            .arg(ident(371).task_id.to_string())
            .query(&mut con)
            .unwrap();
        let error = backend.enqueue(job(371, None, 0)).await.unwrap_err();
        assert!(matches!(error, BackendError::Indeterminate(_)), "{error}");
        assert!(!backend.is_halted("default"));

        let created = enqueue(&backend, job(372, None, 0)).await;
        let EnqueueResult::Enqueued { task_id, .. } = created else { panic!("{created:?}") };
        let error = backend
            .claim(ClaimRequest {
                operation_id: op(372),
                message_id: message(372),
                queue: "default".into(),
                worker_id: "worker-372".into(),
                proposed_token: token(372),
                lease_ms: 15_000,
            })
            .await
            .unwrap_err();
        assert!(matches!(error, BackendError::Conflict(_)), "{error}");
        let error = backend
            .renew(RenewRequest {
                operation_id: op(372),
                message_id: message(372),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(372),
                delivery_token: token(372),
                lease_ms: 15_000,
            })
            .await
            .unwrap_err();
        assert!(matches!(error, BackendError::Conflict(_)), "{error}");
        let error = backend
            .finish(FinishRequest {
                operation_id: op(372),
                message_id: message(372),
                queue: "default".into(),
                task_id,
                cycle_id: cycle(372),
                delivery_token: token(372),
                proposal: outcome(r#"{"outcome":"ok"}"#),
            })
            .await
            .unwrap_err();
        assert!(matches!(error, BackendError::Conflict(_)), "{error}");

        let due = enqueue(&backend, job(373, None, 0)).await;
        let EnqueueResult::Enqueued { task_id, .. } = due else { panic!("{due:?}") };
        let error = backend
            .claim(ClaimRequest {
                operation_id: op(3730),
                message_id: message(373),
                queue: "default".into(),
                worker_id: "worker-373".into(),
                proposed_token: token(373),
                lease_ms: 0,
            })
            .await
            .unwrap_err();
        assert!(matches!(error, BackendError::Input(_)), "{error}");

        let mut only = job(374, None, 0);
        only.queue = "edgeq".into();
        let scheduled = enqueue(&backend, only).await;
        let EnqueueResult::Enqueued { task_id: later, .. } = scheduled else { panic!("{scheduled:?}") };
        let later_key = format!("q:{{edgeq}}:job:{later}");
        let _: i64 = redis::cmd("HSET").arg(&later_key).arg("available_at_ms").arg("9000000000000").query(&mut con).unwrap();
        let _: i64 = redis::cmd("ZADD").arg("q:{edgeq}:inflight").arg(0).arg(later.to_string()).query(&mut con).unwrap();
        let idle = backend
            .claim(ClaimRequest {
                operation_id: op(3740),
                message_id: message(374),
                queue: "edgeq".into(),
                worker_id: "worker-374".into(),
                proposed_token: token(374),
                lease_ms: 15_000,
            })
            .await
            .unwrap();
        assert!(matches!(idle, ClaimResult::Idle), "{idle:?}");

        for index in 0..130 {
            let member = ident(5_000 + index).task_id.to_string();
            let _: i64 = redis::cmd("ZADD").arg("q:{pageq}:ready").arg(1).arg(member).query(&mut con).unwrap();
        }
        let page = backend.admin("pageq").peek("ready", 200, None).await.unwrap();
        assert!(page.tasks.len() > 128, "wide page len {}", page.tasks.len());
        assert!(backend.admin("pageq").peek("ready", 1, Some("v1")).await.is_err());
        assert!(backend.admin("pageq").peek("ready", 1, Some("v1:0:3030:1:task")).await.is_err());

        let operation = op(380).to_string();
        let key = format!("q:{{default}}:receipt:{operation}");
        let _: () = redis::cmd("SET")
            .arg(&key)
            .arg(r#"{"operation_id":"op","cycle_id":4,"result":{"status":"enqueued"}}"#)
            .query(&mut con)
            .unwrap();
        let stored = backend.admin("default").command(&operation).await.unwrap().unwrap();
        assert!(stored.cycle_id.is_none());
        let _: () = redis::cmd("SET").arg(&key).arg(r#"{"operation_id":"op","result":{}}"#).query(&mut con).unwrap();
        assert!(matches!(backend.admin("default").command(&operation).await.unwrap_err(), BackendError::Transport { .. }));

        let corrupt = open_backend(&[]);
        let raw = format!(
            r#"{{"operation_id":"{}","task_id":"{}","op":"enqueue","cycle_id":null,"request":{{}},"result":{{"status":"enqueued"}}}}"#,
            op(9999),
            ident(381).task_id
        );
        let _: () = redis::cmd("SET")
            .arg(format!("q:{{default}}:receipt:{}", op(381)))
            .arg(raw)
            .query(&mut con)
            .unwrap();
        let error = corrupt.enqueue(job(381, None, 0)).await.unwrap_err();
        assert!(matches!(error, BackendError::Inconsistent(_)), "{error}");
        assert!(!corrupt.is_halted("default"));

        let _: i64 = redis::cmd("ZADD").arg("arq:supervisors").arg(1).arg("worker.local.8").query(&mut con).unwrap();
        let _: i64 = redis::cmd("HSET")
            .arg("arq:supervisor:worker.local.9:child:1")
            .arg("host")
            .arg("worker.local")
            .arg("slot")
            .arg("1")
            .arg("alive")
            .arg("0")
            .arg("state")
            .arg("exited")
            .query(&mut con)
            .unwrap();
        let snap = backend.admin("default").snapshot(None).await.unwrap();
        assert!(snap.supervisors.is_empty());
        assert!(!snap.orphans.is_empty());
        let filtered = backend.admin("default").snapshot(Some("other-host")).await.unwrap();
        assert!(filtered.orphans.is_empty());

        let killed = enqueue(&backend, job(382, Some("item:382"), 0)).await;
        let EnqueueResult::Enqueued { task_id, .. } = killed else { panic!("{killed:?}") };
        backend.kill(ControlRequest { operation_id: op(3820), queue: "default".into(), task_id }).await.unwrap();
        let _: () = redis::cmd("SET").arg("q:{default}:uniq:item:382").arg(ident(1).task_id.to_string()).query(&mut con).unwrap();
        let _: i64 = redis::cmd("ZADD").arg("q:{default}:dead").arg(0).arg(task_id.to_string()).query(&mut con).unwrap();
        let report = backend.maintain("default").await.unwrap();
        assert!(report.removed >= 1, "{report:?}");

        let limited = open_backend(&[("ARQ_MAX_READY", "2"), ("ARQ_WATCH_RETRIES", "1")]);
        limited.set_fault(Fault {
            exec: ExecFault::LoseReply,
            before_exec: Some(Arc::new(|keys| {
                let Some(receipt) = keys.iter().find(|key| key.contains(":receipt:")).cloned() else { return };
                let mut con = conn();
                let _: () = redis::cmd("SET").arg(&receipt).arg("x").query(&mut con).unwrap();
                let _: i64 = redis::cmd("DEL").arg(&receipt).query(&mut con).unwrap();
                let _: i64 = redis::cmd("ZADD").arg("q:{fullq}:ready").arg(1).arg("a").query(&mut con).unwrap();
                let _: i64 = redis::cmd("ZADD").arg("q:{fullq}:ready").arg(1).arg("b").query(&mut con).unwrap();
            })),
            ..Fault::default()
        });
        let mut full = job(383, None, 0);
        full.queue = "fullq".into();
        let error = limited.enqueue(full).await.unwrap_err();
        assert!(matches!(error, BackendError::Full(_)), "{error}");

        let racing = open_backend(&[("ARQ_WATCH_RETRIES", "1")]);
        enqueue(&racing, job(384, Some("item:384"), 0)).await;
        let flag = Arc::new(AtomicBool::new(false));
        let once = flag.clone();
        racing.set_fault(Fault {
            exec: ExecFault::LoseReply,
            before_exec: Some(Arc::new(move |keys| touch_receipt_once(&once, keys))),
            ..Fault::default()
        });
        let duplicate = racing.enqueue(job(385, Some("item:384"), 0)).await.unwrap();
        assert!(matches!(duplicate, EnqueueResult::Duplicate(_)), "{duplicate:?}");

        racing.set_fault(Fault {
            exec: ExecFault::LoseReply,
            before_exec: Some(Arc::new(|keys| {
                let Some(job) = keys.iter().find(|key| key.contains(":job:")).cloned() else { return };
                let mut con = conn();
                let _: i64 = redis::cmd("HSET").arg(job).arg("state").arg("ready").arg("actor_name").arg("other").query(&mut con).unwrap();
            })),
            ..Fault::default()
        });
        let error = racing.enqueue(job(386, None, 0)).await.unwrap_err();
        assert!(matches!(error, BackendError::Indeterminate(_)), "{error}");

        let operation = op(387);
        let task = ident(387).task_id.to_string();
        let raw = format!(
            r#"{{"operation_id":"{operation}","task_id":"{task}","op":"claim","cycle_id":null,"request":{{}},"result":{{"status":"claimed"}}}}"#
        );
        racing.set_fault(Fault {
            exec: ExecFault::LoseReply,
            before_exec: Some(Arc::new(move |keys| {
                let Some(receipt) = keys.iter().find(|key| key.contains(":receipt:")).cloned() else { return };
                let mut con = conn();
                let _: () = redis::cmd("SET").arg(receipt).arg(&raw).query(&mut con).unwrap();
            })),
            ..Fault::default()
        });
        let error = racing.enqueue(job(387, None, 0)).await.unwrap_err();
        assert!(matches!(error, BackendError::Conflict(_)), "{error}");

        racing.set_fault(Fault::default());
        let claimed = claim_to(&racing, 384, 15_000).await;
        let owned = delivered(&claimed).task_id.unwrap();
        racing.set_fault(Fault {
            exec: ExecFault::LoseReply,
            before_exec: Some(Arc::new(|keys| {
                let Some(job) = keys.iter().find(|key| key.contains(":job:")).cloned() else { return };
                let mut con = conn();
                let _: i64 = redis::cmd("HSET").arg(&job).arg("state").arg("ready").query(&mut con).unwrap();
                let _: i64 = redis::cmd("HDEL").arg(job).arg("lease_deadline_ms").query(&mut con).unwrap();
            })),
            ..Fault::default()
        });
        let renewed = racing
            .renew(RenewRequest {
                operation_id: op(388),
                message_id: message(384),
                queue: "default".into(),
                task_id: owned,
                cycle_id: cycle(384),
                delivery_token: token(384),
                lease_ms: 15_000,
            })
            .await
            .unwrap();
        assert!(matches!(renewed, arq_redis::RenewResult::NotHeld { .. }), "{renewed:?}");
    });
}
