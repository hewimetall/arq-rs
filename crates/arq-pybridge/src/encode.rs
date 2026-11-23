//! JSON replies the Python package turns into exceptions and dataclasses.

use arq_core::Json;
use arq_redis::{
    ChildView, ControlResult, EnqueueResult, FinishResult, QueueDepth, RequeueResult, Snapshot, StoredReceipt,
    SupervisorView, TaskInfo, TaskPage,
};

use crate::load::BridgeError;

pub fn error_response(error: &BridgeError) -> String {
    let body = match error {
        BridgeError::Input(detail) | BridgeError::Config(detail) | BridgeError::Transport(detail) | BridgeError::Page(detail) => {
            obj(&[("kind", text(error.kind())), ("detail", text(detail))])
        }
        BridgeError::Full { queue, max_ready, ready_zcard } => obj(&[
            ("kind", text("full")),
            ("queue", text(queue)),
            ("max_ready", num(*max_ready)),
            ("ready_zcard", num(*ready_zcard)),
        ]),
        BridgeError::Conflict { operation_id, detail } => {
            obj(&[("kind", text("conflict")), ("operation_id", text(operation_id)), ("detail", text(detail))])
        }
        BridgeError::Inconsistent { queue, detail } => {
            obj(&[("kind", text("inconsistent")), ("queue", text(queue)), ("detail", text(detail))])
        }
        BridgeError::Halted { queue } => obj(&[("kind", text("halted")), ("queue", text(queue))]),
        BridgeError::Indeterminate { operation_id, message_id } => {
            obj(&[("kind", text("indeterminate")), ("operation_id", text(operation_id)), ("message_id", text(message_id))])
        }
        BridgeError::Closed => obj(&[("kind", text("closed")), ("detail", text("client is closed"))]),
    };
    obj(&[("error", body)]).canonical()
}

pub fn submit_response(result: &EnqueueResult) -> String {
    match result {
        EnqueueResult::Enqueued { task_id, cycle_id, replayed } => obj(&[
            ("status", text("enqueued")),
            ("message_id", text(task_id.to_string())),
            ("task_id", text(task_id.to_string())),
            ("cycle_id", text(cycle_id.to_string())),
            ("replayed", Json::Bool(*replayed)),
        ])
        .canonical(),
        EnqueueResult::Duplicate(error) => obj(&[
            ("status", text("duplicate")),
            ("unique_key", text(&error.unique_key)),
            ("task_id", text(error.task_id.to_string())),
            ("owner_task_id", text(error.owner_task_id.to_string())),
        ])
        .canonical(),
    }
}

pub fn control_response(operation_id: &str, task_id: &str, result: &ControlResult) -> String {
    let (status, cycle_id) = match result {
        ControlResult::Accepted { cycle_id, .. } => ("accepted", cycle_id.map(|id| id.to_string())),
        ControlResult::AlreadyTerminal => ("already_terminal", None),
        ControlResult::NotFound => ("not_found", None),
        ControlResult::StillOwned => ("still_owned", None),
        ControlResult::NotBlocked => ("not_blocked", None),
    };
    receipt(operation_id, task_id, status, cycle_id.as_deref())
}

pub fn requeue_response(operation_id: &str, task_id: &str, result: &RequeueResult) -> String {
    let (status, cycle_id) = match result {
        RequeueResult::Requeued { cycle_id, .. } => ("requeued", Some(cycle_id.to_string())),
        RequeueResult::Duplicate(_) => ("duplicate", None),
        RequeueResult::NotDead => ("not_dead", None),
        RequeueResult::NotFound => ("not_found", None),
    };
    receipt(operation_id, task_id, status, cycle_id.as_deref())
}

pub fn finish_response(outcome: &str, result: &FinishResult) -> String {
    match result {
        FinishResult::Finished { replayed } => obj(&[
            ("status", text("finished")),
            ("outcome", text(outcome)),
            ("replayed", Json::Bool(*replayed)),
        ])
        .canonical(),
        FinishResult::Rejected => obj(&[("status", text("rejected")), ("outcome", text(outcome))]).canonical(),
    }
}

pub fn abandoned_response() -> String {
    obj(&[("status", text("abandoned"))]).canonical()
}

pub fn task_response(queue: &str, info: Option<&TaskInfo>) -> String {
    match info {
        None => obj(&[("task", Json::Null)]).canonical(),
        Some(info) => obj(&[("task", task_json(queue, info))]).canonical(),
    }
}

pub fn page_response(queue: &str, page: &TaskPage) -> String {
    let tasks = page.tasks.iter().map(|info| task_json(queue, info)).collect();
    obj(&[
        ("tasks", Json::Array(tasks)),
        ("next_cursor", opt_text(page.next_cursor.as_deref())),
    ])
    .canonical()
}

pub fn command_response(receipt: Option<&StoredReceipt>) -> String {
    match receipt {
        None => obj(&[("receipt", Json::Null)]).canonical(),
        Some(receipt) => obj(&[(
            "receipt",
            obj(&[
                ("operation_id", text(&receipt.operation_id)),
                ("task_id", text(&receipt.task_id)),
                ("op", text(&receipt.op)),
                ("status", text(&receipt.status)),
                ("cycle_id", opt_text(receipt.cycle_id.as_deref())),
            ]),
        )])
        .canonical(),
    }
}

pub fn snapshot_response(snapshot: &Snapshot) -> String {
    let queues = snapshot.queues.iter().map(depth_json).collect();
    let supervisors = snapshot.supervisors.iter().map(supervisor_json).collect();
    let orphans = snapshot.orphans.iter().map(child_json).collect();
    obj(&[
        ("taken_at", num(snapshot.taken_at_ms)),
        ("queues", Json::Array(queues)),
        ("supervisors", Json::Array(supervisors)),
        ("orphans", Json::Array(orphans)),
    ])
    .canonical()
}

fn receipt(operation_id: &str, task_id: &str, status: &str, cycle_id: Option<&str>) -> String {
    obj(&[
        ("operation_id", text(operation_id)),
        ("task_id", text(task_id)),
        ("status", text(status)),
        ("cycle_id", opt_text(cycle_id)),
    ])
    .canonical()
}

fn task_json(queue: &str, info: &TaskInfo) -> Json {
    obj(&[
        ("task_id", text(&info.task_id)),
        ("message_id", opt_text(info.message_id.as_deref())),
        ("state", text(&info.state)),
        ("deliveries", num(info.deliveries)),
        ("retries", num(info.retries)),
        ("score", opt_num(info.score)),
        ("missing_job", Json::Bool(info.missing_job)),
        ("tag", opt_text(info.tag.as_deref())),
        ("reason", text(&info.reason)),
        ("error", text(&info.error)),
        ("cycle_id", opt_text(info.cycle_id.as_deref())),
        ("control_op", text(&info.control_op)),
        ("actor_name", text(&info.actor_name)),
        ("unique_key", opt_text(info.unique_key.as_deref())),
        ("lease_deadline_ms", opt_num(info.lease_deadline_ms)),
        ("started_at_ms", opt_num(info.started_at_ms)),
        ("available_at_ms", opt_num(info.available_at_ms)),
        ("stop_evidence", text(&info.stop_evidence)),
        ("queue", text(queue)),
    ])
}

fn depth_json(depth: &QueueDepth) -> Json {
    obj(&[
        ("name", text(&depth.queue)),
        ("ready", num(depth.ready)),
        ("scheduled", num(depth.scheduled)),
        ("inflight", num(depth.inflight)),
        ("done", num(depth.done)),
        ("dead", num(depth.dead)),
        ("ready_zcard", num(depth.ready_zcard)),
    ])
}

fn child_json(child: &ChildView) -> Json {
    obj(&[
        ("host", text(&child.host)),
        ("parent_pid", num(u64::from(child.parent_pid))),
        ("slot", num(u64::from(child.slot))),
        ("pid", num(u64::from(child.pid))),
        ("ppid", num(u64::from(child.ppid))),
        ("state", text(&child.state)),
        ("reason", text(&child.reason)),
        ("exit_code", opt_i32(child.exit_code)),
        ("rss_bytes", num(child.rss_bytes)),
        ("started_at_ms", num(child.started_at_ms)),
        ("ended_at_ms", opt_num(child.ended_at_ms)),
        ("tasks_done", num(child.tasks_done)),
        ("max_tasks", num(child.max_tasks)),
        ("alive", Json::Bool(child.alive)),
        ("alive_window_ms", num(child.alive_window_ms)),
        ("tasks", Json::Array(Vec::new())),
    ])
}

fn supervisor_json(supervisor: &SupervisorView) -> Json {
    let children = supervisor.children.iter().map(child_json).collect();
    let queues = supervisor.queues.iter().map(|queue| text(queue)).collect();
    obj(&[
        ("host", text(&supervisor.host)),
        ("pid", num(u64::from(supervisor.pid))),
        ("started_at_ms", num(supervisor.started_at_ms)),
        ("version", text(&supervisor.version)),
        ("queues", Json::Array(queues)),
        ("processes", num(supervisor.processes)),
        ("concurrency", num(supervisor.concurrency)),
        ("max_tasks", num(supervisor.max_tasks)),
        ("memory_soft_ratio", text(&supervisor.memory_soft_ratio)),
        ("memory_hard_ratio", text(&supervisor.memory_hard_ratio)),
        ("child_memory_soft", opt_num(supervisor.child_memory_soft)),
        ("child_memory_hard", opt_num(supervisor.child_memory_hard)),
        ("cgroup", text(&supervisor.cgroup)),
        ("recycled", num(supervisor.recycled)),
        ("drains", num(supervisor.drains)),
        ("kills", num(supervisor.kills)),
        ("restarts", num(supervisor.restarts)),
        ("last_seen_ms", num(supervisor.last_seen_ms)),
        ("children", Json::Array(children)),
        ("in_progress", num(supervisor.in_progress)),
    ])
}

fn obj(pairs: &[(&str, Json)]) -> Json {
    Json::object(pairs.to_vec())
}

fn text(value: impl AsRef<str>) -> Json {
    Json::String(value.as_ref().to_string())
}

fn num(value: u64) -> Json {
    Json::Number(value.to_string())
}

fn opt_text(value: Option<&str>) -> Json {
    match value {
        Some(value) => text(value),
        None => Json::Null,
    }
}

fn opt_num(value: Option<u64>) -> Json {
    match value {
        Some(value) => num(value),
        None => Json::Null,
    }
}

fn opt_i32(value: Option<i32>) -> Json {
    match value {
        Some(value) => Json::Number(value.to_string()),
        None => Json::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arq_core::{CanonicalUuid, CycleId, DuplicateTask, MessageIdentity, TaskId};
    use arq_redis::ControlResult;

    fn uuid(n: u64) -> String {
        format!("00000000-0000-4000-8000-{n:012x}")
    }

    fn id(n: u64) -> CanonicalUuid {
        CanonicalUuid::parse(&uuid(n)).unwrap()
    }

    #[test]
    fn replies_cover_each_status() {
        let task = TaskId::from_uuid(id(1));
        let owner = TaskId::from_uuid(id(2));
        let cycle = CycleId::from_uuid(id(3));
        let enqueued = submit_response(&EnqueueResult::Enqueued { task_id: task, cycle_id: cycle, replayed: false });
        assert!(enqueued.contains("enqueued"));
        let duplicate = submit_response(&EnqueueResult::Duplicate(DuplicateTask {
            unique_key: "item:1".into(),
            task_id: task,
            owner_task_id: owner,
        }));
        assert!(duplicate.contains("duplicate"));
        assert!(duplicate.contains("item:1"));
        for result in [
            ControlResult::Accepted { cycle_id: Some(cycle), replayed: false },
            ControlResult::Accepted { cycle_id: None, replayed: true },
            ControlResult::AlreadyTerminal,
            ControlResult::NotFound,
            ControlResult::StillOwned,
            ControlResult::NotBlocked,
        ] {
            let text = control_response(&uuid(9), &uuid(1), &result);
            assert!(text.contains("operation_id"));
        }
        for result in [
            RequeueResult::Requeued { cycle_id: cycle, replayed: false },
            RequeueResult::Duplicate(DuplicateTask {
                unique_key: "item:2".into(),
                task_id: task,
                owner_task_id: owner,
            }),
            RequeueResult::NotDead,
            RequeueResult::NotFound,
        ] {
            assert!(requeue_response(&uuid(9), &uuid(1), &result).contains("status"));
        }
        assert!(finish_response("ok", &FinishResult::Finished { replayed: true }).contains("finished"));
        assert!(finish_response("invalid_outcome", &FinishResult::Rejected).contains("rejected"));
        assert!(abandoned_response().contains("abandoned"));
        assert!(task_response("default", None).contains("null"));
        let info = TaskInfo {
            task_id: uuid(1),
            message_id: Some(uuid(1)),
            state: "ready".into(),
            deliveries: 0,
            retries: 1,
            score: Some(5),
            missing_job: false,
            tag: Some("batch-a".into()),
            reason: String::new(),
            cycle_id: Some(uuid(3)),
            control_op: String::new(),
            actor_name: "process_item".into(),
            unique_key: None,
            lease_deadline_ms: None,
            started_at_ms: None,
            available_at_ms: Some(5),
            error: String::new(),
            stop_evidence: String::new(),
        };
        let page = page_response(
            "default",
            &TaskPage { tasks: vec![info.clone()], next_cursor: Some("v1".into()) },
        );
        assert!(page.contains("process_item"));
        assert!(page_response("default", &TaskPage { tasks: vec![], next_cursor: None }).contains("[]"));
        let encoded = task_response("default", Some(&info));
        assert!(encoded.contains("batch-a"));
        assert!(command_response(None).contains("null"));
        let stored = command_response(Some(&StoredReceipt {
            operation_id: uuid(9),
            task_id: uuid(1),
            op: "cancel".into(),
            status: "accepted".into(),
            cycle_id: None,
            raw: "{}".into(),
        }));
        assert!(stored.contains("cancel"));
        let _ = MessageIdentity::new(id(1));
    }

    #[test]
    fn snapshot_encodes_children_and_an_empty_registry() {
        let child = ChildView {
            host: "redis".into(),
            parent_pid: 1,
            slot: 0,
            pid: 2,
            ppid: 1,
            state: "running".into(),
            reason: String::new(),
            exit_code: Some(0),
            rss_bytes: 10,
            started_at_ms: 1,
            ended_at_ms: None,
            tasks_done: 0,
            max_tasks: 5,
            alive: true,
            alive_window_ms: 15,
        };
        let snapshot = Snapshot {
            taken_at_ms: 9,
            queues: vec![QueueDepth {
                queue: "default".into(),
                ready: 1,
                scheduled: 2,
                inflight: 0,
                done: 0,
                dead: 0,
                ready_zcard: 3,
            }],
            supervisors: vec![SupervisorView {
                host: "redis".into(),
                pid: 1,
                started_at_ms: 1,
                version: "0.1.0".into(),
                queues: vec!["default".into()],
                processes: 1,
                concurrency: 1,
                max_tasks: 5,
                memory_soft_ratio: "0.8".into(),
                memory_hard_ratio: "0.95".into(),
                child_memory_soft: Some(10),
                child_memory_hard: None,
                cgroup: String::new(),
                recycled: 1,
                drains: 2,
                kills: 3,
                restarts: 4,
                last_seen_ms: 8,
                children: vec![child.clone()],
                in_progress: 0,
            }],
            orphans: vec![child],
        };
        let text = snapshot_response(&snapshot);
        assert!(text.contains("ready_zcard"));
        assert!(text.contains("\"tasks\":[]"));
        let empty = snapshot_response(&Snapshot {
            taken_at_ms: 0,
            queues: vec![],
            supervisors: vec![],
            orphans: vec![],
        });
        assert!(empty.contains("\"queues\":[]"));
        let negative = opt_i32(Some(-2));
        assert_eq!(negative, Json::Number("-2".into()));
        assert_eq!(opt_i32(None), Json::Null);
    }

    #[test]
    fn every_bridge_error_is_an_object() {
        let samples = [
            BridgeError::Input("bad".into()),
            BridgeError::Config("bad".into()),
            BridgeError::Transport("down".into()),
            BridgeError::Page("cursor".into()),
            BridgeError::Full { queue: "default".into(), max_ready: 1, ready_zcard: 2 },
            BridgeError::Conflict { operation_id: uuid(1), detail: "op".into() },
            BridgeError::Inconsistent { queue: "default".into(), detail: "type".into() },
            BridgeError::Halted { queue: "default".into() },
            BridgeError::Indeterminate { operation_id: uuid(2), message_id: uuid(3) },
            BridgeError::Closed,
        ];
        for error in samples {
            let text = error_response(&error);
            let parsed = Json::parse(&text).unwrap();
            assert!(parsed.get("error").is_some(), "{text}");
        }
    }
}
