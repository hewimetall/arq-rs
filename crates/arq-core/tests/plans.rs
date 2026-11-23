#[path = "common/mod.rs"]
mod common;
use arq_core::*;
use common::*;

fn kwargs() -> Json {
    object(r#"{"item_id":"1001"}"#)
}
fn headers() -> Json {
    object("{}")
}

fn assert_local(plan: &OperationPlan, uniq: bool) {
    assert!(plan.watch_is_task_local(), "{:?}", plan.watch);
    assert!(plan.watch.keys().all(|key| !is_shared_index(key)));
    assert_eq!(plan.watch.uniq.is_some(), uniq);
    assert!(plan.receipt_is_last());
    assert_eq!(plan.commands.last().unwrap().key(), plan.watch.receipt);
    for command in &plan.commands {
        let _ = command.key();
    }
}

#[test]
fn enqueue_plans_a_local_watch_and_a_soft_ready_ceiling() {
    let config = cfg();
    let kwargs = kwargs();
    let headers = headers();
    let observed = blank();
    let mut input = enq(&config, &kwargs, &headers, &observed, None);
    let plan = match plan_enqueue(&input) {
        EnqueueDecision::Execute(plan) => plan,
        other => panic!("execute {other:?}"),
    };
    assert_local(&plan, true);
    assert_eq!(plan.kind, OperationKind::Enqueue);
    assert_eq!(plan.hset_field("state"), Some("ready"));
    assert_eq!(plan.hset_field("deliveries"), Some("0"));
    assert_eq!(plan.hset_field("actor_name"), Some("process_item"));
    assert_eq!(plan.hset_field("tag"), Some("batch-a"));
    assert_eq!(plan.hset_field("unique_key"), Some("item:1001"));
    assert!(plan.command_kinds().iter().any(|kind| *kind == "ready"));
    assert!(!plan.watch.keys().any(|key| key.ends_with(":ready")));
    assert!(plan.receipt.request_canonical.contains("item:1001"));
    assert!(plan.receipt.request_canonical.contains(r#""delay":0"#));

    input.ready_zcard = config.max_ready - 1;
    assert!(matches!(plan_enqueue(&input), EnqueueDecision::Execute(_)));
    input.ready_zcard = config.max_ready;
    match plan_enqueue(&input) {
        EnqueueDecision::Full(full) => {
            assert_eq!(full.ready_zcard, config.max_ready);
            assert!(full.to_string().contains("full"));
            touch_error(&full);
        }
        other => panic!("full {other:?}"),
    }
    input.ready_zcard = config.max_ready - 1;
    let left = plan_enqueue(&input);
    let right = plan_enqueue(&input);
    assert!(matches!(left, EnqueueDecision::Execute(_)));
    assert!(matches!(right, EnqueueDecision::Execute(_)));
    assert_eq!(soft_ready_ceiling(config.max_ready, 2), config.max_ready + 2);

    input.unique_key = None;
    input.tag = None;
    input.ready_zcard = 0;
    let bare = match plan_enqueue(&input) {
        EnqueueDecision::Execute(plan) => plan,
        other => panic!("bare {other:?}"),
    };
    assert_local(&bare, false);
    assert_eq!(bare.hset_field("tag"), None);
    assert_eq!(bare.hset_field("unique_key"), None);
}

#[test]
fn enqueue_rejects_bad_input_before_any_write() {
    let config = cfg();
    let kwargs = kwargs();
    let headers = headers();
    let observed = blank();
    let mut input = enq(&config, &kwargs, &headers, &observed, None);
    input.identity.task_id = other();
    assert!(matches!(plan_enqueue(&input), EnqueueDecision::Rejected(InputError::Identity)));
    input.identity = identity();
    input.max_deliveries = 0;
    assert!(matches!(plan_enqueue(&input), EnqueueDecision::Rejected(InputError::MaxDeliveries)));
    input.max_deliveries = 5;
    input.time_limit_ms = 0;
    assert!(matches!(plan_enqueue(&input), EnqueueDecision::Rejected(InputError::TimeLimit)));
    input.time_limit_ms = 1;
    input.min_backoff_ms = 0;
    assert!(matches!(plan_enqueue(&input), EnqueueDecision::Rejected(InputError::Backoff)));
    input.min_backoff_ms = 5;
    input.max_backoff_ms = 4;
    assert!(matches!(plan_enqueue(&input), EnqueueDecision::Rejected(InputError::Backoff)));
    input.max_backoff_ms = 5;
    input.queue = "Default";
    assert!(matches!(plan_enqueue(&input), EnqueueDecision::Rejected(InputError::Queue)));
    input.queue = "default";
    input.tag = Some("bad tag");
    assert!(matches!(plan_enqueue(&input), EnqueueDecision::Rejected(InputError::Opaque { field: "tag" })));
    input.tag = Some("batch-b");
    input.unique_key = Some("a{b");
    assert!(matches!(plan_enqueue(&input), EnqueueDecision::Rejected(InputError::Opaque { field: "unique_key" })));
    input.unique_key = Some("item:1001");
    let array = Json::Array(vec![]);
    input.kwargs = &array;
    assert!(matches!(plan_enqueue(&input), EnqueueDecision::Rejected(InputError::Kwargs)));
    input.kwargs = &kwargs;
    input.headers = &array;
    assert!(matches!(plan_enqueue(&input), EnqueueDecision::Rejected(InputError::Headers)));
    let tiny = cfg_with(&[("ARQ_MAX_MESSAGE_BYTES", "1")]);
    input.config = &tiny;
    input.headers = &headers;
    assert!(matches!(plan_enqueue(&input), EnqueueDecision::Rejected(InputError::Payload { field: "kwargs" })));
    let tiny_headers = cfg_with(&[("ARQ_MAX_HEADERS_BYTES", "1")]);
    input.config = &tiny_headers;
    let wide = object(r#"{"item_id":"1001"}"#);
    input.headers = &wide;
    assert!(matches!(plan_enqueue(&input), EnqueueDecision::Rejected(InputError::Payload { field: "headers" })));
    input.config = &config;
    input.headers = &headers;
    input.now_ms = u64::MAX;
    input.delay_ms = 1;
    assert!(matches!(plan_enqueue(&input), EnqueueDecision::Rejected(InputError::Overflow { field: "delay_ms" })));
}

#[test]
fn enqueue_replay_duplicate_and_indeterminate() {
    let config = cfg();
    let kwargs = kwargs();
    let headers = headers();
    let mut observed = ready_job();
    observed.tag = Some("batch-a".into());
    observed.unique_key = Some("item:1001".into());
    observed.uniq_owner = Some(task());
    observed.actor_name = "process_item".into();
    observed.deliveries = 0;
    observed.retries = 0;
    let run = |observed: &ObservedTask, stored: Option<&ReceiptRecord>| {
        let mut input = enq(&config, &kwargs, &headers, observed, stored);
        input.ready_zcard = config.max_ready;
        plan_enqueue(&input)
    };
    match run(&observed, None) {
        EnqueueDecision::Replay(replay) => {
            assert!(replay.proved_without_receipt);
            assert_eq!(replay.status, ReceiptStatus::Enqueued);
        }
        other => panic!("proved {other:?}"),
    }
    observed.ready_score = Some(2);
    assert!(matches!(run(&observed, None), EnqueueDecision::Indeterminate(_)));

    observed.ready_score = Some(1_000);
    let request = Json::object(vec![
        ("actor", Json::String("process_item".into())),
        ("delay", Json::Number("0".into())),
        ("headers", headers.clone()),
        ("kwargs", kwargs.clone()),
        ("message_id", Json::String(TASK.into())),
        ("queue", Json::String("default".into())),
        ("tag", Json::String("batch-a".into())),
        ("unique_key", Json::String("item:1001".into())),
    ]);
    let mut stored = receipt(OperationKind::Enqueue, ReceiptStatus::Enqueued, &request);
    stored.ready_score = Some(1_000);
    match run(&observed, Some(&stored)) {
        EnqueueDecision::Replay(replay) => assert!(!replay.proved_without_receipt),
        other => panic!("receipt {other:?}"),
    }
    stored.ready_score = None;
    match run(&observed, Some(&stored)) {
        EnqueueDecision::Replay(_) => {}
        other => panic!("agree {other:?}"),
    }
    observed.ready_score = Some(9);
    assert!(matches!(run(&observed, Some(&stored)), EnqueueDecision::Inconsistent(_)));
    observed.ready_score = Some(1_000);
    stored.ready_score = Some(1_000);
    stored.status = ReceiptStatus::Claimed;
    assert!(matches!(run(&observed, Some(&stored)), EnqueueDecision::Inconsistent(_)));
    stored.status = ReceiptStatus::Duplicate;
    observed.present = false;
    match run(&observed, Some(&stored)) {
        EnqueueDecision::Replay(replay) => assert_eq!(replay.status, ReceiptStatus::Duplicate),
        other => panic!("dup receipt {other:?}"),
    }
    observed.present = true;
    assert!(matches!(run(&observed, Some(&stored)), EnqueueDecision::Inconsistent(_)));
    stored.request_canonical = "{}".into();
    stored.status = ReceiptStatus::Enqueued;
    match run(&observed, Some(&stored)) {
        EnqueueDecision::Conflict(conflict) => {
            assert!(conflict.to_string().contains("conflicts"));
            touch_error(&conflict);
        }
        other => panic!("conflict {other:?}"),
    }
    stored.request_canonical = request.canonical();
    stored.operation_id = OperationId::parse(OTHER).unwrap();
    assert!(matches!(run(&observed, Some(&stored)), EnqueueDecision::Inconsistent(_)));

    let mut missing = blank();
    missing.uniq_owner = Some(other());
    match run(&missing, None) {
        EnqueueDecision::Duplicate { error, record } => {
            assert_eq!(error.owner_task_id, other());
            assert_eq!(error.unique_key, "item:1001");
            assert!(error.to_string().contains("item:1001"));
            touch_error(&error);
            assert_eq!(record.hset_field("state"), None);
            assert_eq!(record.receipt.status, ReceiptStatus::Duplicate);
            assert_local(&record, true);
        }
        other => panic!("duplicate {other:?}"),
    }
    missing.uniq_owner = Some(task());
    assert!(matches!(run(&missing, None), EnqueueDecision::Indeterminate(_)));
    let mut indexed = blank();
    indexed.ready_score = Some(1);
    match run(&indexed, None) {
        EnqueueDecision::Indeterminate(lost) => {
            assert_eq!(lost.operation_id, op());
            touch_error(&lost);
        }
        other => panic!("indexed {other:?}"),
    }
}

#[test]
fn claim_delivers_blocks_and_dead_letters() {
    let config = cfg();
    let mut job = ready_job();
    job.unique_key = Some("item:1001".into());
    job.tag = Some("batch-a".into());
    job.uniq_owner = Some(task());
    let input = claim_in(&config, &job, None);
    let plan = match plan_claim(&input) {
        ClaimDecision::Execute(plan) => plan,
        other => panic!("deliver {other:?}"),
    };
    assert_local(&plan, true);
    assert_eq!(plan.hset_field("state"), Some("inflight"));
    assert_eq!(plan.hset_field("deliveries"), Some("1"));
    assert_eq!(plan.hset_field("started_at_ms"), Some("1000"));
    assert!(plan.command_kinds().iter().any(|kind| *kind == "inflight"));
    assert!(!plan.watch.keys().any(|key| key.contains(":inflight") || key.ends_with(":ready")));

    job.available_at_ms = Some(2_000);
    assert!(matches!(plan_claim(&claim_in(&config, &job, None)), ClaimDecision::NotDue));
    job.available_at_ms = Some(1_000);
    job.state = Some(TaskState::Done);
    assert!(matches!(plan_claim(&claim_in(&config, &job, None)), ClaimDecision::NotDue));
    assert!(matches!(plan_claim(&claim_in(&config, &blank(), None)), ClaimDecision::NotDue));
    let mut named = blank();
    named.task_id = Some(task());
    assert!(matches!(plan_claim(&claim_in(&config, &named, None)), ClaimDecision::NotDue));
    let mut dirty = blank();
    dirty.in_dead = true;
    assert!(matches!(plan_claim(&claim_in(&config, &dirty, None)), ClaimDecision::Indeterminate(_)));
    let mut present = ready_job();
    present.task_id = None;
    assert!(matches!(plan_claim(&claim_in(&config, &present, None)), ClaimDecision::Inconsistent(_)));

    let mut live = inflight_job();
    match plan_claim(&claim_in(&config, &live, None)) {
        ClaimDecision::Replay(replay) => assert!(replay.proved_without_receipt),
        other => panic!("already delivered {other:?}"),
    }
    live.delivery_token = Some(DeliveryToken::parse(OTHER).unwrap());
    assert!(matches!(plan_claim(&claim_in(&config, &live, None)), ClaimDecision::LiveLease));
    live.lease_deadline_ms = Some(500);
    let again = match plan_claim(&claim_in(&config, &live, None)) {
        ClaimDecision::Execute(plan) => plan,
        other => panic!("reclaim {other:?}"),
    };
    assert_eq!(again.hset_field("deliveries"), Some("2"));
    live.deliveries = 5;
    live.max_deliveries = 5;
    live.lease_deadline_ms = Some(500);
    let dead = match plan_claim(&claim_in(&config, &live, None)) {
        ClaimDecision::DeadLetter(plan) => plan,
        other => panic!("dlq {other:?}"),
    };
    assert_eq!(dead.hset_field("reason"), Some("max_deliveries"));
    assert_eq!(dead.hset_field("state"), Some("dead"));
    assert!(dead.command_kinds().iter().any(|kind| *kind == "dead"));

    let mut ready_dead = ready_job();
    ready_dead.deliveries = 5;
    ready_dead.max_deliveries = 5;
    ready_dead.tag = Some("batch-a".into());
    ready_dead.unique_key = Some("item:1001".into());
    ready_dead.uniq_owner = Some(task());
    let letter = match plan_claim(&claim_in(&config, &ready_dead, None)) {
        ClaimDecision::DeadLetter(plan) => plan,
        other => panic!("ready dlq {other:?}"),
    };
    assert!(letter.commands.iter().any(|command| matches!(command, PlannedCommand::Del { .. })));
    assert!(letter.command_kinds().iter().any(|kind| *kind == "dead"));

    live.control_op = ControlOp::Cancel;
    live.deliveries = 1;
    assert!(matches!(plan_claim(&claim_in(&config, &live, None)), ClaimDecision::Blocked));
    let mut no_time = ready_job();
    no_time.available_at_ms = None;
    assert!(matches!(plan_claim(&claim_in(&config, &no_time, None)), ClaimDecision::Inconsistent(_)));
    let mut no_lease = inflight_job();
    no_lease.lease_deadline_ms = None;
    no_lease.deliveries = 0;
    assert!(matches!(plan_claim(&claim_in(&config, &no_lease, None)), ClaimDecision::Inconsistent(_)));
    let mut no_state = ready_job();
    no_state.state = None;
    assert!(matches!(plan_claim(&claim_in(&config, &no_state, None)), ClaimDecision::Inconsistent(_)));

    let ready = ready_job();
    let mut bad = claim_in(&config, &ready, None);
    bad.worker_id = "";
    assert!(matches!(plan_claim(&bad), ClaimDecision::Rejected(InputError::WorkerId)));
    bad.worker_id = "worker-a";
    bad.lease_ms = 0;
    assert!(matches!(plan_claim(&bad), ClaimDecision::Rejected(InputError::TimeLimit)));
    bad.lease_ms = 1;
    bad.queue = "nope-queue";
    assert!(matches!(plan_claim(&bad), ClaimDecision::Rejected(InputError::Queue)));
    let mut no_cycle = ready_job();
    no_cycle.cycle_id = None;
    assert!(matches!(plan_claim(&claim_in(&config, &no_cycle, None)), ClaimDecision::Rejected(InputError::History)));
    let timed_job = ready_job();
    let mut timed = claim_in(&config, &timed_job, None);
    timed.now_ms = u64::MAX;
    timed.lease_ms = 1;
    assert!(matches!(plan_claim(&timed), ClaimDecision::Rejected(InputError::Overflow { field: "lease_ms" })));
}

#[test]
fn claim_receipt_and_proved_delivery() {
    let config = cfg();
    let mut job = inflight_job();
    job.deliveries = 1;
    let request = Json::object(vec![
        ("task_id", Json::String(TASK.into())),
        ("worker_id", Json::String("worker-a".into())),
    ]);
    let stored = receipt(OperationKind::Claim, ReceiptStatus::Claimed, &request);
    match plan_claim(&claim_in(&config, &job, Some(&stored))) {
        ClaimDecision::Replay(replay) => assert!(!replay.proved_without_receipt),
        other => panic!("claimed {other:?}"),
    }
    job.delivery_token = None;
    assert!(matches!(plan_claim(&claim_in(&config, &job, Some(&stored))), ClaimDecision::Inconsistent(_)));
    let mut missing_cycle = inflight_job();
    missing_cycle.cycle_id = None;
    let mut stored_cycle = stored.clone();
    stored_cycle.cycle_id = None;
    assert!(matches!(
        plan_claim(&claim_in(&config, &missing_cycle, Some(&stored_cycle))),
        ClaimDecision::Inconsistent(_)
    ));
    let mut dead = inflight_job();
    dead.state = Some(TaskState::Dead);
    dead.reason = "max_deliveries".into();
    dead.in_dead = true;
    dead.in_inflight = false;
    dead.ready_score = None;
    dead.tag = None;
    dead.in_dead_tag = false;
    let dead_receipt = receipt(OperationKind::Claim, ReceiptStatus::Dead, &request);
    match plan_claim(&claim_in(&config, &dead, Some(&dead_receipt))) {
        ClaimDecision::Replay(replay) => assert_eq!(replay.status, ReceiptStatus::Dead),
        other => panic!("dead receipt {other:?}"),
    }
    dead.in_inflight = true;
    match plan_claim(&claim_in(&config, &dead, Some(&dead_receipt))) {
        ClaimDecision::Replay(replay) => assert_eq!(replay.status, ReceiptStatus::Dead),
        other => panic!("weak dead {other:?}"),
    }
    dead.reason = "other".into();
    assert!(matches!(plan_claim(&claim_in(&config, &dead, Some(&dead_receipt))), ClaimDecision::Inconsistent(_)));
    let unexpected = receipt(OperationKind::Claim, ReceiptStatus::Enqueued, &request);
    assert!(matches!(plan_claim(&claim_in(&config, &dead, Some(&unexpected))), ClaimDecision::Inconsistent(_)));
    let mut conflict = stored.clone();
    conflict.request_canonical = "{}".into();
    assert!(matches!(plan_claim(&claim_in(&config, &inflight_job(), Some(&conflict))), ClaimDecision::Conflict(_)));
    let mut corrupt = stored.clone();
    corrupt.operation_id = OperationId::parse(OTHER).unwrap();
    assert!(matches!(plan_claim(&claim_in(&config, &inflight_job(), Some(&corrupt))), ClaimDecision::Inconsistent(_)));

    let mut proved = inflight_job();
    proved.delivery_token = Some(token());
    proved.deliveries = 2;
    proved.in_inflight = true;
    proved.ready_score = None;
    match plan_claim(&claim_in(&config, &proved, None)) {
        ClaimDecision::Replay(replay) => assert!(replay.proved_without_receipt),
        other => panic!("proved {other:?}"),
    }
    proved.deliveries = 0;
    assert!(matches!(plan_claim(&claim_in(&config, &proved, None)), ClaimDecision::LiveLease));
}

#[test]
fn renew_extends_only_the_lease() {
    let config = cfg();
    let mut job = inflight_job();
    job.unique_key = Some("item:1001".into());
    let plan = match plan_renew(&renew_in(&config, &job, None)) {
        RenewDecision::Execute(plan) => plan,
        other => panic!("renew {other:?}"),
    };
    assert_local(&plan, false);
    assert_eq!(plan.hset_field("lease_deadline_ms"), Some("16000"));
    assert_eq!(plan.hset_field("started_at_ms"), None);
    assert!(!plan.receipt.request_canonical.contains("16000"));
    job.delivery_token = Some(DeliveryToken::parse(OTHER).unwrap());
    match plan_renew(&renew_in(&config, &job, None)) {
        RenewDecision::NotHeld { exit_code } => assert_eq!(exit_code, 4),
        other => panic!("token {other:?}"),
    }
    job.delivery_token = Some(token());
    job.lease_deadline_ms = Some(1_000);
    assert!(matches!(plan_renew(&renew_in(&config, &job, None)), RenewDecision::NotHeld { exit_code: 4 }));
    job.state = Some(TaskState::Ready);
    assert!(matches!(plan_renew(&renew_in(&config, &job, None)), RenewDecision::NotHeld { .. }));
    let mut missing_lease = inflight_job();
    missing_lease.lease_deadline_ms = None;
    assert!(matches!(plan_renew(&renew_in(&config, &missing_lease, None)), RenewDecision::Inconsistent(_)));
    let mut held = inflight_job();
    held.lease_deadline_ms = Some(u64::MAX);
    let mut input = renew_in(&config, &held, None);
    input.lease_ms = 0;
    assert!(matches!(plan_renew(&input), RenewDecision::Rejected(InputError::TimeLimit)));
    input.lease_ms = 1;
    input.queue = "Bad";
    assert!(matches!(plan_renew(&input), RenewDecision::Rejected(InputError::Queue)));
    input.queue = "default";
    input.now_ms = u64::MAX - 1;
    input.lease_ms = 2;
    assert!(matches!(plan_renew(&input), RenewDecision::Rejected(InputError::Overflow { field: "lease_ms" })));

    let request = Json::object(vec![
        ("cycle_id", Json::String(CYCLE.into())),
        ("delivery_token", Json::String(TOKEN.into())),
        ("task_id", Json::String(TASK.into())),
    ]);
    let mut stored = receipt(OperationKind::Renew, ReceiptStatus::Renewed, &request);
    let mut held = inflight_job();
    held.lease_deadline_ms = stored.lease_deadline_ms;
    match plan_renew(&renew_in(&config, &held, Some(&stored))) {
        RenewDecision::Replay(replay) => assert_eq!(replay.status, ReceiptStatus::Renewed),
        other => panic!("replay {other:?}"),
    }
    stored.lease_deadline_ms = None;
    assert!(matches!(plan_renew(&renew_in(&config, &held, Some(&stored))), RenewDecision::Inconsistent(_)));
    stored.lease_deadline_ms = Some(16_000);
    stored.op = OperationKind::Finish;
    assert!(matches!(plan_renew(&renew_in(&config, &held, Some(&stored))), RenewDecision::Conflict(_)));
    let mut no_start = inflight_job();
    no_start.started_at_ms = None;
    let plan = match plan_renew(&renew_in(&config, &no_start, None)) {
        RenewDecision::Execute(plan) => plan,
        other => panic!("null start {other:?}"),
    };
    assert!(plan.receipt.result_canonical.contains("null") || plan.commands.iter().any(|command| command.key().contains("job")));
}

#[test]
fn finish_commits_ok_skipped_retry_and_dead() {
    let config = cfg();
    let job = inflight_job();
    let ok = parse_outcome(&object(r#"{"outcome":"ok"}"#));
    let plan = match plan_finish(&finish_in(&config, &ok, &job, None)) {
        FinishDecision::Execute(plan) => plan,
        other => panic!("ok {other:?}"),
    };
    assert_local(&plan, false);
    assert_eq!(plan.hset_field("state"), Some("done"));
    assert_eq!(plan.hset_field("result"), Some("ok"));
    assert!(plan.command_kinds().iter().any(|kind| *kind == "done"));
    assert!(!plan.watch.uniq.is_some());

    let skipped = parse_outcome(&object(r#"{"outcome":"skipped"}"#));
    let plan = match plan_finish(&finish_in(&config, &skipped, &job, None)) {
        FinishDecision::Execute(plan) => plan,
        other => panic!("skipped {other:?}"),
    };
    assert_eq!(plan.hset_field("result"), Some("skipped"));

    let retry = parse_outcome(&object(r#"{"outcome":"retry","delay_ms":250}"#));
    let mut owned = job.clone();
    owned.unique_key = Some("item:1001".into());
    owned.uniq_owner = Some(task());
    let plan = match plan_finish(&finish_in(&config, &retry, &owned, None)) {
        FinishDecision::Execute(plan) => plan,
        other => panic!("retry {other:?}"),
    };
    assert_local(&plan, true);
    assert_eq!(plan.hset_field("state"), Some("ready"));
    assert_eq!(plan.hset_field("retries"), Some("1"));
    assert_eq!(plan.hset_field("available_at_ms"), Some("5250"));
    assert!(!plan.commands.iter().any(|command| matches!(command, PlannedCommand::Del { .. })));
    assert!(!plan.command_kinds().iter().any(|kind| *kind == "done"));

    let dead = parse_outcome(&object(r#"{"outcome":"dead","reason":"boom","error":"я"}"#));
    let mut tagged = owned.clone();
    tagged.tag = Some("batch-a".into());
    let tiny = cfg_with(&[("ARQ_MAX_ERROR_BYTES", "1")]);
    let plan = match plan_finish(&finish_in(&tiny, &dead, &tagged, None)) {
        FinishDecision::Execute(plan) => plan,
        other => panic!("dead {other:?}"),
    };
    assert_eq!(plan.hset_field("reason"), Some("boom"));
    assert_eq!(plan.hset_field("error"), Some(""));
    assert!(plan.commands.iter().any(|command| matches!(command, PlannedCommand::Del { .. })));
    let kept = cfg();
    let plan = match plan_finish(&finish_in(&kept, &dead, &job, None)) {
        FinishDecision::Execute(plan) => plan,
        other => panic!("kept {other:?}"),
    };
    assert_eq!(plan.hset_field("error"), Some("я"));

    let invalid = parse_outcome(&Json::Null);
    let plan = match plan_finish(&finish_in(&config, &invalid, &job, None)) {
        FinishDecision::Execute(plan) => plan,
        other => panic!("invalid {other:?}"),
    };
    assert_eq!(plan.hset_field("reason"), Some("invalid_outcome"));
    let raised = dispatch_exception();
    let plan = match plan_finish(&finish_in(&config, &raised, &job, None)) {
        FinishDecision::Execute(plan) => plan,
        other => panic!("dispatch {other:?}"),
    };
    assert_eq!(plan.hset_field("reason"), Some("dispatch_error"));
    assert!(!raised.implies_retry());
}

#[test]
fn finish_rejects_stale_work_and_replays_a_match() {
    let config = cfg();
    let ok = parse_outcome(&object(r#"{"outcome":"ok"}"#));
    let mut job = inflight_job();
    job.delivery_token = Some(DeliveryToken::parse(OTHER).unwrap());
    assert!(matches!(plan_finish(&finish_in(&config, &ok, &job, None)), FinishDecision::Rejected));
    job = inflight_job();
    job.lease_deadline_ms = Some(5_000);
    assert!(matches!(plan_finish(&finish_in(&config, &ok, &job, None)), FinishDecision::Rejected));
    job.lease_deadline_ms = Some(20_000);
    job.control_op = ControlOp::Kill;
    assert!(matches!(plan_finish(&finish_in(&config, &ok, &job, None)), FinishDecision::Rejected));
    job.control_op = ControlOp::Empty;
    job.state = Some(TaskState::Ready);
    assert!(matches!(plan_finish(&finish_in(&config, &ok, &job, None)), FinishDecision::Rejected));

    let mut done = inflight_job();
    done.state = Some(TaskState::Done);
    done.result = None;
    done.in_inflight = false;
    done.ready_score = None;
    assert!(matches!(plan_finish(&finish_in(&config, &ok, &done, None)), FinishDecision::Indeterminate(_)));
    done.result = Some(TaskResult::Ok);
    done.in_done = true;
    done.finished_at_ms = Some(5_000);
    done.uniq_owner = None;
    match plan_finish(&finish_in(&config, &ok, &done, None)) {
        FinishDecision::Replay(replay) => assert!(replay.proved_without_receipt),
        other => panic!("proved {other:?}"),
    }
    let request_job = inflight_job();
    let executed = match plan_finish(&finish_in(&config, &ok, &request_job, None)) {
        FinishDecision::Execute(plan) => plan,
        other => panic!("seed {other:?}"),
    };
    let mut stored = executed.receipt.clone();
    match plan_finish(&finish_in(&config, &ok, &done, Some(&stored))) {
        FinishDecision::Replay(replay) => assert!(!replay.proved_without_receipt),
        other => panic!("receipt {other:?}"),
    }
    stored.request_canonical = "{}".into();
    assert!(matches!(plan_finish(&finish_in(&config, &ok, &done, Some(&stored))), FinishDecision::Conflict(_)));
    done.in_done = false;
    stored = executed.receipt.clone();
    assert!(matches!(plan_finish(&finish_in(&config, &ok, &done, Some(&stored))), FinishDecision::Inconsistent(_)));

    let mut live = inflight_job();
    live.lease_deadline_ms = Some(u64::MAX);
    let retry = parse_outcome(&object(r#"{"outcome":"retry","delay_ms":2}"#));
    let mut input = finish_in(&config, &retry, &live, None);
    input.queue = "NO";
    assert!(matches!(plan_finish(&input), FinishDecision::RejectedInput(InputError::Queue)));
    input.queue = "default";
    input.now_ms = u64::MAX - 1;
    assert!(matches!(plan_finish(&input), FinishDecision::RejectedInput(InputError::Overflow { field: "delay_ms" })));

    let retry_job = inflight_job();
    let proved_retry = parse_outcome(&object(r#"{"outcome":"retry","delay_ms":0}"#));
    let mut back = retry_job.clone();
    back.state = Some(TaskState::Ready);
    back.retries = 1;
    back.delivery_token = None;
    back.in_inflight = false;
    back.ready_score = Some(5_000);
    back.in_done = false;
    back.uniq_owner = None;
    assert!(matches!(
        plan_finish(&finish_in(&config, &proved_retry, &back, None)),
        FinishDecision::Indeterminate(_)
    ));
    back.retries = u64::MAX;
    match plan_finish(&finish_in(&config, &proved_retry, &back, None)) {
        FinishDecision::Replay(replay) => assert!(replay.proved_without_receipt),
        other => panic!("retry proved {other:?}"),
    }
}

#[test]
fn requeue_returns_a_dead_task_and_keeps_a_duplicate_in_the_dlq() {
    let config = cfg();
    let history = Json::Array(vec![]);
    let mut job = inflight_job();
    job.state = Some(TaskState::Dead);
    job.control_op = ControlOp::Empty;
    job.finished_at_ms = Some(4_000);
    job.reason = "boom".into();
    job.error = "nope".into();
    job.unique_key = Some("item:1001".into());
    job.tag = Some("batch-a".into());
    job.in_dead = true;
    job.in_dead_tag = true;
    job.in_inflight = false;
    let plan = match plan_requeue(&requeue_in(&config, &history, &job, None)) {
        RequeueDecision::Execute(plan) => plan,
        other => panic!("requeue {other:?}"),
    };
    assert_local(&plan, true);
    assert_eq!(plan.hset_field("state"), Some("ready"));
    assert_eq!(plan.hset_field("retries"), Some("0"));
    assert_eq!(plan.hset_field("deliveries"), Some("0"));
    assert_eq!(plan.hset_field("cycle_id"), Some(CYCLE2));
    assert!(plan.hset_field("history").unwrap().contains(CYCLE));
    assert!(plan.commands.iter().any(|command| matches!(command, PlannedCommand::Set { .. })));

    let ready = ready_job();
    match plan_requeue(&requeue_in(&config, &history, &ready, None)) {
        RequeueDecision::NotDead(plan) => assert_eq!(plan.receipt.status, ReceiptStatus::NotDead),
        other => panic!("not dead {other:?}"),
    }
    match plan_requeue(&requeue_in(&config, &history, &blank(), None)) {
        RequeueDecision::NotFound(plan) => assert_eq!(plan.receipt.status, ReceiptStatus::NotFound),
        other => panic!("missing {other:?}"),
    }
    let mut dirty = blank();
    dirty.in_dead = true;
    assert!(matches!(plan_requeue(&requeue_in(&config, &history, &dirty, None)), RequeueDecision::Indeterminate(_)));
    job.uniq_owner = Some(other());
    match plan_requeue(&requeue_in(&config, &history, &job, None)) {
        RequeueDecision::Duplicate(error) => assert_eq!(error.owner_task_id, other()),
        other => panic!("dup {other:?}"),
    }
    job.uniq_owner = None;
    let not_array = object(r#"{"history":1}"#);
    assert!(matches!(plan_requeue(&requeue_in(&config, &not_array, &job, None)), RequeueDecision::Rejected(InputError::History)));
    job.finished_at_ms = None;
    assert!(matches!(plan_requeue(&requeue_in(&config, &history, &job, None)), RequeueDecision::Rejected(InputError::History)));
    job.finished_at_ms = Some(4_000);
    job.cycle_id = None;
    assert!(matches!(plan_requeue(&requeue_in(&config, &history, &job, None)), RequeueDecision::Rejected(InputError::History)));
    job.cycle_id = Some(cycle());
    job.state = None;
    assert!(matches!(plan_requeue(&requeue_in(&config, &history, &job, None)), RequeueDecision::NotDead(_)));

    let ready_for_queue = ready_job();
    let mut input = requeue_in(&config, &history, &ready_for_queue, None);
    input.queue = "NO";
    assert!(matches!(plan_requeue(&input), RequeueDecision::Rejected(InputError::Queue)));

    let mut restored = ready_job();
    restored.cycle_id = Some(cycle2());
    restored.retries = 0;
    restored.deliveries = 0;
    restored.history_contains_prior = true;
    restored.ready_score = Some(9_000);
    restored.control_op = ControlOp::Empty;
    restored.delivery_token = None;
    restored.unique_key = Some("item:1001".into());
    restored.uniq_owner = Some(task());
    match plan_requeue(&requeue_in(&config, &history, &restored, None)) {
        RequeueDecision::Replay(replay) => {
            assert!(replay.proved_without_receipt);
            assert_eq!(replay.status, ReceiptStatus::Requeued);
        }
        other => panic!("proved {other:?}"),
    }
    let request = Json::object(vec![
        ("new_cycle_id", Json::String(CYCLE2.into())),
        ("task_id", Json::String(TASK.into())),
    ]);
    let mut stored = receipt(OperationKind::Requeue, ReceiptStatus::Requeued, &request);
    stored.cycle_id = Some(cycle2());
    stored.ready_score = Some(9_000);
    match plan_requeue(&requeue_in(&config, &history, &restored, Some(&stored))) {
        RequeueDecision::Replay(replay) => assert_eq!(replay.status, ReceiptStatus::Requeued),
        other => panic!("receipt {other:?}"),
    }
    stored.ready_score = Some(1);
    assert!(matches!(plan_requeue(&requeue_in(&config, &history, &restored, Some(&stored))), RequeueDecision::Inconsistent(_)));
    stored.ready_score = Some(9_000);
    stored.op = OperationKind::Cancel;
    assert!(matches!(plan_requeue(&requeue_in(&config, &history, &restored, Some(&stored))), RequeueDecision::Conflict(_)));
}

#[test]
fn cancel_and_kill_follow_ready_inflight_and_terminal() {
    let config = cfg();
    let mut ready = ready_job();
    ready.unique_key = Some("item:1001".into());
    ready.uniq_owner = Some(task());
    ready.tag = Some("batch-a".into());
    let plan = match plan_cancel(&control_in(&config, &ready, None)) {
        ControlDecision::Execute(plan) => plan,
        other => panic!("cancel ready {other:?}"),
    };
    assert_local(&plan, false);
    assert_eq!(plan.hset_field("state"), Some("dead"));
    assert_eq!(plan.hset_field("reason"), Some("cancelled"));
    assert_eq!(plan.hset_field("control_op"), Some(""));
    assert!(plan.commands.iter().any(|command| matches!(command, PlannedCommand::Del { .. })));
    assert!(plan.command_kinds().windows(1).any(|kind| kind[0] == "ready" || kind[0] == "done" || kind[0] == "dead"));

    let mut live = inflight_job();
    let plan = match plan_kill(&control_in(&config, &live, None)) {
        ControlDecision::Execute(plan) => plan,
        other => panic!("kill {other:?}"),
    };
    assert_local(&plan, false);
    assert_eq!(plan.hset_field("control_op"), Some("kill"));
    assert_eq!(plan.hset_field("state"), None);
    assert_eq!(plan.receipt.status, ReceiptStatus::Accepted);
    live.cycle_id = None;
    assert!(matches!(plan_kill(&control_in(&config, &live, None)), ControlDecision::Inconsistent(_)));

    let mut done = inflight_job();
    done.state = Some(TaskState::Done);
    done.in_inflight = false;
    let plan = match plan_cancel(&control_in(&config, &done, None)) {
        ControlDecision::Execute(plan) => plan,
        other => panic!("terminal {other:?}"),
    };
    assert_eq!(plan.receipt.status, ReceiptStatus::AlreadyTerminal);
    assert_eq!(plan.commands.len(), 1);
    let plan = match plan_cancel(&control_in(&config, &blank(), None)) {
        ControlDecision::Execute(plan) => plan,
        other => panic!("missing {other:?}"),
    };
    assert_eq!(plan.receipt.status, ReceiptStatus::NotFound);
    assert!(plan.receipt.result_canonical.contains("null") || plan.receipt.cycle_id.is_none());
    let mut dirty = blank();
    dirty.in_done = true;
    assert!(matches!(plan_cancel(&control_in(&config, &dirty, None)), ControlDecision::Indeterminate(_)));
    let mut no_state = ready_job();
    no_state.state = None;
    assert!(matches!(plan_cancel(&control_in(&config, &no_state, None)), ControlDecision::Inconsistent(_)));
    let ready_for_control = ready_job();
    let mut input = control_in(&config, &ready_for_control, None);
    input.queue = "NO";
    assert!(matches!(plan_cancel(&input), ControlDecision::Rejected(InputError::Queue)));

    let request = Json::object(vec![("task_id", Json::String(TASK.into()))]);
    let mut accepted = inflight_job();
    accepted.control_op = ControlOp::Cancel;
    accepted.control_cycle_id = Some(cycle());
    accepted.control_delivery_token = Some(token());
    let stored = receipt(OperationKind::Cancel, ReceiptStatus::Accepted, &request);
    match plan_cancel(&control_in(&config, &accepted, Some(&stored))) {
        ControlDecision::Replay(replay) => assert_eq!(replay.status, ReceiptStatus::Accepted),
        other => panic!("accepted {other:?}"),
    }
    let mut terminal = ready_job();
    terminal.state = Some(TaskState::Dead);
    terminal.reason = "boom".into();
    terminal.in_dead = true;
    terminal.ready_score = None;
    match plan_cancel(&control_in(&config, &terminal, Some(&stored))) {
        ControlDecision::Replay(replay) => assert_eq!(replay.status, ReceiptStatus::AlreadyTerminal),
        other => panic!("already {other:?}"),
    }
    let mut fresh = ready_job();
    assert!(matches!(plan_cancel(&control_in(&config, &fresh, Some(&stored))), ControlDecision::Inconsistent(_)));
    fresh.state = Some(TaskState::Dead);
    fresh.reason = "cancelled".into();
    fresh.in_dead = true;
    fresh.ready_score = None;
    fresh.control_op = ControlOp::Empty;
    fresh.uniq_owner = None;
    match plan_cancel(&control_in(&config, &fresh, None)) {
        ControlDecision::Replay(replay) => assert!(replay.proved_without_receipt),
        other => panic!("proved cancel {other:?}"),
    }
    let kill_receipt = receipt(OperationKind::Kill, ReceiptStatus::Accepted, &request);
    assert!(matches!(plan_cancel(&control_in(&config, &accepted, Some(&kill_receipt))), ControlDecision::Conflict(_)));
    let mut corrupt = stored.clone();
    corrupt.operation_id = OperationId::parse(OTHER).unwrap();
    assert!(matches!(plan_cancel(&control_in(&config, &accepted, Some(&corrupt))), ControlDecision::Inconsistent(_)));
}

#[test]
fn settle_requires_still_owned_not_blocked_or_accepted() {
    let config = cfg();
    let mut live = inflight_job();
    live.control_op = ControlOp::Cancel;
    live.lease_deadline_ms = Some(8_001);
    assert!(matches!(plan_settle(&settle_in(&config, &live, None)), SettleDecision::StillOwned));
    live.lease_deadline_ms = Some(8_000);
    let plan = match plan_settle(&settle_in(&config, &live, None)) {
        SettleDecision::Accepted(plan) => plan,
        other => panic!("accepted {other:?}"),
    };
    assert_local(&plan, false);
    assert_eq!(plan.hset_field("reason"), Some("cancelled"));
    assert_eq!(plan.hset_field("stop_evidence"), Some("lease_expired"));
    assert_eq!(plan.hset_field("control_op"), Some(""));
    assert_eq!(plan.receipt.status, ReceiptStatus::Accepted);
    live.lease_deadline_ms = Some(7_000);
    live.control_op = ControlOp::Kill;
    live.tag = Some("batch-b".into());
    live.unique_key = Some("item:1001".into());
    live.uniq_owner = Some(task());
    let plan = match plan_settle(&settle_in(&config, &live, None)) {
        SettleDecision::Accepted(plan) => plan,
        other => panic!("killed {other:?}"),
    };
    assert_eq!(plan.hset_field("reason"), Some("killed"));
    assert!(plan.commands.iter().any(|command| matches!(command, PlannedCommand::Del { .. })));

    live.control_op = ControlOp::Empty;
    assert!(matches!(plan_settle(&settle_in(&config, &live, None)), SettleDecision::NotBlocked));
    assert!(matches!(plan_settle(&settle_in(&config, &ready_job(), None)), SettleDecision::NotBlocked));
    let mut bare = ready_job();
    bare.state = None;
    assert!(matches!(plan_settle(&settle_in(&config, &bare, None)), SettleDecision::NotBlocked));
    live.control_op = ControlOp::Cancel;
    live.lease_deadline_ms = None;
    assert!(matches!(plan_settle(&settle_in(&config, &live, None)), SettleDecision::Inconsistent(_)));

    let mut dead = ready_job();
    dead.state = Some(TaskState::Dead);
    dead.reason = "retries".into();
    dead.in_dead = true;
    dead.ready_score = None;
    match plan_settle(&settle_in(&config, &dead, None)) {
        SettleDecision::AlreadyTerminal(plan) => assert_eq!(plan.receipt.status, ReceiptStatus::AlreadyTerminal),
        other => panic!("terminal {other:?}"),
    }
    dead.reason = "cancelled".into();
    dead.stop_evidence = StopEvidence::LeaseExpired;
    dead.control_op = ControlOp::Empty;
    dead.in_inflight = false;
    dead.in_dead_tag = false;
    dead.tag = None;
    dead.unique_key = None;
    match plan_settle(&settle_in(&config, &dead, None)) {
        SettleDecision::Replay(replay) => {
            assert!(replay.proved_without_receipt);
            assert_eq!(replay.status, ReceiptStatus::Accepted);
        }
        other => panic!("proved settle {other:?}"),
    }
    dead.stop_evidence = StopEvidence::Empty;
    match plan_settle(&settle_in(&config, &dead, None)) {
        SettleDecision::AlreadyTerminal(_) => {}
        other => panic!("empty evidence {other:?}"),
    }
    match plan_settle(&settle_in(&config, &blank(), None)) {
        SettleDecision::NotFound(plan) => assert_eq!(plan.receipt.status, ReceiptStatus::NotFound),
        other => panic!("missing {other:?}"),
    }
    let mut dirty = blank();
    dirty.ready_score = Some(1);
    assert!(matches!(plan_settle(&settle_in(&config, &dirty, None)), SettleDecision::Indeterminate(_)));
    let missing_job = blank();
    let mut input = settle_in(&config, &missing_job, None);
    input.queue = "NO";
    assert!(matches!(plan_settle(&input), SettleDecision::Rejected(InputError::Queue)));

    let request = Json::object(vec![("task_id", Json::String(TASK.into()))]);
    dead.stop_evidence = StopEvidence::LeaseExpired;
    dead.reason = "killed".into();
    let stored = receipt(OperationKind::Settle, ReceiptStatus::Accepted, &request);
    match plan_settle(&settle_in(&config, &dead, Some(&stored))) {
        SettleDecision::Replay(replay) => assert!(!replay.proved_without_receipt),
        other => panic!("receipt {other:?}"),
    }
    dead.reason = "retries".into();
    assert!(matches!(plan_settle(&settle_in(&config, &dead, Some(&stored))), SettleDecision::Inconsistent(_)));
    let terminal = receipt(OperationKind::Settle, ReceiptStatus::AlreadyTerminal, &request);
    dead.state = Some(TaskState::Done);
    match plan_settle(&settle_in(&config, &dead, Some(&terminal))) {
        SettleDecision::Replay(replay) => assert_eq!(replay.status, ReceiptStatus::AlreadyTerminal),
        other => panic!("already {other:?}"),
    }
    assert!(matches!(plan_settle(&settle_in(&config, &ready_job(), Some(&terminal))), SettleDecision::Inconsistent(_)));
    let missing = receipt(OperationKind::Settle, ReceiptStatus::NotFound, &request);
    match plan_settle(&settle_in(&config, &blank(), Some(&missing))) {
        SettleDecision::Replay(replay) => assert_eq!(replay.status, ReceiptStatus::NotFound),
        other => panic!("not found receipt {other:?}"),
    }
    assert!(matches!(plan_settle(&settle_in(&config, &dirty, Some(&missing))), SettleDecision::Inconsistent(_)));
    let unexpected = receipt(OperationKind::Settle, ReceiptStatus::Claimed, &request);
    assert!(matches!(plan_settle(&settle_in(&config, &blank(), Some(&unexpected))), SettleDecision::Inconsistent(_)));
    let mut corrupt = stored.clone();
    corrupt.operation_id = OperationId::parse(OTHER).unwrap();
    assert!(matches!(plan_settle(&settle_in(&config, &dead, Some(&corrupt))), SettleDecision::Inconsistent(_)));
    stored_conflict(&config, &request);
}

fn stored_conflict(config: &Config, request: &Json) {
    let mut stored = receipt(OperationKind::Enqueue, ReceiptStatus::Accepted, request);
    stored.op = OperationKind::Enqueue;
    let dead = {
        let mut job = ready_job();
        job.state = Some(TaskState::Dead);
        job
    };
    assert!(matches!(plan_settle(&settle_in(config, &dead, Some(&stored))), SettleDecision::Conflict(_)));
}

#[test]
fn postcondition_mismatch_names_the_failed_checks() {
    let observed = ObservedTask::absent();
    let enqueue = EnqueueProof {
        task_id: task(),
        message_id: message(),
        cycle_id: cycle(),
        actor_name: "process_item".into(),
        queue: "default".into(),
        kwargs_canonical: "{}".into(),
        headers_canonical: "{}".into(),
        tag: Some("batch-a".into()),
        unique_key: Some("item:1001".into()),
        available_at_ms: Some(1),
    };
    let mismatch = check_enqueue(&enqueue, &observed).unwrap_err();
    for name in ["hash_exists", "state", "cycle", "task_id", "uniq", "ready_score", "available_at"] {
        assert!(mismatch.contains(name), "{name} {mismatch:?}");
    }
    assert!(!mismatch.contains("missing-name"));
    let mut agree = ready_job();
    agree.tag = Some("batch-a".into());
    agree.unique_key = Some("item:1001".into());
    agree.uniq_owner = Some(task());
    agree.kwargs_canonical = "{}".into();
    agree.headers_canonical = "{}".into();
    let mut scored = enqueue.clone();
    scored.kwargs_canonical = agree.kwargs_canonical.clone();
    scored.available_at_ms = Some(1_000);
    check_enqueue(&scored, &agree).unwrap();
    scored.available_at_ms = None;
    check_enqueue(&scored, &agree).unwrap();
    agree.in_inflight = true;
    assert!(check_enqueue(&scored, &agree).unwrap_err().contains("indexes"));
    agree.in_inflight = false;
    agree.ready_score = Some(2);
    assert!(check_enqueue(&scored, &agree).unwrap_err().contains("ready_score"));
    assert!(!duplicate_holds(&agree));
    assert!(duplicate_holds(&observed));

    let claim = ClaimDeliveryProof { delivery_token: token(), deliveries: Some(2), cycle_id: cycle() };
    assert!(check_claim_delivery(&claim, &observed).unwrap_err().contains("deliveries"));
    let mut inflight = inflight_job();
    inflight.deliveries = 2;
    check_claim_delivery(&claim, &inflight).unwrap();
    let loose = ClaimDeliveryProof { deliveries: None, ..claim };
    inflight.deliveries = 0;
    assert!(check_claim_delivery(&loose, &inflight).unwrap_err().contains("deliveries"));
    inflight.deliveries = 3;
    check_claim_delivery(&loose, &inflight).unwrap();

    let dlq = ClaimDlqProof { uniq_owned_before: true, tag: Some("batch-a".into()) };
    let err = check_claim_dlq(&dlq, &observed).unwrap_err();
    assert!(err.contains("dead_member"));
    assert!(err.contains("dead_tag"));
    let mut dead = inflight.clone();
    dead.state = Some(TaskState::Dead);
    dead.reason = "max_deliveries".into();
    dead.in_dead = true;
    dead.in_inflight = false;
    dead.ready_score = None;
    dead.uniq_owner = None;
    dead.in_dead_tag = true;
    dead.tag = Some("batch-a".into());
    check_claim_dlq(&dlq, &dead).unwrap();
    dead.uniq_owner = Some(task());
    assert!(check_claim_dlq(&dlq, &dead).unwrap_err().contains("uniq_cleared"));
    dead.uniq_owner = None;
    let untagged = ClaimDlqProof { uniq_owned_before: false, tag: None };
    dead.in_dead_tag = false;
    dead.tag = None;
    check_claim_dlq(&untagged, &dead).unwrap();

    let renew = RenewProof {
        delivery_token: token(),
        cycle_id: cycle(),
        lease_deadline_ms: 20_000,
        started_at_ms: Some(1_000),
        deliveries: 1,
    };
    check_renew(&renew, &inflight_job()).unwrap();
    assert!(check_renew(&renew, &observed).unwrap_err().contains("lease_deadline"));

    let finish = FinishProof {
        task_id: task(),
        cycle_id: cycle(),
        shape: FinishShape::Ok,
        uniq_owned_before: true,
        ready_score: 1,
        finished_at_ms: 5,
        tag: None,
        error: String::new(),
    };
    assert!(check_finish(&finish, &observed).unwrap_err().contains("result"));
    let mut done = inflight_job();
    done.state = Some(TaskState::Done);
    done.result = Some(TaskResult::Ok);
    done.in_done = true;
    done.in_inflight = false;
    done.ready_score = None;
    done.finished_at_ms = Some(5);
    done.uniq_owner = None;
    check_finish(&finish, &done).unwrap();
    let skipped = FinishProof { shape: FinishShape::Skipped, uniq_owned_before: false, ..finish.clone() };
    done.result = Some(TaskResult::Skipped);
    done.uniq_owner = Some(other());
    check_finish(&skipped, &done).unwrap();
    let retry = FinishProof {
        shape: FinishShape::Retry { retries_after: 2 },
        uniq_owned_before: true,
        ready_score: 9,
        ..finish.clone()
    };
    let mut back = inflight_job();
    back.state = Some(TaskState::Ready);
    back.retries = 2;
    back.delivery_token = None;
    back.in_inflight = false;
    back.ready_score = Some(9);
    back.in_done = false;
    back.uniq_owner = Some(task());
    check_finish(&retry, &back).unwrap();
    let retry_clear = FinishProof { uniq_owned_before: false, ..retry };
    back.uniq_owner = None;
    check_finish(&retry_clear, &back).unwrap();
    let dead_finish = FinishProof {
        shape: FinishShape::Dead { reason: "boom".into() },
        uniq_owned_before: true,
        tag: Some("batch-a".into()),
        error: "e".into(),
        ..finish
    };
    let mut grave = back.clone();
    grave.state = Some(TaskState::Dead);
    grave.reason = "boom".into();
    grave.error = "e".into();
    grave.in_dead = true;
    grave.in_done = false;
    grave.in_inflight = false;
    grave.in_dead_tag = true;
    grave.uniq_owner = None;
    check_finish(&dead_finish, &grave).unwrap();

    let requeue = RequeueProof { task_id: task(), cycle_id: cycle2(), ready_score: 9, has_unique_key: true };
    assert!(check_requeue(&requeue, &observed).unwrap_err().contains("history"));
    let mut restored = ready_job();
    restored.cycle_id = Some(cycle2());
    restored.history_contains_prior = true;
    restored.ready_score = Some(9);
    restored.delivery_token = None;
    restored.uniq_owner = Some(task());
    check_requeue(&requeue, &restored).unwrap();

    let control = ControlProof {
        task_id: task(),
        shape: ControlShape::Terminal { reason: "cancelled".into(), uniq_owned_before: true },
    };
    assert!(check_control(&control, &observed).unwrap_err().contains("reason"));
    let mut cancelled = ready_job();
    cancelled.state = Some(TaskState::Dead);
    cancelled.reason = "cancelled".into();
    cancelled.ready_score = None;
    cancelled.in_done = false;
    cancelled.in_dead = true;
    cancelled.control_op = ControlOp::Empty;
    cancelled.uniq_owner = None;
    check_control(&control, &cancelled).unwrap();
    let inflight_control = ControlProof {
        task_id: task(),
        shape: ControlShape::Inflight { cycle_id: cycle(), token: token(), op: ControlOp::Kill },
    };
    let mut held = inflight_job();
    held.control_op = ControlOp::Kill;
    held.control_cycle_id = Some(cycle());
    held.control_delivery_token = Some(token());
    check_control(&inflight_control, &held).unwrap();
    assert!(is_terminal_state(&cancelled));
    assert!(!is_terminal_state(&held));

    let settle = SettleProof {
        task_id: task(),
        reason: "cancelled".into(),
        uniq_owned_before: false,
        tag: None,
    };
    assert!(check_settle(&settle, &observed).unwrap_err().contains("stop_evidence"));
    cancelled.stop_evidence = StopEvidence::LeaseExpired;
    cancelled.unique_key = None;
    cancelled.in_inflight = false;
    cancelled.in_dead_tag = false;
    check_settle(&settle, &cancelled).unwrap();
    assert!(settle_success(&cancelled));
    cancelled.reason = "retries".into();
    assert!(!settle_success(&cancelled));
    cancelled.reason = "killed".into();
    cancelled.tag = Some("batch-a".into());
    cancelled.in_dead_tag = false;
    assert!(!settle_success(&cancelled));
    cancelled.in_dead_tag = true;
    cancelled.unique_key = Some("item:1001".into());
    cancelled.uniq_owner = Some(task());
    assert!(!settle_success(&cancelled));
    cancelled.uniq_owner = None;
    assert!(settle_success(&cancelled));
}
