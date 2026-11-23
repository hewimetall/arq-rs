"""Read-only queue views and task control. Keys are the arq hash, not Apalis."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from arq_rs.client import Client, parse_reply
from arq_rs.dispatch import validate_queue

_PEEK_STATES = {"ready", "scheduled", "inflight"}


@dataclass(frozen=True)
class TaskInfo:
    task_id: str
    message_id: str | None
    state: str
    deliveries: int
    retries: int
    score: int | None
    missing_job: bool
    tag: str | None
    reason: str
    error: str
    cycle_id: str | None
    control_op: str
    actor_name: str
    unique_key: str | None
    lease_deadline_ms: int | None
    started_at_ms: int | None
    available_at_ms: int | None
    stop_evidence: str
    queue: str


@dataclass(frozen=True)
class TaskPage:
    tasks: list[TaskInfo]
    next_cursor: str | None


@dataclass(frozen=True)
class DeadPage:
    tasks: list[TaskInfo]
    next_cursor: str | None


@dataclass(frozen=True)
class CommandReceipt:
    operation_id: str
    task_id: str
    status: str
    cycle_id: str | None


@dataclass(frozen=True)
class RequeueResult:
    operation_id: str
    task_id: str
    status: str
    cycle_id: str | None


@dataclass(frozen=True)
class QueueDepth:
    name: str
    ready: int
    scheduled: int
    inflight: int
    done: int
    dead: int
    ready_zcard: int


@dataclass(frozen=True)
class ChildInfo:
    host: str
    parent_pid: int
    slot: int
    pid: int
    ppid: int
    state: str
    reason: str
    exit_code: int | None
    rss_bytes: int
    started_at_ms: int
    ended_at_ms: int | None
    tasks_done: int
    max_tasks: int
    alive: bool
    alive_window_ms: int
    tasks: list


@dataclass(frozen=True)
class SupervisorInfo:
    host: str
    pid: int
    started_at_ms: int
    version: str
    queues: list[str]
    processes: int
    concurrency: int
    max_tasks: int
    memory_soft_ratio: str
    memory_hard_ratio: str
    child_memory_soft: int | None
    child_memory_hard: int | None
    cgroup: str
    recycled: int
    drains: int
    kills: int
    restarts: int
    last_seen_ms: int
    children: list[ChildInfo]
    in_progress: int


@dataclass(frozen=True)
class Snapshot:
    taken_at: int
    queues: list[QueueDepth]
    supervisors: list[SupervisorInfo]
    orphans: list[ChildInfo]


class QueueAdmin:
    def __init__(self, client: Client, queue: str = "default") -> None:
        validate_queue(queue)
        self.client = client
        self.queue = queue

    async def snapshot(self, host: str | None = None) -> Snapshot:
        self._open()
        raw = await asyncio.to_thread(self.client._backend.snapshot, self.queue, host or "")
        return _snapshot(parse_reply(raw))

    async def get(self, task_id: str) -> TaskInfo | None:
        self._open()
        raw = await asyncio.to_thread(self.client._backend.get, self.queue, task_id)
        task = parse_reply(raw).get("task")
        if task is None:
            return None
        return _task(task)

    async def peek(self, state: str, limit: int = 50, cursor: str | None = None) -> TaskPage:
        self._open()
        if state not in _PEEK_STATES:
            raise ValueError("peek state must be ready, scheduled, or inflight")
        raw = await asyncio.to_thread(self.client._backend.peek, self.queue, state, _limit(limit), cursor or "")
        return _page(parse_reply(raw), TaskPage)

    async def dead(self, limit: int = 50, cursor: str | None = None) -> DeadPage:
        self._open()
        raw = await asyncio.to_thread(self.client._backend.dead, self.queue, _limit(limit), cursor or "")
        return _page(parse_reply(raw), DeadPage)

    async def dead_for_tag(self, tag: str, limit: int = 50, cursor: str | None = None) -> DeadPage:
        self._open()
        raw = await asyncio.to_thread(self.client._backend.dead_for_tag, self.queue, tag, _limit(limit), cursor or "")
        return _page(parse_reply(raw), DeadPage)

    async def command(self, operation_id: str) -> CommandReceipt | None:
        self._open()
        raw = await asyncio.to_thread(self.client._backend.command, self.queue, operation_id)
        receipt = parse_reply(raw).get("receipt")
        if receipt is None:
            return None
        return _receipt(receipt)

    def _open(self) -> None:
        if self.client._closed:
            raise RuntimeError("client is closed")


class QueueControl:
    def __init__(self, client: Client, queue: str = "default") -> None:
        validate_queue(queue)
        self.client = client
        self.queue = queue

    async def cancel(self, task_id: str, *, operation_id: str | None = None) -> CommandReceipt:
        return await self._control("cancel", task_id, operation_id)

    async def kill(self, task_id: str, *, operation_id: str | None = None) -> CommandReceipt:
        return await self._control("kill", task_id, operation_id)

    async def settle(self, task_id: str, *, operation_id: str | None = None) -> CommandReceipt:
        return await self._control("settle", task_id, operation_id)

    async def requeue(self, task_id: str, *, operation_id: str | None = None) -> RequeueResult:
        self._open()
        operation = _operation(operation_id)
        raw = await asyncio.to_thread(self.client._backend.control, "requeue", self.queue, task_id, operation)
        data = parse_reply(raw)
        return RequeueResult(data["operation_id"], data["task_id"], data["status"], data.get("cycle_id"))

    async def _control(self, kind: str, task_id: str, operation_id: str | None) -> CommandReceipt:
        self._open()
        operation = _operation(operation_id)
        raw = await asyncio.to_thread(self.client._backend.control, kind, self.queue, task_id, operation)
        data = parse_reply(raw)
        return CommandReceipt(data["operation_id"], data["task_id"], data["status"], data.get("cycle_id"))

    def _open(self) -> None:
        if self.client._closed:
            raise RuntimeError("client is closed")


def _limit(limit: int) -> int:
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1 or limit > 200:
        raise ValueError("limit must be an int from 1 to 200")
    return limit


def _operation(operation_id: str | None) -> str:
    from arq_rs.client import _canonical

    return _canonical(operation_id, "operation_id")


def _page(data: dict, kind):
    tasks = [_task(item) for item in data.get("tasks") or []]
    return kind(tasks, data.get("next_cursor"))


def _task(data: dict) -> TaskInfo:
    return TaskInfo(
        task_id=str(data.get("task_id") or ""),
        message_id=data.get("message_id"),
        state=str(data.get("state") or ""),
        deliveries=int(data.get("deliveries") or 0),
        retries=int(data.get("retries") or 0),
        score=_opt_int(data.get("score")),
        missing_job=bool(data.get("missing_job")),
        tag=data.get("tag"),
        reason=str(data.get("reason") or ""),
        error=str(data.get("error") or ""),
        cycle_id=data.get("cycle_id"),
        control_op=str(data.get("control_op") or ""),
        actor_name=str(data.get("actor_name") or ""),
        unique_key=data.get("unique_key"),
        lease_deadline_ms=_opt_int(data.get("lease_deadline_ms")),
        started_at_ms=_opt_int(data.get("started_at_ms")),
        available_at_ms=_opt_int(data.get("available_at_ms")),
        stop_evidence=str(data.get("stop_evidence") or ""),
        queue=str(data.get("queue") or ""),
    )


def _receipt(data: dict) -> CommandReceipt:
    return CommandReceipt(
        str(data.get("operation_id") or ""),
        str(data.get("task_id") or ""),
        str(data.get("status") or ""),
        data.get("cycle_id"),
    )


def _snapshot(data: dict) -> Snapshot:
    return Snapshot(
        taken_at=int(data.get("taken_at") or 0),
        queues=[_depth(item) for item in data.get("queues") or []],
        supervisors=[_supervisor(item) for item in data.get("supervisors") or []],
        orphans=[_child(item) for item in data.get("orphans") or []],
    )


def _depth(data: dict) -> QueueDepth:
    return QueueDepth(
        name=str(data.get("name") or ""),
        ready=int(data.get("ready") or 0),
        scheduled=int(data.get("scheduled") or 0),
        inflight=int(data.get("inflight") or 0),
        done=int(data.get("done") or 0),
        dead=int(data.get("dead") or 0),
        ready_zcard=int(data.get("ready_zcard") or 0),
    )


def _child(data: dict) -> ChildInfo:
    return ChildInfo(
        host=str(data.get("host") or ""),
        parent_pid=int(data.get("parent_pid") or 0),
        slot=int(data.get("slot") or 0),
        pid=int(data.get("pid") or 0),
        ppid=int(data.get("ppid") or 0),
        state=str(data.get("state") or ""),
        reason=str(data.get("reason") or ""),
        exit_code=_opt_int(data.get("exit_code")),
        rss_bytes=int(data.get("rss_bytes") or 0),
        started_at_ms=int(data.get("started_at_ms") or 0),
        ended_at_ms=_opt_int(data.get("ended_at_ms")),
        tasks_done=int(data.get("tasks_done") or 0),
        max_tasks=int(data.get("max_tasks") or 0),
        alive=bool(data.get("alive")),
        alive_window_ms=int(data.get("alive_window_ms") or 0),
        tasks=list(data.get("tasks") or []),
    )


def _supervisor(data: dict) -> SupervisorInfo:
    return SupervisorInfo(
        host=str(data.get("host") or ""),
        pid=int(data.get("pid") or 0),
        started_at_ms=int(data.get("started_at_ms") or 0),
        version=str(data.get("version") or ""),
        queues=list(data.get("queues") or []),
        processes=int(data.get("processes") or 0),
        concurrency=int(data.get("concurrency") or 0),
        max_tasks=int(data.get("max_tasks") or 0),
        memory_soft_ratio=str(data.get("memory_soft_ratio") or ""),
        memory_hard_ratio=str(data.get("memory_hard_ratio") or ""),
        child_memory_soft=_opt_int(data.get("child_memory_soft")),
        child_memory_hard=_opt_int(data.get("child_memory_hard")),
        cgroup=str(data.get("cgroup") or ""),
        recycled=int(data.get("recycled") or 0),
        drains=int(data.get("drains") or 0),
        kills=int(data.get("kills") or 0),
        restarts=int(data.get("restarts") or 0),
        last_seen_ms=int(data.get("last_seen_ms") or 0),
        children=[_child(item) for item in data.get("children") or []],
        in_progress=int(data.get("in_progress") or 0),
    )


def _opt_int(value):
    if value is None:
        return None
    return int(value)
