import asyncio
import time
import uuid

import pytest

from arq_rs import Client, Config, QueueAdmin, QueueControl, actor
from arq_rs.admin import _child, _supervisor

URL = "redis://redis:6379/0"


def item() -> str:
    return f"item:{time.monotonic_ns()}"


def test_admin_and_control_round_trip():
    @actor(queue="default", max_retries=0, time_limit=1000, min_backoff=10, max_backoff=20)
    async def process_item(item_id: str) -> None:
        return None

    async def scenario():
        client = Client(URL, config=Config())
        admin = QueueAdmin(client, queue="default")
        control = QueueControl(client, queue="default")
        try:
            message_id = await client.send_with_options(
                process_item,
                kwargs={"item_id": "7"},
                unique_key=item(),
                tag="batch-a",
            )
            info = await admin.get(message_id)
            assert info is not None
            assert info.actor_name == "process_item"
            assert info.message_id == info.task_id == message_id
            assert info.tag == "batch-a"
            assert await admin.get(str(uuid.uuid4())) is None
            page = await admin.peek("ready", limit=1)
            assert page.tasks
            assert await admin.peek("scheduled", limit=1)
            assert await admin.peek("inflight", limit=1)
            with pytest.raises(ValueError):
                await admin.peek("active", limit=1)
            with pytest.raises(ValueError):
                await admin.peek("ready", limit=0)
            with pytest.raises(ValueError):
                await admin.peek("ready", limit=201)
            with pytest.raises(ValueError):
                await admin.peek("ready", limit=True)
            with pytest.raises(ValueError):
                await admin.dead(limit=1, cursor="nope")
            snapshot = await admin.snapshot(host="redis")
            assert snapshot.taken_at > 0
            assert snapshot.queues[0].name == "default"
            assert snapshot.queues[0].ready_zcard >= snapshot.queues[0].ready
            operation = str(uuid.uuid4())
            cancelled = await control.cancel(message_id, operation_id=operation)
            assert cancelled.status == "accepted"
            assert cancelled.task_id == message_id
            receipt = await admin.command(operation)
            assert receipt is not None and receipt.status == "accepted"
            assert await admin.command(str(uuid.uuid4())) is None
            killed = await control.kill(message_id)
            assert killed.status == "already_terminal"
            settled = await control.settle(message_id)
            assert settled.status == "already_terminal"
            missing = await control.settle(str(uuid.uuid4()))
            assert missing.status == "not_found"
            dead = await admin.dead(limit=50)
            assert any(task.task_id == message_id for task in dead.tasks)
            tagged = await admin.dead_for_tag("batch-a", limit=50)
            assert any(task.task_id == message_id for task in tagged.tasks)
            requeued = await control.requeue(message_id)
            assert requeued.status in {"requeued", "duplicate", "not_dead"}
            await client.close()
            with pytest.raises(RuntimeError):
                await admin.get(message_id)
            with pytest.raises(RuntimeError):
                await control.cancel(message_id)
        finally:
            await client.close()

    asyncio.run(scenario())
    with pytest.raises(ValueError):
        QueueAdmin(Client(URL), queue="Bad")


def test_snapshot_parsers_keep_child_tasks_empty():
    child = _child(
        {
            "host": "redis",
            "parent_pid": 1,
            "slot": 0,
            "pid": 2,
            "ppid": 1,
            "state": "running",
            "reason": "",
            "exit_code": 0,
            "rss_bytes": 10,
            "started_at_ms": 1,
            "ended_at_ms": None,
            "tasks_done": 0,
            "max_tasks": 5,
            "alive": True,
            "alive_window_ms": 15,
            "tasks": [],
        }
    )
    assert child.tasks == []
    assert child.exit_code == 0
    supervisor = _supervisor(
        {
            "host": "redis",
            "pid": 1,
            "started_at_ms": 1,
            "version": "0.1.0",
            "queues": ["default"],
            "processes": 1,
            "concurrency": 1,
            "max_tasks": 5,
            "memory_soft_ratio": "0.8",
            "memory_hard_ratio": "0.95",
            "child_memory_soft": 10,
            "child_memory_hard": None,
            "cgroup": "",
            "recycled": 1,
            "drains": 2,
            "kills": 3,
            "restarts": 4,
            "last_seen_ms": 8,
            "children": [
                {
                    "host": "redis",
                    "pid": 2,
                    "tasks": [{"task_id": "ignored"}],
                }
            ],
            "in_progress": 0,
        }
    )
    assert supervisor.drains == 2
    assert supervisor.children[0].tasks == [{"task_id": "ignored"}]
