import json
import time
import uuid

import pytest

from arq_rs import Client, Config, QueueAdmin, actor
from arq_rs.client import _raise_native, parse_reply
from arq_rs.errors import ConfigError, DuplicateTask, OperationConflict, QueueFull, RedisError

URL = "redis://redis:6379/0"


def item() -> str:
    return f"item:{time.monotonic_ns()}"


def declare():
    @actor(queue="default", max_retries=0, time_limit=1000, min_backoff=10, max_backoff=20)
    async def process_item(item_id: str) -> None:
        return None

    return process_item


def test_config_from_env_and_native_errors(monkeypatch):
    config = Config()
    assert config.poll_interval_ms == 500
    assert config.child_memory_soft is None
    monkeypatch.setenv("ARQ_MAX_DELIVERIES", "3")
    monkeypatch.setenv("SIMPLE_Q_LEASE_MS", "1")
    with pytest.raises(ConfigError):
        Config.from_env()
    monkeypatch.delenv("SIMPLE_Q_LEASE_MS")
    loaded = Config.from_env()
    assert loaded.max_deliveries == 3
    with pytest.raises(ConfigError):
        Config({"ARQ_LEASE_MS": "0"})
    with pytest.raises(ConfigError):
        Config({"ARQ_UNKNOWN": "1"})
    assert isinstance(_raise_native(ValueError("nope")), RedisError)
    assert isinstance(_raise_native(RuntimeError(json.dumps({"ok": 1}))), RedisError)
    assert isinstance(_raise_native(RuntimeError(json.dumps({"error": {"kind": "config", "detail": "bad"}}))), ConfigError)
    with pytest.raises(ValueError):
        parse_reply(json.dumps({"error": {"kind": "page", "detail": "cursor"}}))
    with pytest.raises(RedisError):
        Client("redis://127.0.0.1:1", config=Config())


def test_send_duplicate_conflict_full_and_hooks():
    process_item = declare()

    async def scenario():
        client = Client(URL, config=Config())
        try:
            sent = []

            def remember(actor_name, kwargs, headers):
                sent.append((actor_name, dict(kwargs), dict(headers)))
                return {"source": "bridge"}

            def observe(actor_name, kwargs, headers):
                sent.append(("observed", dict(headers)))
                return None

            client.on_send(remember)
            client.on_send(observe)
            message_id = await client.send(process_item, item_id="1")
            assert message_id == str(uuid.UUID(message_id))
            assert sent[0][0] == "process_item"
            assert sent[0][1] == {"item_id": "1"}
            assert sent[0][2] == {}
            assert sent[1] == ("observed", {"source": "bridge"})
            key = item()
            first = await client.send_with_options(
                process_item,
                kwargs={"item_id": "2"},
                unique_key=key,
                tag="batch-a",
                headers={"source": "bridge"},
            )
            assert first
            assert await client.send_with_options(process_item, kwargs={"item_id": "2"}, unique_key=key) is None
            other = await client.send(process_item, item_id="3", unique_key=key)
            assert other
            admin = QueueAdmin(client, queue="default")
            info = await admin.get(other)
            assert info is not None and info.unique_key is None

            def explode(actor_name, kwargs, headers):
                raise RuntimeError("hook")

            client.on_send(explode)
            held = item()
            with pytest.raises(RuntimeError, match="hook"):
                await client.send_with_options(process_item, kwargs={"item_id": "4"}, unique_key=held)
            client._on_send.pop()
            assert await client.send_with_options(process_item, kwargs={"item_id": "4"}, unique_key=held)

            operation = str(uuid.uuid4())
            await client.send_with_options(process_item, kwargs={"item_id": "5"}, operation_id=operation)
            with pytest.raises(OperationConflict):
                await client.send_with_options(process_item, kwargs={"item_id": "6"}, operation_id=operation)

            depth = (await admin.snapshot()).queues[0].ready_zcard
            limited = Client(URL, config=Config({"ARQ_MAX_READY": str(depth + 1)}))
            try:
                await limited.send(process_item, item_id="7")
                with pytest.raises(QueueFull):
                    await limited.send(process_item, item_id="8")
            finally:
                await limited.close()

            class Fake:
                def submit(self, spec):
                    return json.dumps(
                        {"status": "duplicate", "unique_key": "item:9", "task_id": message_id, "owner_task_id": message_id}
                    )

            client._backend = Fake()
            with pytest.raises(DuplicateTask) as caught:
                await client.send(process_item, item_id="9")
            assert caught.value.unique_key == "item:9"
            with pytest.raises(ValueError):
                await client.send_with_options(process_item, kwargs={"item_id": "1"}, delay=-1)
            with pytest.raises(ValueError):
                await client.send_with_options(process_item, kwargs={"item_id": "1"}, unique_key="a b")
            with pytest.raises(ValueError):
                await client.send_with_options(process_item, kwargs={"item_id": "1"}, tag="")
            with pytest.raises(ValueError):
                await client.send_with_options(process_item, kwargs={"item_id": "1"}, message_id="NOPE")
            with pytest.raises(TypeError):
                await client.send_with_options(process_item, kwargs={"item_id": "1"}, headers={"n": 1})
            with pytest.raises(TypeError):
                await client.send_with_options(process_item, kwargs={"item_id": {1}})
            with pytest.raises(TypeError):
                await client.send_with_options(process_item, kwargs={"item_id": "1"}, headers="no")
            with pytest.raises(TypeError):
                await client.send_with_options(process_item, kwargs={"item_id": "1"}, unique_key=1)
            assert await client.send_with_options(process_item, kwargs={"item_id": "1"}, delay=5) is None
            with pytest.raises(TypeError):
                await client.send("process_item", item_id="1")
            enqueue = client._enqueue

            async def empty(*args, **kwargs):
                return None

            client._enqueue = empty
            with pytest.raises(RuntimeError, match="message_id"):
                await client.send(process_item, item_id="11")
            client._enqueue = enqueue
            await client.close()
            with pytest.raises(RuntimeError):
                await client.send(process_item, item_id="10")
        finally:
            await client.close()

    import asyncio

    asyncio.run(scenario())
