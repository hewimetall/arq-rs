import asyncio
import logging

import pytest

from arq_rs import App, actor, build_message
from arq_rs.dispatch import CancelGraceExceeded, apply_send_hooks, validate_queue
from arq_rs.memory import MemoryClient
from arq_rs.message import CurrentMessage, current_message, worker_name
from arq_rs.tracing import Tracing


def _app(handler, **kwargs):
    return App(client=MemoryClient(), actors=[handler], **kwargs)


def test_actor_rejects_locks_bounds_and_duplicates():
    with pytest.raises(TypeError):
        actor(lock_key="item")
    with pytest.raises(TypeError):
        actor(lock_ttl=1)
    with pytest.raises(TypeError):
        actor(skip_on_lock=True)
    with pytest.raises(TypeError):
        actor(unexpected=1)
    with pytest.raises(ValueError):
        actor(queue="Bad")
    with pytest.raises(ValueError):
        actor(max_retries=-1)
    with pytest.raises(ValueError):
        actor(max_deliveries=0)
    with pytest.raises(ValueError):
        actor(time_limit=0)
    with pytest.raises(ValueError):
        actor(min_backoff=0, max_backoff=10)
    with pytest.raises(ValueError):
        actor(min_backoff=20, max_backoff=10)
    with pytest.raises(TypeError):
        actor(max_retries=True)

    def plain(item_id: str):
        return None

    with pytest.raises(TypeError):
        actor(plain)

    @actor
    async def process_item(item_id: str):
        return item_id

    again = actor(process_item.function)
    client = MemoryClient()
    with pytest.raises(ValueError):
        App(client=client, actors=[process_item, again])
    assert process_item.name == "process_item"
    assert process_item.queue == "default"
    assert validate_queue("default") == "default"


def test_build_message_checks_shape():
    with pytest.raises(TypeError):
        build_message(actor_name="process_item", queue="default", kwargs=[], headers={}, max_retries=0)
    with pytest.raises(TypeError):
        build_message(
            actor_name="process_item",
            queue="default",
            kwargs={},
            headers={"n": 1},
            max_retries=0,
        )
    with pytest.raises(ValueError):
        build_message(
            actor_name="process_item",
            queue="default",
            kwargs={},
            headers={},
            max_retries=0,
            unique_key="",
        )
    with pytest.raises(ValueError):
        build_message(actor_name="process_item", queue="default", kwargs={}, headers={}, max_retries=0, tag="")
    with pytest.raises(TypeError):
        build_message(actor_name="process_item", queue="default", kwargs={}, headers={}, max_retries=0, unique_key=1)
    with pytest.raises(TypeError):
        apply_send_hooks([lambda actor_name, kwargs, headers: "nope"], "process_item", {}, {})
    with pytest.raises(TypeError):
        apply_send_hooks([lambda actor_name, kwargs, headers: {1: "a"}], "process_item", {}, {})
    message = build_message(
        actor_name="process_item",
        queue="default",
        kwargs={"item_id": "1"},
        headers={"source": "test"},
        max_retries=1,
        unique_key="item:1",
        tag="batch-a",
        cancel_grace_ms=25,
    )
    assert message["task_id"] == message["message_id"]
    assert message["version"] == 1
    assert message["tag"] == "batch-a"


def test_hooks_order_current_message_and_replacement(caplog):
    caplog.set_level(logging.ERROR, logger="arq_rs")
    order = []

    @actor(queue="default", max_retries=1, time_limit=1000, min_backoff=10, max_backoff=40)
    async def process_item(item_id: str):
        order.append(("handler", current_message().actor_name, worker_name()))
        if item_id == "1":
            raise RuntimeError("boom")
        return item_id

    def send_a(actor_name, kwargs, headers):
        order.append("send-a")
        return {"source": "a"}

    def send_b(actor_name, kwargs, headers):
        order.append("send-b")
        return None

    async def start_a():
        order.append("start-a")

    def start_b():
        order.append("start-b")
        raise RuntimeError("start failed")

    async def start_c():
        order.append("start-c")

    async def stop_a():
        order.append("stop-a")

    def stop_broken():
        raise RuntimeError("stop failed")

    seen = {}
    replace_now = {"on": False}

    async def around_a(message, call_next):
        order.append("enter-a")
        seen["during"] = current_message().message_id
        try:
            result = await call_next()
        finally:
            order.append("exit-a")
            seen["inside"] = current_message() is not None
        if replace_now["on"] and isinstance(result, dict) and result.get("outcome") == "dead":
            order.append("replace")
            return {"outcome": "retry", "delay_ms": 5}
        return result

    async def around_b(message, call_next):
        order.append("enter-b")
        try:
            return await call_next()
        finally:
            order.append("exit-b")

    def dead_a(letter):
        order.append(("dead-a", letter.reason, letter.kwargs["item_id"]))
        letter.kwargs["item_id"] = "mutated"

    async def dead_b(letter):
        order.append(("dead-b", letter.kwargs["item_id"]))
        raise RuntimeError("dead failed")

    async def dead_c(letter):
        order.append("dead-c")

    class Exporter:
        def __init__(self):
            self.stopped = False

        def shutdown(self):
            self.stopped = True
            raise RuntimeError("flush failed")

    exporter = Exporter()
    client = MemoryClient()
    client.on_send(send_a)
    client.on_send(send_b)
    app = App(client=client, actors=[process_item], tracing=Tracing(exporter=exporter, resource={}))
    app.on_start(start_a)
    app.on_start(start_b)
    app.on_start(start_c)
    app.on_stop(stop_broken)
    app.on_stop(stop_a)
    app.on_dead(dead_a)
    app.on_dead(dead_b)
    app.on_dead(dead_c)
    app.around_dispatch(around_a)
    app.around_dispatch(around_b)

    async def scenario():
        await client.send(process_item, item_id="1")
        await app.startup()
        message = client._pop()
        message["retries"] = 1
        dead = await app.dispatch(message)
        assert dead["reason"] == "retries"
        replace_now["on"] = True
        await client.send(process_item, item_id="1")
        again = client._pop()
        again["retries"] = 1
        replaced = await app.dispatch(again)
        assert replaced == {"outcome": "retry", "delay_ms": 5}
        await app.shutdown()

    asyncio.run(scenario())
    assert order[0:2] == ["send-a", "send-b"]
    assert "start-a" in order and order.index("start-a") < order.index("start-b") < order.index("start-c")
    handler_at = next(index for index, item in enumerate(order) if isinstance(item, tuple) and item[0] == "handler")
    assert order.index("enter-a") < order.index("enter-b") < handler_at
    assert order.index("dead-c") < order.index("exit-b") < order.index("exit-a")
    assert order.index("replace") > order.index("exit-a")
    assert seen["inside"] is True
    assert current_message() is None
    assert worker_name() == ""
    assert exporter.stopped is True
    assert "start hook failed" in caplog.text
    assert "dead hook failed" in caplog.text
    assert "tracing flush failed" in caplog.text
    assert "stop hook failed" in caplog.text


def test_outcomes_timeout_and_unknown_actor():
    @actor(queue="default", max_retries=1, time_limit=30, min_backoff=10, max_backoff=20)
    async def process_item(item_id: str):
        if item_id == "sleep":
            try:
                await asyncio.sleep(5)
            except Exception:
                return "swallowed"
        if item_id == "hold":
            try:
                await asyncio.sleep(5)
            except BaseException:
                await asyncio.sleep(0.2)
        if item_id == "quiet":
            try:
                await asyncio.sleep(5)
            except BaseException:
                return "continued"
        if item_id == "bad":
            raise RuntimeError("boom")
        return "ok"

    app = _app(process_item)

    async def dispatch(item_id, **extra):
        message = build_message(
            actor_name="process_item",
            queue="default",
            kwargs={"item_id": item_id},
            headers={},
            max_retries=process_item.max_retries,
            time_limit_ms=process_item.time_limit,
            min_backoff_ms=10,
            max_backoff_ms=20,
            retries=extra.pop("retries", 0),
            worker_name="worker",
            **extra,
        )
        return await app.dispatch(message)

    async def scenario():
        ok = await dispatch("1")
        assert ok == {"outcome": "ok"}
        assert current_message() is None
        failed = await dispatch("bad")
        assert failed["outcome"] == "retry"
        assert failed["delay_ms"] >= 0
        assert current_message() is None
        exhausted = await dispatch("bad", retries=1)
        assert exhausted["reason"] == "retries"
        timed = await dispatch("sleep")
        assert timed["outcome"] == "retry"
        last = await dispatch("sleep", retries=1)
        assert last["reason"] == "timeout"
        assert last["error"].startswith("CancelledError")
        quiet = await dispatch("quiet", cancel_grace_ms=200)
        assert quiet == {"outcome": "ok"}
        with pytest.raises(CancelGraceExceeded):
            await dispatch("hold", cancel_grace_ms=30)
        assert current_message() is None
        letters = []
        app.on_dead(lambda letter: letters.append(letter.reason))
        unknown = await app.dispatch({"actor_name": "missing", "kwargs": {"item_id": "1"}, "retries": "x", "deliveries": "nope"})
        assert unknown["reason"] == "unknown_actor"
        assert letters[-1] == "unknown_actor"
        assert current_message() is None

        async def outer(message, call_next):
            assert current_message() is not None
            raise RuntimeError("middleware")

        app.around_dispatch(outer)
        with pytest.raises(RuntimeError, match="middleware"):
            await dispatch("1")
        assert current_message() is None

        def skip(message, call_next):
            return {"outcome": "maybe"}

        bare = _app(process_item)
        bare.around_dispatch(skip)
        weird = await bare.dispatch(build_message(
            actor_name="process_item",
            queue="default",
            kwargs={"item_id": "1"},
            headers={},
            max_retries=0,
        ))
        assert weird == {"outcome": "maybe"}
        bare2 = _app(process_item)

        def empty(message, call_next):
            return {"result": 1}

        bare2.around_dispatch(empty)
        assert (await bare2.dispatch(build_message(
            actor_name="process_item", queue="default", kwargs={"item_id": "1"}, headers={}, max_retries=0
        ))) == {"result": 1}

    asyncio.run(scenario())


def test_note_dead_copies_kwargs_and_sync_around_can_skip():
    @actor(queue="default", max_retries=0, time_limit=1000, min_backoff=10, max_backoff=20)
    async def process_item(item_id: str):
        return item_id

    letters = []

    async def remember(letter):
        letters.append(letter)

    app = _app(process_item)
    app.on_dead(remember)

    async def scenario():
        await app.note_dead("process_item", {"item_id": "1"}, "max_deliveries", "m", 0, 5, error=None)
        letters[0].kwargs["item_id"] = "changed"
        await app.note_dead("process_item", {"item_id": "1"}, "invalid_message", "m", 1, 2, error="bad")
        assert letters[0].kwargs["item_id"] == "changed"
        assert letters[1].kwargs["item_id"] == "1"
        assert letters[1].error == "bad"

        def skip(message, call_next):
            return {"outcome": "skipped"}

        app.around_dispatch(skip)
        result = await app.dispatch({"actor_name": "process_item", "kwargs": {"item_id": "9"}, "headers": {"n": 1}})
        assert result == {"outcome": "skipped"}
        assert current_message().__class__ is type(None) or current_message() is None

    asyncio.run(scenario())
    assert current_message() is None


def test_cancel_grace_exception_and_outer_cancel():
    seen = CurrentMessage(
        message_id="m",
        task_id="m",
        cycle_id="c",
        retries=0,
        max_retries=0,
        deliveries=1,
        max_deliveries=5,
        queue_name="default",
        actor_name="process_item",
        kwargs={"item_id": "1"},
        headers={},
        tag="batch-a",
        worker_name="",
    )
    assert seen.asdict()["queue_name"] == "default"

    @actor(queue="default", max_retries=0, time_limit=30, min_backoff=10, max_backoff=20)
    async def process_item(item_id: str):
        try:
            await asyncio.sleep(5)
        except BaseException:
            raise RuntimeError("after cancel")

    app = _app(process_item)

    async def grace_error():
        result = await app.dispatch(
            build_message(
                actor_name="process_item",
                queue="default",
                kwargs={"item_id": "1"},
                headers={},
                max_retries=0,
                time_limit_ms=30,
                cancel_grace_ms=200,
            )
        )
        assert result["outcome"] == "dead"
        assert result["reason"] == "retries"

    asyncio.run(grace_error())

    @actor(queue="default", max_retries=0, time_limit=1000, min_backoff=10, max_backoff=20)
    async def waiting(item_id: str):
        await asyncio.sleep(5)

    waiting_app = _app(waiting)

    async def outer():
        task = asyncio.create_task(
            waiting_app.dispatch(
                build_message(
                    actor_name="waiting",
                    queue="default",
                    kwargs={"item_id": "1"},
                    headers={},
                    max_retries=0,
                    time_limit_ms=1000,
                )
            )
        )
        await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(outer())
