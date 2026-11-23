"""Hook scenarios in a fresh interpreter. The parent starts this file as a process."""

import asyncio
import json
import sys

from arq_rs import App, actor
from arq_rs.memory import MemoryClient
from arq_rs.message import current_message


class Layer:
    def __init__(self, label: str, order: list[str], replace: bool) -> None:
        self.label = label
        self.order = order
        self.replace = replace

    def install(self, client, app) -> None:
        client.on_send(self.on_send)
        app.on_start(self.on_start)
        app.on_stop(self.on_stop)
        app.on_dead(self.on_dead)
        app.around_dispatch(self.around)

    def on_send(self, actor_name, kwargs, headers):
        self.order.append(f"send-{self.label}")
        return None

    async def on_start(self):
        self.order.append(f"start-{self.label}")

    async def on_stop(self):
        self.order.append(f"stop-{self.label}")

    async def on_dead(self, letter):
        self.order.append(f"dead-{self.label}:{letter.reason}")

    async def around(self, message, call_next):
        self.order.append(f"enter-{self.label}")
        seen = current_message()
        self.order.append(f"visible-{self.label}-{seen.actor_name if seen else 'none'}")
        try:
            result = await call_next()
        finally:
            self.order.append(f"exit-{self.label}")
        if self.replace and self.label == "a" and isinstance(result, dict) and result.get("outcome") == "dead":
            self.order.append("replace")
            return {"outcome": "retry", "delay_ms": 5}
        return result


def declare(order: list[str], *, limit: int, retries: int, body):
    @actor(queue="default", max_retries=retries, time_limit=limit, min_backoff=10, max_backoff=20)
    async def process_item(item_id: str):
        order.append(f"handler-{item_id}")
        await body()

    return process_item


async def run(mode: str) -> None:
    order: list[str] = []
    replace = mode == "replace"
    limit = 40 if mode in {"timeout", "swallow", "grace"} else 1000
    retries = 0

    async def body():
        if mode == "timeout":
            try:
                await asyncio.sleep(5)
            except Exception:
                order.append("swallowed-exception")
        elif mode == "swallow":
            try:
                await asyncio.sleep(5)
            except BaseException:
                return None
        elif mode == "grace":
            try:
                await asyncio.sleep(5)
            except BaseException:
                await asyncio.sleep(5)
        elif mode != "unknown":
            raise RuntimeError("boom")

    process_item = declare(order, limit=limit, retries=retries, body=body)
    grace = 40 if mode == "grace" else 500
    client = MemoryClient(config=type("Config", (), {"cancel_grace_ms": grace})())
    app = App(client=client, actors=[] if mode == "unknown" else [process_item])
    Layer("a", order, replace).install(client, app)
    Layer("b", order, replace).install(client, app)
    if mode == "start":

        def broken():
            raise RuntimeError("start failed")

        async def later():
            order.append("start-later")

        app.on_start(broken)
        app.on_start(later)
    if mode == "replace":

        def fail(letter):
            order.append("dead-fail")
            raise RuntimeError("dead hook failed")

        app.on_dead(fail)
    outcome = None
    if mode == "unknown":
        await app.startup()
        result = await app.dispatch(
            {
                "actor_name": "missing",
                "queue": "default",
                "kwargs": {"item_id": "1"},
                "headers": {},
                "message_id": "00000000-0000-4000-8000-000000000001",
                "task_id": "00000000-0000-4000-8000-000000000001",
                "retries": 0,
                "deliveries": 1,
            }
        )
        outcome = result.get("outcome")
        reason = result.get("reason")
        await app.shutdown()
    elif mode in {"replace", "timeout", "swallow", "grace"}:
        await client.send(process_item, item_id="1")
        await app.startup()
        result = await app.dispatch(client._pop())
        outcome = result.get("outcome")
        reason = result.get("reason")
        await app.shutdown()
    else:
        await client.send(process_item, item_id="1")
        await client.drain(app)
        reason = None
    after = current_message()
    json.dump(
        {
            "order": order,
            "after": None if after is None else after.actor_name,
            "mode": mode,
            "outcome": outcome,
            "reason": reason,
        },
        sys.stdout,
    )


if __name__ == "__main__":
    asyncio.run(run(sys.argv[1] if len(sys.argv) > 1 else "order"))
