"""Адаптер стенда на arq-rs.

Сценарии и актёры те же, что у simple-q в research-репозитории.
Ключи с фигурными скобками стенд пишет как ``item:{job}:1``. Контракт
``Client`` такие ключи отвергает до Redis. ``legal_key`` убирает скобки,
чтобы измерить поведение очереди. Отдельный прогон ``key_shape`` шлёт
исходную строку без замены.
"""

from __future__ import annotations

import os
from typing import Any

import arq_rs
from arq_rs import App, Client, Config, DeadLetter, QueueAdmin, actor, current_message, worker_name

from bench import core

_client: Client | None = None


def legal_key(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.replace("{", "").replace("}", "").replace(" ", "-")
    if cleaned != value:
        return cleaned
    return value


def _cfg() -> Config:
    picked = {}
    for key, value in os.environ.items():
        if key.startswith("ARQ_"):
            picked[key] = value
    return Config(picked)


def client() -> Client:
    global _client
    if _client is None:
        _client = Client(os.environ["REDIS_URL"], _cfg())
        _client.on_send(_trace_header)
    return _client


def _meta() -> str:
    message = current_message()
    if message is None:
        return ""
    return f"retries={message.retries} deliveries={message.deliveries} id={message.message_id}"


@actor(name="probe", queue="bench", max_retries=0, time_limit=30_000, max_deliveries=3, min_backoff=100, max_backoff=300)
async def probe(token: str) -> None:
    path = os.environ.get("PROBE_PATH", "/tmp/bench-probe")
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(token + "\n")


@actor(name="pause", queue="bench", max_retries=0, time_limit=180_000, max_deliveries=3, min_backoff=100, max_backoff=300)
async def pause(token: str, seconds: float) -> None:
    import asyncio

    path = os.environ.get("EFFECT_PATH", "/tmp/bench-effect")
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(f"start {token}\n")
        handle.flush()
    await asyncio.sleep(float(seconds))
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(f"end {token}\n")
        handle.flush()


@actor(
    name="failonce",
    queue="bench",
    max_retries=1,
    min_backoff=100,
    max_backoff=300,
    time_limit=30_000,
    max_deliveries=3,
)
async def failonce(token: str) -> None:
    path = os.environ.get("EFFECT_PATH", "/tmp/bench-effect")
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(token + "\n")
        handle.flush()
    seen = [line.strip() for line in open(path, encoding="utf-8") if line.strip()]
    if seen.count(token) == 1:
        raise RuntimeError("fail once")


@actor(
    name="item",
    queue="bench",
    max_retries=3,
    min_backoff=200,
    max_backoff=2_000,
    time_limit=120_000,
    max_deliveries=5,
)
async def item(job_id: str, n: int) -> None:
    await core.do_item(job_id, int(n), worker=worker_name(), note=_meta())


@actor(
    name="fanout",
    queue="bench",
    max_retries=2,
    min_backoff=200,
    max_backoff=2_000,
    time_limit=120_000,
    max_deliveries=5,
)
async def fanout(job_id: str, n: int) -> None:
    await core.note_attempt(job_id, -2, worker_name(), "fanout " + _meta())
    for i in range(int(n)):
        await client().send_with_options(
            item,
            kwargs={"job_id": job_id, "n": i},
            unique_key=legal_key(f"item:{job_id}:{i}"),
        )


@actor(
    name="chain",
    queue="bench",
    max_retries=3,
    min_backoff=200,
    max_backoff=2_000,
    time_limit=120_000,
    max_deliveries=5,
)
async def chain(job_id: str, step: int, total: int) -> None:
    await core.note_attempt(job_id, int(step), worker_name(), "chain " + _meta())
    await core.mark_progress(job_id, int(step))
    nxt = int(step) + 1
    if nxt < int(total):
        delay = 2000 if int(step) == 1 else None
        await client().send_with_options(
            chain,
            kwargs={"job_id": job_id, "step": nxt, "total": int(total)},
            delay=delay,
            unique_key=legal_key(f"chain:{job_id}:{nxt}"),
        )


@actor(
    name="longjob",
    queue="bench",
    max_retries=30,
    min_backoff=500,
    max_backoff=2_000,
    time_limit=600_000,
    max_deliveries=8,
)
async def longjob(job_id: str, steps: int, step_s: float) -> None:
    await core.note_attempt(job_id, -1, worker_name(), "long " + _meta())
    await core.long_job(job_id, int(steps), float(step_s))


@actor(
    name="poisonjob",
    queue="bench",
    max_retries=30,
    min_backoff=200,
    max_backoff=1_000,
    time_limit=600_000,
    max_deliveries=5,
)
async def poisonjob(job_id: str, steps: int, step_s: float) -> None:
    await core.note_attempt(job_id, -1, worker_name(), "poison " + _meta())
    await core.long_job(job_id, int(steps), float(step_s))


@actor(
    name="boom",
    queue="bench",
    max_retries=2,
    min_backoff=100,
    max_backoff=300,
    time_limit=30_000,
    max_deliveries=8,
)
async def boom(job_id: str) -> None:
    await core.note_attempt(job_id, 0, worker_name(), "boom " + _meta())
    raise RuntimeError("boom")


@actor(
    name="snooze",
    queue="bench",
    max_retries=0,
    time_limit=600_000,
    max_deliveries=3,
    min_backoff=100,
    max_backoff=300,
)
async def snooze(job_id: str, n: int, seconds: float) -> None:
    import asyncio

    await core.note_attempt(job_id, int(n), worker_name(), "snooze-start")
    await asyncio.sleep(float(seconds))
    await core.note_attempt(job_id, int(n), worker_name(), "snooze-end")


@actor(
    name="blocker",
    queue="bench",
    max_retries=0,
    time_limit=600_000,
    max_deliveries=3,
    min_backoff=100,
    max_backoff=300,
)
async def blocker(job_id: str, seconds: float) -> None:
    import time

    await core.note_attempt(job_id, 0, worker_name(), "block-start " + _meta())
    time.sleep(float(seconds))
    await core.note_attempt(job_id, 1, worker_name(), "block-end " + _meta())


@actor(
    name="fast",
    queue="bench",
    max_retries=0,
    time_limit=60_000,
    max_deliveries=3,
    min_backoff=100,
    max_backoff=300,
)
async def fast(job_id: str, n: int, sent_ms: int) -> None:
    core.fast_record(job_id, int(n), int(sent_ms))


@actor(
    name="limitjob",
    queue="bench",
    max_retries=1,
    min_backoff=100,
    max_backoff=200,
    time_limit=1_500,
    max_deliveries=5,
)
async def limitjob(job_id: str) -> None:
    import asyncio

    await core.note_attempt(job_id, 0, worker_name(), "limit " + _meta())
    await asyncio.sleep(30)


async def on_dead(letter: DeadLetter) -> None:
    job_id = letter.kwargs.get("job_id")
    if not job_id:
        return
    await core.mark_failed(str(job_id), f"{letter.reason} deliveries={letter.deliveries} retries={letter.retries}")


async def around(message: Any, call_next):
    current = current_message()
    header = ""
    job_id = None
    if current is not None:
        header = (current.headers or {}).get("traceparent", "")
        job_id = (current.kwargs or {}).get("job_id")
    elif isinstance(message, dict):
        header = (message.get("headers") or {}).get("traceparent", "")
        job_id = (message.get("kwargs") or {}).get("job_id")
    if header:
        await core.note(str(job_id) if job_id else None, "traceparent", header)
    return await call_next()


def _trace_header(actor_name: str, kwargs: dict, headers: dict) -> dict:
    updated = dict(headers)
    updated.setdefault("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
    return updated


def create_app() -> App:
    return App(
        client=client(),
        actors=[
            probe,
            pause,
            failonce,
            item,
            fanout,
            chain,
            longjob,
            poisonjob,
            boom,
            snooze,
            blocker,
            fast,
            limitjob,
        ],
        on_start=[core.open_deps],
        on_stop=[core.close_deps],
        on_dead=[on_dead],
        around_dispatch=[around],
    )


ACTORS = {
    "probe": probe,
    "pause": pause,
    "failonce": failonce,
    "item": item,
    "fanout": fanout,
    "chain": chain,
    "longjob": longjob,
    "poisonjob": poisonjob,
    "boom": boom,
    "snooze": snooze,
    "blocker": blocker,
    "fast": fast,
    "limitjob": limitjob,
}


async def enqueue(op: str, payload: dict) -> dict:
    declared = ACTORS[op]
    kwargs = dict(payload.get("kwargs") or {})
    unique = payload.get("unique_key")
    raw_unique = unique
    unique = legal_key(unique)
    delay = payload.get("delay_ms")
    headers = payload.get("headers")
    options: dict[str, Any] = {"kwargs": kwargs}
    if delay:
        options["delay"] = int(delay)
    if unique:
        options["unique_key"] = unique
    if headers:
        options["headers"] = headers
    message_id = await client().send_with_options(declared, **options)
    return {
        "accepted": message_id is not None,
        "id": message_id,
        "unique_key": unique,
        "unique_key_raw": raw_unique,
    }


async def admin_snapshot() -> dict:
    admin = QueueAdmin(client(), "bench")
    snap = await admin.snapshot()
    dead = await admin.dead(limit=20)
    waiting = await admin.peek("ready", limit=20)
    return {
        "snapshot": _freeze(snap),
        "dead": [_freeze(item) for item in dead.tasks],
        "waiting": [_freeze(item) for item in waiting.tasks],
    }


def _freeze(value: Any) -> Any:
    if hasattr(value, "__dataclass_fields__"):
        raw = {name: getattr(value, name) for name in value.__dataclass_fields__}
        return {k: _freeze(v) for k, v in raw.items()}
    if isinstance(value, dict):
        return {str(k): _freeze(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_freeze(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)
