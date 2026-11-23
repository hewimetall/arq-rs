"""Actor declarations and the worker-side dispatch decision.

``@actor`` returns a declaration. It is not bound to an ``App`` or a connection.
``App.dispatch`` returns a dictionary. Rust commits that dictionary.
``on_dead`` runs inside dispatch, before the dictionary is returned.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import uuid
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any

from arq_rs.backoff import compute_backoff
from arq_rs.message import CurrentMessage, bind_message, reset_message
from arq_rs.metrics import finish_attempt, start_attempt
from arq_rs.tracing import Tracing
from arq_rs.tracking import tracked

log = logging.getLogger("arq_rs")

DEFAULT_TIME_LIMIT_MS = 1_800_000
DEFAULT_MIN_BACKOFF_MS = 15_000
DEFAULT_MAX_BACKOFF_MS = 86_400_000 * 7
DEFAULT_MAX_DELIVERIES = 5
MESSAGE_VERSION = 1
_LOCK_PARAMETERS = {"lock_key", "lock_ttl", "skip_on_lock"}


class CancelGraceExceeded(BaseException):
    """The handler ignored cancellation until ``cancel_grace_ms`` elapsed.

    This is a ``BaseException``. ``except Exception`` does not catch it.
    Rust does not finish the delivery.
    """


def validate_queue(queue: str) -> str:
    if (
        not isinstance(queue, str)
        or not queue
        or any(not (("a" <= character <= "z") or ("0" <= character <= "9")) for character in queue)
    ):
        raise ValueError(f"queue {queue!r} must match [a-z0-9]+")
    return queue


def _integer(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int")
    return value


def _number(message: dict[str, Any], key: str, default: int) -> int:
    raw = message.get(key)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _delivery_count(message: dict[str, Any]) -> int:
    raw = message.get("deliveries")
    if raw is None:
        return 1
    try:
        count = int(raw)
    except (TypeError, ValueError):
        return 1
    return count if count > 0 else 1


def _exception_text(caught: BaseException) -> str:
    return f"{type(caught).__name__}: {caught}"


def _dead_result(reason: str, error: str | None) -> dict[str, Any]:
    return {"outcome": "dead", "reason": reason, "error": error}


def apply_send_hooks(hooks, actor_name: str, kwargs: dict[str, Any], headers: dict[str, str]) -> dict[str, str]:
    current = dict(headers)
    for hook in hooks:
        updated = hook(actor_name, kwargs, current)
        if updated is None:
            continue
        if not isinstance(updated, dict):
            raise TypeError("on_send hook must return a headers dict")
        rewritten = {}
        for key, value in updated.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise TypeError("headers must be dict[str, str]")
            rewritten[key] = value
        current = rewritten
    return current


@dataclass(frozen=True)
class DeadLetter:
    actor_name: str
    kwargs: dict[str, Any]
    reason: str
    message_id: str
    retries: int
    deliveries: int
    error: str | None


@dataclass(frozen=True)
class Actor:
    function: Callable[..., Awaitable[Any]]
    name: str
    queue: str
    max_retries: int
    time_limit: int
    min_backoff: int
    max_backoff: int
    max_deliveries: int


def actor(
    fn: Callable[..., Awaitable[Any]] | None = None,
    *,
    name: str | None = None,
    queue: str = "default",
    max_retries: int = 0,
    time_limit: int = DEFAULT_TIME_LIMIT_MS,
    min_backoff: int = DEFAULT_MIN_BACKOFF_MS,
    max_backoff: int = DEFAULT_MAX_BACKOFF_MS,
    max_deliveries: int = DEFAULT_MAX_DELIVERIES,
    **rejected: Any,
):
    if rejected:
        key = next(iter(rejected))
        if key in _LOCK_PARAMETERS:
            raise TypeError(f"{key} is not a parameter of actor()")
        raise TypeError(f"actor() got an unexpected keyword argument {key!r}")
    validate_queue(queue)
    max_retries = _integer("max_retries", max_retries)
    time_limit = _integer("time_limit", time_limit)
    min_backoff = _integer("min_backoff", min_backoff)
    max_backoff = _integer("max_backoff", max_backoff)
    max_deliveries = _integer("max_deliveries", max_deliveries)
    if max_retries < 0:
        raise ValueError("max_retries must be >= 0")
    if max_deliveries < 1:
        raise ValueError("max_deliveries must be >= 1")
    if time_limit <= 0:
        raise ValueError("time_limit must be > 0")
    if min_backoff <= 0 or min_backoff > max_backoff:
        raise ValueError("backoff requires 0 < min_backoff <= max_backoff")

    def decorate(function: Callable[..., Awaitable[Any]]) -> Actor:
        if not inspect.iscoroutinefunction(function):
            raise TypeError("actor handler must be an async function")
        return Actor(
            function,
            name=name or function.__name__,
            queue=queue,
            max_retries=max_retries,
            time_limit=time_limit,
            min_backoff=min_backoff,
            max_backoff=max_backoff,
            max_deliveries=max_deliveries,
        )

    if fn is not None:
        return decorate(fn)
    return decorate


def build_message(
    *,
    actor_name: str,
    queue: str,
    kwargs: dict,
    headers: dict,
    max_retries: int,
    time_limit_ms: int = DEFAULT_TIME_LIMIT_MS,
    min_backoff_ms: int = DEFAULT_MIN_BACKOFF_MS,
    max_backoff_ms: int = DEFAULT_MAX_BACKOFF_MS,
    max_deliveries: int = DEFAULT_MAX_DELIVERIES,
    retries: int = 0,
    deliveries: int = 1,
    message_id: str | None = None,
    task_id: str | None = None,
    cycle_id: str | None = None,
    unique_key: str | None = None,
    tag: str | None = None,
    worker_name: str = "",
    cancel_grace_ms: int | None = None,
) -> dict[str, Any]:
    validate_queue(queue)
    if not isinstance(kwargs, dict):
        raise TypeError("kwargs must be a dict")
    if any(not isinstance(key, str) or not isinstance(value, str) for key, value in headers.items()):
        raise TypeError("headers must be dict[str, str]")
    if unique_key is not None and not isinstance(unique_key, str):
        raise TypeError("unique_key must be a str")
    if unique_key == "":
        raise ValueError("unique_key must not be empty")
    if tag == "":
        raise ValueError("tag must not be empty")
    chosen = message_id or str(uuid.uuid4())
    message: dict[str, Any] = {
        "version": MESSAGE_VERSION,
        "message_id": chosen,
        "task_id": task_id or chosen,
        "cycle_id": cycle_id or str(uuid.uuid4()),
        "actor_name": actor_name,
        "queue": queue,
        "kwargs": dict(kwargs),
        "headers": dict(headers),
        "retries": retries,
        "max_retries": max_retries,
        "deliveries": deliveries,
        "max_deliveries": max_deliveries,
        "time_limit_ms": time_limit_ms,
        "min_backoff_ms": min_backoff_ms,
        "max_backoff_ms": max_backoff_ms,
        "worker_name": worker_name,
    }
    if unique_key is not None:
        message["unique_key"] = unique_key
    if tag is not None:
        message["tag"] = tag
    if cancel_grace_ms is not None:
        message["cancel_grace_ms"] = cancel_grace_ms
    return message


class App:
    def __init__(
        self,
        *,
        client,
        actors: Iterable[Actor],
        on_start=None,
        on_stop=None,
        on_dead=None,
        around_dispatch=None,
        tracing: Tracing | None = None,
    ) -> None:
        self.client = client
        self.tracing = tracing
        self.actors: dict[str, Actor] = {}
        for declared in actors:
            if declared.name in self.actors:
                raise ValueError(f"actor {declared.name!r} is already registered")
            self.actors[declared.name] = declared
        self._on_start = list(on_start or [])
        self._on_stop = list(on_stop or [])
        self._on_dead = list(on_dead or [])
        self._around_dispatch = list(around_dispatch or [])

    def on_start(self, fn):
        self._on_start.append(fn)
        return fn

    def on_stop(self, fn):
        self._on_stop.append(fn)
        return fn

    def on_dead(self, fn):
        self._on_dead.append(fn)
        return fn

    def around_dispatch(self, fn):
        self._around_dispatch.append(fn)
        return fn

    async def startup(self) -> None:
        await self._run_hooks(self._on_start, "start")

    async def shutdown(self) -> None:
        if self.tracing is not None:
            try:
                self.tracing.exporter.shutdown()
            except Exception:
                log.exception("tracing flush failed")
        await self._run_hooks(self._on_stop, "stop")

    async def dispatch(self, message: dict[str, Any]) -> dict[str, Any]:
        actor_name = str(message.get("actor_name") or "")
        declared = self.actors.get(actor_name)
        queue = message.get("queue") or (declared.queue if declared is not None else "default")
        raw_kwargs = message.get("kwargs")
        kwargs = dict(raw_kwargs) if isinstance(raw_kwargs, dict) else {}
        raw_headers = message.get("headers") or {}
        headers = {}
        if isinstance(raw_headers, dict):
            headers = {str(key): str(value) for key, value in raw_headers.items()}
        message_id = str(message.get("message_id") or "")
        current = CurrentMessage(
            message_id=message_id,
            task_id=str(message.get("task_id") or message_id),
            cycle_id=str(message.get("cycle_id") or ""),
            retries=_number(message, "retries", 0),
            max_retries=_number(message, "max_retries", declared.max_retries if declared else 0),
            deliveries=_delivery_count(message),
            max_deliveries=_number(
                message,
                "max_deliveries",
                declared.max_deliveries if declared else DEFAULT_MAX_DELIVERIES,
            ),
            queue_name=str(queue),
            actor_name=actor_name,
            kwargs=kwargs,
            headers=headers,
            tag=message.get("tag") if isinstance(message.get("tag"), str) else None,
            worker_name=str(message.get("worker_name") or ""),
        )
        token = bind_message(current)
        try:

            async def call_next():
                return await self._dispatch_bound(message, declared, current)

            return await self._run_around(message, call_next)
        finally:
            reset_message(token)

    async def note_dead(
        self,
        actor_name: str,
        kwargs: dict[str, Any],
        reason: str,
        message_id: str,
        retries: int,
        deliveries: int = 0,
        *,
        error: str | None = None,
    ) -> None:
        await self._dead(actor_name, dict(kwargs or {}), reason, str(message_id), int(retries), int(deliveries), error=error)

    async def _dispatch_bound(self, message, declared: Actor | None, current: CurrentMessage) -> dict[str, Any]:
        if declared is None:
            await self._dead(
                current.actor_name,
                current.kwargs,
                "unknown_actor",
                current.message_id,
                current.retries,
                current.deliveries,
                error=None,
            )
            return _dead_result("unknown_actor", None)
        state = {"error": False}
        start_attempt(current.message_id, current.queue_name, current.actor_name)
        try:
            return await self._invoke(message, declared, current, state)
        finally:
            finish_attempt(
                current.message_id,
                current.queue_name,
                current.actor_name,
                error=state["error"],
                rejected=False,
            )

    async def _invoke(self, message, declared: Actor, current: CurrentMessage, state: dict[str, bool]) -> dict[str, Any]:
        limit_ms = int(message.get("time_limit_ms") or declared.time_limit)
        grace = self._grace_seconds(message)

        async def run():
            return await tracked(declared.function(**current.kwargs), declared.name)

        task = asyncio.create_task(run())
        try:
            await asyncio.wait_for(asyncio.shield(task), limit_ms / 1000)
        except TimeoutError:
            task.cancel()
            try:
                await asyncio.wait_for(task, grace)
            except TimeoutError:
                state["error"] = True
                raise CancelGraceExceeded()
            except asyncio.CancelledError:
                state["error"] = True
                return await self._timeout(message, declared, current)
            except Exception as caught:
                state["error"] = True
                return await self._failed(message, declared, current, caught, "retries")
            else:
                return {"outcome": "ok"}
        except asyncio.CancelledError:
            task.cancel()
            raise
        except Exception as caught:
            state["error"] = True
            return await self._failed(message, declared, current, caught, "retries")
        else:
            return {"outcome": "ok"}

    def _grace_seconds(self, message: dict[str, Any]) -> float:
        raw = message.get("cancel_grace_ms")
        if raw is None:
            config = getattr(self.client, "config", None)
            raw = getattr(config, "cancel_grace_ms", None) if config is not None else None
        if raw is None:
            raw = 5000
        return int(raw) / 1000

    async def _timeout(self, message, declared: Actor, current: CurrentMessage) -> dict[str, Any]:
        text = "CancelledError: "
        if current.retries >= current.max_retries:
            await self._dead(
                current.actor_name,
                current.kwargs,
                "timeout",
                current.message_id,
                current.retries,
                current.deliveries,
                error=text,
            )
            return _dead_result("timeout", text)
        return await self._retry(message, declared, current)

    async def _failed(self, message, declared: Actor, current: CurrentMessage, caught: BaseException, reason: str) -> dict[str, Any]:
        text = _exception_text(caught)
        if current.retries >= current.max_retries:
            await self._dead(
                current.actor_name,
                current.kwargs,
                reason,
                current.message_id,
                current.retries,
                current.deliveries,
                error=text,
            )
            return _dead_result(reason, text)
        return await self._retry(message, declared, current)

    async def _run_around(self, message, call_next):
        chain = call_next
        for hook in reversed(self._around_dispatch):
            chain = _bind_around(hook, message, chain)
        return await chain()

    async def _retry(self, message, declared: Actor, current: CurrentMessage) -> dict[str, Any]:
        delay_ms = compute_backoff(
            current.retries,
            min_backoff=int(message.get("min_backoff_ms") or declared.min_backoff),
            max_backoff=int(message.get("max_backoff_ms") or declared.max_backoff),
        )
        return {"outcome": "retry", "delay_ms": delay_ms}

    async def _dead(self, actor_name, kwargs, reason, message_id, retries, deliveries, *, error: str | None) -> None:
        letter = DeadLetter(
            actor_name=actor_name,
            kwargs=dict(kwargs),
            reason=reason,
            message_id=message_id,
            retries=retries,
            deliveries=deliveries,
            error=error,
        )
        for hook in self._on_dead:
            try:
                result = hook(letter)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                log.exception("dead hook failed for %s", actor_name)

    async def _run_hooks(self, hooks, label: str) -> None:
        for hook in hooks:
            try:
                result = hook()
                if inspect.isawaitable(result):
                    await result
            except Exception:
                log.exception("%s hook failed", label)


def _bind_around(hook, message, inner):
    async def wrapped():
        result = hook(message, inner)
        if inspect.isawaitable(result):
            return await result
        return result

    return wrapped

