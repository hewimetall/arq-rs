"""In-memory client for tests. It is not part of the public import surface.

``drain`` runs queued messages through ``App.dispatch``. A retry is queued
again immediately, without waiting out the backoff, and its unique key stays
held. A terminal outcome releases the key. This client does not open Redis
and does not implement locks.
"""

from __future__ import annotations

import uuid
from typing import Any

from arq_rs.dispatch import Actor, apply_send_hooks, build_message
from arq_rs.errors import DuplicateTask


class MemoryClient:
    def __init__(self, config: Any = None) -> None:
        self.config = config
        self._queues: dict[str, list[dict[str, Any]]] = {}
        self._held: dict[tuple[str, str], str] = {}
        self._on_send = []
        self._closed = False

    def on_send(self, fn):
        self._on_send.append(fn)
        return fn

    async def send(self, actor: Actor, /, **kwargs: Any) -> str:
        message_id = await self.send_with_options(actor, kwargs=kwargs, _raise=True)
        assert message_id is not None
        return message_id

    async def send_with_options(
        self,
        actor: Actor,
        *,
        kwargs: dict | None = None,
        delay: int | None = None,
        headers: dict[str, str] | None = None,
        unique_key: str | None = None,
        tag: str | None = None,
        message_id: str | None = None,
        operation_id: str | None = None,
        _raise: bool = False,
    ) -> str | None:
        if self._closed:
            raise RuntimeError("client is closed")
        if delay is not None and (isinstance(delay, bool) or not isinstance(delay, int) or delay < 0):
            raise ValueError("delay must be a non-negative int")
        body = dict(kwargs or {})
        header_map = apply_send_hooks(self._on_send, actor.name, body, dict(headers or {}))
        chosen = message_id or str(uuid.uuid4())
        payload = build_message(
            actor_name=actor.name,
            queue=actor.queue,
            kwargs=body,
            headers=header_map,
            max_retries=actor.max_retries,
            time_limit_ms=actor.time_limit,
            min_backoff_ms=actor.min_backoff,
            max_backoff_ms=actor.max_backoff,
            max_deliveries=actor.max_deliveries,
            message_id=chosen,
            unique_key=unique_key,
            tag=tag,
        )
        if delay:
            payload["delay_ms"] = int(delay)
        if operation_id is not None:
            payload["operation_id"] = operation_id
        token = self._token(payload)
        if token is not None and token in self._held:
            if _raise:
                raise DuplicateTask(token[1], chosen, self._held[token])
            return None
        if token is not None:
            self._held[token] = chosen
        self._queues.setdefault(str(payload["queue"]), []).append(payload)
        return payload["message_id"]

    async def close(self) -> None:
        self._closed = True

    def depth(self, queue: str) -> int:
        return len(self._queues.get(str(queue), ()))

    async def drain(self, app) -> None:
        await app.startup()
        try:
            while True:
                message = self._pop()
                if message is None:
                    return
                try:
                    result = await app.dispatch(message)
                except Exception:
                    self._release_unique(message)
                    raise
                outcome = result.get("outcome") if isinstance(result, dict) else None
                if outcome == "retry":
                    again = dict(message)
                    again["retries"] = int(again.get("retries") or 0) + 1
                    again.pop("delay_ms", None)
                    self._queues.setdefault(str(again["queue"]), []).append(again)
                    continue
                self._release_unique(message)
        finally:
            await app.shutdown()

    def _token(self, message: dict[str, Any]) -> tuple[str, str] | None:
        key = message.get("unique_key")
        if not isinstance(key, str) or not key:
            return None
        return (str(message.get("queue")), key)

    def _pop(self) -> dict[str, Any] | None:
        for pending in self._queues.values():
            if pending:
                return pending.pop(0)
        return None

    def _release_unique(self, message: dict[str, Any]) -> None:
        token = self._token(message)
        if token is not None:
            self._held.pop(token, None)
