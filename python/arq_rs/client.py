"""Async Redis client. Constructing it opens the socket. Importing this module does not."""

from __future__ import annotations

import asyncio
import json
import os
import re
import uuid
from typing import Any

from arq_rs import _native
from arq_rs.dispatch import Actor, apply_send_hooks
from arq_rs.errors import ConfigError, DuplicateTask, RedisError, map_error

_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")


class Config:
    def __init__(self, values: dict[str, str] | None = None) -> None:
        self.values = {str(key): str(value) for key, value in (values or {}).items()}
        payload = json.loads(_native.parse_config(json.dumps(self.values)))
        if "error" in payload:
            detail = payload["error"].get("detail") or "invalid config"
            raise ConfigError(detail)
        self.fields = payload["config"]
        for key, value in self.fields.items():
            setattr(self, key, value)

    @classmethod
    def from_env(cls) -> Config:
        picked = {}
        for key, value in os.environ.items():
            if key.startswith("ARQ_") or key.startswith("SIMPLE_Q_"):
                picked[key] = value
        return cls(picked)


class Client:
    def __init__(self, url: str, config: Config | None = None) -> None:
        self.url = url
        self.config = config if config is not None else Config()
        self._closed = False
        self._on_send = []
        try:
            self._backend = _native.Backend(url, json.dumps(self.config.values))
        except (ValueError, RuntimeError) as error:
            raise _raise_native(error) from error

    def on_send(self, fn):
        self._on_send.append(fn)
        return fn

    async def send(self, actor: Actor, /, **kwargs: Any) -> str:
        message_id = await self._enqueue(actor, kwargs=kwargs, raise_duplicate=True)
        if not isinstance(message_id, str):
            raise RuntimeError("send did not return a message_id")
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
    ) -> str | None:
        return await self._enqueue(
            actor,
            kwargs=kwargs,
            delay=delay,
            headers=headers,
            unique_key=unique_key,
            tag=tag,
            message_id=message_id,
            operation_id=operation_id,
            raise_duplicate=False,
        )

    async def close(self) -> None:
        self._closed = True

    async def _enqueue(
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
        raise_duplicate: bool,
    ) -> str | None:
        if self._closed:
            raise RuntimeError("client is closed")
        if not isinstance(actor, Actor):
            raise TypeError("expected an Actor")
        body = dict(kwargs or {})
        header_map = _headers(headers)
        delay_ms = _delay(delay)
        _opaque("unique_key", unique_key)
        _opaque("tag", tag)
        try:
            json.dumps(body)
        except (TypeError, ValueError) as error:
            raise TypeError("kwargs must be a JSON object") from error
        chosen_message = _canonical(message_id, "message_id")
        chosen_operation = _canonical(operation_id, "operation_id")
        cycle_id = str(uuid.uuid4())
        header_map = apply_send_hooks(self._on_send, actor.name, body, header_map)
        spec = {
            "queue": actor.queue,
            "actor_name": actor.name,
            "kwargs": body,
            "headers": header_map,
            "unique_key": unique_key,
            "tag": tag,
            "delay_ms": delay_ms,
            "message_id": chosen_message,
            "operation_id": chosen_operation,
            "cycle_id": cycle_id,
            "max_retries": actor.max_retries,
            "max_deliveries": actor.max_deliveries,
            "time_limit_ms": actor.time_limit,
            "min_backoff_ms": actor.min_backoff,
            "max_backoff_ms": actor.max_backoff,
        }
        raw = await asyncio.to_thread(self._backend.submit, json.dumps(spec))
        data = json.loads(raw)
        if "error" in data:
            raise map_error(data["error"])
        if data.get("status") == "duplicate":
            error = DuplicateTask(str(data.get("unique_key") or ""), str(data.get("task_id") or ""), str(data.get("owner_task_id") or ""))
            if raise_duplicate:
                raise error
            return None
        return str(data["message_id"])


def parse_reply(raw: str) -> dict:
    data = json.loads(raw)
    if isinstance(data, dict) and "error" in data:
        raise map_error(data["error"])
    return data


def _raise_native(error: BaseException) -> BaseException:
    text = str(error)
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return RedisError(text)
    if isinstance(data, dict) and "error" in data:
        return map_error(data["error"])
    return RedisError(text)


def _headers(headers: dict[str, str] | None) -> dict[str, str]:
    if headers is None:
        return {}
    if not isinstance(headers, dict):
        raise TypeError("headers must be dict[str, str]")
    copied = {}
    for key, value in headers.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError("headers must be dict[str, str]")
        copied[key] = value
    return copied


def _delay(delay: int | None) -> int:
    if delay is None:
        return 0
    if isinstance(delay, bool) or not isinstance(delay, int) or delay < 0:
        raise ValueError("delay must be a non-negative int")
    return delay


def _opaque(field: str, value: str | None) -> None:
    if value is None:
        return
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a str")
    raw = value.encode("utf-8")
    if not raw or len(raw) > 256 or any(byte < 0x20 or byte == 0x7F or byte in b" {}" for byte in raw):
        raise ValueError(f"{field} must be 1 to 256 bytes without controls, spaces, or braces")


def _canonical(value: str | None, field: str) -> str:
    if value is None:
        return str(uuid.uuid4())
    if not isinstance(value, str) or _UUID.fullmatch(value) is None:
        raise ValueError(f"{field} must be a canonical lowercase UUID")
    return value
