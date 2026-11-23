"""The message a handler is allowed to see. No Redis and no queue client."""

from __future__ import annotations

import contextvars
from dataclasses import dataclass
from typing import Any

_CURRENT: contextvars.ContextVar[CurrentMessage | None] = contextvars.ContextVar(
    "arq_rs_current_message",
    default=None,
)


@dataclass
class CurrentMessage:
    message_id: str
    task_id: str
    cycle_id: str
    retries: int
    max_retries: int
    deliveries: int
    max_deliveries: int
    queue_name: str
    actor_name: str
    kwargs: dict[str, Any]
    headers: dict[str, str]
    tag: str | None = None
    worker_name: str = ""

    def asdict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "task_id": self.task_id,
            "cycle_id": self.cycle_id,
            "retries": self.retries,
            "max_retries": self.max_retries,
            "deliveries": self.deliveries,
            "max_deliveries": self.max_deliveries,
            "queue_name": self.queue_name,
            "actor_name": self.actor_name,
            "kwargs": self.kwargs,
            "headers": self.headers,
            "tag": self.tag,
            "worker_name": self.worker_name,
        }


def current_message() -> CurrentMessage | None:
    return _CURRENT.get()


def worker_name() -> str:
    """Empty outside a handler. A bound message returns its worker name, which may be empty."""

    message = current_message()
    if message is None:
        return ""
    return message.worker_name


def bind_message(message: CurrentMessage):
    return _CURRENT.set(message)


def reset_message(token) -> None:
    _CURRENT.reset(token)
