"""Exceptions the Redis client raises. Importing them does not open a socket."""

from __future__ import annotations


class DuplicateTask(Exception):
    def __init__(self, unique_key: str, task_id: str, owner_task_id: str) -> None:
        super().__init__(
            f"duplicate task for unique key {unique_key}: {task_id} is held by {owner_task_id}"
        )
        self.unique_key = unique_key
        self.task_id = task_id
        self.owner_task_id = owner_task_id


class IndeterminateOperation(Exception):
    def __init__(self, operation_id: str, message_id: str) -> None:
        super().__init__(f"indeterminate operation {operation_id} for message {message_id}")
        self.operation_id = operation_id
        self.message_id = message_id


class QueueFull(Exception):
    def __init__(self, queue: str, max_ready: int, ready_zcard: int) -> None:
        super().__init__(f"queue {queue} is full: ready zcard {ready_zcard} >= max_ready {max_ready}")
        self.queue = queue
        self.max_ready = max_ready
        self.ready_zcard = ready_zcard


class OperationConflict(Exception):
    def __init__(self, operation_id: str, detail: str) -> None:
        super().__init__(detail)
        self.operation_id = operation_id
        self.detail = detail


class StorageInconsistency(Exception):
    def __init__(self, queue: str, detail: str) -> None:
        super().__init__(f"storage inconsistency in queue {queue}: {detail}")
        self.queue = queue
        self.detail = detail


class ConfigError(Exception):
    pass


class RedisError(Exception):
    pass


def map_error(body: dict) -> Exception:
    kind = str(body.get("kind") or "")
    detail = str(body.get("detail") or kind or "redis error")
    if kind == "input" or kind == "page":
        return ValueError(detail)
    if kind == "config":
        return ConfigError(detail)
    if kind == "full":
        return QueueFull(str(body.get("queue") or ""), int(body.get("max_ready") or 0), int(body.get("ready_zcard") or 0))
    if kind == "conflict":
        return OperationConflict(str(body.get("operation_id") or ""), detail)
    if kind == "inconsistent":
        return StorageInconsistency(str(body.get("queue") or ""), detail)
    if kind == "indeterminate":
        return IndeterminateOperation(str(body.get("operation_id") or ""), str(body.get("message_id") or ""))
    if kind == "closed":
        return RuntimeError(detail)
    return RedisError(detail)
