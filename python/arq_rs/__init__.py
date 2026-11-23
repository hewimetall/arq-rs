"""Python facade for arq-rs.

Importing this package does not open Redis and does not load the native module.
``Client`` opens the connection.
"""

from arq_rs.dispatch import MESSAGE_VERSION, Actor, App, CancelGraceExceeded, DeadLetter, actor, build_message
from arq_rs.errors import (
    ConfigError,
    DuplicateTask,
    IndeterminateOperation,
    OperationConflict,
    QueueFull,
    RedisError,
    StorageInconsistency,
)
from arq_rs.interfaces import Middleware
from arq_rs.message import current_message, worker_name
from arq_rs.tracing import Tracing

__all__ = [
    "Actor",
    "App",
    "CancelGraceExceeded",
    "Client",
    "CommandReceipt",
    "Config",
    "ConfigError",
    "DeadLetter",
    "DuplicateTask",
    "IndeterminateOperation",
    "MESSAGE_VERSION",
    "Middleware",
    "OperationConflict",
    "QueueAdmin",
    "QueueControl",
    "QueueFull",
    "RedisError",
    "RequeueResult",
    "StorageInconsistency",
    "Tracing",
    "actor",
    "build_message",
    "current_message",
    "worker_name",
]


def __getattr__(name: str):
    if name in {"Client", "Config"}:
        from arq_rs.client import Client, Config

        value = Client if name == "Client" else Config
        globals()[name] = value
        return value
    if name in {"QueueAdmin", "QueueControl", "CommandReceipt", "RequeueResult"}:
        from arq_rs import admin as admin_module

        value = getattr(admin_module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
