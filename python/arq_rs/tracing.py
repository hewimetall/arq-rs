"""Tracing configuration handed to the child.

OpenTelemetry stays off until ``App`` is constructed with a ``Tracing``.
The queue bounds match the donor: 2048 spans, batches of 512.
An omitted target map uses the arq crates. An explicit empty map stays empty.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

DEFAULT_TARGETS = {
    "arq_worker": "info",
    "arq_pybridge": "info",
    "arq_redis": "info",
    "arq_supervisor": "info",
}


class SpanExporter(Protocol):
    def export(self, spans: Sequence[Any]) -> Any: ...

    def shutdown(self) -> None: ...


class Tracing:
    def __init__(
        self,
        *,
        exporter: SpanExporter,
        resource: Any,
        targets: dict[str, str] | None = None,
        max_queue_size: int = 2048,
        max_export_batch_size: int = 512,
        scheduled_delay_ms: int = 5000,
        export_timeout_ms: int = 10000,
    ) -> None:
        self.exporter = exporter
        self.resource = resource
        self.targets = dict(DEFAULT_TARGETS if targets is None else targets)
        self.max_queue_size = max_queue_size
        self.max_export_batch_size = max_export_batch_size
        self.scheduled_delay_ms = scheduled_delay_ms
        self.export_timeout_ms = export_timeout_ms
