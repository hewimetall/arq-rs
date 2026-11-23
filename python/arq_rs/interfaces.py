"""Structural contracts. ``App`` and ``Client`` do not inherit from these protocols."""

from __future__ import annotations

from collections.abc import Awaitable
from typing import Any, Protocol


class Middleware(Protocol):
    def install(self, client: Any, app: Any) -> None: ...
