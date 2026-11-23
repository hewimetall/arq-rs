import asyncio
from collections.abc import Awaitable
from typing import TypeVar

T = TypeVar("T")


async def tracked(awaitable: Awaitable[T], name: str) -> T:
    """Name the running asyncio task. The name is diagnostic and is not a failure mark."""

    current = asyncio.current_task()
    if current is not None:
        current.set_name(name)
    return await awaitable
