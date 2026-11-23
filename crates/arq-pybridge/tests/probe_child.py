"""Objects for the in-process child interpreter test. Executed with run(), not imported."""

import asyncio
import sys
import types

from arq_rs import App, actor
from arq_rs import _native
from arq_rs.memory import MemoryClient

assert _native.injected is True
NATIVE = _native


def build_app():
    @actor(queue="default", max_retries=0, time_limit=40, min_backoff=10, max_backoff=20)
    async def process_item(item_id: str):
        if item_id == "grace":
            try:
                await asyncio.sleep(5)
            except BaseException:
                await asyncio.sleep(5)
            return None
        if item_id == "boom":
            raise RuntimeError("boom")
        return None

    async def escape(message, call_next):
        kwargs = message.get("kwargs") or {}
        if kwargs.get("item_id") == "escape":
            raise RuntimeError("escape")
        return await call_next()

    client = MemoryClient(config=type("Config", (), {"cancel_grace_ms": 40})())
    app = App(client=client, actors=[process_item], around_dispatch=[escape])

    async def dead(letter):
        if letter.reason == "invalid_message":
            raise RuntimeError("dead hook failed")

    app.on_dead(dead)
    return app


class SyncNote:
    def __init__(self):
        self.client = object()

    def dispatch(self, message):
        return None

    def note_dead(self, *args, **kwargs):
        return None


class BoomNote:
    def __init__(self):
        self.client = object()

    def dispatch(self, message):
        return None

    def note_dead(self, *args, **kwargs):
        async def boom():
            raise RuntimeError("note failed")

        return boom()


class MissingNote:
    def __init__(self):
        self.client = object()

    def dispatch(self, message):
        return None


APP = build_app()
SYNC = SyncNote()
BARE = MissingNote()
BOOM = BoomNote()

module = types.ModuleType("probeapp")


class FactoryApp:
    def __init__(self):
        self.client = object()

    def dispatch(self, message):
        return None


def create_app():
    return FactoryApp()


def broken_factory():
    raise RuntimeError("factory")


module.app = FactoryApp()
module.create_app = create_app
module.broken_factory = broken_factory
module.not_app = object()
sys.modules["probeapp"] = module
