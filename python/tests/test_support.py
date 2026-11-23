import asyncio
import os
import random
from pathlib import Path

import pytest

from arq_rs.backoff import compute_backoff
from arq_rs.errors import (
    ConfigError,
    DuplicateTask,
    IndeterminateOperation,
    OperationConflict,
    QueueFull,
    RedisError,
    StorageInconsistency,
    map_error,
)
from arq_rs.exposition import child_memory_samples, exposition_address, make_server, serve
from arq_rs.memory import MemoryClient
from arq_rs.metrics import (
    DURATION_BUCKETS,
    finish_attempt,
    install_slot_identity,
    note_retry,
    q_name,
    reject,
    render,
    reset,
    start_attempt,
    value,
)
from arq_rs.multiproc import (
    pid_alive,
    prepare_multiproc_dir,
    reap_dead_processes,
    resolve_db_path,
    write_slot_pid,
)
from arq_rs.multiproc import ReapingMultiProcessCollector
from arq_rs.tracking import tracked
from arq_rs.tracing import DEFAULT_TARGETS, Tracing


def test_backoff_matches_the_donor_formula():
    left = random.Random(1)
    right = random.Random(1)
    assert compute_backoff(0, min_backoff=100, max_backoff=1000, rng=left) == compute_backoff(
        0, min_backoff=100, max_backoff=1000, rng=right
    )
    capped = compute_backoff(100, min_backoff=10, max_backoff=100, rng=random.Random(2))
    assert 50 <= capped <= 99
    low = compute_backoff(0, min_backoff=100, max_backoff=1000, rng=random.Random(3))
    assert 50 <= low <= 99


def test_metric_names_buckets_and_delayed_gauge(tmp_path, monkeypatch):
    reset()
    assert q_name("default.DQ") == "default"
    assert q_name("default.XQ") == "default"
    assert q_name("default") == "default"
    monkeypatch.delenv("ARQ_SLOT", raising=False)
    install_slot_identity()
    monkeypatch.setenv("SIMPLE_Q_SLOT", "4")
    install_slot_identity()
    monkeypatch.setenv("ARQ_SLOT", "3")
    monkeypatch.setenv("PROMETHEUS_MULTIPROC_DIR", str(tmp_path))
    install_slot_identity()
    assert (tmp_path / "slot_3.pid").is_file()
    note_retry("default.DQ", "process_item")
    assert value("dramatiq_message_retries_total", "default", "process_item") == 1
    assert value("delayed", "default", "process_item") == 0
    start_attempt("m1", "default", "process_item")
    finish_attempt("m1", "default", "process_item", error=True, rejected=True)
    finish_attempt("missing", "default", "process_item")
    reject("default", "process_item")
    text = render()
    assert "900000" in text
    assert "+Inf" in text
    assert "dramatiq_messages_total" in text
    assert DURATION_BUCKETS[-2] == 900000
    with pytest.raises(KeyError):
        value("nope", "default", "process_item")
    reset()
    from arq_rs.metrics import setup

    setup()
    setup()


def test_multiproc_reaps_a_dead_slot(tmp_path, monkeypatch):
    path = prepare_multiproc_dir(tmp_path, clean=False)
    assert resolve_db_path(path) == str(tmp_path)
    (tmp_path / "counter_1.db").write_text("keep", encoding="ascii")
    (tmp_path / "gauge_liveall_9.db").write_text("x", encoding="ascii")
    (tmp_path / "gauge_skip.db").write_text("x", encoding="ascii")
    write_slot_pid(path, 9, pid=2**30)
    assert reap_dead_processes(path) == [9]
    assert not (tmp_path / "gauge_liveall_9.db").exists()
    assert (tmp_path / "counter_1.db").is_file()
    prepare_multiproc_dir(path, clean=True)
    assert not (tmp_path / "counter_1.db").exists()
    assert reap_dead_processes(tmp_path / "missing-dir") == []
    assert pid_alive(0) is False
    assert pid_alive(os.getpid()) is True
    assert pid_alive(2**30) is False

    def deny(_pid, _sig):
        raise PermissionError

    monkeypatch.setattr(os, "kill", deny)
    assert pid_alive(4) is True

    def broken(_pid, _sig):
        raise OSError("no")

    monkeypatch.setattr(os, "kill", broken)
    assert pid_alive(4) is False
    (tmp_path / "slot_4.pid").write_text("nope", encoding="ascii")
    from arq_rs.multiproc import _slot_owner

    assert _slot_owner(str(tmp_path), 4) is None
    assert _slot_owner(str(tmp_path), 8) is None
    (tmp_path / "gauge_liveall_7.db").write_text("x", encoding="ascii")
    write_slot_pid(path, 7, pid=os.getpid())
    monkeypatch.undo()
    assert reap_dead_processes(path) == []
    for key in ("DRAMATIQ_PROM_DB", "dramatiq_prom_db", "PROMETHEUS_MULTIPROC_DIR", "prometheus_multiproc_dir"):
        monkeypatch.delenv(key, raising=False)
    assert resolve_db_path().endswith("arq-prometheus")
    own = tmp_path / f"gauge_liveall_{os.getpid()}.db"
    own.write_text("x", encoding="ascii")
    assert os.getpid() not in reap_dead_processes(path)
    (tmp_path / "gauge_liveall_1073741824.db").write_text("x", encoding="ascii")

    def missing(target):
        raise FileNotFoundError(target)

    monkeypatch.setattr(os, "remove", missing)
    assert reap_dead_processes(path) == [1073741824]
    monkeypatch.undo()
    removed = {"gone": False}

    def vanish(target):
        removed["gone"] = True
        raise FileNotFoundError(target)

    monkeypatch.setattr(os, "remove", vanish)
    prepare_multiproc_dir(path, clean=True)
    assert removed["gone"] is True
    collector = ReapingMultiProcessCollector(object(), path)
    assert collector.collect() == []


def test_exposition_serves_text_and_child_memory(tmp_path, monkeypatch):
    monkeypatch.setenv("PROMETHEUS_MULTIPROC_DIR", str(tmp_path))
    monkeypatch.setenv("DRAMATIQ_PROM_HOST", "127.0.0.1")
    monkeypatch.setenv("DRAMATIQ_PROM_PORT", "9191")
    assert exposition_address() == ("127.0.0.1", 9191)
    (tmp_path / "arq_child_memory.prom").write_text(
        "\n".join(
            [
                "# comment",
                "other 3",
                "arq_child_memory_drains_total nope",
                "arq_child_memory_drains_total -1",
                "arq_child_memory_drains_total inf",
                "arq_child_memory_drains_total nan",
                "arq_child_memory_kills_total 2",
                "short",
            ]
        ),
        encoding="utf-8",
    )
    text = child_memory_samples(tmp_path).decode()
    assert "arq_child_memory_drains_total 0" in text
    assert "arq_child_memory_kills_total 2" in text
    (tmp_path / "arq_child_memory.prom").write_text("arq_child_memory_drains_total inf\n", encoding="utf-8")
    assert b"arq_child_memory_drains_total 0" in child_memory_samples(tmp_path)
    (tmp_path / "arq_child_memory.prom").write_bytes(b"\xff")
    assert b"arq_child_memory_drains_total 0" in child_memory_samples(tmp_path)
    assert b"arq_child_memory_drains_total 0" in child_memory_samples(tmp_path / "absent")
    reset()
    defaults = make_server(port=0)
    defaults.server_close()
    server = make_server("127.0.0.1", 0)
    port = server.server_address[1]
    import threading
    from urllib.request import urlopen

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with urlopen(f"http://127.0.0.1:{port}/metrics") as response:
            body = response.read().decode()
        assert "dramatiq_messages_total" in body
        assert "arq_child_memory_kills_total" in body
    finally:
        server.shutdown()
    called = {}

    def fake_server():
        class Dummy:
            def serve_forever(self):
                called["ran"] = True

        return Dummy()

    monkeypatch.setattr("arq_rs.exposition.make_server", fake_server)
    serve()
    assert called["ran"] is True


def test_tracing_targets_and_error_mapping():
    class Exporter:
        def export(self, spans):
            return None

        def shutdown(self):
            return None

    tracing = Tracing(exporter=Exporter(), resource={"service.name": "arq"})
    assert tracing.targets == DEFAULT_TARGETS
    assert tracing.max_queue_size == 2048
    assert tracing.max_export_batch_size == 512
    empty = Tracing(exporter=Exporter(), resource={}, targets={})
    assert empty.targets == {}
    explicit = Tracing(exporter=Exporter(), resource={}, targets={"arq_worker": "debug"})
    assert explicit.targets == {"arq_worker": "debug"}
    assert isinstance(map_error({"kind": "input", "detail": "bad"}), ValueError)
    assert isinstance(map_error({"kind": "page", "detail": "cursor"}), ValueError)
    assert isinstance(map_error({"kind": "config", "detail": "nope"}), ConfigError)
    full = map_error({"kind": "full", "queue": "default", "max_ready": 1, "ready_zcard": 2})
    assert isinstance(full, QueueFull) and full.ready_zcard == 2
    conflict = map_error({"kind": "conflict", "operation_id": "op", "detail": "diff"})
    assert isinstance(conflict, OperationConflict) and conflict.operation_id == "op"
    broken = map_error({"kind": "inconsistent", "queue": "default", "detail": "type"})
    assert isinstance(broken, StorageInconsistency)
    lost = map_error({"kind": "indeterminate", "operation_id": "op", "message_id": "msg"})
    assert isinstance(lost, IndeterminateOperation) and lost.message_id == "msg"
    assert isinstance(map_error({"kind": "closed", "detail": "client is closed"}), RuntimeError)
    assert isinstance(map_error({"kind": "transport", "detail": "down"}), RedisError)
    assert isinstance(map_error({"kind": "halted"}), RedisError)
    duplicate = DuplicateTask("item:1", "a", "b")
    assert duplicate.owner_task_id == "b"


def test_tracked_names_the_task(monkeypatch):
    async def work():
        return await tracked(asyncio.sleep(0, result=7), "process_item")

    async def named():
        result = await work()
        assert asyncio.current_task().get_name() == "process_item"
        return result

    assert asyncio.run(named()) == 7
    monkeypatch.setattr(asyncio, "current_task", lambda: None)

    async def unnamed():
        return await tracked(asyncio.sleep(0, result=1), "process_item")

    assert asyncio.run(unnamed()) == 1


def test_memory_duplicate_retry_and_release():
    from arq_rs import App, actor

    calls = {"n": 0}

    @actor(queue="default", max_retries=1, time_limit=1000, min_backoff=10, max_backoff=20)
    async def process_item(item_id: str) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("again")

    client = MemoryClient()
    sent = []

    def remember(actor_name, kwargs, headers):
        sent.append(actor_name)
        return {"source": "memory"}

    client.on_send(remember)
    app = App(client=client, actors=[process_item])

    async def scenario():
        message_id = await client.send_with_options(process_item, kwargs={"item_id": "1"}, unique_key="item:1")
        assert message_id
        with pytest.raises(DuplicateTask):
            await client.send_with_options(process_item, kwargs={"item_id": "1"}, unique_key="item:1", _raise=True)
        assert await client.send_with_options(process_item, kwargs={"item_id": "1"}, unique_key="item:1") is None
        await client.drain(app)
        assert calls["n"] == 2
        assert client.depth("default") == 0
        again = await client.send_with_options(process_item, kwargs={"item_id": "2"}, unique_key="item:1")
        assert again
        await client.close()
        with pytest.raises(RuntimeError):
            await client.send(process_item, item_id="3")

    asyncio.run(scenario())
    assert sent[0] == "process_item"

    def explode(actor_name, kwargs, headers):
        raise RuntimeError("hook")

    fresh = MemoryClient()
    fresh.on_send(explode)

    async def aborted():
        with pytest.raises(RuntimeError):
            await fresh.send(process_item, item_id="4")

    asyncio.run(aborted())
    assert fresh.depth("default") == 0
    with pytest.raises(ValueError):
        asyncio.run(fresh.send_with_options(process_item, kwargs={"item_id": "1"}, delay=-1))

    async def escape(message, call_next):
        raise RuntimeError("dispatch")

    leaking = MemoryClient()
    escaped = App(client=leaking, actors=[process_item], around_dispatch=[escape])

    async def released():
        sent_id = await leaking.send_with_options(
            process_item,
            kwargs={"item_id": "5"},
            unique_key="item:5",
            delay=5,
            operation_id="00000000-0000-4000-8000-000000000005",
        )
        assert sent_id
        with pytest.raises(RuntimeError, match="dispatch"):
            await leaking.drain(escaped)
        again = await leaking.send_with_options(process_item, kwargs={"item_id": "5"}, unique_key="item:5")
        assert again

    asyncio.run(released())
