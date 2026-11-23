"""Multiprocess metric directory helpers.

Slot files are named from ``ARQ_SLOT``. Live gauges of a dead owner are removed
without importing ``prometheus_client``. Counter files stay on disk.
"""

from __future__ import annotations

import glob
import os
import tempfile

_ENV_KEYS = (
    "DRAMATIQ_PROM_DB",
    "dramatiq_prom_db",
    "PROMETHEUS_MULTIPROC_DIR",
    "prometheus_multiproc_dir",
)


def resolve_db_path(path: str | os.PathLike[str] | None = None) -> str:
    if path is not None and str(path):
        return os.fspath(path)
    for key in _ENV_KEYS:
        value = os.environ.get(key)
        if value:
            return value
    return os.path.join(tempfile.gettempdir(), "arq-prometheus")


def publish_db_path(path: str) -> None:
    for key in _ENV_KEYS:
        os.environ[key] = path


def prepare_multiproc_dir(path: str | os.PathLike[str] | None = None, *, clean: bool = False) -> str:
    resolved = resolve_db_path(path)
    os.makedirs(resolved, exist_ok=True)
    if clean:
        for name in os.listdir(resolved):
            if name.endswith(".db"):
                file_path = os.path.join(resolved, name)
                try:
                    os.remove(file_path)
                except FileNotFoundError:
                    continue
    publish_db_path(resolved)
    return resolved


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def write_slot_pid(path: str, slot: int, pid: int | None = None) -> None:
    target = os.path.join(path, f"slot_{slot}.pid")
    temporary = f"{target}.tmp"
    with open(temporary, "w", encoding="ascii") as handle:
        handle.write(str(os.getpid() if pid is None else pid))
    os.replace(temporary, target)


def _pids_in_db(path: str) -> set[int]:
    pids: set[int] = set()
    for file_name in glob.glob(os.path.join(path, "gauge_*_*.db")):
        stem = os.path.basename(file_name)[: -len(".db")]
        pid_text = stem.rsplit("_", 1)[-1]
        if pid_text.isdigit():
            pids.add(int(pid_text))
    return pids


def _slot_owner(path: str, slot: int) -> int | None:
    pid_file = os.path.join(path, f"slot_{slot}.pid")
    try:
        text = open(pid_file, encoding="ascii").read().strip()
    except OSError:
        return None
    if not text.isdigit():
        return None
    return int(text)


def mark_process_dead(file_id: int, path: str) -> None:
    prefix = "gauge_live"
    suffix = f"_{file_id}.db"
    for name in os.listdir(path):
        if name.startswith(prefix) and name.endswith(suffix):
            try:
                os.remove(os.path.join(path, name))
            except FileNotFoundError:
                continue


def reap_dead_processes(path: str | os.PathLike[str] | None = None) -> list[int]:
    resolved = resolve_db_path(path)
    if not os.path.isdir(resolved):
        return []
    publish_db_path(resolved)
    reaped: list[int] = []
    for file_id in sorted(_pids_in_db(resolved)):
        owner = _slot_owner(resolved, file_id)
        if owner is None:
            if file_id == os.getpid() or pid_alive(file_id):
                continue
        elif pid_alive(owner):
            continue
        mark_process_dead(file_id, resolved)
        reaped.append(file_id)
        pid_file = os.path.join(resolved, f"slot_{file_id}.pid")
        try:
            os.remove(pid_file)
        except FileNotFoundError:
            pass
    return reaped


class ReapingMultiProcessCollector:
    def __init__(self, registry, path: str | os.PathLike[str] | None = None) -> None:
        self._path = prepare_multiproc_dir(path, clean=False)
        self._registry = registry

    def collect(self):
        reap_dead_processes(self._path)
        return []
