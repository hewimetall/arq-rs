"""Metrics HTTP server.

Any path returns the in-process registry plus the supervisor child-memory
counters from ``arq_child_memory.prom``. A missing file is zero for both samples.
"""

from __future__ import annotations

import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_CHILD_MEMORY_PROM = "arq_child_memory.prom"
_CHILD_MEMORY_COUNTERS = (
    ("arq_child_memory_drains_total", "Children drained because a memory limit was crossed."),
    ("arq_child_memory_kills_total", "Children killed because a memory limit was crossed."),
)


def exposition_address() -> tuple[str, int]:
    host = os.environ.get("DRAMATIQ_PROM_HOST") or os.environ.get("dramatiq_prom_host") or "0.0.0.0"
    port = os.environ.get("DRAMATIQ_PROM_PORT") or os.environ.get("dramatiq_prom_port") or "9191"
    return host, int(port)


def _counter_value(text: str, name: str) -> int:
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2 or parts[0] != name:
            continue
        try:
            value = float(parts[1])
        except ValueError:
            return 0
        if value < 0 or value != value or value == float("inf"):
            return 0
        return int(value)
    return 0


def child_memory_samples(path: str | os.PathLike[str] | None = None) -> bytes:
    from arq_rs.multiproc import resolve_db_path

    file_path = os.path.join(resolve_db_path(path), _CHILD_MEMORY_PROM)
    try:
        with open(file_path, encoding="utf-8") as handle:
            text = handle.read()
    except (OSError, UnicodeError):
        text = ""
    lines: list[str] = []
    for name, help_text in _CHILD_MEMORY_COUNTERS:
        value = _counter_value(text, name)
        lines.append(f"# HELP {name} {help_text}\n")
        lines.append(f"# TYPE {name} counter\n")
        lines.append(f"{name} {value}\n")
    return "".join(lines).encode("utf-8")


def _handler() -> type[BaseHTTPRequestHandler]:
    from arq_rs.metrics import render

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            body = render().encode("utf-8") + child_memory_samples()
            self.send_response(200)
            self.send_header("content-type", "text/plain; version=0.0.4")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def make_server(host: str | None = None, port: int | None = None) -> ThreadingHTTPServer:
    from arq_rs.metrics import install_slot_identity
    from arq_rs.multiproc import prepare_multiproc_dir

    prepare_multiproc_dir(clean=False)
    install_slot_identity()
    if host is None or port is None:
        default_host, default_port = exposition_address()
        host = default_host if host is None else host
        port = default_port if port is None else port
    return ThreadingHTTPServer((host, port), _handler())


def serve() -> None:
    make_server().serve_forever()
