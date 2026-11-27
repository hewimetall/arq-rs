"""In-process counters with Dramatiq names, labels, and histogram buckets.

``arq_storage_inconsistent`` and ``arq_quarantine_total`` are recorded here when
the child observes a halt or a quarantine move. The other ``arq_*`` names stay
unimplemented. ``queue_name`` drops one ``.DQ`` or ``.XQ`` suffix. The delayed
gauge stays at 0.
"""

from __future__ import annotations

import os
import threading
import time

DURATION_BUCKETS = (
    5,
    10,
    25,
    50,
    75,
    100,
    250,
    500,
    750,
    1000,
    2500,
    5000,
    7500,
    10000,
    30000,
    60000,
    600000,
    900000,
    float("inf"),
)

_LABELS = ("queue_name", "actor_name")
_READY = False
_LOCK = threading.Lock()
_STARTED: dict[str, float] = {}
_VALUES: dict[str, dict[tuple[str, str], float]] = {}
_OBSERVATIONS: dict[tuple[str, str], list[float]] = {}

_NAMES = {
    "total": "dramatiq_messages_total",
    "errors": "dramatiq_message_errors_total",
    "retries": "dramatiq_message_retries_total",
    "rejects": "dramatiq_message_rejects_total",
    "inprogress": "dramatiq_messages_inprogress",
    "delayed": "dramatiq_delayed_messages_inprogress",
    "duration": "dramatiq_message_duration_milliseconds",
}

_ARQ_GAUGE: dict[str, float] = {}
_ARQ_QUARANTINE: dict[tuple[str, str], float] = {}


def q_name(queue_name: str) -> str:
    if queue_name.endswith(".DQ") or queue_name.endswith(".XQ"):
        return queue_name[:-3]
    return queue_name


def install_slot_identity() -> None:
    """Name the multiprocess file after ``ARQ_SLOT`` before any sample is recorded."""

    slot = os.environ.get("ARQ_SLOT")
    path = (
        os.environ.get("DRAMATIQ_PROM_DB")
        or os.environ.get("dramatiq_prom_db")
        or os.environ.get("PROMETHEUS_MULTIPROC_DIR")
        or os.environ.get("prometheus_multiproc_dir")
    )
    if not slot or not path:
        return
    os.environ["PROMETHEUS_MULTIPROC_DIR"] = path
    os.environ["prometheus_multiproc_dir"] = path
    os.environ["DRAMATIQ_PROM_DB"] = path
    os.environ["dramatiq_prom_db"] = path
    os.makedirs(path, exist_ok=True)
    from arq_rs.multiproc import write_slot_pid

    write_slot_pid(path, int(slot))


def reset() -> None:
    global _READY
    _READY = False
    _STARTED.clear()
    _VALUES.clear()
    _OBSERVATIONS.clear()
    _ARQ_GAUGE.clear()
    _ARQ_QUARANTINE.clear()


def setup(*, multiprocess: bool | None = None) -> None:
    global _READY
    if _READY:
        return
    if multiprocess is None:
        multiprocess = bool(os.environ.get("PROMETHEUS_MULTIPROC_DIR") or os.environ.get("DRAMATIQ_PROM_DB"))
    if multiprocess:
        install_slot_identity()
    for key in _NAMES:
        _VALUES[key] = {}
    _READY = True


def _labels(queue: str, actor: str) -> tuple[str, str]:
    setup()
    return (q_name(queue), actor)


def _add(key: str, labels: tuple[str, str], amount: float) -> None:
    table = _VALUES[key]
    table[labels] = table.get(labels, 0.0) + amount


def note_storage_inconsistent(queue_name: str) -> None:
    setup()
    with _LOCK:
        _ARQ_GAUGE[q_name(queue_name)] = 1.0


def note_quarantine(queue_name: str, reason: str) -> None:
    setup()
    key = (q_name(queue_name), reason)
    with _LOCK:
        _ARQ_QUARANTINE[key] = _ARQ_QUARANTINE.get(key, 0.0) + 1.0


def arq_value(name: str, queue_name: str, reason: str = "") -> float:
    setup()
    queue = q_name(queue_name)
    if name in {"arq_storage_inconsistent", "inconsistent"}:
        return _ARQ_GAUGE.get(queue, 0.0)
    if name in {"arq_quarantine_total", "quarantine"}:
        return _ARQ_QUARANTINE.get((queue, reason), 0.0)
    raise KeyError(name)


def note_retry(queue: str, actor: str) -> None:
    setup()
    _add("retries", _labels(queue, actor), 1)


def start_attempt(message_id: str, queue: str, actor: str) -> None:
    setup()
    _add("inprogress", _labels(queue, actor), 1)
    with _LOCK:
        _STARTED[message_id] = time.monotonic()


def finish_attempt(
    message_id: str,
    queue: str,
    actor: str,
    *,
    error: bool = False,
    rejected: bool = False,
) -> None:
    setup()
    labels = _labels(queue, actor)
    with _LOCK:
        started = _STARTED.pop(message_id, None)
    elapsed_ms = 0.0 if started is None else (time.monotonic() - started) * 1000
    _OBSERVATIONS.setdefault(labels, []).append(elapsed_ms)
    _add("inprogress", labels, -1)
    _add("total", labels, 1)
    if error:
        _add("errors", labels, 1)
    if rejected:
        _add("rejects", labels, 1)


def reject(queue: str, actor: str) -> None:
    setup()
    _add("rejects", _labels(queue, actor), 1)


def value(name: str, queue: str, actor: str) -> float:
    setup()
    key = _key_for(name)
    return _VALUES[key].get(_labels(queue, actor), 0.0)


def _key_for(name: str) -> str:
    for key, public in _NAMES.items():
        if name == key or name == public:
            return key
    raise KeyError(name)


def render() -> str:
    setup()
    lines: list[str] = []
    for key, public in _NAMES.items():
        kind = "histogram" if key == "duration" else "gauge" if key in {"inprogress", "delayed"} else "counter"
        lines.append(f"# TYPE {public} {kind}")
        if key == "duration":
            seen = set(_OBSERVATIONS) | set(_VALUES["inprogress"])
            for labels in sorted(seen):
                _render_histogram(lines, public, labels)
            continue
        for labels, sample in sorted(_VALUES[key].items()):
            lines.append(f'{public}{{queue_name="{labels[0]}",actor_name="{labels[1]}"}} {sample}')
        if key == "delayed" and not _VALUES[key]:
            lines.append(f"# {public} stays 0")
    lines.append("# TYPE arq_storage_inconsistent gauge")
    for queue_name, sample in sorted(_ARQ_GAUGE.items()):
        lines.append(f'arq_storage_inconsistent{{queue_name="{queue_name}"}} {sample}')
    lines.append("# TYPE arq_quarantine_total counter")
    for (queue_name, reason), sample in sorted(_ARQ_QUARANTINE.items()):
        lines.append(f'arq_quarantine_total{{queue_name="{queue_name}",reason="{reason}"}} {sample}')
    lines.append(f"# buckets {_LABELS[0]} {_LABELS[1]} " + ",".join(_bucket_text(bound) for bound in DURATION_BUCKETS))
    return "\n".join(lines) + "\n"


def _render_histogram(lines: list[str], public: str, labels: tuple[str, str]) -> None:
    samples = _OBSERVATIONS.get(labels, [])
    label = f'queue_name="{labels[0]}",actor_name="{labels[1]}"'
    for bound in DURATION_BUCKETS:
        count = sum(1 for sample in samples if sample <= bound)
        lines.append(f'{public}_bucket{{{label},le="{_bucket_text(bound)}"}} {count}')
    lines.append(f"{public}_count{{{label}}} {len(samples)}")
    lines.append(f"{public}_sum{{{label}}} {sum(samples)}")


def _bucket_text(bound: float) -> str:
    if bound == float("inf"):
        return "+Inf"
    return str(int(bound))
