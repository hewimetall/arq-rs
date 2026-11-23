import json
import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).with_name("child_main.py")


def run_child(mode: str) -> dict:
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), mode],
        check=False,
        capture_output=True,
        text=True,
    )
    if mode == "grace":
        assert completed.returncode != 0, completed.stdout
        assert "CancelGraceExceeded" in completed.stderr
        return {}
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


def test_child_hook_order_and_visibility():
    payload = run_child("order")
    order = payload["order"]
    assert order[:2] == ["send-a", "send-b"]
    assert order[2:4] == ["start-a", "start-b"]
    assert order.index("enter-a") < order.index("enter-b") < order.index("handler-1")
    assert order.index("handler-1") < order.index("dead-a:retries") < order.index("dead-b:retries")
    assert order.index("dead-b:retries") < order.index("exit-b") < order.index("exit-a")
    assert order.index("visible-a-process_item") < order.index("enter-b")
    assert payload["after"] is None
    assert order[-2:] == ["stop-a", "stop-b"]


def test_child_replaces_dead_after_on_dead_and_keeps_going_after_start_failure():
    replaced = run_child("replace")
    order = replaced["order"]
    assert replaced["outcome"] == "retry"
    assert order.index("dead-a:retries") < order.index("dead-fail") < order.index("replace")
    assert replaced["after"] is None
    started = run_child("start")
    assert "start-later" in started["order"]
    assert started["order"].index("start-b") < started["order"].index("start-later")
    assert "handler-1" in started["order"]


def test_child_unknown_actor_and_timeout_rules():
    unknown = run_child("unknown")
    assert unknown["reason"] == "unknown_actor"
    assert "dead-a:unknown_actor" in unknown["order"]
    assert unknown["order"].index("visible-a-missing") < unknown["order"].index("dead-a:unknown_actor")
    assert unknown["after"] is None
    timed = run_child("timeout")
    assert timed["reason"] == "timeout"
    assert "swallowed-exception" not in timed["order"]
    assert timed["after"] is None
    swallowed = run_child("swallow")
    assert swallowed["outcome"] == "ok"
    run_child("grace")
