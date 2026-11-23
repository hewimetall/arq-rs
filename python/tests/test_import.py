import subprocess
import sys
import textwrap


def test_import_does_not_open_redis():
    script = textwrap.dedent(
        """
        import sys
        import arq_rs
        assert "arq_rs._native" not in sys.modules
        assert "arq_rs.client" not in sys.modules
        from arq_rs import actor, App, current_message, worker_name
        assert worker_name() == ""
        assert current_message() is None
        import arq_rs._native as native
        assert native.redis_connects() == 0
        """
    )
    completed = subprocess.run([sys.executable, "-c", script], check=False, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr
