from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[1]


def _load_script(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


probe = _load_script("audit_probe_acquisition", REPO_ROOT / "scripts/probe-acquisition.py")
benchmark = _load_script("audit_benchmark_pipeline", REPO_ROOT / "scripts/benchmark-pipeline.py")


@pytest.mark.parametrize(
    ("reaped", "expected_returncode", "expected_stop_reason"),
    [
        (True, -9, "sample deadline exceeded; child terminated"),
        (False, None, "child could not be reaped within the termination grace"),
    ],
)
def test_acquisition_probe_distinguishes_timeout_from_failed_reap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reaped: bool,
    expected_returncode: int | None,
    expected_stop_reason: str,
) -> None:
    class FakeProcess:
        def __init__(self) -> None:
            self.killed = False
            self.returncode = None
            self.wait_calls = 0

        def wait(self, timeout: float | None = None) -> int:
            self.wait_calls += 1
            if self.wait_calls == 1:
                raise subprocess.TimeoutExpired(["synthetic-worker"], timeout)
            if reaped:
                self.returncode = -9
                return self.returncode
            raise subprocess.TimeoutExpired(["synthetic-worker"], timeout)

        def kill(self) -> None:
            self.killed = True

    process = FakeProcess()
    monkeypatch.setattr(probe.subprocess, "Popen", lambda *_args, **_kwargs: process)
    paths = {name: tmp_path / name for name in ("roots", "metrics", "logs", "reports")}
    for path in paths.values():
        path.mkdir()

    started = probe.time.monotonic()
    sample = probe._sample(
        5.0,
        "identity",
        0,
        paths,
        started,
        started + 60.0,
        {
            "attempts": 0,
            "downloaded_bytes": 0,
            "retained_bytes": 0,
            "temporary_bytes": 0,
            "temporary_high_water_bytes": 0,
        },
    )

    assert process.killed is True
    assert sample["status"] == "timed_out"
    assert sample["returncode"] == expected_returncode
    assert sample["stop_reason"] == expected_stop_reason


@pytest.mark.parametrize(
    ("stdout", "stderr", "expected_stdout", "expected_stderr"),
    [
        ("partial stdout", "partial stderr", "partial stdout", "partial stderr"),
        (
            b"partial stdout",
            b"partial stderr \xff",
            "partial stdout",
            "partial stderr \ufffd",
        ),
    ],
)
def test_duckdb_profile_timeout_saves_partial_child_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stdout: str | bytes,
    stderr: str | bytes,
    expected_stdout: str,
    expected_stderr: str,
) -> None:
    class FakeProcess:
        pid = 456
        returncode = -9

        def communicate(self, timeout: float | None = None) -> tuple[str | bytes, str | bytes]:
            raise subprocess.TimeoutExpired(
                ["synthetic-profiler"], timeout, output=stdout, stderr=stderr
            )

    monkeypatch.setattr(benchmark.subprocess, "Popen", lambda *_args, **_kwargs: FakeProcess())
    monkeypatch.setattr(benchmark, "_terminate_worker_group", lambda *_args: True)
    monkeypatch.setattr(benchmark, "_worker_group_alive", lambda _process: False)

    report_dir = tmp_path / "report"
    run_root = report_dir / "root-candidate-0"
    (run_root / "builds" / "capture-1").mkdir(parents=True)
    profile_dir = report_dir / "duckdb-profiles" / "candidate-pair-0"
    with pytest.raises(subprocess.TimeoutExpired) as caught:
        benchmark._run_duckdb_profile_json(
            REPO_ROOT, report_dir, run_root, "capture-1", 1, "candidate", 0
        )

    assert caught.value.output == stdout
    assert caught.value.stderr == stderr
    assert (profile_dir / "stdout.log").read_text(encoding="utf-8") == expected_stdout
    assert (profile_dir / "stderr.log").read_text(encoding="utf-8") == (
        expected_stderr + "\nChild timed out after 180s.\n"
    )


def test_supervisor_keeps_completed_captured_output(monkeypatch: pytest.MonkeyPatch):
    class FakeProcess:
        pid = 789
        returncode = 0

        def communicate(self, timeout: float | None = None) -> tuple[str, str]:
            return "complete stdout", "complete stderr"

    monkeypatch.setattr(benchmark.subprocess, "Popen", lambda *_args, **_kwargs: FakeProcess())
    monkeypatch.setattr(benchmark, "_worker_group_alive", lambda _process: False)

    completed = benchmark._run_supervised(
        ["synthetic-profiler"], timeout=2, capture_output=True, text=True
    )

    assert completed.returncode == 0
    assert completed.stdout == "complete stdout"
    assert completed.stderr == "complete stderr"
