"""Adversarial boundaries for the production memory guard."""

from __future__ import annotations

import importlib.util
import json
import signal
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts" / "production_run.py"


@pytest.fixture(scope="module")
def production():
    spec = importlib.util.spec_from_file_location("production_run_memory_safety_test", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _sample(group: int | None, *, pressure_at: float = 100.0) -> dict[str, object]:
    return {
        "child_process_group_rss_bytes": None if group is None else 0,
        "system_compressed_bytes": 0,
        "system_compressor_occupied_bytes": 0,
        "system_memory_pressure": "normal",
        "system_memory_pressure_observed_monotonic": pressure_at,
        "child_process_group_physical_footprint_bytes": None if group is None else 0,
        "supervisor_physical_footprint_bytes": 0,
    }


@pytest.mark.parametrize("observed_at", [True, -1.0, float("inf"), 101.0])
def test_sample_memory_rejects_non_monotonic_or_future_pressure_timestamps(
    production, monkeypatch, observed_at
):
    payload = _sample(123, pressure_at=observed_at)

    class Worker:
        returncode = 0

        def communicate(self, *, timeout):
            assert timeout == 4.5
            return json.dumps(payload, allow_nan=True), ""

    monkeypatch.setattr(production.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(production.subprocess, "Popen", lambda *_a, **_kw: Worker())

    with pytest.raises(production.RunnerError, match="timestamp"):
        production._sample_memory(123)


def test_sample_memory_caps_worker_timeout_and_reaps_it_after_timeout(production, monkeypatch):
    clock = [100.0]
    timeouts = []
    killed = []

    class Worker:
        pid = 456
        returncode = None

        def communicate(self, *, timeout):
            timeouts.append(timeout)
            if len(timeouts) == 1:
                clock[0] += timeout
                raise subprocess.TimeoutExpired("memory-worker", timeout)
            return "", ""

    def start_worker(*_args, **kwargs):
        assert kwargs["start_new_session"] is True
        return Worker()

    monkeypatch.setattr(production.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(production.subprocess, "Popen", start_worker)
    monkeypatch.setattr(
        production.os,
        "killpg",
        lambda pid, sig: killed.append((pid, sig)),
    )

    with pytest.raises(production.RunnerError, match="timed out"):
        production._sample_memory(123, timeout=8.0)

    assert timeouts == [4.5, 0.5]
    assert sum(timeouts) == 5.0
    assert killed == [(456, signal.SIGKILL)]


def test_warning_gap_at_22_5_seconds_remains_continuous(production, tmp_path, monkeypatch):
    clock = [100.0]
    sample_count = 0

    class Child:
        pid = 123
        returncode = None

        def poll(self):
            return self.returncode

        def wait(self, *, timeout):
            return -15

    child = Child()
    terminated = []

    def sample(group, *, timeout):
        nonlocal sample_count
        sample_count += 1
        if sample_count == 2:
            clock[0] += 5.0
        value = _sample(group, pressure_at=clock[0])
        value["system_memory_pressure"] = "warn"
        return value

    def sleep(delay):
        clock[0] += delay
        if clock[0] == 115.0:
            clock[0] += 2.5

    monkeypatch.setattr(production.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(production.time, "sleep", sleep)
    monkeypatch.setattr(production, "_sample_memory", sample)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)
    monkeypatch.setattr(production, "_terminate", lambda _child: terminated.append(True) or True)
    monkeypatch.setattr(production, "_merge_capture_notification", lambda *_args: None)
    monkeypatch.setattr(production, "_atomic_report", lambda *_args: None)
    monkeypatch.setattr(production, "_utc", lambda: "test")
    monkeypatch.setattr(
        production.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=1000 * 1024**3),
    )
    report = {"resource_samples": [], "peak_child_process_group_rss_bytes": 0}

    code, reason, reaped = production._monitor(
        child, tmp_path, tmp_path / "operator", report, tmp_path / "session.json", 500
    )

    assert code == -15 and reaped
    assert reason == "system memory pressure remained warning for 60 seconds"
    assert sample_count == 5
    assert report["resource_samples"][-1]["system_memory_pressure_observed_monotonic"] == 162.5
    assert terminated == [True]
