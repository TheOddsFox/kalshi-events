"""Regressions for production-run cutoff accounting and child startup races."""

from __future__ import annotations

import contextlib
import importlib.util
import json
import signal
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts" / "production_run.py"


@pytest.fixture(scope="module")
def production():
    spec = importlib.util.spec_from_file_location("production_audit_regressions", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_reconciliation_transport_pin_matches_current_source(production):
    assert production._transport_source_sha256() == production.RECONCILIATION_TRANSPORT_SHA256


@pytest.mark.parametrize(
    ("cutoff", "expected_reason"),
    [
        ("monotonic", "insufficient time remains to start and reap a child"),
        ("wall", "insufficient wall-clock time remains to start and reap a child"),
    ],
)
def test_invoke_proves_no_child_started_at_either_deadline(
    production, tmp_path, monkeypatch, cutoff, expected_reason
):
    monkeypatch.setattr(
        production.subprocess, "Popen", lambda *_a, **_kw: pytest.fail("late child started")
    )
    deadline = 100.0
    if cutoff == "monotonic":
        monkeypatch.setattr(production.time, "monotonic", lambda: 95.0)
        monkeypatch.setattr(production, "_child_wall_deadline_epoch", lambda *_a, **_kw: None)
    else:
        monkeypatch.setattr(production.time, "monotonic", lambda: 10.0)
        monkeypatch.setattr(production.time, "time", lambda: 195.0)
        monkeypatch.setattr(production, "_child_wall_deadline_epoch", lambda *_a, **_kw: 200.0)

    result = production._invoke(
        [sys.executable, "-c", "pass"],
        directory=tmp_path,
        operator=tmp_path / "operator",
        report={"system": {"filesystem_device": tmp_path.stat().st_dev}},
        report_path=tmp_path / "session.json",
        deadline=deadline,
        stdout_path=tmp_path / "stdout",
        stderr_path=tmp_path / "stderr",
    )

    assert result == (None, expected_reason, True)
    assert not (tmp_path / "stdout").exists()
    assert not (tmp_path / "stderr").exists()


def _resume_harness(
    production,
    tmp_path,
    monkeypatch,
    *,
    status,
    cli_result=None,
    reconcile=False,
    bounded=False,
):
    capture_id, root_id = "a" * 32, "b" * 32
    report_path = tmp_path / "runs" / "synthetic-session" / "session.json"
    logs = report_path.parent / "logs"
    logs.mkdir(parents=True)
    now_mono, now_epoch = time.monotonic(), time.time()
    report = {
        "version": 1,
        "session_id": "synthetic-session",
        "mode": "full",
        "started_epoch": now_epoch - 60,
        "started_monotonic": now_mono - 60,
        "time_budget_started_epoch": now_epoch - 60,
        "time_budget_started_monotonic": now_mono - 60,
        "deadline_epoch": now_epoch + 3600,
        "deadline_monotonic": now_mono + 3600,
        "last_observed_epoch": now_epoch - 1,
        "capture_id": capture_id,
        "root_id": root_id,
        "phase": "stopped",
        "phase_seconds": {},
        "resource_samples": [],
        "peak_child_process_group_rss_bytes": 0,
        "accounting_complete": True,
        "requests": 73,
        "downloaded_bytes": 8192,
        "capture_complete": False,
        "publication_state": "not_attempted",
        "production_state": "not_attempted",
        "system": {
            "filesystem_device": tmp_path.stat().st_dev,
            "uv_lock_sha256": "c" * 64,
            "package_versions": {},
            "transport_sha256": "d" * 64,
        },
        "error": None,
    }
    report_path.write_text(json.dumps(report))
    paths = {
        "directory": tmp_path,
        "report": report_path,
        "logs": logs,
        "metrics": report_path.parent / "metrics.json",
        "metrics_progress": report_path.parent / "metrics.progress.json",
        "capture_notification": report_path.parent / "capture-notification.json",
    }
    snapshot = {
        "attempts": 73,
        "downloaded_or_reserved_bytes": 8192,
        "accounting_complete": not bounded,
        "unfinished_reservations": [],
        "storage": {"retained_bytes": 0, "temporary_bytes": 0, "database_bytes": 0},
        "root_id": root_id,
    }
    prior = {
        "attempts": 73,
        "downloaded_bytes": None if bounded else 8192,
        "downloaded_bytes_upper_bound": 8192 if bounded else None,
        "accounting_complete": not bounded,
        "accounting_basis": "reserved-upper-bound" if bounded else "exact",
        "accounting_bound_verified": bounded,
        "unfinished_reservation_ids": ["reservation"] if bounded else [],
        "reservation_inventory_sha256": "e" * 64,
    }

    class Awake:
        pid = 900001

    def invoke(_command, *, stdout_path, **_kwargs):
        if cli_result is not None:
            stdout_path.write_text(json.dumps(cli_result))
        return status

    def atomic(path, value):
        path.write_text(json.dumps(value, sort_keys=True, allow_nan=False))

    monkeypatch.setattr(production, "_validate_runtime_versions", lambda *_a, **_kw: None)
    monkeypatch.setattr(production, "prepare_directory", lambda directory, _mode: directory)
    monkeypatch.setattr(production, "_run_lock", lambda _directory: contextlib.nullcontext())
    monkeypatch.setattr(
        production,
        "_load_resume",
        lambda *_a, **_kw: (report_path, report, paths),
    )
    monkeypatch.setattr(production, "_prepare_run_paths", lambda *_a: paths)
    monkeypatch.setattr(
        production, "_system_info", lambda directory: {"filesystem_device": directory.stat().st_dev}
    )
    monkeypatch.setattr(production, "_revision", lambda: "f" * 40)
    monkeypatch.setattr(production, "_monitor_deadline", lambda *_a, **_kw: time.monotonic() + 3600)
    monkeypatch.setattr(production, "_resource_guard", lambda *_a, **_kw: snapshot)
    monkeypatch.setattr(production, "_external_bytes", lambda _directory: 0)
    monkeypatch.setattr(production, "_kill_awake_guard", lambda _awake: None)
    monkeypatch.setattr(production, "_atomic_report", atomic)
    monkeypatch.setattr(production, "_invoke", invoke)
    monkeypatch.setattr(production.subprocess, "Popen", lambda *_a, **_kw: Awake())
    monkeypatch.setattr(
        production.shutil, "disk_usage", lambda _path: SimpleNamespace(free=1000 * 1024**3)
    )
    from kalshi_events import transport

    monkeypatch.setattr(transport, "source_url", lambda: transport.BASE_URL)
    if reconcile:
        monkeypatch.setattr(production, "_reconciliation_accounting", lambda *_a: prior)
    return report, paths, snapshot, capture_id


@pytest.mark.parametrize(
    "reason",
    [
        "insufficient time remains to start and reap a child",
        "insufficient wall-clock time remains to start and reap a child",
    ],
)
def test_proven_no_start_preserves_exact_accounting_and_time_budget(
    production, tmp_path, monkeypatch, reason
):
    report, _paths, _snapshot, capture_id = _resume_harness(
        production, tmp_path, monkeypatch, status=(None, reason, True)
    )
    original_deadlines = (
        report["time_budget_started_epoch"],
        report["time_budget_started_monotonic"],
        report["deadline_epoch"],
        report["deadline_monotonic"],
    )

    result, code = production.run_production(tmp_path, capture_id=capture_id)

    assert code == 3
    assert result["accounting_complete"] is True
    assert result["requests"] == 73
    assert result["downloaded_bytes"] == 8192
    assert result["network_activity_this_invocation"] == {
        "requests": 0,
        "downloaded_bytes": 0,
        "accounting_complete": True,
    }
    assert result["error"] == reason
    assert (
        result["time_budget_started_epoch"],
        result["time_budget_started_monotonic"],
        result["deadline_epoch"],
        result["deadline_monotonic"],
    ) == original_deadlines
    assert json.loads(_paths["report"].read_text())["requests"] == 73


def test_proven_no_start_preserves_verified_conservative_bound_for_reconciliation(
    production, tmp_path, monkeypatch
):
    reason = "insufficient time remains to start and reap a child"
    report, _paths, _snapshot, capture_id = _resume_harness(
        production,
        tmp_path,
        monkeypatch,
        status=(None, reason, True),
        reconcile=True,
        bounded=True,
    )

    result, code = production.run_production(
        tmp_path, capture_id=capture_id, reconcile_accounting=True
    )

    assert code == 3
    assert result["accounting_complete"] is False
    assert result["requests"] == 73
    assert result["downloaded_bytes"] is None
    assert result["downloaded_bytes_upper_bound"] == 8192
    assert result["accounting_basis"] == "reserved-upper-bound"
    assert result["accounting_bound_verified"] is True
    assert result["consumption_state"] == "reserved-upper-bound"
    assert production._is_recoverable_accounting_stop(result, capture_id, True)


def test_started_child_without_result_keeps_accounting_unknown(production, tmp_path, monkeypatch):
    report, _paths, _snapshot, capture_id = _resume_harness(
        production, tmp_path, monkeypatch, status=(0, None, True)
    )

    result, code = production.run_production(tmp_path, capture_id=capture_id)

    assert code == 1
    assert result["accounting_complete"] is False
    assert result["requests"] is None
    assert result["downloaded_bytes"] is None
    assert result["error"] == "refresh failed with unknown child accounting"
    assert report["accounting_complete"] is False


def test_started_child_zero_delta_keeps_cumulative_accounting_exact(
    production, tmp_path, monkeypatch
):
    result_data = {
        "complete": False,
        "capture_id": "a" * 32,
        "capture_complete": False,
        "resumable": True,
        "publication_state": "unknown",
        "accounting_complete": True,
        "requests": 0,
        "downloaded_bytes": 0,
        "error": "synthetic budget stop",
    }
    report, _paths, _snapshot, capture_id = _resume_harness(
        production, tmp_path, monkeypatch, status=(3, None, True), cli_result=result_data
    )

    result, code = production.run_production(tmp_path, capture_id=capture_id)

    assert code == 3
    assert result["accounting_complete"] is True
    assert result["requests"] == 73
    assert result["downloaded_bytes"] == 8192
    assert result["error"] == "synthetic budget stop"
    assert report["accounting_complete"] is True


def test_signal_during_spawn_keeps_durable_process_group_when_reaping_fails(
    production, tmp_path, monkeypatch
):
    report_path = tmp_path / "session.json"
    report = {
        "resource_samples": [],
        "peak_child_process_group_rss_bytes": 0,
        "system": {"filesystem_device": tmp_path.stat().st_dev},
    }
    child = SimpleNamespace(pid=424242)

    def spawn(*_args, **_kwargs):
        assert production._SPAWNING_CHILD is True
        production._PENDING_STOP_SIGNAL = signal.SIGTERM
        production._STOP_IN_PROGRESS = True
        return child

    monkeypatch.setattr(production, "_PENDING_STOP_SIGNAL", None)
    monkeypatch.setattr(production, "_STOP_IN_PROGRESS", False)
    monkeypatch.setattr(production, "_SPAWNING_CHILD", False)
    monkeypatch.setattr(production.subprocess, "Popen", spawn)
    monkeypatch.setattr(production, "_terminate", lambda _process: False)

    with pytest.raises(production.RunnerError, match="could not be reaped"):
        production._invoke(
            ["synthetic-child"],
            directory=tmp_path,
            operator=tmp_path / "operator",
            report=report,
            report_path=report_path,
            deadline=time.monotonic() + 30,
            stdout_path=tmp_path / "stdout",
            stderr_path=tmp_path / "stderr",
        )

    saved = json.loads(report_path.read_text())
    assert saved["active_process_group"] == child.pid
    assert report["active_process_group"] == child.pid
    assert production._SPAWNING_CHILD is False
