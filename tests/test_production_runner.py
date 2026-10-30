"""Production session boundaries must fail before starting acquisition."""

from __future__ import annotations

import contextlib
import importlib.util
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts" / "production_run.py"


def _memory_sample(
    group,
    *,
    rss=0,
    pressure="normal",
    stored=0,
    occupied=0,
    pressure_at=None,
    group_footprint=0,
    supervisor_footprint=0,
):
    return {
        "child_process_group_rss_bytes": None if group is None else rss,
        "system_compressed_bytes": stored,
        "system_compressor_occupied_bytes": occupied,
        "system_memory_pressure": pressure,
        "system_memory_pressure_observed_monotonic": (
            time.monotonic() if pressure_at is None else pressure_at
        ),
        "child_process_group_physical_footprint_bytes": (
            None if group is None else group_footprint
        ),
        "supervisor_physical_footprint_bytes": supervisor_footprint,
    }


def _resource_row(index, *, rss=None, group_footprint=None, supervisor_footprint=None):
    return {
        "sample_number": index,
        "child_process_group_rss_bytes": index if rss is None else rss,
        "system_compressed_bytes": index * 2,
        "system_compressor_occupied_bytes": index * 3,
        "child_process_group_physical_footprint_bytes": (
            index if group_footprint is None else group_footprint
        ),
        "supervisor_physical_footprint_bytes": (
            index if supervisor_footprint is None else supervisor_footprint
        ),
    }


def _checkout(path: Path) -> Path:
    path.mkdir(parents=True)
    (path / ".git").mkdir()
    (path / "pyproject.toml").write_text('[project]\nname = "oddsfox-kalshi-events"\n')
    return path


def _resume_report_fixture(production, tmp_path, monkeypatch, *, mode="full", phase="stopped"):
    directory = tmp_path / "production"
    directory.mkdir()
    capture_id = "a" * 32
    root_id = "b" * 32
    session_id = "c" * 32
    now_epoch = time.time()
    now_monotonic = time.monotonic()
    report_path = directory / "runs" / session_id / production.REPORT_NAME
    report_path.parent.mkdir(parents=True)
    report = {
        "version": 1,
        "session_id": session_id,
        "mode": mode,
        "capture_id": capture_id,
        "root_id": root_id,
        "phase": phase,
        "phase_seconds": {},
        "accounting_complete": True,
        "requests": 17,
        "downloaded_bytes": 1234,
        "publication_state": "not_attempted",
        "production_state": "not_attempted",
        "started_epoch": now_epoch - 60,
        "started_monotonic": now_monotonic - 60,
        "time_budget_started_epoch": now_epoch - 60,
        "time_budget_started_monotonic": now_monotonic - 60,
        "deadline_epoch": now_epoch + 3600,
        "deadline_monotonic": now_monotonic + 3600,
        "last_observed_at": "before-resume",
        "last_observed_epoch": now_epoch - 1,
        "boot_stamp": "current-boot",
        "system": {
            "filesystem_device": directory.stat().st_dev,
            "filesystem_volume_uuid": "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE",
        },
        "limits": {"legacy_system_compressed_bytes": 987},
        "resource_samples": [],
        "peak_child_process_group_rss_bytes": 20 * 1024**3,
        "invocation_peak_child_process_group_rss_bytes": 19 * 1024**3,
        "error": "previous stop",
        "recovery": "preserve the prior capture",
    }
    report_path.write_text(json.dumps(report))
    monkeypatch.setattr(production, "_owner_root_id", lambda _directory: root_id)
    monkeypatch.setattr(production, "_boot_stamp", lambda: "current-boot")
    return directory, report_path, report, capture_id, root_id


def _initial_historical_accounting_fixture(production, tmp_path):
    from conftest import FakeClient

    from kalshi_events import capture
    from kalshi_events.common import Store
    from kalshi_events.incremental import root_id

    directory = tmp_path / "historical-production"
    (directory / "runs").mkdir(parents=True)
    operator = directory / "operator"
    store = Store(operator)
    with store.lock():
        logical_root_id = root_id(store, create=True)
    state = capture.capture(store, client_factory=FakeClient)
    baseline_temporary_bytes = store.usage()["temporary_bytes"]
    with store.lock():
        stage = store.path(f"tmp/build-historical-{'f' * 32}")
        stage.mkdir(parents=True)
        (stage / "partial.parquet").write_bytes(b"abandoned stage bytes")
        store.sync("tmp")

    session_id = "c" * 32
    report_path = directory / "runs" / session_id / production.REPORT_NAME
    report_path.parent.mkdir(parents=True)
    report = {
        "version": 1,
        "session_id": session_id,
        "mode": "full",
        "capture_id": state["capture_id"],
        "root_id": logical_root_id,
        "revision": "d" * 40,
        "phase": "preflight",
        "phase_seconds": {},
        "requests": 6,
        "downloaded_bytes": 1077,
        "network_activity_this_invocation": {
            "requests": 0,
            "downloaded_bytes": 0,
            "accounting_complete": True,
        },
        "active_process_group": None,
        "started_epoch": time.time(),
        "started_monotonic": time.monotonic(),
        "time_budget_started_epoch": time.time(),
        "time_budget_started_monotonic": time.monotonic(),
        "deadline_epoch": time.time() + 3600,
        "deadline_monotonic": time.monotonic() + 3600,
        "system": {"filesystem_device": directory.stat().st_dev},
        "resource_samples": [],
        "peak_child_process_group_rss_bytes": 0,
    }
    production._atomic_report(
        directory / production.OWNER_MARKER,
        {"version": 1, "root_id": logical_root_id},
    )
    production._atomic_report(report_path, report)
    paths = production._prepare_run_paths(directory, report_path)
    return (
        directory,
        operator,
        store,
        report_path,
        report,
        paths,
        state["capture_id"],
        stage,
        baseline_temporary_bytes,
    )


@pytest.fixture(scope="module")
def production():
    spec = importlib.util.spec_from_file_location("production_run", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("full", "capture_id", "activate_current"),
    [(True, None, False), (False, "a" * 32, False), (False, None, True)],
)
def test_exactly_one_production_mode_is_accepted(production, full, capture_id, activate_current):
    production.validate_mode(full, capture_id, activate_current)


@pytest.mark.parametrize(
    ("full", "capture_id", "activate_current"),
    [
        (False, None, False),
        (True, "a" * 32, False),
        (True, None, True),
        (False, "a" * 32, True),
        (True, "a" * 32, True),
    ],
)
def test_missing_or_conflicting_modes_are_rejected(production, full, capture_id, activate_current):
    with pytest.raises((ValueError, RuntimeError)):
        production.validate_mode(full, capture_id, activate_current)


@pytest.mark.parametrize("capture_id", ["", "a" * 31, "a" * 33, "g" * 32, "../outside", True])
def test_resume_requires_a_native_capture_id(production, capture_id):
    with pytest.raises((ValueError, RuntimeError)):
        production.validate_mode(False, capture_id, False)


def test_rss_stop_requires_a_continuous_minute_strictly_over_the_limit(production):
    limit = 24 * 1024**3
    assert not production.rss_limit_exceeded(
        [(instant, limit + 1) for instant in (0, 15, 30, 45, 59.999)]
    )
    assert production.rss_limit_exceeded([(instant, limit + 1) for instant in (0, 15, 30, 45, 60)])
    assert not production.rss_limit_exceeded([(instant, limit) for instant in (0, 15, 30, 45, 60)])
    assert not production.rss_limit_exceeded([])


def test_under_limit_rss_sample_resets_the_continuous_stop_window(production):
    limit = 24 * 1024**3
    assert not production.rss_limit_exceeded(
        [(0, limit + 1), (15, limit + 1), (30, limit)]
        + [(instant, limit + 1) for instant in (45, 60, 75, 90)]
    )
    assert production.rss_limit_exceeded(
        [(0, limit + 1), (15, limit + 1), (30, limit)]
        + [(instant, limit + 1) for instant in (45, 60, 75, 90, 105)]
    )


def test_missed_rss_samples_do_not_establish_a_continuous_minute(production):
    limit = 24 * 1024**3
    assert not production.rss_limit_exceeded([(0, limit + 1), (60, limit + 1)])


def test_resource_samples_keep_a_bounded_tail_archive_once_and_accumulate_peaks(
    production, tmp_path
):
    report_path = tmp_path / "runs" / ("a" * 32) / "session.json"
    report_path.parent.mkdir(parents=True)
    report = {
        "resource_samples": [],
        "resource_samples_total": 0,
        "resource_samples_dropped": 0,
        "peak_child_process_group_rss_bytes": 0,
    }

    for number in range(270):
        production._append_resource_sample(report, report_path, _resource_row(number))

    assert len(report["resource_samples"]) == production.RESOURCE_SAMPLE_LIMIT
    assert [row["sample_number"] for row in report["resource_samples"]] == list(range(14, 270))
    assert report["resource_samples_total"] == 270
    assert report["resource_samples_dropped"] == 14
    assert report["peak_child_process_group_rss_bytes"] == 269
    assert report["peak_child_process_group_physical_footprint_bytes"] == 269
    assert report["peak_supervisor_physical_footprint_bytes"] == 269
    assert report["peak_system_compressed_bytes"] == 538
    assert report["peak_system_compressor_occupied_bytes"] == 807
    assert {row["memory_policy_version"] for row in report["resource_samples"]} == {2}

    archive_path = production._resource_sample_archive_path(report_path)
    archived = json.loads(archive_path.read_text())
    assert [row["sample_number"] for row in archived["resource_samples"]] == list(range(256))
    before = archive_path.read_bytes()
    production._append_resource_sample(report, report_path, _resource_row(270))
    assert archive_path.read_bytes() == before
    assert report["resource_samples_total"] == 271
    assert report["resource_samples_dropped"] == 15


def test_legacy_resource_samples_are_archived_before_trimming_without_retagging(
    production, tmp_path
):
    report_path = tmp_path / "runs" / ("b" * 32) / "session.json"
    report_path.parent.mkdir(parents=True)
    legacy_rows = [_resource_row(index) for index in range(260)]
    legacy_rows[0].update(
        child_process_group_rss_bytes=999,
        system_compressed_bytes=998,
        system_compressor_occupied_bytes=997,
        child_process_group_physical_footprint_bytes=996,
        supervisor_physical_footprint_bytes=995,
    )
    archived_rows = [dict(row) for row in legacy_rows]
    report = {"resource_samples": legacy_rows, "limits": {"system_compressed_bytes": 123}}

    samples, total, dropped = production._resource_sample_state(report, report_path)

    assert len(samples) == 256
    assert total == 260 and dropped == 4
    assert "memory_policy_version" not in samples[0]
    assert report["limits"] == {"system_compressed_bytes": 123}
    assert report["peak_child_process_group_rss_bytes"] == 999
    assert report["peak_child_process_group_physical_footprint_bytes"] == 996
    assert report["peak_supervisor_physical_footprint_bytes"] == 995
    assert report["peak_system_compressed_bytes"] == 998
    assert report["peak_system_compressor_occupied_bytes"] == 997
    assert (
        json.loads(production._resource_sample_archive_path(report_path).read_text())[
            "resource_samples"
        ]
        == archived_rows
    )


def test_resource_sample_archive_failure_does_not_truncate_or_append(
    production, tmp_path, monkeypatch
):
    report_path = tmp_path / "runs" / ("c" * 32) / "session.json"
    report_path.parent.mkdir(parents=True)
    rows = [_resource_row(index) for index in range(256)]
    report = {
        "resource_samples": rows.copy(),
        "resource_samples_total": 256,
        "resource_samples_dropped": 0,
        "peak_child_process_group_rss_bytes": 255,
    }
    monkeypatch.setattr(
        production,
        "_atomic_report",
        lambda path, _report: (_ for _ in ()).throw(OSError("archive write failed")),
    )

    with pytest.raises(OSError, match="archive write failed"):
        production._append_resource_sample(report, report_path, _resource_row(256))

    assert report["resource_samples"] == rows
    assert report["resource_samples_total"] == 256
    assert report["resource_samples_dropped"] == 0
    assert "resource_samples_archive" not in report


def test_memory_preflight_samples_keep_bounded_tail_and_archive_once(production, tmp_path):
    report_path = tmp_path / "runs" / ("d" * 32) / "session.json"
    report_path.parent.mkdir(parents=True)
    report = {
        "memory_preflight_samples": [],
        "memory_preflight_samples_total": 0,
        "memory_preflight_samples_dropped": 0,
    }

    for number in range(270):
        production._append_memory_preflight_sample(
            report, report_path, {"sample_number": number, "system_memory_pressure": "normal"}
        )

    samples = report["memory_preflight_samples"]
    assert len(samples) == production.RESOURCE_SAMPLE_LIMIT
    assert [row["sample_number"] for row in samples] == list(range(14, 270))
    assert report["memory_preflight_samples_total"] == 270
    assert report["memory_preflight_samples_dropped"] == 14
    assert {row["memory_policy_version"] for row in samples} == {2}

    archive_path = production._resource_sample_archive_path(report_path)
    archived = json.loads(archive_path.read_text())
    assert [row["sample_number"] for row in archived["memory_preflight_samples"]] == list(
        range(256)
    )
    before = archive_path.read_bytes()
    production._append_memory_preflight_sample(
        report, report_path, {"sample_number": 270, "system_memory_pressure": "normal"}
    )
    assert archive_path.read_bytes() == before
    assert report["memory_preflight_samples_total"] == 271
    assert report["memory_preflight_samples_dropped"] == 15


def test_legacy_memory_preflight_samples_archive_before_trimming_without_retagging(
    production, tmp_path
):
    report_path = tmp_path / "runs" / ("e" * 32) / "session.json"
    report_path.parent.mkdir(parents=True)
    legacy_rows = [
        {"sample_number": index, "system_memory_pressure": "normal"} for index in range(260)
    ]
    archived_rows = [dict(row) for row in legacy_rows]
    report = {
        "memory_preflight_samples": legacy_rows,
        "limits": {"legacy_system_compressed_bytes": 123},
    }

    samples, total, dropped = production._memory_preflight_sample_state(report, report_path)

    assert len(samples) == 256
    assert total == 260 and dropped == 4
    assert "memory_policy_version" not in samples[0]
    assert report["memory_preflight_samples_total"] == 260
    assert report["memory_preflight_samples_dropped"] == 4
    assert report["limits"] == {"legacy_system_compressed_bytes": 123}
    assert (
        json.loads(production._resource_sample_archive_path(report_path).read_text())[
            "memory_preflight_samples"
        ]
        == archived_rows
    )


def test_memory_preflight_archive_failure_does_not_truncate_or_append(
    production, tmp_path, monkeypatch
):
    report_path = tmp_path / "runs" / ("f" * 32) / "session.json"
    report_path.parent.mkdir(parents=True)
    rows = [{"sample_number": index} for index in range(256)]
    report = {
        "memory_preflight_samples": rows.copy(),
        "memory_preflight_samples_total": 256,
        "memory_preflight_samples_dropped": 0,
    }
    monkeypatch.setattr(
        production,
        "_atomic_report",
        lambda path, _report: (_ for _ in ()).throw(OSError("archive write failed")),
    )

    with pytest.raises(OSError, match="archive write failed"):
        production._append_memory_preflight_sample(report, report_path, {"sample_number": 256})

    assert report["memory_preflight_samples"] == rows
    assert report["memory_preflight_samples_total"] == 256
    assert report["memory_preflight_samples_dropped"] == 0
    assert "resource_samples_archive" not in report


def test_current_memory_policy_is_versioned_and_has_no_compressor_stop_threshold(production):
    policy = production._effective_memory_policy()
    assert policy["version"] == 2
    assert policy["process_group_rss_limit_bytes"] == 24 * 1024**3
    assert policy["process_group_rss_continuous_seconds"] == 60
    assert policy["sample_interval_seconds"] == 15
    assert policy["sample_timeout_seconds"] == 5
    assert policy["pressure_sample_max_gap_seconds"] == 22.5
    assert policy["warning_pressure_continuous_seconds"] == 60
    assert "system_compressed_bytes" not in policy


def test_validated_resume_clears_current_stop_and_preserves_legacy_limits(
    production, tmp_path, monkeypatch
):
    directory, _path, original, capture_id, _root_id = _resume_report_fixture(
        production, tmp_path, monkeypatch
    )

    _path, report, _paths = production._load_resume(directory, capture_id)

    assert report["phase"] == "preflight"
    assert report["error"] is None and report["recovery"] is None
    assert report["previous_stop"]["error"] == original["error"]
    assert report["previous_stop"]["recovery"] == original["recovery"]
    assert report["stop_history"][-1] == report["previous_stop"]
    assert report["effective_memory_policy"]["version"] == 2
    assert report["limits"] == original["limits"]
    assert report["requests"] == 17 and report["downloaded_bytes"] == 1234
    assert report["peak_child_process_group_rss_bytes"] == 20 * 1024**3
    assert (
        report["previous_stop"]["resource_peaks"]["invocation_peak_child_process_group_rss_bytes"]
        == 19 * 1024**3
    )
    assert report["invocation_peak_child_process_group_rss_bytes"] == 0


def test_validated_resume_bounds_legacy_memory_preflight_history(production, tmp_path, monkeypatch):
    directory, report_path, original, capture_id, _root_id = _resume_report_fixture(
        production, tmp_path, monkeypatch
    )
    original["memory_preflight_samples"] = [{"sample_number": index} for index in range(260)]
    report_path.write_text(json.dumps(original))

    _path, report, _paths = production._load_resume(directory, capture_id)

    samples = report["memory_preflight_samples"]
    assert len(samples) == 256
    assert [row["sample_number"] for row in samples] == list(range(4, 260))
    assert "memory_policy_version" not in samples[0]
    assert report["memory_preflight_samples_total"] == 260
    assert report["memory_preflight_samples_dropped"] == 4
    archive = json.loads(production._resource_sample_archive_path(report_path).read_text())
    assert [row["sample_number"] for row in archive["memory_preflight_samples"]] == list(range(260))


def test_crash_before_historical_child_keeps_expired_grant_renewable(
    production, tmp_path, monkeypatch
):
    directory, _path, report, capture_id, root_id = _resume_report_fixture(
        production, tmp_path, monkeypatch, mode="historical", phase="stopped"
    )
    report["publication_state"] = "unknown"
    now_epoch = time.time()
    now_monotonic = time.monotonic()
    report["offline_historical_window"] = {
        "version": 2,
        "capture_id": capture_id,
        "root_id": root_id,
        "revision": "d" * 40,
        "started_at": "2026-10-30T00:00:00Z",
        "started_epoch": now_epoch - 3601,
        "started_monotonic": now_monotonic - 3601,
        "deadline_epoch": now_epoch - 1,
        "deadline_monotonic": now_monotonic - 1,
    }
    _path = directory / "runs" / report["session_id"] / production.REPORT_NAME
    _path.write_text(json.dumps(report))
    monkeypatch.setattr(
        production,
        "_volume_uuid",
        lambda _directory: "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE",
    )
    monkeypatch.setattr(
        production,
        "_validate_offline_renewal_root",
        lambda *_args, **_kwargs: (1, 2),
    )
    monkeypatch.setattr(production, "_revision", lambda: "e" * 40)

    _path, resumed, _paths = production._load_resume(
        directory,
        capture_id,
        historical=True,
        offline_duration_seconds=3600,
        renew_offline_window=True,
    )

    original_window = dict(resumed["offline_historical_window"])
    assert resumed["phase"] == "stopped"
    assert resumed["publication_state"] == "unknown"
    assert resumed["effective_memory_policy"]["version"] == 2

    production._configure_offline_historical_window(resumed, capture_id, 3600, renew=True)
    production._atomic_report(_path, resumed)
    renewed_window = resumed["offline_historical_window"]
    assert resumed["phase"] == "stopped"
    assert resumed["offline_historical_window_history"] == [original_window]
    assert resumed["requests"] == 17 and resumed["downloaded_bytes"] == 1234

    monkeypatch.setattr(production.time, "time", lambda: renewed_window["deadline_epoch"] + 1)
    monkeypatch.setattr(
        production.time,
        "monotonic",
        lambda: renewed_window["deadline_monotonic"] + 1,
    )
    _path, retried, _paths = production._load_resume(
        directory,
        capture_id,
        historical=True,
        offline_duration_seconds=3600,
        renew_offline_window=True,
    )

    assert retried["phase"] == "stopped"
    assert retried["publication_state"] == "unknown"
    assert retried["offline_historical_window_history"] == [original_window]
    assert retried["offline_historical_window"] == renewed_window
    assert retried["requests"] == 17 and retried["downloaded_bytes"] == 1234


def test_initial_historical_accounting_uses_closed_cleanup_command(
    production, tmp_path, monkeypatch
):
    paths = {
        "directory": tmp_path,
        "report": tmp_path / "runs" / ("a" * 32) / production.REPORT_NAME,
        "logs": tmp_path / "logs",
    }
    paths["report"].parent.mkdir(parents=True)
    paths["logs"].mkdir()
    report = {
        "mode": "full",
        "root_id": "b" * 32,
        "revision": "c" * 40,
        "active_process_group": None,
    }
    captured = {}

    def invoke(command, *, stdout_path, phase_on_start, **_kwargs):
        captured["command"] = command
        captured["phase_on_start"] = phase_on_start
        stdout_path.write_text(
            json.dumps(
                {
                    "accounting_complete": True,
                    "storage": {
                        "retained_bytes": 1,
                        "temporary_bytes": 2,
                        "database_bytes": 3,
                    },
                }
            )
        )
        return 0, None, True

    monkeypatch.setattr(production, "_invoke", invoke)
    value = production._child_accounting(
        tmp_path / "operator",
        time.monotonic() + 30,
        paths,
        report,
        require_complete=False,
        capture_id="d" * 32,
        cleanup_historical_stages=True,
        expected_root_id="b" * 32,
    )

    assert captured["command"][2] == "--_account-historical-initial"
    assert captured["command"][3:] == [
        str(tmp_path / "operator"),
        str(tmp_path),
        str(paths["report"]),
        "d" * 32,
        "b" * 32,
        "c" * 40,
    ]
    assert captured["phase_on_start"] == "accounting"
    assert value["storage"]["retained_bytes"] == 1


def test_initial_historical_accounting_cleanup_refuses_prior_active_group(
    production, tmp_path, monkeypatch
):
    paths = {"directory": tmp_path, "report": tmp_path / "session.json", "logs": tmp_path}
    report = {
        "mode": "historical",
        "root_id": "b" * 32,
        "revision": "c" * 40,
        "active_process_group": 123,
    }
    monkeypatch.setattr(
        production,
        "_invoke",
        lambda *_args, **_kwargs: pytest.fail("cleanup worker must not start"),
    )

    with pytest.raises(RuntimeError, match="not session-bound"):
        production._child_accounting(
            tmp_path / "operator",
            time.monotonic() + 30,
            paths,
            report,
            capture_id="d" * 32,
            cleanup_historical_stages=True,
            expected_root_id="b" * 32,
        )


def test_offline_refresh_phase_is_durable_only_when_child_starts(production, tmp_path, monkeypatch):
    logs = tmp_path / "logs"
    logs.mkdir()
    report_path = tmp_path / "session.json"
    paths = {
        "directory": tmp_path,
        "report": report_path,
        "logs": logs,
        "capture_notification": tmp_path / "capture-notification.json",
    }
    report = {"phase": "stopped", "phase_seconds": {}, "active_process_group": None}
    atomic_calls = []
    started = {}

    def atomic(*_args):
        atomic_calls.append(True)

    def invoke(_command, *, phase_on_start, **_kwargs):
        assert atomic_calls == []
        assert report["phase"] == "stopped"
        started["phase"] = phase_on_start
        report["phase"] = phase_on_start
        report["active_process_group"] = 123
        return 0, None, True

    monkeypatch.setattr(production, "_atomic_report", atomic)
    monkeypatch.setattr(production, "_invoke", invoke)

    production._run_external_child(
        ["child"],
        directory=tmp_path,
        operator=tmp_path / "operator",
        paths=paths,
        report=report,
        deadline=time.monotonic() + 30,
        label="refresh",
        capture_id="a" * 32,
        preserve_phase_until_start=True,
    )

    assert started["phase"] == "refresh"
    assert report["phase"] == "refresh"
    assert report["active_process_group"] == 123
    assert report["phase_seconds"]["refresh"] >= 0


@pytest.mark.parametrize(
    ("field", "expected"),
    [("active_process_group", 123), ("root_id", "d" * 32), ("session_id", "e" * 32)],
)
def test_initial_historical_cleanup_rejects_wrong_report_binding(
    production, tmp_path, monkeypatch, field, expected
):
    directory = tmp_path / "production"
    operator = directory / "operator"
    operator.mkdir(parents=True)
    report_path = directory / "runs" / ("a" * 32) / production.REPORT_NAME
    report_path.parent.mkdir(parents=True)
    report = {
        "version": 1,
        "mode": "historical",
        "session_id": "a" * 32,
        "capture_id": "b" * 32,
        "root_id": "c" * 32,
        "revision": "d" * 40,
        "active_process_group": -1,
    }
    report[field] = expected
    report_path.write_text(json.dumps(report))
    monkeypatch.setattr(
        production,
        "_validate_offline_renewal_root",
        lambda *_args, **_kwargs: pytest.fail("invalid report must fail before root validation"),
    )

    with pytest.raises(RuntimeError, match="session binding changed"):
        production._validate_initial_historical_accounting_session(
            operator,
            directory,
            report_path,
            "b" * 32,
            "c" * 32,
            "d" * 40,
        )


def test_supervised_initial_historical_accounting_prunes_before_quota_admission(
    production, tmp_path, monkeypatch
):
    (
        directory,
        operator,
        _store,
        _report_path,
        report,
        paths,
        capture_id,
        stage,
        baseline_temporary_bytes,
    ) = _initial_historical_accounting_fixture(production, tmp_path)
    monkeypatch.setattr(
        production,
        "_sample_memory",
        lambda group, **_kwargs: _memory_sample(group),
    )
    monkeypatch.setattr(
        production.shutil,
        "disk_usage",
        lambda _directory: SimpleNamespace(free=1000 * 1024**3),
    )
    deadline = time.monotonic() + 60

    before = production._resource_guard(
        directory,
        operator,
        report=report,
        deadline=deadline,
        paths=paths,
        capture_id=capture_id,
    )
    assert before["storage"]["temporary_bytes"] > baseline_temporary_bytes
    assert stage.is_dir()
    assert report["active_process_group"] is None

    monkeypatch.setitem(production.RUN_LIMITS, "temporary_bytes", baseline_temporary_bytes)
    after = production._resource_guard(
        directory,
        operator,
        report=report,
        deadline=deadline,
        paths=paths,
        capture_id=capture_id,
        require_complete_accounting=False,
        workflow_deadline=True,
        cleanup_historical_stages=True,
    )

    assert not stage.exists()
    assert after["storage"]["temporary_bytes"] == baseline_temporary_bytes
    assert report["storage"]["operator_temporary_bytes"] == baseline_temporary_bytes
    assert report["active_process_group"] is None
    assert report["requests"] == 6 and report["downloaded_bytes"] == 1077
    assert report["network_activity_this_invocation"] == {
        "requests": 0,
        "downloaded_bytes": 0,
        "accounting_complete": True,
    }


def test_interrupted_historical_refresh_preserves_zero_network_activity_and_accounting(
    production, tmp_path, monkeypatch
):
    (
        directory,
        _operator,
        _store,
        report_path,
        report,
        paths,
        capture_id,
        _stage,
        _baseline_temporary_bytes,
    ) = _initial_historical_accounting_fixture(production, tmp_path)
    report.update(
        mode="historical",
        phase="stopped",
        accounting_complete=True,
        publication_state="unknown",
        production_state="not_attempted",
        error="previous stop",
        recovery="retry offline",
    )
    production._atomic_report(report_path, report)
    phase_during_child = []
    original_atomic_report = production._atomic_report
    original_invoke = production._invoke

    class AwakeGuard:
        pid = 987654

    def atomic_report(path, value):
        if path == report_path and value.get("active_process_group") is not None:
            phase_during_child.append(value["phase"])
        original_atomic_report(path, value)

    def invoke(command, *, phase_on_start=None, report: dict, report_path: Path, **kwargs):
        if phase_on_start != "refresh":
            return original_invoke(
                command,
                phase_on_start=phase_on_start,
                report=report,
                report_path=report_path,
                **kwargs,
            )
        report["phase"] = phase_on_start
        report["active_process_group"] = 987653
        atomic_report(report_path, report)
        report["active_process_group"] = None
        raise production.RunnerInterrupted("simulated historical child interruption")

    original_popen = production.subprocess.Popen

    def popen(command, *args, **kwargs):
        if command[0] == "caffeinate":
            return AwakeGuard()
        return original_popen(command, *args, **kwargs)

    monkeypatch.setattr(production, "_validate_runtime_versions", lambda _versions: None)
    monkeypatch.setattr(production, "prepare_directory", lambda _directory, _mode: directory)
    monkeypatch.setattr(
        production, "_load_resume", lambda *_args, **_kwargs: (report_path, report, paths)
    )
    monkeypatch.setattr(production, "_revision", lambda: "d" * 40)
    monkeypatch.setattr(
        production,
        "_system_info",
        lambda _directory: {"filesystem_device": directory.stat().st_dev},
    )
    monkeypatch.setattr(
        production.shutil,
        "disk_usage",
        lambda _directory: SimpleNamespace(
            free=200 * 1024**3,
            total=250 * 1024**3,
            used=50 * 1024**3,
        ),
    )
    monkeypatch.setattr(
        production, "_sample_memory", lambda group, **_kwargs: _memory_sample(group)
    )
    monkeypatch.setattr(production.subprocess, "Popen", popen)
    monkeypatch.setattr(production, "_kill_awake_guard", lambda _process: None)
    monkeypatch.setattr(production, "_atomic_report", atomic_report)
    monkeypatch.setattr(production, "_invoke", invoke)

    result, code = production.run_production(
        directory,
        capture_id=capture_id,
        historical=True,
    )

    saved = json.loads(report_path.read_text())
    assert code == 3
    assert result["phase"] == "stopped"
    assert phase_during_child[0] == "accounting"
    assert "refresh" in phase_during_child
    assert saved["phase"] == "stopped"
    assert saved["error"] == "simulated historical child interruption"
    assert saved["requests"] == 6 and saved["downloaded_bytes"] == 1077
    assert saved["accounting_complete"] is True
    assert saved["network_activity_this_invocation"] == {
        "requests": 0,
        "downloaded_bytes": 0,
        "accounting_complete": True,
    }


def test_supervised_historical_cleanup_fails_closed_on_writer_lock_contention(
    production, tmp_path, monkeypatch
):
    (
        directory,
        operator,
        store,
        _report_path,
        report,
        paths,
        capture_id,
        stage,
        _baseline_temporary_bytes,
    ) = _initial_historical_accounting_fixture(production, tmp_path)
    monkeypatch.setattr(
        production,
        "_sample_memory",
        lambda group, **_kwargs: _memory_sample(group),
    )

    with (
        store.lock(),
        pytest.raises(production.RunnerError, match="durable operator accounting or storage"),
    ):
        production._resource_guard(
            directory,
            operator,
            report=report,
            deadline=time.monotonic() + 60,
            paths=paths,
            capture_id=capture_id,
            require_complete_accounting=False,
            workflow_deadline=True,
            cleanup_historical_stages=True,
        )

    assert stage.is_dir()
    assert report["active_process_group"] is None


@pytest.mark.parametrize(
    ("pressures", "expected_samples", "stale_first_sample"),
    [
        (["warn"] * 5, 5, False),
        (["warn", "warn", "normal", "warn", "warn", "warn", "warn", "warn"], 8, False),
        (["warn"] * 6, 6, True),
    ],
)
def test_memory_pressure_warning_requires_continuity_and_resets(
    production, tmp_path, monkeypatch, pressures, expected_samples, stale_first_sample
):
    clock = [100.0]
    terminated = []

    class Child:
        pid = 123
        returncode = None

        def poll(self):
            return self.returncode

        def wait(self, *, timeout):
            return -15

    child = Child()
    pressure_iter = iter(pressures)
    atomic_calls = []

    def sample(group, *, timeout):
        return _memory_sample(
            group,
            pressure=next(pressure_iter),
            pressure_at=clock[0],
        )

    def atomic(*_args):
        if stale_first_sample and not atomic_calls:
            clock[0] += 23
        atomic_calls.append(True)

    monkeypatch.setattr(production.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        production.time, "sleep", lambda delay: clock.__setitem__(0, clock[0] + delay)
    )
    monkeypatch.setattr(production, "_sample_memory", sample)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)
    monkeypatch.setattr(production, "_terminate", lambda _child: terminated.append(True) or True)
    monkeypatch.setattr(production, "_merge_capture_notification", lambda *_a: None)
    monkeypatch.setattr(production, "_atomic_report", atomic)
    monkeypatch.setattr(production, "_utc", lambda: "test")
    monkeypatch.setattr(
        production.shutil, "disk_usage", lambda _path: SimpleNamespace(free=1000 * 1024**3)
    )
    report = {"resource_samples": [], "peak_child_process_group_rss_bytes": 0}

    code, reason, reaped = production._monitor(
        child, tmp_path, tmp_path / "operator", report, tmp_path / "session.json", 300
    )

    assert code == -15 and reaped
    assert reason == "system memory pressure remained warning for 60 seconds"
    assert len(report["resource_samples"]) == expected_samples
    assert terminated == [True]


def test_memory_pressure_critical_stops_on_first_sample(production, tmp_path, monkeypatch):
    class Child:
        pid = 123
        returncode = None

        def poll(self):
            return self.returncode

        def wait(self, *, timeout):
            return -15

    child = Child()
    terminated = []
    monkeypatch.setattr(
        production,
        "_sample_memory",
        lambda group, **_kw: _memory_sample(group, pressure="critical"),
    )
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)
    monkeypatch.setattr(production, "_terminate", lambda _child: terminated.append(True) or True)
    monkeypatch.setattr(production, "_merge_capture_notification", lambda *_a: None)
    monkeypatch.setattr(production, "_atomic_report", lambda *_a: None)
    monkeypatch.setattr(production, "_utc", lambda: "test")
    monkeypatch.setattr(
        production.shutil, "disk_usage", lambda _path: SimpleNamespace(free=1000 * 1024**3)
    )
    report = {"resource_samples": [], "peak_child_process_group_rss_bytes": 0}

    code, reason, reaped = production._monitor(
        child,
        tmp_path,
        tmp_path / "operator",
        report,
        tmp_path / "session.json",
        time.monotonic() + 30,
    )

    assert code == -15 and reaped
    assert reason == "system memory pressure reached critical"
    assert len(report["resource_samples"]) == 1
    assert terminated == [True]


def test_normal_pressure_does_not_stop_on_logical_or_physical_compressor_bytes(
    production, tmp_path, monkeypatch
):
    class Child:
        pid = 123
        polls = 0

        def poll(self):
            self.polls += 1
            return None if self.polls == 1 else 0

        def wait(self, *, timeout):
            return 0

    monkeypatch.setattr(
        production,
        "_sample_memory",
        lambda group, **_kw: _memory_sample(
            group,
            stored=50 * 1024**3,
            occupied=30 * 1024**3,
            group_footprint=60 * 1024**3,
            supervisor_footprint=70 * 1024**3,
        ),
    )
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)
    monkeypatch.setattr(production, "_merge_capture_notification", lambda *_a: None)
    monkeypatch.setattr(production, "_atomic_report", lambda *_a: None)
    monkeypatch.setattr(production, "_utc", lambda: "test")
    monkeypatch.setattr(
        production.shutil, "disk_usage", lambda _path: SimpleNamespace(free=1000 * 1024**3)
    )
    report = {"resource_samples": [], "peak_child_process_group_rss_bytes": 0}

    code, reason, reaped = production._monitor(
        Child(),
        tmp_path,
        tmp_path / "operator",
        report,
        tmp_path / "session.json",
        time.monotonic() + 30,
    )

    assert code == 0 and reason is None and reaped
    assert report["resource_samples"][0]["system_compressed_bytes"] == 50 * 1024**3
    assert report["resource_samples"][0]["system_compressor_occupied_bytes"] == 30 * 1024**3
    assert (
        report["resource_samples"][0]["child_process_group_physical_footprint_bytes"]
        == 60 * 1024**3
    )
    assert report["resource_samples"][0]["supervisor_physical_footprint_bytes"] == 70 * 1024**3
    assert report["resource_samples"][0]["memory_policy_version"] == 2
    assert report["invocation_peak_child_process_group_rss_bytes"] == 0
    assert report["invocation_peak_child_process_group_physical_footprint_bytes"] == 60 * 1024**3
    assert report["invocation_peak_supervisor_physical_footprint_bytes"] == 70 * 1024**3


def test_resume_subtracts_trustworthy_prior_acquisition_from_aggregate_limits(production):
    limits = production.RUN_LIMITS
    prior = {"attempts": 123, "downloaded_bytes": 456, "accounting_complete": True}
    remaining = production.remaining_allowances(prior, 100, now=160)
    assert remaining["max-requests"] == limits["attempts"] - 123
    assert remaining["max-download-bytes"] == limits["downloaded_bytes"] - 456
    assert remaining["max-duration-seconds"] <= limits["workflow_seconds"] - 60
    assert remaining["max-rate"] == 5
    assert remaining["compression"] == "gzip"
    assert remaining["rate-policy"] == "decrease-on-429"
    assert remaining["max-response-bytes"] == 384 * 1024**2
    assert remaining["max-decoded-response-bytes"] == 768 * 1024**2


@pytest.mark.parametrize(
    "prior",
    [
        {"attempts": None, "downloaded_bytes": 0, "accounting_complete": True},
        {"attempts": 0, "downloaded_bytes": None, "accounting_complete": True},
        {"attempts": 0, "downloaded_bytes": 0, "accounting_complete": False},
        {"attempts": -1, "downloaded_bytes": 0, "accounting_complete": True},
        {"attempts": True, "downloaded_bytes": 0, "accounting_complete": True},
        {"attempts": 0, "downloaded_bytes": float("nan"), "accounting_complete": True},
    ],
)
def test_unknown_or_malformed_prior_accounting_never_becomes_zero(production, prior):
    with pytest.raises((ValueError, RuntimeError)):
        production.remaining_allowances(prior, 100, now=100)


@pytest.mark.parametrize("exhausted", ["attempts", "downloaded_bytes", "time"])
def test_exhausted_budget_stops_before_starting_another_workflow(production, exhausted):
    prior = {"attempts": 0, "downloaded_bytes": 0, "accounting_complete": True}
    now = 100
    if exhausted == "time":
        now += production.RUN_LIMITS["workflow_seconds"]
    else:
        prior[exhausted] = production.RUN_LIMITS[exhausted]
    with pytest.raises((ValueError, RuntimeError)):
        production.remaining_allowances(prior, 100, now=now)


def test_response_reservation_is_clamped_to_remaining_download_budget(production):
    remaining = production.remaining_allowances(
        {
            "attempts": 0,
            "downloaded_bytes": production.RUN_LIMITS["downloaded_bytes"] - 100,
            "accounting_complete": True,
        },
        100,
        now=100,
    )
    assert remaining["max-download-bytes"] == 100
    assert 0 < remaining["max-response-bytes"] <= 100


def test_reserved_upper_bound_accounting_is_explicit_and_charged(production):
    reservation = {
        "capture_id": "a" * 32,
        "request_id": "b" * 64,
        "reserved_bytes": 384 * 1024**2,
        "status": None,
        "accounting_complete": False,
        "acquisition_protocol": 5,
        "version": 1,
    }
    snapshot = {
        "attempts": 10,
        "downloaded_or_reserved_bytes": 416 * 1024**2 + 64 * 1024,
        "accounting_complete": False,
        "unfinished_reservations": [reservation],
    }
    bound = production._reserved_upper_bound(snapshot, "a" * 32)
    assert bound["accounting_complete"] is False
    assert bound["accounting_basis"] == "reserved-upper-bound"
    assert bound["downloaded_bytes_upper_bound"] == 416 * 1024**2 + 64 * 1024
    assert bound["unfinished_reservation_ids"] == ["b" * 64]
    assert bound["reservation_inventory_sha256"]
    allowances = production.remaining_allowances(
        bound, 100, now=100, started_epoch=100, wall_now=100
    )
    assert allowances["max-requests"] == production.RUN_LIMITS["attempts"] - 10
    assert allowances["max-download-bytes"] == production.RUN_LIMITS["downloaded_bytes"] - (
        416 * 1024**2 + 64 * 1024
    )


@pytest.mark.parametrize(
    "change",
    [
        {"capture_id": "c" * 32},
        {"request_id": "bad"},
        {"reserved_bytes": True},
        {"reserved_bytes": 384 * 1024**2 + 1},
        {"status": 200},
        {"acquisition_protocol": 4},
        {"version": True},
    ],
)
def test_reserved_upper_bound_rejects_untrusted_receipt_fields(production, change):
    reservation = {
        "capture_id": "a" * 32,
        "request_id": "b" * 64,
        "reserved_bytes": 1024,
        "status": None,
        "accounting_complete": False,
        "acquisition_protocol": 5,
        "version": 1,
    }
    reservation.update(change)
    snapshot = {
        "attempts": 1,
        "downloaded_or_reserved_bytes": reservation.get("reserved_bytes", 1024),
        "accounting_complete": False,
        "unfinished_reservations": [reservation],
    }
    with pytest.raises(RuntimeError):
        production._reserved_upper_bound(snapshot, "a" * 32)


def test_recovery_keeps_fully_finalized_receipts_exact(production):
    snapshot = {
        "attempts": 7,
        "downloaded_or_reserved_bytes": 12345,
        "accounting_complete": True,
        "unfinished_reservations": [],
    }
    recovered = production._reconciliation_accounting(snapshot, "a" * 32)
    assert recovered["accounting_complete"] is True
    assert recovered["accounting_basis"] == "exact"
    assert recovered["attempts"] == 7
    assert recovered["downloaded_bytes"] == 12345
    assert recovered["downloaded_bytes_upper_bound"] is None
    assert production.remaining_allowances(recovered, 100, now=160)["max-requests"] == (
        production.RUN_LIMITS["attempts"] - 7
    )


@pytest.mark.parametrize(
    "reason",
    [
        "refresh failed with unknown child accounting",
        "resumed child did not report exact invocation accounting",
        "resource accounting failed: Command '['ps', '-axo', 'pgid=,rss=']' timed out after 5.0 seconds",
        "resource accounting failed: process-group RSS sampling timed out",
        "process-group RSS exceeded 24 GiB for 60 seconds",
        "system compressed memory reached the conservative stop threshold",
        "SSD free space fell below 150 GiB",
    ],
)
def test_explicit_accounting_recovery_accepts_only_known_resource_stops(production, reason):
    report = {
        "capture_id": "a" * 32,
        "accounting_complete": False,
        "phase": "stopped",
        "production_state": "not_attempted",
        "error": reason,
        "started_epoch": 1_000,
        "deadline_epoch": 1_000 + production.RUN_LIMITS["session_seconds"],
        "started_monotonic": 200,
        "deadline_monotonic": 200 + production.RUN_LIMITS["session_seconds"],
    }
    saved_window = (
        report["started_epoch"],
        report["deadline_epoch"],
        report["started_monotonic"],
        report["deadline_monotonic"],
    )
    assert production._is_recoverable_accounting_stop(report, "a" * 32, True)
    assert saved_window == (
        report["started_epoch"],
        report["deadline_epoch"],
        report["started_monotonic"],
        report["deadline_monotonic"],
    )


@pytest.mark.parametrize(
    "change",
    [
        {"error": "filesystem mount changed during the run"},
        {"error": "resource accounting failed: malformed vm_stat"},
        {"error": "resource accounting failed: process-group RSS sampling timed out after retry"},
        {
            "error": "resource accounting failed: Command '['ps', '-axo', 'pgid=,rss=']' timed out after 5.1 seconds"
        },
        {"error": "session or workflow deadline reached termination reserve"},
        {"phase": "complete"},
        {"accounting_complete": True},
        {"production_state": "verified"},
        {"capture_id": "b" * 32},
    ],
)
def test_explicit_accounting_recovery_rejects_unknown_or_ineligible_stops(production, change):
    report = {
        "capture_id": "a" * 32,
        "accounting_complete": False,
        "phase": "stopped",
        "production_state": "not_attempted",
        "error": "refresh failed with unknown child accounting",
    }
    report.update(change)
    assert not production._is_recoverable_accounting_stop(report, "a" * 32, True)
    assert not production._is_recoverable_accounting_stop(report, "a" * 32, False)


def test_missing_delta_stop_requires_explicit_reconciliation(production):
    report = {
        "capture_id": "a" * 32,
        "accounting_complete": False,
        "phase": "stopped",
        "production_state": "not_attempted",
        "error": "resumed child did not report exact invocation accounting",
    }
    assert production._is_recoverable_accounting_stop(report, "a" * 32, True)
    assert not production._is_recoverable_accounting_stop(report, "a" * 32, False)


def test_effective_session_deadline_clamps_wall_and_monotonic_stages(production, monkeypatch):
    monkeypatch.setattr(production.time, "monotonic", lambda: 301)
    monkeypatch.setattr(production.time, "time", lambda: 10003)
    report = {
        "started_monotonic": 100,
        "started_epoch": 1000,
        "time_budget_started_monotonic": 300,
        "time_budget_started_epoch": 10000,
        "deadline_monotonic": 350,
        "deadline_epoch": 10090,
    }
    assert production._monitor_deadline(report, workflow=False) == 350
    assert production._child_wall_deadline_epoch(report, 500) == 10090


@pytest.mark.parametrize(
    "report",
    [
        {"started_monotonic": 100, "started_epoch": 100, "time_budget_started_epoch": "bad"},
        {
            "started_monotonic": 100,
            "started_epoch": 100,
            "time_budget_started_epoch": float("inf"),
        },
        {
            "started_monotonic": 100,
            "started_epoch": 100,
            "deadline_epoch": float("nan"),
        },
    ],
)
def test_invalid_effective_time_fields_fail_closed(production, report):
    with pytest.raises(production.RunnerError, match="time budget"):
        production._monitor_deadline(report, workflow=True)


def test_vm_stat_compressed_logical_pages_are_parsed_and_bounded(production, monkeypatch):
    result = SimpleNamespace(
        stdout=(
            "Mach Virtual Memory Statistics: (page size of 16384 bytes)\n"
            "Pages stored in compressor: 1048576.\n"
        )
    )
    monkeypatch.setattr(production.subprocess, "run", lambda *a, **k: result)
    assert production._system_compressed_bytes() == 16 * 1024**3


def _fake_rusage_buffer(production, footprint, start_abstime):
    value = bytearray(96)
    value[72:80] = footprint.to_bytes(8, production.sys.byteorder)
    value[80:88] = start_abstime.to_bytes(8, production.sys.byteorder)
    return bytes(value)


def test_proc_pid_footprint_uses_v0_and_rejects_pid_reuse(production):
    class ProcCall:
        def __init__(self, samples):
            self.samples = iter(samples)

        def __call__(self, pid, flavor, buffer):
            assert pid == 77
            assert flavor == 0
            production.ctypes.memmove(buffer, next(self.samples), 96)
            return 0

    assert (
        production._proc_pid_footprint(
            ProcCall(
                [
                    _fake_rusage_buffer(production, 100, 123),
                    _fake_rusage_buffer(production, 200, 123),
                ]
            ),
            77,
        )
        == 200
    )
    with pytest.raises(production.RunnerError, match="identity changed"):
        production._proc_pid_footprint(
            ProcCall(
                [
                    _fake_rusage_buffer(production, 100, 123),
                    _fake_rusage_buffer(production, 200, 124),
                ]
            ),
            77,
        )


@pytest.mark.parametrize("still_exists", [False, True])
def test_proc_pid_rusage_esrch_is_ignored_only_after_exit_confirmation(
    production, monkeypatch, still_exists
):
    def vanished(*_args):
        production.ctypes.set_errno(production.errno.ESRCH)
        return -1

    monkeypatch.setattr(production, "_pid_exists", lambda _pid: still_exists)
    if still_exists:
        with pytest.raises(OSError):
            production._proc_pid_footprint(vanished, 77)
    else:
        assert production._proc_pid_footprint(vanished, 77) is None


def test_vm_stat_snapshot_keeps_logical_and_physical_compressor_pages_separate(production):
    text = (
        "Mach Virtual Memory Statistics: (page size of 16384 bytes)\n"
        "Pages stored in compressor: 10.\n"
        "Pages occupied by compressor: 3.\n"
    )
    assert production._vm_stat_compressor_metrics(text) == (10 * 16384, 3 * 16384)
    with pytest.raises(production.RunnerError):
        production._vm_stat_compressor_metrics(
            text.replace("Pages occupied by compressor: 3.\n", "")
        )


def test_memory_sample_collects_pressure_rss_and_separate_footprints(production, monkeypatch):
    commands = []

    def run(command, **_kwargs):
        commands.append(command)
        if command == ["vm_stat"]:
            stdout = (
                "Mach Virtual Memory Statistics: (page size of 16384 bytes)\n"
                "Pages stored in compressor: 10.\n"
                "Pages occupied by compressor: 3.\n"
            )
        elif command == ["sysctl", "-n", "kern.memorystatus_vm_pressure_level"]:
            stdout = "1\n"
        else:
            assert command == ["ps", "-g", "123", "-o", "pgid=,pid=,rss="]
            stdout = "123 456 7\n"
        return SimpleNamespace(returncode=0, stdout=stdout)

    class ProcCall:
        def __init__(self):
            self.samples = iter(
                [
                    _fake_rusage_buffer(production, 400, 99),
                    _fake_rusage_buffer(production, 500, 99),
                    _fake_rusage_buffer(production, 100, 33),
                    _fake_rusage_buffer(production, 200, 33),
                ]
            )

        def __call__(self, _pid, flavor, buffer):
            assert flavor == 0
            production.ctypes.memmove(buffer, next(self.samples), 96)
            return 0

    proc_call = ProcCall()
    monkeypatch.setattr(production.subprocess, "run", run)
    monkeypatch.setattr(
        production.ctypes, "CDLL", lambda *_a, **_kw: SimpleNamespace(proc_pid_rusage=proc_call)
    )
    monkeypatch.setattr(production.os, "getppid", lambda: 999)
    monkeypatch.setattr(production.os, "getpgid", lambda _pid: 123)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: True)

    sample = production._memory_sample_worker(123)

    assert commands == [
        ["vm_stat"],
        ["sysctl", "-n", "kern.memorystatus_vm_pressure_level"],
        ["ps", "-g", "123", "-o", "pgid=,pid=,rss="],
    ]
    assert sample["child_process_group_rss_bytes"] == 7 * 1024
    assert sample["system_compressed_bytes"] == 10 * 16384
    assert sample["system_compressor_occupied_bytes"] == 3 * 16384
    assert sample["system_memory_pressure"] == "normal"
    assert sample["child_process_group_physical_footprint_bytes"] == 200
    assert sample["supervisor_physical_footprint_bytes"] == 500
    assert isinstance(sample["system_memory_pressure_observed_monotonic"], float)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.pop("system_compressor_occupied_bytes"),
        lambda value: value.update(system_memory_pressure="unknown"),
        lambda value: value.update(system_memory_pressure_observed_monotonic=float("nan")),
        lambda value: value.update(system_memory_pressure_observed_monotonic=101.0),
        lambda value: value.update(system_compressed_bytes=True),
        lambda value: value.update(child_process_group_physical_footprint_bytes=None),
    ],
)
def test_sample_memory_rejects_partial_or_invalid_metrics(production, monkeypatch, mutate):
    value = _memory_sample(123, pressure_at=100.0)
    mutate(value)
    monkeypatch.setattr(production.time, "monotonic", lambda: 100.0)

    class Worker:
        returncode = 0

        def communicate(self, *, timeout):
            assert 0 < timeout <= 5
            return json.dumps(value, allow_nan=True), ""

    monkeypatch.setattr(production.subprocess, "Popen", lambda *_a, **_kw: Worker())

    with pytest.raises(production.RunnerError):
        production._sample_memory(123)


def test_sample_memory_timeout_kills_and_reaps_inside_its_five_second_budget(
    production, monkeypatch
):
    clock = [10.0]
    timeouts = []
    killed = []

    class Worker:
        pid = 777
        returncode = None

        def communicate(self, *, timeout):
            timeouts.append(timeout)
            if len(timeouts) == 1:
                clock[0] += timeout
                raise production.subprocess.TimeoutExpired("memory sampler", timeout)
            clock[0] += 0.2
            self.returncode = -9
            return "", ""

    worker = Worker()
    monkeypatch.setattr(production.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(production.subprocess, "Popen", lambda *_a, **_kw: worker)
    monkeypatch.setattr(production.os, "killpg", lambda pid, sig: killed.append((pid, sig)))

    with pytest.raises(production.RunnerError, match="timed out"):
        production._sample_memory(None)

    assert timeouts == [4.5, 0.5]
    assert clock[0] <= 15
    assert killed == [(777, production.signal.SIGKILL)]


def test_sample_memory_propagates_operator_interrupt_after_reaping_worker(production, monkeypatch):
    killed = []

    class Worker:
        pid = 778
        returncode = None

        def __init__(self):
            self.calls = 0

        def communicate(self, *, timeout):
            self.calls += 1
            if self.calls == 1:
                raise production.RunnerInterrupted("operator stop")
            self.returncode = -9
            return "", ""

    worker = Worker()
    monkeypatch.setattr(production.subprocess, "Popen", lambda *_a, **_kw: worker)
    monkeypatch.setattr(production.os, "killpg", lambda pid, sig: killed.append((pid, sig)))

    with pytest.raises(production.RunnerInterrupted, match="operator stop"):
        production._sample_memory(None)

    assert worker.returncode == -9
    assert killed == [(778, production.signal.SIGKILL)]


def test_process_group_rss_retries_one_timeout_within_the_same_budget(production, monkeypatch):
    clock = [100.0]
    timeouts = []

    def run(command, *, timeout, **_kwargs):
        assert command == ["ps", "-g", "123", "-o", "pgid=,rss="]
        timeouts.append(timeout)
        if len(timeouts) == 1:
            clock[0] += timeout
            raise subprocess.TimeoutExpired(command, timeout)
        return SimpleNamespace(stdout="123 2\n123 3\n")

    monkeypatch.setattr(production.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(production.subprocess, "run", run)

    assert production._process_group_rss(123) == 5 * 1024
    assert timeouts == [2.5, 2.5]


def test_process_group_rss_retry_gets_only_the_fresh_budget_remainder(production, monkeypatch):
    clock = [100.0]
    timeouts = []

    def run(command, *, timeout, **_kwargs):
        timeouts.append(timeout)
        if len(timeouts) == 1:
            clock[0] += 1.25
            raise subprocess.TimeoutExpired(command, timeout)
        return SimpleNamespace(stdout="123 2\n")

    monkeypatch.setattr(production.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(production.subprocess, "run", run)

    assert production._process_group_rss(123, timeout=4.0) == 2 * 1024
    assert timeouts == [2.0, 2.75]


def test_process_group_rss_repeated_timeout_fails_with_a_stable_reason(production, monkeypatch):
    clock = [100.0]
    timeouts = []

    def run(command, *, timeout, **_kwargs):
        timeouts.append(timeout)
        clock[0] += timeout
        raise subprocess.TimeoutExpired(command, timeout)

    monkeypatch.setattr(production.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(production.subprocess, "run", run)

    with pytest.raises(production.RunnerError, match="process-group RSS sampling timed out"):
        production._process_group_rss(123)
    assert timeouts == [2.5, 2.5]


def test_process_group_rss_does_not_retry_after_the_total_budget_is_spent(production, monkeypatch):
    clock = [100.0]
    timeouts = []

    def run(command, *, timeout, **_kwargs):
        timeouts.append(timeout)
        clock[0] += 0.5
        raise subprocess.TimeoutExpired(command, timeout)

    monkeypatch.setattr(production.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(production.subprocess, "run", run)

    with pytest.raises(production.RunnerError, match="process-group RSS sampling timed out"):
        production._process_group_rss(123, timeout=0.25)
    assert timeouts == [0.125]


def test_monitor_stops_and_reaps_after_memory_sampling_timeout(production, tmp_path, monkeypatch):
    clock = [100.0]
    calls = []
    terminated = []

    class Child:
        pid = 123
        returncode = None

        def poll(self):
            return self.returncode

        def wait(self, *, timeout):
            return self.returncode

    child = Child()

    def sample(_group, *, timeout):
        calls.append(timeout)
        clock[0] += timeout
        raise production.RunnerError("memory sample timed out")

    def terminate(actual):
        assert actual is child
        child.returncode = -15
        terminated.append(True)
        return True

    monkeypatch.setattr(production.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(production, "_sample_memory", sample)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)
    monkeypatch.setattr(production, "_terminate", terminate)
    monkeypatch.setattr(production, "_merge_capture_notification", lambda *_args: None)
    monkeypatch.setattr(production, "_atomic_report", lambda *_args: None)
    monkeypatch.setattr(
        production.shutil, "disk_usage", lambda _path: SimpleNamespace(free=1000 * 1024**3)
    )
    report = {"resource_samples": [], "peak_child_process_group_rss_bytes": 0}

    code, reason, reaped = production._monitor(
        child,
        tmp_path,
        tmp_path / "operator",
        report,
        tmp_path / "session.json",
        200,
    )

    assert code == -15
    assert reason == "resource accounting failed: memory sample timed out"
    assert reaped is True
    assert calls == [5.0]
    assert terminated == [True]
    assert report["resource_samples"] == []


@pytest.mark.parametrize("stdout", ["bad output\n", "123 10\n456 20\n", "123 10\nbroken\n"])
def test_process_group_rss_rejects_malformed_or_unscoped_rows_without_retry(
    production, monkeypatch, stdout
):
    calls = []

    def run(*_args, **_kwargs):
        calls.append(True)
        return SimpleNamespace(stdout=stdout)

    monkeypatch.setattr(production.subprocess, "run", run)
    with pytest.raises(production.RunnerError):
        production._process_group_rss(123)
    assert calls == [True]


def test_process_group_rss_does_not_retry_os_errors(production, monkeypatch):
    calls = []

    def run(*_args, **_kwargs):
        calls.append(True)
        raise OSError("ps unavailable")

    monkeypatch.setattr(production.subprocess, "run", run)
    with pytest.raises(OSError, match="ps unavailable"):
        production._process_group_rss(123)
    assert calls == [True]


def test_process_group_rss_accepts_no_process_only_when_group_is_absent(production, monkeypatch):
    monkeypatch.setattr(
        production.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(stdout=""),
    )
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)
    assert production._process_group_rss(123) == 0

    monkeypatch.setattr(production, "_process_group_exists", lambda _group: True)
    with pytest.raises(production.RunnerError, match="no processes for a live process group"):
        production._process_group_rss(123)


def test_process_group_rss_accepts_ps_exit_one_only_when_group_is_absent(production, monkeypatch):
    def run(command, **_kwargs):
        raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(production.subprocess, "run", run)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)
    assert production._process_group_rss(123) == 0

    monkeypatch.setattr(production, "_process_group_exists", lambda _group: True)
    with pytest.raises(subprocess.CalledProcessError):
        production._process_group_rss(123)


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS ps -g selects process-group leaders")
def test_process_group_rss_includes_real_descendants_in_the_group(production, monkeypatch):
    real_run = subprocess.run
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']); print('ready', flush=True); time.sleep(60)",
        ],
        stdout=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        assert child.stdout is not None
        assert child.stdout.readline().strip() == "ready"
        observed_rows = []

        def capture_rows(command, **kwargs):
            result = real_run(command, **kwargs)
            if command == ["ps", "-g", str(child.pid), "-o", "pgid=,rss="]:
                observed_rows.extend(line.split() for line in result.stdout.splitlines())
            return result

        monkeypatch.setattr(production.subprocess, "run", capture_rows)
        total = production._process_group_rss(child.pid)
        rows = observed_rows
        assert len(rows) >= 2
        assert all(len(row) == 2 and int(row[0]) == child.pid for row in rows)
        assert total == sum(int(row[1]) * 1024 for row in rows)
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(child.pid, signal.SIGKILL)
        child.wait(timeout=5)
        if child.stdout is not None:
            child.stdout.close()


@pytest.mark.parametrize("clock_source", ["monotonic", "wall"])
def test_monitor_rechecks_both_deadlines_after_memory_sample(
    production, tmp_path, monkeypatch, clock_source
):
    clock = {"monotonic": 100.0, "wall": 200.0}
    terminated = []

    class Process:
        pid = 123

        def poll(self):
            return None

        def wait(self, *, timeout):
            return -15

    def advance_sample(group, *, timeout):
        assert group == 123
        assert timeout == 1.0
        clock[clock_source] += 1
        return _memory_sample(group)

    monkeypatch.setattr(production.time, "monotonic", lambda: clock["monotonic"])
    monkeypatch.setattr(production.time, "time", lambda: clock["wall"])
    monkeypatch.setattr(production, "_sample_memory", advance_sample)
    monkeypatch.setattr(production, "_terminate", lambda _process: terminated.append(True) or True)
    monkeypatch.setattr(production, "_process_group_exists", lambda _pid: False)
    monkeypatch.setattr(production, "_merge_capture_notification", lambda *_a: None)
    monkeypatch.setattr(production, "_atomic_report", lambda *_a: None)
    monkeypatch.setattr(production, "_utc", lambda: "test")
    monkeypatch.setattr(
        production.shutil, "disk_usage", lambda _path: SimpleNamespace(free=1000 * 1024**3)
    )

    code, reason, reaped = production._monitor(
        Process(),
        tmp_path,
        tmp_path / "operator",
        {"resource_samples": [], "peak_child_process_group_rss_bytes": 0},
        tmp_path / "session.json",
        106,
        wall_deadline_epoch=206,
    )

    assert code == -15
    assert reason == "session or workflow deadline reached termination reserve"
    assert reaped is True
    assert terminated == [True]


@pytest.mark.parametrize("clock_source", ["monotonic", "wall"])
def test_monitor_clamps_memory_sample_to_fresh_deadline_remainder(
    production, tmp_path, monkeypatch, clock_source
):
    clock = {"monotonic": 100.0, "wall": 200.0}
    probe_timeouts = []

    class Process:
        pid = 123

        def poll(self):
            return None

        def wait(self, *, timeout):
            return -15

    def sample(group, *, timeout):
        probe_timeouts.append(timeout)
        return _memory_sample(group, pressure="critical")

    terminated = []
    monkeypatch.setattr(production.time, "monotonic", lambda: clock["monotonic"])
    monkeypatch.setattr(production.time, "time", lambda: clock["wall"])
    monkeypatch.setattr(production, "_sample_memory", sample)
    monkeypatch.setattr(production, "_terminate", lambda _process: terminated.append(True) or True)
    monkeypatch.setattr(production, "_process_group_exists", lambda _pid: False)
    monkeypatch.setattr(production, "_merge_capture_notification", lambda *_a: None)
    monkeypatch.setattr(production, "_atomic_report", lambda *_a: None)
    monkeypatch.setattr(production, "_utc", lambda: "test")
    monkeypatch.setattr(
        production.shutil, "disk_usage", lambda _path: SimpleNamespace(free=1000 * 1024**3)
    )

    code, reason, reaped = production._monitor(
        Process(),
        tmp_path,
        tmp_path / "operator",
        {"resource_samples": [], "peak_child_process_group_rss_bytes": 0},
        tmp_path / "session.json",
        109,
        wall_deadline_epoch=209,
    )

    assert code == -15
    assert reason == "system memory pressure reached critical"
    assert reaped is True
    assert probe_timeouts == [4.0]
    assert terminated == [True]


@pytest.mark.parametrize(
    ("clock_source", "advance", "wall_deadline", "expect_stop"),
    [
        ("monotonic", 5, 210, True),
        ("wall", 5, 210, True),
        ("wall", 1, 210, False),
    ],
)
def test_monitor_rechecks_deadlines_after_successful_memory_sample_before_persisting(
    production, tmp_path, monkeypatch, clock_source, advance, wall_deadline, expect_stop
):
    clock = {"monotonic": 100.0, "wall": 200.0}
    terminated = []
    persisted = []

    class Process:
        pid = 123

        def __init__(self):
            self.polls = 0

        def poll(self):
            self.polls += 1
            if not expect_stop and self.polls > 1:
                return 0
            return None

        def wait(self, *, timeout):
            return -15 if expect_stop else 0

    def sample(group, *, timeout):
        assert timeout == 5.0
        clock[clock_source] += advance
        return _memory_sample(group)

    monkeypatch.setattr(production.time, "monotonic", lambda: clock["monotonic"])
    monkeypatch.setattr(production.time, "time", lambda: clock["wall"])
    monkeypatch.setattr(
        production.time,
        "sleep",
        lambda delay: clock.__setitem__("monotonic", clock["monotonic"] + delay),
    )
    monkeypatch.setattr(production, "_sample_memory", sample)
    monkeypatch.setattr(production, "_terminate", lambda _process: terminated.append(True) or True)
    monkeypatch.setattr(production, "_process_group_exists", lambda _pid: False)
    monkeypatch.setattr(production, "_merge_capture_notification", lambda *_a: None)
    monkeypatch.setattr(production, "_atomic_report", lambda *_a: persisted.append(True))
    monkeypatch.setattr(production, "_utc", lambda: "test")
    monkeypatch.setattr(
        production.shutil, "disk_usage", lambda _path: SimpleNamespace(free=1000 * 1024**3)
    )
    report = {"resource_samples": [], "peak_child_process_group_rss_bytes": 0}

    code, reason, reaped = production._monitor(
        Process(),
        tmp_path,
        tmp_path / "operator",
        report,
        tmp_path / "session.json",
        110,
        wall_deadline_epoch=wall_deadline,
    )

    if expect_stop:
        assert code == -15
        assert reason == "session or workflow deadline reached termination reserve"
        assert reaped is True
        assert terminated == [True]
        assert report["resource_samples"] == []
        assert persisted == []
    else:
        assert code == 0
        assert reason is None
        assert reaped is True
        assert terminated == []
        assert len(report["resource_samples"]) == 1
        assert persisted == [True]


@pytest.mark.parametrize("clock_source", ["monotonic", "wall"])
def test_monitor_preserves_deadline_reason_when_memory_sample_times_out(
    production, tmp_path, monkeypatch, clock_source
):
    clock = {"monotonic": 100.0, "wall": 200.0}
    probe_timeouts = []

    class Process:
        pid = 123

        def poll(self):
            return None

        def wait(self, *, timeout):
            return -15

    def timed_out(_group, *, timeout):
        probe_timeouts.append(timeout)
        clock[clock_source] += 4
        raise production.RunnerError("memory sampler timed out")

    terminated = []
    monkeypatch.setattr(production.time, "monotonic", lambda: clock["monotonic"])
    monkeypatch.setattr(production.time, "time", lambda: clock["wall"])
    monkeypatch.setattr(production, "_sample_memory", timed_out)
    monkeypatch.setattr(production, "_terminate", lambda _process: terminated.append(True) or True)
    monkeypatch.setattr(production, "_process_group_exists", lambda _pid: False)
    monkeypatch.setattr(production, "_merge_capture_notification", lambda *_a: None)
    monkeypatch.setattr(production, "_atomic_report", lambda *_a: None)
    monkeypatch.setattr(production, "_utc", lambda: "test")
    monkeypatch.setattr(
        production.shutil, "disk_usage", lambda _path: SimpleNamespace(free=1000 * 1024**3)
    )

    code, reason, reaped = production._monitor(
        Process(),
        tmp_path,
        tmp_path / "operator",
        {"resource_samples": [], "peak_child_process_group_rss_bytes": 0},
        tmp_path / "session.json",
        109,
        wall_deadline_epoch=209,
    )

    assert code == -15
    assert reason == "session or workflow deadline reached termination reserve"
    assert reaped is True
    assert probe_timeouts == [4.0]
    assert terminated == [True]


def _live_process(pid):
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "stat="],
        capture_output=True,
        text=True,
        check=False,
        timeout=2,
    )
    return result.returncode == 0 and not result.stdout.strip().startswith("Z")


def test_termination_reaps_leader_and_kills_a_term_ignoring_descendant(production, tmp_path):
    ready = tmp_path / "ready.json"
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import json,os,signal,sys,time; "
            "pid=os.fork(); "
            "signal.signal(signal.SIGTERM,signal.SIG_IGN if pid==0 else signal.SIG_DFL); "
            "open(sys.argv[1],'w').write(json.dumps({'pid':os.getpid()})) if pid==0 else None; "
            "time.sleep(60)",
            str(ready),
        ],
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    descendant = None
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if ready.exists():
                try:
                    descendant = json.loads(ready.read_text())["pid"]
                except (ValueError, KeyError):
                    pass
                else:
                    break
            time.sleep(0.01)
        assert descendant is not None, "descendant failed to initialize"
        assert _live_process(descendant)
        assert production._terminate(child) is True
        deadline = time.monotonic() + 2
        while _live_process(descendant) and time.monotonic() < deadline:
            time.sleep(0.01)
        assert not _live_process(descendant), "leader exit left a networking-capable child alive"
        assert child.poll() is not None
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(child.pid, signal.SIGKILL)
        child.wait(timeout=3)


def test_child_startup_failure_does_not_proceed_to_monitoring(production, tmp_path, monkeypatch):
    def fail_start(_command, **kwargs):
        assert kwargs["shell"] is False
        assert kwargs["start_new_session"] is True
        raise OSError("injected child startup failure")

    monkeypatch.setattr(production, "_sample_memory", lambda group, **_kw: _memory_sample(group))
    monkeypatch.setattr(production.subprocess, "Popen", fail_start)
    monkeypatch.setattr(
        production, "_monitor", lambda *_a, **_kw: pytest.fail("monitor ran without a child")
    )
    with pytest.raises(OSError, match="child startup failure"):
        production._invoke(
            [sys.executable, "-c", "pass"],
            directory=tmp_path,
            operator=tmp_path / "operator",
            report={"system": {"filesystem_device": tmp_path.stat().st_dev}},
            report_path=tmp_path / "session.json",
            deadline=time.monotonic() + 10,
            stdout_path=tmp_path / "stdout",
            stderr_path=tmp_path / "stderr",
        )


@pytest.mark.parametrize("fail_at", [1, 2])
def test_report_failure_after_child_startup_still_terminates_the_child(
    production, tmp_path, monkeypatch, fail_at
):
    started = []
    original = subprocess.Popen
    writes = []

    def start(*args, **kwargs):
        child = original(*args, **kwargs)
        started.append(child)
        return child

    def fail_report(*_args, **_kwargs):
        writes.append(True)
        if len(writes) == fail_at:
            raise OSError("injected report synchronization failure")

    def sample(group, *, timeout):
        assert 0 < timeout <= 5
        return _memory_sample(group)

    monkeypatch.setattr(production.subprocess, "Popen", start)
    monkeypatch.setattr(production, "_atomic_report", fail_report)
    monkeypatch.setattr(production, "_sample_memory", sample)
    monkeypatch.setattr(
        production.shutil, "disk_usage", lambda _path: SimpleNamespace(free=1000 * 1024**3)
    )
    try:
        with pytest.raises(OSError, match="report synchronization failure"):
            production._invoke(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                directory=tmp_path,
                operator=tmp_path / "operator",
                report={
                    "resource_samples": [],
                    "peak_child_process_group_rss_bytes": 0,
                    "system": {"filesystem_device": tmp_path.stat().st_dev},
                },
                report_path=tmp_path / "session.json",
                deadline=time.monotonic() + 10,
                stdout_path=tmp_path / "stdout",
                stderr_path=tmp_path / "stderr",
            )
        assert len(started) == 1
        assert started[0].poll() is not None, "report failure leaked the running child"
    finally:
        for child in started:
            if child.poll() is None:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(child.pid, signal.SIGKILL)
                child.wait(timeout=3)


def test_failed_reaping_does_not_receive_a_second_termination_allowance(
    production, tmp_path, monkeypatch
):
    calls = []

    class UnreapedChild:
        pid = 424242

        def poll(self):
            return None

        def wait(self, timeout):
            raise subprocess.TimeoutExpired("unreaped child", timeout)

    monkeypatch.setattr(production.time, "monotonic", lambda: 100)
    monkeypatch.setattr(production, "_terminate", lambda _child: calls.append(True) or False)
    code, reason, reaped = production._monitor(
        UnreapedChild(), tmp_path, tmp_path / "operator", {}, tmp_path / "session.json", 100
    )
    assert code is None
    assert reason
    assert reaped is False
    assert len(calls) == 1, "one stop received more than its five-second termination allowance"


def test_fresh_session_rejects_nonempty_unmanaged_directory_without_modification(
    production, tmp_path
):
    directory = tmp_path / "unmanaged"
    directory.mkdir()
    sentinel = directory / "unrelated.txt"
    sentinel.write_text("preserve")
    with pytest.raises((ValueError, RuntimeError)):
        production.prepare_directory(directory, "full")
    assert list(directory.iterdir()) == [sentinel]
    assert sentinel.read_text() == "preserve"


@pytest.mark.parametrize("mode", ["full", "resume", "activate-current"])
def test_symlinked_production_directory_is_rejected(production, tmp_path, mode):
    target = tmp_path / "real"
    target.mkdir()
    link = tmp_path / "alias"
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises((ValueError, RuntimeError)):
        production.prepare_directory(link, mode)
    assert not list(target.iterdir())


def test_production_directory_cannot_be_inside_checkout(production):
    with pytest.raises((ValueError, RuntimeError)):
        production.prepare_directory(SCRIPT.parent.parent / "production-not-allowed", "full")


def test_production_directory_inside_checkout_data_is_allowed(production, tmp_path, monkeypatch):
    checkout = _checkout(tmp_path / "checkout")
    data = checkout / "data"
    data.mkdir()
    monkeypatch.setattr(production, "SOURCE_ROOT", checkout)
    monkeypatch.setenv("KALSHI_EVENTS_PROJECT_ROOT", str(checkout))

    directory = data / "nested" / "production"
    assert production.prepare_directory(directory, "full") == directory
    assert (directory / "runs").is_dir()
    nested_data = data / "safe-local"
    assert production._external_path(nested_data) == nested_data


@pytest.mark.parametrize(
    "relative_path",
    [Path(), Path("data"), Path("src/production"), Path("tests/production")],
)
def test_production_directory_rejects_checkout_root_and_non_data_paths(
    production, tmp_path, monkeypatch, relative_path
):
    checkout = tmp_path / "checkout"
    _checkout(checkout)
    (checkout / "data").mkdir()
    monkeypatch.setattr(production, "SOURCE_ROOT", checkout)
    monkeypatch.setenv("KALSHI_EVENTS_PROJECT_ROOT", str(checkout))

    with pytest.raises((ValueError, RuntimeError)):
        production._external_path(checkout / relative_path)


def test_production_directory_rejects_paths_that_escape_checkout_data(
    production, tmp_path, monkeypatch
):
    checkout = tmp_path / "checkout"
    _checkout(checkout)
    (checkout / "data").mkdir()
    monkeypatch.setattr(production, "SOURCE_ROOT", checkout)
    monkeypatch.setenv("KALSHI_EVENTS_PROJECT_ROOT", str(checkout))

    with pytest.raises((ValueError, RuntimeError)):
        production._external_path(checkout / "data" / ".." / "src" / "production")


def test_production_directory_rejects_symlinked_checkout_data(production, tmp_path, monkeypatch):
    checkout = tmp_path / "checkout"
    _checkout(checkout)
    outside = tmp_path / "outside"
    outside.mkdir()
    (checkout / "data").symlink_to(outside, target_is_directory=True)
    monkeypatch.setattr(production, "SOURCE_ROOT", checkout)
    monkeypatch.setenv("KALSHI_EVENTS_PROJECT_ROOT", str(checkout))

    with pytest.raises((ValueError, RuntimeError)):
        production._external_path(checkout / "data" / "nested" / "production")


def test_hidden_workers_reject_source_tree_write_paths(production, capsys):
    source = SCRIPT.parents[1] / "src" / "kalshi_events"
    operator = source / "operator"
    assert (
        production._refresh_worker_main(
            operator,
            source / "session.json",
            source / "metrics.json",
            source / "notification.json",
            "historical",
            "a" * 32,
            "{}",
        )
        == 1
    )
    assert not (source / "session.json").exists()
    assert not (source / "metrics.json").exists()
    capsys.readouterr()

    assert production._activation_worker(operator, source, "a" * 64) == 1
    assert not (source / "production.duckdb").exists()


@pytest.mark.parametrize(
    "arguments",
    [
        lambda root: ["--_account-root", str(root)],
        lambda root: ["--_account-recovery", str(root), "a" * 32],
        lambda root: ["--_root-summary", str(root)],
    ],
)
def test_hidden_read_workers_do_not_create_missing_operator_roots(
    production, tmp_path, monkeypatch, capsys, arguments
):
    checkout = _checkout(tmp_path / "checkout")
    (checkout / "data").mkdir()
    monkeypatch.setenv("KALSHI_EVENTS_PROJECT_ROOT", str(checkout))
    missing = checkout / "data" / "missing-operator"

    assert production._main_hidden(arguments(missing)) == 1
    assert not missing.exists()
    assert "existing operator root" in capsys.readouterr().out


@pytest.mark.parametrize(
    "arguments",
    [
        lambda root: ["--_account-root", str(root)],
        lambda root: ["--_account-recovery", str(root), "a" * 32],
        lambda root: ["--_root-summary", str(root)],
    ],
)
def test_hidden_read_workers_reject_checkout_source_paths(production, capsys, arguments):
    source = SCRIPT.parents[1] / "src" / "kalshi_events"
    assert production._main_hidden(arguments(source)) == 1
    assert "strict descendant" in capsys.readouterr().out


@pytest.mark.parametrize(
    "stop", ["rss", "free_space", "unknown_resources", "deadline", "mount_change"]
)
def test_resource_monitor_stops_and_reaps_without_starting_more_work(
    production, tmp_path, monkeypatch, stop
):
    clock = [100.0]
    terminated = []

    class Child:
        pid = 424242
        returncode = None

        def poll(self):
            return self.returncode

        def wait(self, timeout):
            return self.returncode

    child = Child()

    def terminate(actual_child):
        assert actual_child is child
        terminated.append(clock[0])
        child.returncode = -15
        return True

    def sample(group, *, timeout):
        assert 0 < timeout <= 5
        if stop == "unknown_resources":
            raise production.RunnerError(
                "bounded memory sample failed: resource inspection unavailable"
            )
        return _memory_sample(group, rss=24 * 1024**3 + 1 if stop == "rss" else 0)

    free = production.RUN_LIMITS["minimum_free_bytes"] - (1 if stop == "free_space" else 0)
    monkeypatch.setattr(production.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        production.time, "sleep", lambda duration: clock.__setitem__(0, clock[0] + duration)
    )
    monkeypatch.setattr(production, "_sample_memory", sample)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)
    monkeypatch.setattr(production, "_terminate", terminate)
    monkeypatch.setattr(production, "_atomic_report", lambda *_a: None)
    monkeypatch.setattr(production.shutil, "disk_usage", lambda _path: SimpleNamespace(free=free))
    report = {"resource_samples": [], "peak_child_process_group_rss_bytes": 0}
    code, reason, reaped = production._monitor(
        child,
        tmp_path,
        tmp_path / "operator",
        report,
        tmp_path / "session.json",
        100 if stop == "deadline" else 200,
        original_device=tmp_path.stat().st_dev + 1 if stop == "mount_change" else None,
    )
    assert code == -15
    assert reason
    assert reaped is True
    assert len(terminated) == 1
    if stop == "rss":
        assert terminated == [160]
        assert len(report["resource_samples"]) == 5
    elif stop == "unknown_resources":
        assert reason == "resource accounting failed: memory snapshot unavailable"
        assert reason in production.RECOVERABLE_ACCOUNTING_STOP_REASONS
    else:
        assert terminated == [100]


@pytest.mark.parametrize("filename", ["run-production.py", "production_run.py"])
def test_script_entries_work_outside_checkout_with_only_src_on_pythonpath(tmp_path, filename):
    script = SCRIPT.with_name(filename)
    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(SCRIPT.parent.parent / "src")},
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "--directory" in result.stdout
    assert "--activate-current" in result.stdout


def test_allocation_survives_parent_heartbeat_before_child_result_is_available(
    production, tmp_path, monkeypatch
):
    notification = tmp_path / "capture-notification.json"
    report_path = tmp_path / "session.json"
    report = {
        "capture_id": None,
        "root_id": None,
        "accounting_complete": True,
        "resumable": None,
        "resource_samples": [],
        "peak_child_process_group_rss_bytes": 0,
        "system": {"filesystem_device": tmp_path.stat().st_dev},
    }

    class Child:
        pid = 424242
        returncode = None

        def poll(self):
            return self.returncode

        def wait(self, timeout):
            return self.returncode

    child = Child()

    def allocate_during_sample(group, *, timeout):
        assert 0 < timeout <= 5
        if group is not None:
            production._write_capture_notification(notification, "b" * 32, "a" * 32)
            child.returncode = -15
        return _memory_sample(group)

    monkeypatch.setattr(production.subprocess, "Popen", lambda *_a, **_kw: child)
    monkeypatch.setattr(production, "_sample_memory", allocate_during_sample)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)
    monkeypatch.setattr(production.time, "sleep", lambda _duration: None)
    monkeypatch.setattr(
        production.shutil, "disk_usage", lambda _path: SimpleNamespace(free=1000 * 1024**3)
    )
    code, _reason, reaped = production._invoke(
        [sys.executable, "-c", "pass"],
        directory=tmp_path,
        operator=tmp_path / "operator",
        report=report,
        report_path=report_path,
        deadline=time.monotonic() + 10,
        stdout_path=tmp_path / "stdout",
        stderr_path=tmp_path / "stderr",
        capture_notification=notification,
    )
    assert code == -15 and reaped
    assert report["capture_id"] == "b" * 32
    assert report["root_id"] == "a" * 32
    assert report["accounting_complete"] is False
    assert report["resumable"] is None
    assert json.loads(notification.read_text())["capture_id"] == "b" * 32


@pytest.mark.parametrize("pressure", ["warn", "critical"])
def test_invoke_requires_normal_pressure_before_starting_any_child(
    production, tmp_path, monkeypatch, pressure
):
    monkeypatch.setattr(
        production,
        "_sample_memory",
        lambda group, **_kw: _memory_sample(group, pressure=pressure),
    )
    monkeypatch.setattr(
        production.subprocess,
        "Popen",
        lambda *_a, **_kw: pytest.fail("child started without NORMAL preflight pressure"),
    )
    report = {"system": {"filesystem_device": tmp_path.stat().st_dev}}

    result = production._invoke(
        ["synthetic-child"],
        directory=tmp_path,
        operator=tmp_path / "operator",
        report=report,
        report_path=tmp_path / "session.json",
        deadline=time.monotonic() + 30,
        stdout_path=tmp_path / "stdout",
        stderr_path=tmp_path / "stderr",
    )

    assert result == (None, "memory pressure was not normal before supervised work", True)
    saved = json.loads((tmp_path / "session.json").read_text())
    assert saved["memory_preflight_samples"][0]["system_memory_pressure"] == pressure
    assert saved["memory_preflight_samples"][0]["memory_policy_version"] == 2
    assert saved["memory_preflight_samples_total"] == 1
    assert saved["memory_preflight_samples_dropped"] == 0
    assert not (tmp_path / "stdout").exists()
    assert not (tmp_path / "stderr").exists()


def test_invoke_fails_closed_when_preflight_metrics_are_unknown(production, tmp_path, monkeypatch):
    def missing(_group, **_kwargs):
        raise production.RunnerError("compressor measurement missing")

    monkeypatch.setattr(production, "_sample_memory", missing)
    monkeypatch.setattr(
        production.subprocess,
        "Popen",
        lambda *_a, **_kw: pytest.fail("child started without complete memory metrics"),
    )
    report = {"system": {"filesystem_device": tmp_path.stat().st_dev}}

    result = production._invoke(
        ["synthetic-child"],
        directory=tmp_path,
        operator=tmp_path / "operator",
        report=report,
        report_path=tmp_path / "session.json",
        deadline=time.monotonic() + 30,
        stdout_path=tmp_path / "stdout",
        stderr_path=tmp_path / "stderr",
    )

    assert result == (None, "memory metrics unavailable before supervised work", True)
    assert "compressor measurement missing" in report["memory_preflight_error"]


def test_invoke_does_not_convert_operator_interrupt_into_preflight_failure(
    production, tmp_path, monkeypatch
):
    def interrupt(*_args, **_kwargs):
        raise production.RunnerInterrupted("operator stop")

    monkeypatch.setattr(production, "_sample_memory", interrupt)
    monkeypatch.setattr(
        production.subprocess,
        "Popen",
        lambda *_a, **_kw: pytest.fail("child started after operator interrupt"),
    )

    with pytest.raises(production.RunnerInterrupted, match="operator stop"):
        production._invoke(
            ["synthetic-child"],
            directory=tmp_path,
            operator=tmp_path / "operator",
            report={"system": {"filesystem_device": tmp_path.stat().st_dev}},
            report_path=tmp_path / "session.json",
            deadline=time.monotonic() + 30,
            stdout_path=tmp_path / "stdout",
            stderr_path=tmp_path / "stderr",
        )


def test_invoke_clamps_preflight_to_shared_deadline_and_records_metrics(
    production, tmp_path, monkeypatch
):
    timeouts = []

    class Child:
        pid = 123

        def poll(self):
            return 0

        def wait(self, *, timeout):
            return 0

    monkeypatch.setattr(
        production,
        "_sample_memory",
        lambda group, *, timeout: timeouts.append(timeout) or _memory_sample(group),
    )
    monkeypatch.setattr(production.subprocess, "Popen", lambda *_a, **_kw: Child())
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)
    report = {"system": {"filesystem_device": tmp_path.stat().st_dev}}

    result = production._invoke(
        ["synthetic-child"],
        directory=tmp_path,
        operator=tmp_path / "operator",
        report=report,
        report_path=tmp_path / "session.json",
        deadline=time.monotonic() + 8,
        stdout_path=tmp_path / "stdout",
        stderr_path=tmp_path / "stderr",
    )

    assert result == (0, None, True)
    assert len(timeouts) == 1 and 2 < timeouts[0] <= 3
    assert report["memory_preflight_samples"][0]["system_memory_pressure"] == "normal"
    assert report["memory_preflight_samples"][0]["memory_policy_version"] == 2
    assert report["memory_preflight_samples_total"] == 1


def test_refresh_worker_rejects_nonpublic_origin_before_creating_operator_data(
    production, tmp_path, monkeypatch, capsys
):
    from kalshi_events import transport

    report = tmp_path / "session.json"
    report.write_text("{}")
    root = tmp_path / "operator"
    monkeypatch.setattr(transport, "source_url", lambda: "https://untrusted.example/trade-api/v2")
    limits = production.remaining_allowances(
        {"attempts": 0, "downloaded_bytes": 0, "accounting_complete": True}, 100, now=100
    )
    code = production._refresh_worker_main(
        root,
        report,
        tmp_path / "metrics.json",
        tmp_path / "capture-notification.json",
        "full",
        None,
        json.dumps(limits),
    )
    value = json.loads(capsys.readouterr().out)
    assert code == 1
    assert "public Kalshi API origin" in value["error"]
    assert not root.exists()
    assert not (tmp_path / "metrics.json").exists()


@pytest.mark.parametrize("workflow", [False, True])
@pytest.mark.parametrize("wall_elapsed", [-100, 20, 200])
def test_wall_clock_changes_cannot_extend_monotonic_stage_deadline(
    production, monkeypatch, workflow, wall_elapsed
):
    report = {"started_monotonic": 100, "started_epoch": 1000}
    monkeypatch.setattr(production.time, "monotonic", lambda: 160)
    monkeypatch.setattr(production.time, "time", lambda: 1000 + wall_elapsed)
    limit = production.RUN_LIMITS["workflow_seconds" if workflow else "active_cutoff_seconds"]
    deadline = production._monitor_deadline(report, workflow=workflow)
    assert deadline == min(100 + limit, 160 + limit - wall_elapsed)
    assert deadline <= 100 + limit


@pytest.mark.parametrize("workflow", [False, True])
def test_sleep_or_forward_wall_jump_exhausts_stage_even_if_monotonic_barely_advanced(
    production, monkeypatch, workflow
):
    report = {"started_monotonic": 100, "started_epoch": 1000}
    limit = production.RUN_LIMITS["workflow_seconds" if workflow else "active_cutoff_seconds"]
    monkeypatch.setattr(production.time, "monotonic", lambda: 160)
    monkeypatch.setattr(production.time, "time", lambda: 1000 + limit + 1)
    assert production._monitor_deadline(report, workflow=workflow) == 160


def test_child_start_is_refused_without_its_termination_reserve(production, tmp_path, monkeypatch):
    monkeypatch.setattr(
        production.subprocess, "Popen", lambda *_a, **_kw: pytest.fail("late child was started")
    )
    code, reason, reaped = production._invoke(
        [sys.executable, "-c", "pass"],
        directory=tmp_path,
        operator=tmp_path / "operator",
        report={"system": {"filesystem_device": tmp_path.stat().st_dev}},
        report_path=tmp_path / "session.json",
        deadline=time.monotonic() + production.RUN_LIMITS["terminate_seconds"] - 0.1,
        stdout_path=tmp_path / "stdout",
        stderr_path=tmp_path / "stderr",
    )
    assert code is None
    assert reason
    assert reaped is True
    assert not (tmp_path / "stdout").exists()


@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGHUP])
def test_public_supervisor_signal_reaps_its_isolated_worker_and_records_uncertainty(
    tmp_path, stop_signal
):
    directory = tmp_path / "production"
    ready = tmp_path / "worker.json"
    harness = """
import json,os,pathlib,runpy,sys,time
from types import SimpleNamespace
sys.path.insert(0,sys.argv[1])
import production_run as runner
from kalshi_events import transport
runner._revision=lambda:'a'*40
runner._boot_stamp=lambda:'test boot'
runner._system_info=lambda directory:{'filesystem_device':directory.stat().st_dev}
runner._sample_memory=lambda group,**kw:{'child_process_group_rss_bytes':None if group is None else 0,'system_compressed_bytes':0,'system_compressor_occupied_bytes':0,'system_memory_pressure':'normal','system_memory_pressure_observed_monotonic':time.monotonic(),'child_process_group_physical_footprint_bytes':None if group is None else 0,'supervisor_physical_footprint_bytes':0}
runner.shutil.disk_usage=lambda directory:SimpleNamespace(free=1000*1024**3)
transport.source_url=lambda:transport.BASE_URL
worker=('import json,os,sys,time; '
        'open(sys.argv[1],"w").write(json.dumps({"pid":os.getpid(),"pgid":os.getpgrp()})); '
        'time.sleep(60)')
ready_path=sys.argv[4]
runner._refresh_command=lambda *a,**kw:[sys.executable,'-c',worker,ready_path]
sys.argv=[sys.argv[2],'--directory',sys.argv[3],'--full']
runpy.run_path(sys.argv[0],run_name='__main__')
"""
    supervisor = subprocess.Popen(
        [
            sys.executable,
            "-c",
            harness,
            str(SCRIPT.parent),
            str(SCRIPT.with_name("run-production.py")),
            str(directory),
            str(ready),
        ],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(SCRIPT.parent.parent / "src")},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    worker = None
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if ready.exists():
                with contextlib.suppress(ValueError):
                    worker = json.loads(ready.read_text())
                    break
            if supervisor.poll() is not None:
                break
            time.sleep(0.01)
        assert worker is not None, supervisor.communicate(timeout=7)
        assert worker["pgid"] != supervisor.pid
        assert _live_process(worker["pid"])
        supervisor.send_signal(stop_signal)
        stdout, stderr = supervisor.communicate(timeout=7)
        assert supervisor.returncode != 0, (stdout, stderr)
        assert not _live_process(worker["pid"]), "public stop left its isolated worker running"
        report = json.loads(next((directory / "runs").glob("*/session.json")).read_text())
        assert report["phase"] == "stopped"
        assert report["accounting_complete"] is False
        assert report["requests"] is None
        assert report["downloaded_bytes"] is None
        assert report["production_state"] != "verified"
        assert report["active_process_group"] is None
        assert report["error"]
        assert not (directory / "production.duckdb").exists()
    finally:
        if supervisor.poll() is None:
            supervisor.send_signal(signal.SIGTERM)
            try:
                supervisor.wait(timeout=7)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(supervisor.pid, signal.SIGKILL)
                supervisor.wait(timeout=3)
        if worker is None and ready.exists():
            with contextlib.suppress(ValueError):
                worker = json.loads(ready.read_text())
        if worker is not None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(worker["pgid"], signal.SIGKILL)


@pytest.mark.parametrize("returncode", [0, 1])
def test_standalone_verification_records_elapsed_time_on_success_and_failure(
    production, tmp_path, monkeypatch, returncode
):
    clock = [100.0]
    release = tmp_path / "release"
    report = {"phase_seconds": {"standalone-verification": 2.5, "capture": 99}}

    def invoke(command, **kwargs):
        assert command[0] == sys.executable
        assert command[command.index("--release") + 1] == str(release)
        assert kwargs["deadline"] == 150
        clock[0] += 7.5
        kwargs["stdout_path"].write_text(json.dumps({"source_release_id": "a" * 64}))
        return returncode, None, True

    monkeypatch.setattr(production.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(production, "_invoke", invoke)
    args = (
        release,
        tmp_path,
        tmp_path / "operator",
        {"logs": tmp_path, "report": tmp_path / "session.json"},
        report,
        150,
    )
    if returncode:
        with pytest.raises(production.RunnerError, match="standalone release verification"):
            production._release_verify(*args)
    else:
        assert production._release_verify(*args)["source_release_id"] == "a" * 64
    assert report["phase_seconds"] == {"standalone-verification": 10.0, "capture": 99}


def test_stop_during_resource_termination_is_guarded_and_restores_previous_handlers(
    production, monkeypatch
):
    child = SimpleNamespace(pid=424242, poll=lambda: 0)
    original_handlers = {
        item: signal.getsignal(item) for item in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)
    }
    calls = []

    def killpg(group, delivered):
        assert group == child.pid and delivered == signal.SIGTERM
        assert production._STOP_IN_PROGRESS is True
        handler = signal.getsignal(signal.SIGTERM)
        assert callable(handler)
        handler(signal.SIGTERM, None)
        calls.append(True)

    monkeypatch.setattr(production.os, "killpg", killpg)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)
    monkeypatch.setattr(production, "_TERMINATION_ATTEMPTED", set())
    with production._graceful_stop_handlers():
        assert production._STOP_IN_PROGRESS is False
        assert production._terminate(child) is True
        assert calls == [True]
        assert production._STOP_IN_PROGRESS is False
        production._STOP_IN_PROGRESS = True
        assert production._terminate(child) is True
        assert production._STOP_IN_PROGRESS is True
    assert production._STOP_IN_PROGRESS is False
    assert {item: signal.getsignal(item) for item in original_handlers} == original_handlers


@pytest.mark.parametrize("leader_reaped", [False, True])
def test_unreaped_group_receives_only_one_grace_even_if_cleanup_is_reentered(
    production, monkeypatch, leader_reaped
):
    clock = [100.0]
    exists = [True]
    signals = []

    class Child:
        pid = 424242

        def poll(self):
            return 0 if leader_reaped else None

        def wait(self, timeout):
            raise subprocess.TimeoutExpired("unreaped child", timeout)

    monkeypatch.setattr(production.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        production.time, "sleep", lambda duration: clock.__setitem__(0, clock[0] + duration)
    )
    monkeypatch.setattr(production.os, "killpg", lambda group, sent: signals.append((group, sent)))
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: exists[0])
    monkeypatch.setattr(production, "_TERMINATION_ATTEMPTED", set())
    child = Child()
    assert production._terminate(child) is False
    assert clock[0] == pytest.approx(105)
    prior_signals = signals.copy()
    assert production._terminate(child) is False
    assert clock[0] == pytest.approx(105), "reentry granted a second five-second grace"
    assert signals == prior_signals
    exists[0] = False
    assert production._terminate(child) is leader_reaped
    status = child._production_termination_outcome
    assert status["leader_reaped"] is leader_reaped
    assert status["group_absent"] is True
    assert status["uncertain"] is (not leader_reaped)
    assert clock[0] == pytest.approx(105)


def test_environment_report_uses_the_installed_distribution_name(production, monkeypatch):
    import importlib.metadata

    requested = []

    def version(distribution):
        requested.append(distribution)
        return "test-version"

    monkeypatch.setattr(importlib.metadata, "version", version)
    versions = production._package_versions()
    assert versions["oddsfox-kalshi-events"] == "test-version"
    assert versions["httpcore"] == "test-version"
    assert versions["dbt-duckdb"] == "test-version"
    assert "kalshi-events" not in requested


@pytest.mark.parametrize("movement", ["forward_wall", "forced_sleep", "backward_wall"])
def test_active_monitor_rechecks_wall_deadline_without_extending_monotonic_limit(
    production, tmp_path, monkeypatch, movement
):
    clock = {"mono": 100.0, "wall": 1000.0}
    sleeps = []
    stops = []
    timeouts = []

    class Child:
        pid = 424242
        returncode = None

        def poll(self):
            if self.returncode is None and len(sleeps) >= 6:
                self.returncode = 0
            return self.returncode

        def wait(self, timeout):
            return self.returncode

    child = Child()

    def sleep(duration):
        sleeps.append(duration)
        clock["mono"] += 0.001 if movement == "forced_sleep" else duration
        clock["wall"] += duration
        if len(sleeps) == 1:
            clock["wall"] += -600 if movement == "backward_wall" else 48 * 3600 + 1

    def terminate(actual):
        assert actual is child
        stops.append(clock.copy())
        child.returncode = -15
        return True

    def sample(group, *, timeout):
        assert 0 < timeout <= 5
        timeouts.append(timeout)
        return _memory_sample(group)

    monkeypatch.setattr(production.time, "monotonic", lambda: clock["mono"])
    monkeypatch.setattr(production.time, "time", lambda: clock["wall"])
    monkeypatch.setattr(production.time, "sleep", sleep)
    monkeypatch.setattr(production, "_sample_memory", sample)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)
    monkeypatch.setattr(production, "_terminate", terminate)
    monkeypatch.setattr(production, "_atomic_report", lambda *_a: None)
    monkeypatch.setattr(
        production.shutil, "disk_usage", lambda _path: SimpleNamespace(free=1000 * 1024**3)
    )
    report = {
        "mode": "full",
        "phase": "refresh",
        "started_epoch": 1000,
        "started_monotonic": 100,
        "deadline_epoch": 1000 + 48 * 3600,
        "resource_samples": [],
        "peak_child_process_group_rss_bytes": 0,
    }
    deadline = 108 if movement == "backward_wall" else 100_000
    code, reason, reaped = production._monitor(
        child,
        tmp_path,
        tmp_path / "operator",
        report,
        tmp_path / "session.json",
        deadline,
        wall_deadline_epoch=1000 + 46 * 3600,
    )
    assert code == -15 and reason and reaped
    assert len(stops) == 1
    assert timeouts
    if movement == "backward_wall":
        assert stops[0]["mono"] == 103
    else:
        assert len(sleeps) == 1, "wall exhaustion was deferred until a later resource sample"
        assert stops[0]["mono"] < 102


def test_active_monitor_reserves_five_wall_seconds_and_clamps_ps_timeout_and_sleep(
    production, tmp_path, monkeypatch
):
    clock = {"mono": 100.0, "wall": 1093.75}
    sleeps = []
    timeouts = []
    stops = []

    class Child:
        pid = 424242
        returncode = None

        def poll(self):
            return self.returncode

        def wait(self, timeout):
            return self.returncode

    child = Child()

    def sleep(duration):
        sleeps.append(duration)
        clock["mono"] += duration
        clock["wall"] += duration

    def sample(group, *, timeout):
        timeouts.append(timeout)
        return _memory_sample(group)

    def terminate(_child):
        stops.append(clock.copy())
        child.returncode = -15
        return True

    monkeypatch.setattr(production.time, "monotonic", lambda: clock["mono"])
    monkeypatch.setattr(production.time, "time", lambda: clock["wall"])
    monkeypatch.setattr(production.time, "sleep", sleep)
    monkeypatch.setattr(production, "_sample_memory", sample)
    monkeypatch.setattr(production, "_process_group_exists", lambda _group: False)
    monkeypatch.setattr(production, "_terminate", terminate)
    monkeypatch.setattr(production, "_atomic_report", lambda *_a: None)
    monkeypatch.setattr(
        production.shutil, "disk_usage", lambda _path: SimpleNamespace(free=1000 * 1024**3)
    )
    code, reason, reaped = production._monitor(
        child,
        tmp_path,
        tmp_path / "operator",
        {"resource_samples": [], "peak_child_process_group_rss_bytes": 0},
        tmp_path / "session.json",
        10_000,
        wall_deadline_epoch=1100,
    )
    assert code == -15 and reason and reaped
    assert timeouts == [1.25]
    assert sleeps == [1, 0.25]
    assert stops == [{"mono": 101.25, "wall": 1095.0}]


@pytest.mark.parametrize(
    ("mode", "phase", "workflow_deadline", "seconds"),
    [
        ("full", "refresh", True, 46 * 3600),
        ("resume", "accounting", True, 46 * 3600),
        ("full", "accounting", False, 47 * 3600 + 50 * 60),
        ("full", "standalone-verification", False, 47 * 3600 + 50 * 60),
        ("resume", "activation", False, 47 * 3600 + 50 * 60),
        ("activate-current", "root-summary", False, 47 * 3600 + 50 * 60),
        ("activate-current", "activation", False, 47 * 3600 + 50 * 60),
    ],
)
def test_child_wall_deadline_keeps_original_workflow_and_verification_stage_caps(
    production, monkeypatch, mode, phase, workflow_deadline, seconds
):
    monkeypatch.setattr(production.time, "monotonic", lambda: 101)
    monkeypatch.setattr(production.time, "time", lambda: 1001)
    report = {
        "mode": mode,
        "phase": phase,
        "started_epoch": 1000,
        "started_monotonic": 100,
        "deadline_epoch": 1000 + 48 * 3600,
    }
    assert (
        production._child_wall_deadline_epoch(report, 300_000, workflow_deadline=workflow_deadline)
        == 1000 + seconds
    )


def test_shorter_child_duration_is_also_a_wall_deadline(production, monkeypatch):
    monkeypatch.setattr(production.time, "monotonic", lambda: 101)
    monkeypatch.setattr(production.time, "time", lambda: 1001)
    report = {
        "mode": "full",
        "phase": "refresh",
        "started_epoch": 1000,
        "started_monotonic": 100,
        "deadline_epoch": 1000 + 48 * 3600,
    }
    assert production._child_wall_deadline_epoch(report, 111, workflow_deadline=True) == 1011


def test_expired_wall_deadline_refuses_child_start_even_with_remaining_monotonic_time(
    production, tmp_path, monkeypatch
):
    monkeypatch.setattr(production.time, "monotonic", lambda: 101)
    monkeypatch.setattr(production.time, "time", lambda: 1000 + 46 * 3600)
    monkeypatch.setattr(
        production.subprocess, "Popen", lambda *_a, **_kw: pytest.fail("expired wall child started")
    )
    report = {
        "mode": "full",
        "phase": "refresh",
        "started_epoch": 1000,
        "started_monotonic": 100,
        "deadline_epoch": 1000 + 48 * 3600,
        "system": {"filesystem_device": tmp_path.stat().st_dev},
    }
    code, reason, reaped = production._invoke(
        [sys.executable, "-c", "pass"],
        directory=tmp_path,
        operator=tmp_path / "operator",
        report=report,
        report_path=tmp_path / "session.json",
        deadline=100_000,
        stdout_path=tmp_path / "stdout",
        stderr_path=tmp_path / "stderr",
        workflow_deadline=True,
    )
    assert code is None and reason and reaped
    assert not (tmp_path / "stdout").exists()


@pytest.mark.parametrize("label", ["refresh", "activation", "accounting"])
def test_stage_dispatch_explicitly_selects_workflow_or_active_wall_deadline(
    production, tmp_path, monkeypatch, label
):
    forwarded = []

    def invoke(_command, **kwargs):
        forwarded.append(kwargs)
        return 0, None, True

    monkeypatch.setattr(production, "_invoke", invoke)
    monkeypatch.setattr(production, "_atomic_report", lambda *_a: None)
    report = {"phase_seconds": {}}
    production._run_external_child(
        [sys.executable, "-c", "pass"],
        directory=tmp_path,
        operator=tmp_path / "operator",
        paths={"logs": tmp_path, "report": tmp_path / "session.json"},
        report=report,
        deadline=time.monotonic() + 100,
        label=label,
    )
    assert len(forwarded) == 1
    assert forwarded[0]["workflow_deadline"] is (label == "refresh")
