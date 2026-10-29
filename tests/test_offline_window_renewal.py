from __future__ import annotations

import fcntl
import importlib.util
import json
import os
import sys
import time
from pathlib import Path

import pytest
from test_historical_production import (
    CAPTURE_ID,
    _historical_report,
    _renewable_session,
    _snapshot,
)

RUNNER = Path(__file__).parents[1] / "scripts" / "production_run.py"


@pytest.fixture(scope="module")
def production():
    spec = importlib.util.spec_from_file_location("offline_window_renewal_test_runner", RUNNER)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _prepare_renewal(directory, production, monkeypatch):
    report_path, report, operator = _renewable_session(directory, production)
    monkeypatch.setattr(production, "_revision", lambda: "e" * 40)
    monkeypatch.setattr(production, "_boot_stamp", lambda: report["boot_stamp"])
    monkeypatch.setattr(
        production,
        "_volume_uuid",
        lambda _path: "ACAD66E7-42DA-426B-B20D-FB87A453396B",
    )
    return report_path, report, operator


def _load_renewal(production, directory):
    return production._load_resume(
        directory,
        CAPTURE_ID,
        historical=True,
        offline_duration_seconds=600,
        renew_offline_window=True,
    )


def test_explicit_renewal_rejects_a_canonical_writer_lock_held_by_another_writer(
    production, tmp_path, monkeypatch
):
    directory = tmp_path / "production"
    report_path, report, operator = _prepare_renewal(directory, production, monkeypatch)
    before = production._strict_json(report_path)

    with (operator / "writer.lock").open("rb") as held_lock:
        fcntl.flock(held_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            with pytest.raises(production.RunnerError, match="another writer holds"):
                _load_renewal(production, directory)
        finally:
            fcntl.flock(held_lock.fileno(), fcntl.LOCK_UN)

    assert production._strict_json(report_path) == before


def test_explicit_renewal_checks_capture_genesis_without_constructing_store(
    production, tmp_path, monkeypatch
):
    directory = tmp_path / "production"
    _report_path, report, _operator = _prepare_renewal(directory, production, monkeypatch)

    from kalshi_events import common

    monkeypatch.setattr(
        common,
        "Store",
        lambda *_args, **_kwargs: pytest.fail("renewal must not scan the operator root"),
    )

    _loaded_path, loaded, _paths = _load_renewal(production, directory)

    assert loaded["offline_historical_window"] == report["offline_historical_window"]


def test_explicit_24_hour_renewal_does_not_launder_oversized_v1_history(
    production, tmp_path, monkeypatch
):
    directory = tmp_path / "production"
    report_path, report, _operator = _prepare_renewal(directory, production, monkeypatch)
    current = report["offline_historical_window"]
    legacy_end_epoch = current["started_epoch"] - 1
    legacy_end_monotonic = current["started_monotonic"] - 1
    report["offline_historical_window_history"] = [
        {
            "version": 1,
            "capture_id": CAPTURE_ID,
            "root_id": report["root_id"],
            "revision": "f" * 40,
            "started_at": production._utc(),
            "started_epoch": legacy_end_epoch - 14_401,
            "started_monotonic": legacy_end_monotonic - 14_401,
            "deadline_epoch": legacy_end_epoch,
            "deadline_monotonic": legacy_end_monotonic,
        }
    ]
    production._atomic_report(report_path, report)
    before = production._strict_json(report_path)

    with pytest.raises(production.RunnerError, match="duration is invalid"):
        production._load_resume(
            directory,
            CAPTURE_ID,
            historical=True,
            offline_duration_seconds=86_400,
            renew_offline_window=True,
        )

    assert production._strict_json(report_path) == before


def test_initial_24_hour_window_reaches_historical_child_duration(
    production, tmp_path, monkeypatch
):
    directory = tmp_path / "production"
    directory.mkdir()
    (directory / "runs").mkdir()
    report_path, report = _historical_report(directory, directory / "operator", production)
    wall_now = int(time.time())
    monotonic_now = int(time.monotonic())
    report.update(
        started_epoch=wall_now - 10,
        time_budget_started_epoch=wall_now - 10,
        deadline_epoch=wall_now + production.RUN_LIMITS["session_seconds"] - 10,
        started_monotonic=monotonic_now - 10,
        time_budget_started_monotonic=monotonic_now - 10,
        deadline_monotonic=monotonic_now + production.RUN_LIMITS["session_seconds"] - 10,
        last_observed_epoch=wall_now,
    )
    production._atomic_report(report_path, report)

    class Clock:
        def time(self):
            return wall_now

        def monotonic(self):
            return monotonic_now

    monkeypatch.setattr(production, "time", Clock())
    snapshot = _snapshot()
    snapshot["root_id"] = report["root_id"]

    class Awake:
        pid = 424242

        def __init__(self, *_args, **_kwargs):
            pass

        def poll(self):
            return None

    class StopBeforeChild(Exception):
        pass

    monkeypatch.setattr(
        production, "_package_versions", lambda: production.SUPPORTED_RUNTIME_VERSIONS
    )
    monkeypatch.setattr(production, "_revision", lambda: "e" * 40)
    monkeypatch.setattr(production, "_boot_stamp", lambda: report["boot_stamp"])
    monkeypatch.setattr(
        production, "_system_info", lambda path: {"filesystem_device": path.stat().st_dev}
    )
    monkeypatch.setattr(production, "_validate_runtime_versions", lambda _versions: None)
    monkeypatch.setattr(production.subprocess, "Popen", Awake)
    monkeypatch.setattr(production, "_kill_awake_guard", lambda _awake: None)
    monkeypatch.setattr(
        production.shutil,
        "disk_usage",
        lambda _path: type("Usage", (), {"free": 1_000 * 1024**3})(),
    )
    monkeypatch.setattr(production, "_resource_guard", lambda *_args, **_kwargs: snapshot)
    child_commands = []

    def stop_before_child(command, **_kwargs):
        child_commands.append(command)
        raise StopBeforeChild()

    monkeypatch.setattr(production, "_run_external_child", stop_before_child)

    result, code = production.run_production(
        directory,
        capture_id=CAPTURE_ID,
        historical=True,
        offline_duration_seconds=86_400,
    )

    assert code == 1
    assert len(child_commands) == 1
    allowance = json.loads(child_commands[0][-1])
    assert 14_400 < allowance["max-duration-seconds"] <= 86_400
    assert result["offline_historical_window"]["version"] == 2
    assert (
        production._strict_json(report_path)["offline_historical_window"]
        == result["offline_historical_window"]
    )


def test_explicit_renewal_rejects_canonical_lock_replaced_after_acquisition(
    production, tmp_path, monkeypatch
):
    directory = tmp_path / "production"
    report_path, report, operator = _prepare_renewal(directory, production, monkeypatch)
    before = production._strict_json(report_path)
    original_strict_json = production._strict_json
    replaced = False
    replacement_lock_was_independently_lockable = False

    def replace_lock_before_read(path, *args, **kwargs):
        nonlocal replaced, replacement_lock_was_independently_lockable
        path = Path(path)
        if path == operator / "root.json" and not replaced:
            lock_path = operator / "writer.lock"
            detached_lock = operator / "writer.lock.detached"
            lock_path.rename(detached_lock)
            lock_path.write_bytes(b"")

            # The runner holds the detached lock inode. Another writer can
            # acquire the replacement inode still named writer.lock while the
            # runner reads this same root's marker and capture genesis.
            descriptor = os.open(lock_path, os.O_RDWR)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                replacement_lock_was_independently_lockable = True
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)
            replaced = True
        return original_strict_json(path, *args, **kwargs)

    monkeypatch.setattr(production, "_strict_json", replace_lock_before_read)

    failure = None
    with production._operator_writer_lock(operator) as lock_identity:
        try:
            production._validate_offline_renewal_root(
                directory,
                operator,
                str(report["root_id"]),
                CAPTURE_ID,
                writer_lock_identity=lock_identity,
            )
        except production.RunnerError as exc:
            failure = str(exc)

    assert replaced
    assert replacement_lock_was_independently_lockable
    assert failure is not None and "writer lock" in failure.lower(), (
        "renewal must reject identity reads after the canonical lock path is replaced; "
        f"observed failure: {failure!r}"
    )
    assert original_strict_json(report_path) == before
