"""Explicit reboot recovery must not weaken volume or wall-clock boundaries."""

from __future__ import annotations

import json
import plistlib
from types import SimpleNamespace

import pytest
import test_production_runner as runner_tests

production = runner_tests.production

VOLUME_UUID = "ACAD66E7-42DA-426B-B20D-FB87A453396B"
OTHER_UUID = "00000000-0000-0000-0000-000000000001"


@pytest.fixture
def stopped_session(production, tmp_path, monkeypatch):
    directory = tmp_path / "production"
    (directory / "operator").mkdir(parents=True)
    path = directory / "runs" / "session" / "session.json"
    path.parent.mkdir(parents=True)
    now = production.time.time()
    mono = production.time.monotonic()
    capture_id = "a" * 32
    report = {
        "capture_id": capture_id,
        "root_id": "b" * 32,
        "accounting_complete": True,
        "production_state": "not_attempted",
        "phase": "stopped",
        "active_process_group": None,
        "boot_stamp": "old boot",
        "started_epoch": now - 100,
        "time_budget_started_epoch": now - 50,
        "last_observed_epoch": now - 10,
        "deadline_epoch": now + 100,
        "started_monotonic": mono - 10_000,
        "time_budget_started_monotonic": mono - 9_000,
        "deadline_monotonic": mono - 1,
        "system": {
            "filesystem_device": directory.stat().st_dev + 1,
            "package_versions": production.SUPPORTED_RUNTIME_VERSIONS,
            "uv_lock_sha256": production.RECONCILIATION_UV_LOCK_SHA256,
            "transport_sha256": production.RECONCILIATION_TRANSPORT_SHA256,
        },
    }
    path.write_text(json.dumps(report))
    monkeypatch.setattr(production, "_boot_stamp", lambda: "current boot")
    monkeypatch.setattr(production, "_owner_root_id", lambda _path: report["root_id"])
    monkeypatch.setattr(
        production, "_package_versions", lambda: production.SUPPORTED_RUNTIME_VERSIONS
    )
    monkeypatch.setattr(production, "_volume_uuid", lambda _path: VOLUME_UUID)
    return directory, path, report, capture_id


def _resume(production, stopped_session, **kwargs):
    directory, _path, _report, capture_id = stopped_session
    return production._load_resume(
        directory,
        capture_id,
        reconcile_accounting=True,
        recover_reboot=True,
        recovery_volume_uuid=kwargs.pop("recovery_volume_uuid", VOLUME_UUID),
        **kwargs,
    )


@pytest.mark.parametrize("invalid", ["expired_wall", "backward_wall", "saved_uuid", "active_child"])
def test_invalid_reboot_context_does_not_rewrite_session(production, stopped_session, invalid):
    _directory, path, report, _capture_id = stopped_session
    if invalid == "expired_wall":
        report["deadline_epoch"] = production.time.time() - 1
    elif invalid == "backward_wall":
        report["last_observed_epoch"] = production.time.time() + 100
    elif invalid == "saved_uuid":
        report["system"]["filesystem_volume_uuid"] = OTHER_UUID
    else:
        report["active_process_group"] = 1234
    path.write_text(json.dumps(report))
    before = path.read_bytes()
    with pytest.raises(production.RunnerError):
        _resume(production, stopped_session)
    assert path.read_bytes() == before


@pytest.mark.parametrize("supplied", [OTHER_UUID, "not-a-uuid"])
def test_same_boot_recovery_does_not_persist_unverified_volume(
    production, stopped_session, supplied
):
    directory, path, report, _capture_id = stopped_session
    report["boot_stamp"] = "current boot"
    report["system"]["filesystem_device"] = directory.stat().st_dev
    report["deadline_monotonic"] = production.time.monotonic() + 100
    path.write_text(json.dumps(report))
    before = path.read_bytes()
    with pytest.raises(production.RunnerError, match="UUID|volume"):
        _resume(production, stopped_session, recovery_volume_uuid=supplied)
    assert path.read_bytes() == before


def test_crash_after_clock_checkpoint_can_retry_without_extending_deadline(
    production, stopped_session, monkeypatch
):
    directory, path, saved, _capture_id = stopped_session
    atomic_report = production._atomic_report

    class PowerLoss(BaseException):
        pass

    def commit_then_crash(path, report):
        atomic_report(path, report)
        raise PowerLoss

    monkeypatch.setattr(production, "_atomic_report", commit_then_crash)
    with pytest.raises(PowerLoss):
        _resume(production, stopped_session)
    checkpoint = json.loads(path.read_text())
    assert checkpoint["boot_stamp"] == "current boot"
    assert checkpoint["system"]["filesystem_device"] == directory.stat().st_dev
    assert (
        checkpoint["original_system"]["filesystem_device"] == saved["system"]["filesystem_device"]
    )
    monkeypatch.setattr(production, "_atomic_report", atomic_report)
    _path, resumed, _paths = _resume(production, stopped_session)
    assert resumed["deadline_epoch"] == saved["deadline_epoch"]
    assert resumed["deadline_monotonic"] == checkpoint["deadline_monotonic"]
    assert len(resumed["reboot_recovery_history"]) == 1


@pytest.mark.parametrize("invalid", ["bad_uuid", "missing_uuid", "wrong_mount", "malformed_plist"])
def test_volume_metadata_failure_cannot_select_a_filesystem(
    production, tmp_path, monkeypatch, invalid
):
    directory = tmp_path / "production"
    directory.mkdir()
    outside = tmp_path / "different-volume"
    outside.mkdir()
    info = {"VolumeUUID": VOLUME_UUID, "MountPoint": str(tmp_path)}
    if invalid == "bad_uuid":
        info["VolumeUUID"] = "invalid"
    elif invalid == "missing_uuid":
        del info["VolumeUUID"]
    elif invalid == "wrong_mount":
        info["MountPoint"] = str(outside)
    body = b"not a plist" if invalid == "malformed_plist" else plistlib.dumps(info)
    monkeypatch.setattr(
        production.subprocess, "run", lambda *_args, **_kwargs: SimpleNamespace(stdout=body)
    )
    with pytest.raises(production.RunnerError):
        production._volume_uuid(directory)


def test_volume_metadata_timeout_is_fail_closed(production, tmp_path, monkeypatch):
    def timeout(command, **kwargs):
        assert kwargs["timeout"] == 5
        raise production.subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr(production.subprocess, "run", timeout)
    with pytest.raises(production.RunnerError, match="cannot verify"):
        production._volume_uuid(tmp_path)


def test_volume_uuid_is_bound_to_containing_mount(production, tmp_path, monkeypatch):
    directory = tmp_path / "production"
    directory.mkdir()
    info = {"VolumeUUID": VOLUME_UUID.lower(), "MountPoint": str(tmp_path)}
    monkeypatch.setattr(
        production.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(stdout=plistlib.dumps(info)),
    )
    assert production._volume_uuid(directory) == VOLUME_UUID
