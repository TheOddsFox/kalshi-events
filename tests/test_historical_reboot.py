"""Offline historical reboot recovery is read-only until exact accounting passes."""

from __future__ import annotations

import importlib.util
import shutil
import time
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "historical_reboot_under_test", ROOT / "scripts" / "production_run.py"
)
assert SPEC and SPEC.loader
production = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(production)

CAPTURE_ID = "a" * 32
SESSION_ID = "b" * 32
ROOT_ID = "c" * 32
VOLUME_UUID = "ACAD66E7-42DA-426B-B20D-FB87A453396B"


@pytest.fixture
def stopped_session(monkeypatch):
    directory = ROOT / "data" / "testing" / "historical-reboot" / uuid.uuid4().hex
    session = directory / "runs" / SESSION_ID
    session.mkdir(parents=True, mode=0o700)
    (directory / "operator").mkdir(mode=0o700)
    now_epoch = time.time()
    now_mono = time.monotonic()
    report = {
        "version": 1,
        "session_id": SESSION_ID,
        "mode": "full",
        "capture_id": CAPTURE_ID,
        "root_id": ROOT_ID,
        "phase": "stopped",
        "production_state": "not_attempted",
        "active_process_group": 987_654,
        "boot_stamp": "old-boot",
        "started_epoch": now_epoch - 30_000,
        "deadline_epoch": now_epoch - 1_000,
        "time_budget_started_epoch": now_epoch - 25_000,
        "last_observed_epoch": now_epoch - 10,
        "started_monotonic": now_mono - 30_000,
        "deadline_monotonic": now_mono - 1_000,
        "time_budget_started_monotonic": now_mono - 25_000,
        "system": {
            "filesystem_device": directory.stat().st_dev + 1,
            "filesystem_volume_uuid": VOLUME_UUID,
        },
        "accounting_complete": False,
        "accounting_basis": "reserved-upper-bound",
        "requests": 5,
        "downloaded_bytes": None,
        "downloaded_bytes_upper_bound": 500,
        "unfinished_reservation_ids": ["d" * 64],
        "offline_historical_window": {
            "version": 2,
            "capture_id": CAPTURE_ID,
            "root_id": ROOT_ID,
            "revision": "e" * 40,
            "started_at": "2026-11-02T08:00:00Z",
            "started_epoch": now_epoch - 13 * 3600,
            "deadline_epoch": now_epoch - 3600,
            "started_monotonic": now_mono - 13 * 3600,
            "deadline_monotonic": now_mono - 3600,
        },
        "resource_profile": "expanded",
        "limits": dict(production.RUN_LIMITS) | production.RESOURCE_PROFILES["expanded"],
        "effective_resource_policy": production._resource_policy_record(
            "expanded",
            dict(production.RUN_LIMITS) | production.RESOURCE_PROFILES["expanded"],
        ),
        "resource_profile_history": [
            {
                "profile": "expanded",
                "limits": dict(production.RUN_LIMITS) | production.RESOURCE_PROFILES["expanded"],
                "policy": production._resource_policy_record(
                    "expanded",
                    dict(production.RUN_LIMITS) | production.RESOURCE_PROFILES["expanded"],
                ),
                "selected_at": "2026-11-02T08:00:00Z",
                "reason": "historical grant profile",
            }
        ],
    }
    report_path = session / "session.json"
    production._atomic_report(report_path, report)
    production._atomic_report(
        directory / production.OWNER_MARKER,
        {"version": 1, "root_id": ROOT_ID},
    )
    monkeypatch.setattr(production, "_boot_stamp", lambda: "new-boot")
    monkeypatch.setattr(production, "_owner_root_id", lambda _path: ROOT_ID)
    monkeypatch.setattr(production, "_volume_uuid", lambda _path: VOLUME_UUID)
    yield directory, report_path, report
    shutil.rmtree(directory, ignore_errors=True)


def _load(directory):
    return production._load_resume(
        directory,
        CAPTURE_ID,
        historical=True,
        reconcile_accounting=True,
        recover_reboot=True,
        recovery_volume_uuid=VOLUME_UUID,
    )


def _exact_snapshot():
    return {
        "attempts": 7,
        "downloaded_or_reserved_bytes": 700,
        "accounting_complete": True,
        "unfinished_reservations": [],
        "storage": {"retained_bytes": 1, "temporary_bytes": 0, "database_bytes": 1},
        "root_id": ROOT_ID,
    }


def test_changed_boot_reconciles_exact_accounting_without_starting_work(
    stopped_session, monkeypatch
):
    directory, report_path, original = stopped_session
    raw_before = report_path.read_bytes()
    killed = []
    monkeypatch.setattr(production.os, "killpg", lambda *args: killed.append(args))
    monkeypatch.setattr(production.os, "kill", lambda *args: killed.append(args))
    monkeypatch.setattr(production, "_terminate", lambda *_args: killed.append("terminate"))
    monkeypatch.setattr(production, "_read_accounting_worker", lambda *_args: _exact_snapshot())

    _path, pending, _paths = _load(directory)
    assert report_path.read_bytes() == raw_before
    assert (
        pending["_pending_historical_reboot_reconciliation"]["previous_active_process_group"]
        == 987_654
    )

    reconciled = production._finish_historical_reboot_reconciliation(
        directory, report_path, pending, CAPTURE_ID
    )

    assert reconciled["boot_stamp"] == "new-boot"
    assert reconciled["active_process_group"] is None
    assert reconciled["accounting_complete"] is True
    assert reconciled["accounting_basis"] == "exact"
    assert reconciled["requests"] == 7
    assert reconciled["offline_historical_window"] == original["offline_historical_window"]
    audit = reconciled["reboot_recovery_history"][-1]
    assert audit["prior_process_group_ignored_after_boot_change"] is True
    assert audit["processing_worker_started"] is False
    assert audit["grant_started"] is False
    assert killed == []


def test_same_boot_recovery_is_rejected_without_rewriting_session(stopped_session, monkeypatch):
    directory, report_path, report = stopped_session
    report["boot_stamp"] = "new-boot"
    production._atomic_report(report_path, report)
    before = report_path.read_bytes()
    with pytest.raises(production.RunnerError, match="verified boot change"):
        _load(directory)
    assert report_path.read_bytes() == before


def test_incomplete_receipt_accounting_cannot_mint_new_monotonic_clocks(
    stopped_session, monkeypatch
):
    directory, report_path, _report = stopped_session
    _path, pending, _paths = _load(directory)
    before = report_path.read_bytes()
    incomplete = _exact_snapshot()
    incomplete["accounting_complete"] = False
    incomplete["unfinished_reservations"] = [{"request_id": "d" * 64}]
    monkeypatch.setattr(production, "_read_accounting_worker", lambda *_args: incomplete)
    with pytest.raises(production.RunnerError, match="complete exact receipt accounting"):
        production._finish_historical_reboot_reconciliation(
            directory, report_path, pending, CAPTURE_ID
        )
    assert report_path.read_bytes() == before


def test_wrong_volume_uuid_fails_before_session_write(stopped_session, monkeypatch):
    directory, report_path, _report = stopped_session
    monkeypatch.setattr(
        production, "_volume_uuid", lambda _path: "00000000-0000-0000-0000-000000000001"
    )
    before = report_path.read_bytes()
    with pytest.raises(production.RunnerError, match="does not match"):
        _load(directory)
    assert report_path.read_bytes() == before
