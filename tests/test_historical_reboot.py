"""Offline historical reboot recovery is read-only until exact accounting passes."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import shutil
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
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


def _bounded_snapshot():
    reservations = [
        {
            "capture_id": CAPTURE_ID,
            "request_id": f"{index:064x}",
            "reserved_bytes": 1024,
            "status": None,
            "accounting_complete": False,
            "acquisition_protocol": 5,
            "version": 2,
        }
        for index in range(1, 5)
    ]
    amount = len(reservations) * (
        int(production.RUN_LIMITS["response_bytes"]) + production.HTTPX_MAX_RAW_CHUNK_BYTES
    )
    return {
        "attempts": 8,
        "downloaded_or_reserved_bytes": amount,
        "accounting_complete": False,
        "unfinished_reservations": reservations,
        "storage": {"retained_bytes": 1, "temporary_bytes": 0, "database_bytes": 1},
        "root_id": ROOT_ID,
    }


def _legacy_temporary_stop(directory, report_path, report):
    current_device = directory.stat().st_dev
    frozen_device = current_device + 100
    original_device = frozen_device + 4
    expanded_limits = production._limits_for_profile("expanded")
    threshold = expanded_limits["storage_temporary_stop_bytes"]
    wall_deadlines = {
        "started_epoch": report["started_epoch"],
        "time_budget_started_epoch": report["time_budget_started_epoch"],
        "deadline_epoch": report["deadline_epoch"],
    }
    report.update(
        boot_stamp="frozen-boot",
        phase="stopped",
        active_process_group=68295,
        error=f"live temporary storage reached the {threshold // 1024**3} GiB stop threshold",
        original_system={
            "filesystem_device": original_device,
            "filesystem_volume_uuid": VOLUME_UUID,
        },
        system={"filesystem_device": frozen_device, "filesystem_volume_uuid": VOLUME_UUID},
        storage_safety_stop={
            "version": 1,
            "mode": "historical",
            "category": "temporary",
            "invocation_id": "e" * 32,
            "capture_id": CAPTURE_ID,
            "root_id": ROOT_ID,
            "revision": "f" * 40,
            "revision_source": "session.revision",
            "observed_bytes": threshold + 1,
            "threshold_bytes": threshold,
            "threshold_inclusive": True,
            "original_process_group": 68295,
        },
        reboot_recovery_history=[
            {
                "recovered_at": "2026-10-28T12:00:00Z",
                "previous_boot_stamp": "pre-freeze-boot",
                "boot_stamp": "frozen-boot",
                "previous_filesystem_device": original_device,
                "filesystem_device": frozen_device,
                "volume_uuid": VOLUME_UUID,
                "prior_volume_uuid_available": True,
                "previous_monotonic": {
                    "started_monotonic": report["started_monotonic"],
                    "time_budget_started_monotonic": report["time_budget_started_monotonic"],
                    "deadline_monotonic": report["deadline_monotonic"],
                },
                "wall_deadlines_preserved": wall_deadlines,
                "accounting_reconciliation_required": True,
            }
        ],
        stopped_worker_reconciliation={
            "event_id": "9" * 64,
            "audit_file": "stopped-worker-reconciliation.jsonl",
            "boot_stamp": "frozen-boot",
            "stale_process_group": 57889,
            "supervisor_pid": 99,
            "supervisor_pgid": 99,
            "primary_stop_reason": "resource accounting failed: memory snapshot unavailable",
            "reconciliation_kind": "memory_snapshot_unavailable",
        },
    )
    (directory / "operator" / "writer.lock").touch(mode=0o600)
    (directory / production.OWNER_MARKER).write_text(
        json.dumps({"version": 1, "root_id": ROOT_ID}) + "\n"
    )
    production._atomic_report(report_path, report)
    return frozen_device


def _stub_temporary_stop_validator(monkeypatch):
    calls = []

    def evidence(_directory, _report_path, report, _capture_id, **_kwargs):
        calls.append(report["boot_stamp"])
        value = dict(report["storage_safety_stop"])
        value.update(
            launch_revision_source="session.revision",
            processing_revision=None,
            reconciliation_runtime_revision=None,
            supervisor_pid=101,
            supervisor_pgid=202,
            process_identity_utc="101 101 launch identity",
        )
        return (
            value,
            Path("/offline/evidence.json"),
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode(),
            Path("/offline/latest-launch.json"),
            b"launch record",
            None,
            None,
        )

    monkeypatch.setattr(production, "_stopped_worker_evidence", evidence)
    return calls


def _stub_locked_accounting_store(monkeypatch, operator):
    class LockedStore:
        def __init__(self, root):
            self.root = Path(root).resolve()
            self.locked = False

        @property
        def writer_locked_by_current_thread(self):
            return self.locked

        @contextmanager
        def writer_guard(self):
            self.locked = True
            try:
                yield
            finally:
                self.locked = False

    store = LockedStore(operator)
    monkeypatch.setattr(production, "_accounting_store", lambda _root: store)
    return store


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


def test_legacy_temporary_stop_accepts_bound_reservations_and_preserves_old_history(
    stopped_session, monkeypatch
):
    directory, report_path, report = stopped_session
    frozen_device = _legacy_temporary_stop(directory, report_path, report)
    snapshot = _bounded_snapshot()
    receipt_path = directory / "operator" / "raw" / f"{1:064x}" / "manifest.json"
    receipt_path.parent.mkdir(parents=True)
    receipt_path.write_bytes(b"durable unfinished receipt\n")
    receipt_before = receipt_path.read_bytes()
    memory_audit = {
        "version": 1,
        "event_id": "9" * 64,
        "session_id": SESSION_ID,
        "capture_id": CAPTURE_ID,
        "root_id": ROOT_ID,
        "boot_stamp": "frozen-boot",
        "stale_process_group": 57889,
        "supervisor_pid": 99,
        "supervisor_pgid": 99,
    }
    audit_path = directory / "operator" / "stopped-worker-reconciliation.jsonl"
    audit_path.write_text(json.dumps(memory_audit) + "\n")
    validator_calls = _stub_temporary_stop_validator(monkeypatch)
    store = _stub_locked_accounting_store(monkeypatch, directory / "operator")
    accounting_snapshot = [snapshot]

    def read_accounting(*_args, **kwargs):
        if "locked_store" in kwargs:
            assert kwargs["locked_store"] is store
            assert store.writer_locked_by_current_thread
        return accounting_snapshot[0]

    monkeypatch.setattr(production, "_read_accounting_worker", read_accounting)
    monkeypatch.setattr(
        production,
        "_pid_exists",
        lambda *_args: pytest.fail("prior-boot process IDs must not be queried"),
    )
    monkeypatch.setattr(
        production,
        "_process_group_exists",
        lambda *_args: pytest.fail("prior-boot process groups must not be queried"),
    )
    monkeypatch.setattr(
        production.os,
        "killpg",
        lambda *_args: pytest.fail("reboot recovery must not signal prior-boot groups"),
    )
    monkeypatch.setattr(
        production.os,
        "kill",
        lambda *_args: pytest.fail("reboot recovery must not signal prior-boot processes"),
    )
    before = report_path.read_bytes()
    identity = {
        "capture_id": CAPTURE_ID,
        "session_id": SESSION_ID,
        "root_id": ROOT_ID,
        "prior_session_sha256": hashlib.sha256(before).hexdigest(),
    }

    _path, pending, _paths = _load(directory)
    reconciled = production._finish_historical_reboot_reconciliation(
        directory, report_path, pending, CAPTURE_ID
    )

    audit = reconciled["reboot_recovery_history"][-1]
    assert audit["accounting_basis"] == "reserved-upper-bound"
    assert audit["accounting_complete"] is False
    assert audit["accounting_bound_verified"] is True
    assert len(audit["unfinished_reservation_ids"]) == 4
    assert reconciled["accounting_complete"] is False
    assert reconciled["downloaded_bytes"] is None
    assert reconciled["consumption_state"] == "reserved-upper-bound"
    assert reconciled["started_epoch"] == report["started_epoch"]
    assert reconciled["deadline_epoch"] == report["deadline_epoch"]
    assert reconciled["reboot_recovery_history"][0]["filesystem_device"] == frozen_device
    assert (
        audit["pre_recovery_history_sha256"]
        == hashlib.sha256(
            json.dumps(
                reconciled["reboot_recovery_history"][:-1],
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
    )
    assert receipt_path.read_bytes() == receipt_before
    assert validator_calls == ["frozen-boot"]

    mount_proof = production._historical_reboot_mount_proof(
        reconciled,
        identity,
        {"root_device": frozen_device, "root_volume_uuid": VOLUME_UUID},
        final_device=directory.stat().st_dev,
        final_volume_uuid=VOLUME_UUID,
        final_boot_stamp="new-boot",
    )
    assert mount_proof is audit
    authority = {
        "report": reconciled,
        "identity": identity,
        "operator": directory / "operator",
        "freeze": {"root_device": frozen_device, "root_volume_uuid": VOLUME_UUID},
        "current_mount": {"device": directory.stat().st_dev, "volume_uuid": VOLUME_UUID},
    }
    source_proof = production._historical_reconciliation_audit(authority)
    assert source_proof["recovery_kind"] == "prior_boot_temporary_stop"
    assert source_proof["accounting_basis"] == "reserved-upper-bound"

    tampered = copy.deepcopy(reconciled)
    tampered["reboot_recovery_history"][0]["filesystem_device"] += 1
    with pytest.raises(production.RunnerError, match="legacy reboot history"):
        production._historical_reboot_mount_proof(
            tampered,
            identity,
            {"root_device": frozen_device, "root_volume_uuid": VOLUME_UUID},
            final_device=directory.stat().st_dev,
            final_volume_uuid=VOLUME_UUID,
            final_boot_stamp="new-boot",
        )

    tampered_accounting = copy.deepcopy(reconciled)
    tampered_accounting["downloaded_bytes_upper_bound"] += 1
    with pytest.raises(production.RunnerError, match="bounded reboot proof"):
        production._historical_reconciliation_audit({**authority, "report": tampered_accounting})

    accounting_snapshot[0] = _exact_snapshot()
    monkeypatch.setattr(production, "_boot_stamp", lambda: "third-boot")
    _path, next_pending, _paths = _load(directory)
    with pytest.raises(production.RunnerError, match="regressed its saved request count"):
        production._finish_historical_reboot_reconciliation(
            directory, report_path, next_pending, CAPTURE_ID
        )

    accounting_snapshot[0] = _exact_snapshot() | {"attempts": 8}
    _path, next_pending, _paths = _load(directory)
    exact_recovery = production._finish_historical_reboot_reconciliation(
        directory, report_path, next_pending, CAPTURE_ID
    )
    assert exact_recovery["historical_reboot_reconciliation"]["accounting_basis"] == "exact"
    assert "recovery_kind" not in exact_recovery["historical_reboot_reconciliation"]
    second_mount_proof = production._historical_reboot_mount_proof(
        exact_recovery,
        identity,
        {"root_device": frozen_device, "root_volume_uuid": VOLUME_UUID},
        final_device=directory.stat().st_dev,
        final_volume_uuid=VOLUME_UUID,
        final_boot_stamp="third-boot",
    )
    assert second_mount_proof is exact_recovery["reboot_recovery_history"][-1]
    exact_authority = {
        **authority,
        "report": exact_recovery,
        "current_mount": {"device": directory.stat().st_dev, "volume_uuid": VOLUME_UUID},
    }
    exact_source_proof = production._historical_reconciliation_audit(exact_authority)
    assert exact_source_proof == source_proof

    accounting_snapshot[0] = _exact_snapshot() | {
        "attempts": 9,
        "downloaded_or_reserved_bytes": 699,
    }
    monkeypatch.setattr(production, "_boot_stamp", lambda: "fourth-boot")
    _path, regressed_pending, _paths = _load(directory)
    with pytest.raises(production.RunnerError, match="regressed its saved byte count"):
        production._finish_historical_reboot_reconciliation(
            directory, report_path, regressed_pending, CAPTURE_ID
        )


def test_prior_boot_temporary_stop_audit_append_is_idempotent_after_crash(
    stopped_session, monkeypatch
):
    directory, report_path, report = stopped_session
    _legacy_temporary_stop(directory, report_path, report)
    snapshot = _bounded_snapshot()
    _stub_temporary_stop_validator(monkeypatch)
    store = _stub_locked_accounting_store(monkeypatch, directory / "operator")

    def read_accounting(*_args, **kwargs):
        assert kwargs["locked_store"] is store
        assert store.writer_locked_by_current_thread
        return snapshot

    monkeypatch.setattr(production, "_read_accounting_worker", read_accounting)
    original_atomic = production._atomic_report
    audit_path = directory / "operator" / "stopped-worker-reconciliation.jsonl"
    memory_audit = {
        "version": 1,
        "event_id": "8" * 64,
        "session_id": SESSION_ID,
        "capture_id": CAPTURE_ID,
        "root_id": ROOT_ID,
        "boot_stamp": "frozen-boot",
        "stale_process_group": 57889,
        "supervisor_pid": 98,
        "supervisor_pgid": 98,
    }
    audit_path.write_text(json.dumps(memory_audit) + "\n")
    _path, pending, _paths = _load(directory)
    raw_before = report_path.read_bytes()
    monkeypatch.setattr(
        production,
        "_atomic_report",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("simulated crash")),
    )
    with pytest.raises(OSError, match="simulated crash"):
        production._finish_historical_reboot_reconciliation(
            directory, report_path, pending, CAPTURE_ID
        )
    assert report_path.read_bytes() == raw_before
    after_first_append = [json.loads(line) for line in audit_path.read_text().splitlines()]
    typed_event = next(row for row in after_first_append if row.get("recovery_kind"))

    monkeypatch.setattr(production, "_atomic_report", original_atomic)
    _path, retry_pending, _paths = _load(directory)
    recovered = production._finish_historical_reboot_reconciliation(
        directory, report_path, retry_pending, CAPTURE_ID
    )
    after_retry = [json.loads(line) for line in audit_path.read_text().splitlines()]
    assert sum(row.get("event_id") == typed_event["event_id"] for row in after_retry) == 1
    assert recovered["stopped_worker_reconciliation"]["event_id"] == typed_event["event_id"]


def test_processing_launch_validation_accepts_only_explicit_saved_boot(tmp_path, monkeypatch):
    now_epoch = time.time()
    now_monotonic = time.monotonic()
    launched_at = datetime.fromtimestamp(now_epoch - 30, UTC).replace(microsecond=0)
    checked_at = datetime.fromtimestamp(now_epoch - 2, UTC).replace(microsecond=0)
    window = {
        "started_epoch": now_epoch - 60,
        "deadline_epoch": now_epoch + 600,
        "started_monotonic": now_monotonic - 60,
        "deadline_monotonic": now_monotonic + 600,
    }
    report_path = tmp_path / "session.json"
    report = {
        "boot_stamp": "saved-boot",
        "revision": "d" * 40,
        "root_id": ROOT_ID,
        "capture_id": CAPTURE_ID,
        "offline_historical_window": window,
    }
    pid = 12345
    command = [
        sys.executable,
        str(tmp_path / "processing.py"),
        "--historical",
        "--capture-id",
        CAPTURE_ID,
        "--directory",
        str(tmp_path),
    ]
    launch = {
        "processing_revision": report["revision"],
        "revision": "e" * 40,
        "reconciliation_code_source": {"revision": "e" * 40},
        "session": str(report_path),
        "capture_id": CAPTURE_ID,
        "root_id": ROOT_ID,
        "pid": pid,
        "pgid": pid,
        "state": "launched",
        "offline_only": True,
        "automatic_retry": False,
        "command": command,
        "launched_at_utc": launched_at.isoformat().replace("+00:00", "Z"),
        "process_identity_utc": (
            f"{pid} {pid} {launched_at.strftime('%a %b %d %H:%M:%S %Y')} {' '.join(command)}"
        ),
        "started_epoch": window["started_epoch"],
        "processing_deadline_epoch": window["deadline_epoch"] - 30,
        "active_end_epoch": window["deadline_epoch"],
        "outer_deadline_epoch": window["deadline_epoch"] + 60,
        "launched_monotonic": now_monotonic - 30,
        "processing_deadline_monotonic": window["deadline_monotonic"] - 30,
        "active_end_monotonic": window["deadline_monotonic"],
        "outer_deadline_monotonic": window["deadline_monotonic"] + 60,
    }
    checks_path = tmp_path / "launch-checks.json"
    checks = {
        "checked_at_utc": checked_at.isoformat().replace("+00:00", "Z"),
        "fresh_processing_cutoff_and_active_end_enforced": True,
        "original_grant_and_reconciliation_history_preserved": True,
        "new_window": window,
    }
    checks_path.write_text(json.dumps(checks) + "\n")
    monkeypatch.setattr(production, "_boot_stamp", lambda: "current-boot")
    monkeypatch.setattr(production, "_revision", lambda: "f" * 40)

    with pytest.raises(production.RunnerError, match="not bound to this stopped session"):
        production._validate_current_processing_launch(
            tmp_path / "latest-launch.json",
            report_path,
            report,
            launch,
            command,
            CAPTURE_ID,
            pid,
            pid,
        )

    result_path, raw, _revision = production._validate_current_processing_launch(
        tmp_path / "latest-launch.json",
        report_path,
        report,
        launch,
        command,
        CAPTURE_ID,
        pid,
        pid,
        expected_report_boot_stamp="saved-boot",
    )
    assert result_path == checks_path
    assert json.loads(raw) == checks
