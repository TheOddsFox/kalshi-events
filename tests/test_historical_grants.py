"""Private historical grants are finite, immutable, and phase-bound."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "historical_grants_under_test", ROOT / "scripts" / "historical_grants.py"
)
assert SPEC and SPEC.loader
grants = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(grants)
PRODUCTION_SPEC = importlib.util.spec_from_file_location(
    "production_run_historical_controls_under_test", ROOT / "scripts" / "production_run.py"
)
assert PRODUCTION_SPEC and PRODUCTION_SPEC.loader
production = importlib.util.module_from_spec(PRODUCTION_SPEC)
PRODUCTION_SPEC.loader.exec_module(production)
_TEST_CREATED_CONTROL_ROOTS: list[Path] = []


@pytest.fixture
def artifacts():
    root = ROOT / "data" / "testing" / "historical-grants" / uuid.uuid4().hex
    root.mkdir(parents=True, mode=0o700)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)
        for control_root in _TEST_CREATED_CONTROL_ROOTS:
            shutil.rmtree(control_root, ignore_errors=True)
        _TEST_CREATED_CONTROL_ROOTS.clear()


@pytest.fixture
def isolated_control_root():
    runtime_root = ROOT / "data" / "runtime"
    runtime_root.mkdir(parents=True, exist_ok=True)
    if runtime_root.is_symlink() or runtime_root.resolve() != runtime_root:
        raise RuntimeError("test runtime root is not an owned workspace directory")
    control_root = None
    for _attempt in range(8):
        candidate = runtime_root / (
            "kalshi-historical-completion-" + f"{uuid.uuid4().int % 100_000_000:08d}"
        )
        try:
            candidate.mkdir(mode=0o700)
        except FileExistsError:
            continue
        control_root = candidate
        break
    else:
        raise RuntimeError("could not create an isolated managed control root")
    try:
        control_root.chmod(0o700)
        if (control_root / "source").exists():
            raise RuntimeError("isolated test control root unexpectedly contains source")
        yield control_root
    finally:
        shutil.rmtree(control_root)


@pytest.fixture
def identity():
    return {
        "capture_id": "a" * 32,
        "session_id": "b" * 32,
        "root_id": "c" * 32,
        "stage_name": "build-historical-" + "d" * 32,
        "failed_attempts_sha256": "e" * 64,
        "prior_session_sha256": "f" * 64,
        "state_sha256": "1" * 64,
        "source_sha256": "2" * 64,
        "runtime_sha256": "3" * 64,
    }


def _grant(identity, *, now_epoch=None, now_monotonic=None, **changes):
    now_epoch = time.time() if now_epoch is None else now_epoch
    now_monotonic = time.monotonic() if now_monotonic is None else now_monotonic
    grant_id = changes.pop("grant_id", "4" * 32)
    approval = {
        "version": 1,
        "grant_id": grant_id,
        "phase": "readiness",
        "phases": ["readiness"],
        "bindings": identity,
        "approved_at_epoch": now_epoch - 1,
        "duration_seconds": 24 * 60 * 60,
        "forecast_seconds": 16 * 60 * 60,
        "measured_final_verification_seconds": 0,
        "measurements": {
            "report_sha256": "7" * 64,
            "session_report_sha256": "8" * 64,
            "measured_at_epoch": now_epoch - 0.75,
            "forecast_seconds": 16 * 60 * 60,
            "final_verification_seconds": 0,
            "projected_database_bytes": 90 * grants.GIB,
            "projected_temporary_bytes": 130 * grants.GIB,
            "projected_root_bytes": 300 * grants.GIB,
            "projected_aggregate_bytes": 350 * grants.GIB,
            "projected_free_bytes": 160 * grants.GIB,
            "inventory_entries": 50_000,
            "projected_peak_rss_bytes": 20 * grants.GIB,
        },
        "resource_admission": grants.expected_resource_admission(),
        "freeze": {"engineering": True, "runtime": True, "controls": True},
        "retire_source": False,
        "retirement_policy": None,
        "activation_audit_sha256": None,
    }
    approval.update(changes)
    latch = {
        "version": 1,
        "grant_id": grant_id,
        "approval_sha256": grants.sha256_json(approval),
        "nonce": "5" * 32,
        "created_at_epoch": now_epoch - 0.5,
    }
    launch = grants.create_launch_record(
        approval,
        latch,
        now_epoch=now_epoch,
        now_monotonic=now_monotonic,
        boot_stamp="boot-current",
        launch_id="6" * 32,
        session_binding_sha256="9" * 64,
    )
    return approval, latch, launch, now_epoch, now_monotonic


def _resource_baseline_workspace(tmp_path, production):
    from kalshi_events.incremental import root_id

    directory = tmp_path / "production"
    operator = directory / "operator"
    session_id = "d" * 32
    capture_id = "a" * 32
    report_path = directory / "runs" / session_id / production.REPORT_NAME
    (report_path.parent / "logs").mkdir(parents=True)
    operator.mkdir(parents=True)
    directory.mkdir(exist_ok=True)
    (directory / ".production-run.lock").touch()
    store = production._historical_store(operator, max_duration_seconds=3600)
    with store.writer_guard():
        logical_root_id = root_id(store, create=True)
    production._atomic_report(
        directory / production.OWNER_MARKER,
        {"version": 1, "root_id": logical_root_id},
    )
    production._atomic_report(
        report_path,
        {
            "version": 1,
            "session_id": session_id,
            "capture_id": capture_id,
            "root_id": logical_root_id,
            "peak_child_process_group_rss_bytes": 0,
            "peak_child_process_group_physical_footprint_bytes": 0,
            "invocation_peak_child_process_group_rss_bytes": 0,
            "invocation_peak_child_process_group_physical_footprint_bytes": 0,
        },
    )
    store.reconcile()
    return directory, operator, store, report_path, capture_id, logical_root_id


def _set_inventory_limit(monkeypatch, production, limit):
    limits = dict(production._effective_limits())
    limits["storage_inventory_entries"] = limit
    monkeypatch.setattr(production, "_effective_limits", lambda: limits)


def test_baseline_inventory_excludes_immutable_capture_files_but_keeps_byte_accounting(
    tmp_path, monkeypatch
):
    directory, operator, store, report_path, capture_id, root_id = _resource_baseline_workspace(
        tmp_path, production
    )
    raw = operator / "captures" / capture_id
    raw.mkdir(parents=True)
    for index in range(20):
        (raw / f"receipt-{index:02d}.json").write_bytes(b"immutable receipt evidence")
    store.reconcile()
    _set_inventory_limit(monkeypatch, production, 11)

    deadline_checks = []
    check_deadline = store.check_deadline

    def observed_deadline_check():
        deadline_checks.append(True)
        check_deadline()

    monkeypatch.setattr(store, "check_deadline", observed_deadline_check)
    report = production._strict_json(report_path)
    baseline = production._historical_resource_baseline(directory, store, report)

    assert baseline["inventory_entries"] == 11  # captures namespace + operator/root/runs entries
    assert baseline["root_bytes"] == store.usage()["retained_bytes"]
    assert baseline["root_bytes"] >= sum(path.stat().st_size for path in raw.iterdir())
    bytes_before_receipt = baseline["root_bytes"]
    (raw / "receipt-extra.json").write_bytes(b"more immutable receipt evidence")
    store.reconcile()
    expanded = production._historical_resource_baseline(
        directory, store, production._strict_json(report_path)
    )
    assert expanded["inventory_entries"] == baseline["inventory_entries"]
    assert expanded["root_bytes"] == store.usage()["retained_bytes"]
    assert expanded["root_bytes"] > bytes_before_receipt
    assert len(deadline_checks) >= 20


def test_baseline_inventory_counts_tmp_roots_with_runs_at_exact_bound(tmp_path, monkeypatch):
    directory, operator, store, report_path, _capture_id, _root_id = _resource_baseline_workspace(
        tmp_path, production
    )
    temporary = operator / "tmp"
    temporary.mkdir()
    (temporary / ("build-historical-" + "a" * 32)).mkdir()
    (temporary / ("parent-index-1-" + "b" * 12)).mkdir()
    _set_inventory_limit(monkeypatch, production, 13)

    baseline = production._historical_resource_baseline(
        directory, store, production._strict_json(report_path)
    )
    assert (
        baseline["inventory_entries"] == 13
    )  # operator namespace, tmp roots and runs/root entries

    (report_path.parent / "logs" / "additional.log").write_bytes(b"x")
    with pytest.raises(production.RunnerError, match="entry bound"):
        production._historical_resource_baseline(
            directory, store, production._strict_json(report_path)
        )


def test_baseline_inventory_counts_empty_active_build_root(tmp_path, monkeypatch):
    directory, operator, store, report_path, capture_id, _root_id = _resource_baseline_workspace(
        tmp_path, production
    )
    (operator / "builds-historical" / capture_id).mkdir(parents=True)
    _set_inventory_limit(monkeypatch, production, 12)

    baseline = production._historical_resource_baseline(
        directory, store, production._strict_json(report_path)
    )
    assert baseline["inventory_entries"] == 12

    _set_inventory_limit(monkeypatch, production, 11)
    with pytest.raises(production.RunnerError, match="entry bound"):
        production._historical_resource_baseline(
            directory, store, production._strict_json(report_path)
        )


def test_baseline_inventory_counts_tmp_atomic_regularfile(tmp_path, monkeypatch):
    directory, operator, store, report_path, _capture_id, _root_id = _resource_baseline_workspace(
        tmp_path, production
    )
    temporary = operator / "tmp"
    temporary.mkdir()
    (temporary / ".write-abcdefgh").write_bytes(b"atomic temporary")
    _set_inventory_limit(monkeypatch, production, 12)

    baseline = production._historical_resource_baseline(
        directory, store, production._strict_json(report_path)
    )
    assert baseline["inventory_entries"] == 12

    _set_inventory_limit(monkeypatch, production, 11)
    with pytest.raises(production.RunnerError, match="entry bound"):
        production._historical_resource_baseline(
            directory, store, production._strict_json(report_path)
        )


def test_baseline_inventory_counts_all_five_sets_once_at_exact_bound(tmp_path, monkeypatch):
    directory, operator, store, report_path, capture_id, _root_id = _resource_baseline_workspace(
        tmp_path, production
    )
    raw = operator / "captures" / capture_id
    raw.mkdir(parents=True)
    for index in range(4):
        (raw / f"receipt-{index}.json").write_bytes(b"immutable")
    (operator / ".write-abcdefgh").write_bytes(b"operator atomic")

    temporary = operator / "tmp"
    temporary.mkdir()
    (temporary / ".write-ijklmnop").write_bytes(b"tmp atomic")
    tmp_workspace = temporary / ("load-" + "a" * 32)
    tmp_workspace.mkdir()
    (tmp_workspace / "partial.bin").write_bytes(b"tmp work")

    build = operator / "builds-historical" / capture_id
    build.mkdir(parents=True)
    (build / "artifact.bin").write_bytes(b"build work")
    (report_path.parent / "logs" / "run-artifact.log").write_bytes(b"run work")
    activation_temp = directory / (".production.duckdb." + "b" * 32 + ".tmp")
    activation_temp.write_bytes(b"activation temporary")
    store.reconcile()

    # Seven operator entries + three tmp entries + two build entries + five run entries + four root entries.
    _set_inventory_limit(monkeypatch, production, 21)
    baseline = production._historical_resource_baseline(
        directory, store, production._strict_json(report_path)
    )
    assert baseline["inventory_entries"] == 21
    assert baseline["root_bytes"] == store.usage()["retained_bytes"]

    (report_path.parent / "logs" / "overflow.log").write_bytes(b"one more")
    with pytest.raises(production.RunnerError, match="entry bound"):
        production._historical_resource_baseline(
            directory, store, production._strict_json(report_path)
        )


def test_baseline_inventory_caps_mutable_operator_and_external_run_entries(tmp_path, monkeypatch):
    directory, operator, store, report_path, capture_id, _root_id = _resource_baseline_workspace(
        tmp_path, production
    )
    _set_inventory_limit(monkeypatch, production, 12)
    recovery = operator / "tmp" / "recovery-index"
    recovery.mkdir(parents=True)
    for index in range(4):
        (recovery / f"index-{index}.bin").write_bytes(b"x")
    build = operator / "builds-historical" / capture_id
    build.mkdir(parents=True)
    for index in range(3):
        (build / f"artifact-{index}.bin").write_bytes(b"x")
    logs = report_path.parent / "logs"
    for index in range(4):
        (logs / f"line-{index}.log").write_bytes(b"x")
    store.reconcile()

    with pytest.raises(production.RunnerError, match="entry bound"):
        production._historical_resource_baseline(
            directory, store, production._strict_json(report_path)
        )


@pytest.mark.parametrize("unsafe", ["symlink", "fifo", "foreign-device"])
def test_baseline_inventory_rejects_unsafe_immutable_tree_entries(tmp_path, monkeypatch, unsafe):
    from types import SimpleNamespace

    directory, operator, store, report_path, capture_id, _root_id = _resource_baseline_workspace(
        tmp_path, production
    )
    _set_inventory_limit(monkeypatch, production, 32)
    raw = operator / "captures" / capture_id
    raw.mkdir(parents=True)
    entry = raw / "unsafe-entry"
    if unsafe == "symlink":
        entry.symlink_to(tmp_path / "outside", target_is_directory=True)
    elif unsafe == "fifo":
        os.mkfifo(entry)
    else:
        entry.write_bytes(b"raw evidence")
        real_scandir = production.os.scandir

        class ScandirResult:
            def __init__(self, entries):
                self._entries = entries

            def __enter__(self):
                return iter(self._entries)

            def __exit__(self, *_args):
                return None

        def foreign_device_scandir(path):
            if Path(path) != raw:
                return real_scandir(path)
            with real_scandir(path) as entries:
                captured = list(entries)
                rows = [
                    SimpleNamespace(
                        name=item.name,
                        path=item.path,
                        stat=lambda *, follow_symlinks=False, item=item: SimpleNamespace(
                            st_dev=item.stat(follow_symlinks=follow_symlinks).st_dev + 1,
                            st_mode=item.stat(follow_symlinks=follow_symlinks).st_mode,
                        ),
                    )
                    for item in captured
                ]
            return ScandirResult(rows)

        monkeypatch.setattr(production.os, "scandir", foreign_device_scandir)

    with pytest.raises(production.RunnerError, match="historical resource inventory contains"):
        production._historical_resource_baseline(
            directory, store, production._strict_json(report_path)
        )


def test_baseline_external_bytes_remain_hardlink_deduplicated(tmp_path, monkeypatch):
    directory, operator, store, report_path, _capture_id, _root_id = _resource_baseline_workspace(
        tmp_path, production
    )
    _set_inventory_limit(monkeypatch, production, 32)
    production_db = directory / "production.duckdb"
    production_db.write_bytes(b"production bytes")
    logs = report_path.parent / "logs"
    payload = logs / "payload.bin"
    payload.write_bytes(b"one external payload")
    os.link(payload, logs / "payload-copy.bin")
    store.reconcile()

    report = production._strict_json(report_path)
    baseline = production._historical_resource_baseline(directory, store, report)
    expected_external = (
        production_db.stat().st_size + report_path.stat().st_size + payload.stat().st_size
    )

    assert production._external_bytes(directory) == expected_external
    assert baseline["aggregate_bytes"] - baseline["root_bytes"] == expected_external


def _private_uncertain_attempt(
    artifacts,
    identity,
    *,
    boot_stamp="boot-current",
    freeze_root_device=None,
    saved_root_device=None,
    mode="historical",
    include_mode=True,
):
    control_root_parent = ROOT / "data" / "runtime"
    control_root_parent.mkdir(parents=True, exist_ok=True)
    control_root = control_root_parent / (
        "kalshi-historical-completion-" + f"{uuid.uuid4().int % 100_000_000:08d}"
    )
    control_root.mkdir(mode=0o700)
    _TEST_CREATED_CONTROL_ROOTS.append(control_root)
    directory = artifacts / "production"
    report_dir = directory / "runs" / identity["session_id"]
    logs = report_dir / "logs"
    operator = directory / "operator"
    report_dir.mkdir(parents=True, mode=0o700)
    logs.mkdir(mode=0o700)
    operator.mkdir(parents=True, mode=0o700)
    (operator / "root.json").write_text("{}\n")
    (operator / "writer.lock").touch(mode=0o600)
    volume_uuid = "ACAD66E7-42DA-426B-B20D-FB87A453396B"
    device = directory.stat().st_dev
    freeze_root_device = device if freeze_root_device is None else freeze_root_device
    saved_root_device = device if saved_root_device is None else saved_root_device
    production._atomic_report(
        directory / production.OWNER_MARKER,
        {"version": 1, "root_id": identity["root_id"]},
    )

    report_path = report_dir / production.REPORT_NAME
    initial_report = {
        "version": 1,
        "session_id": identity["session_id"],
        "mode": mode,
        "capture_id": identity["capture_id"],
        "root_id": identity["root_id"],
        "phase": "stopped",
        "production_state": "not_attempted",
        "active_process_group": None,
        "boot_stamp": boot_stamp,
        "started_epoch": time.time() - 10,
        "started_monotonic": time.monotonic() - 10,
        "time_budget_started_epoch": time.time() - 10,
        "time_budget_started_monotonic": time.monotonic() - 10,
        "deadline_epoch": time.time() + 12 * 60 * 60,
        "deadline_monotonic": time.monotonic() + 12 * 60 * 60,
        "last_observed_epoch": time.time() - 5,
        "system": {
            "filesystem_device": saved_root_device,
            "filesystem_volume_uuid": volume_uuid,
        },
        "resource_profile": "expanded",
    }
    expanded_limits = dict(production.RUN_LIMITS) | production.RESOURCE_PROFILES["expanded"]
    initial_report.update(
        {
            "limits": expanded_limits,
            "effective_resource_policy": production._resource_policy_record(
                "expanded", expanded_limits
            ),
            "resource_profile_history": [
                {
                    "profile": "expanded",
                    "limits": expanded_limits,
                    "policy": production._resource_policy_record("expanded", expanded_limits),
                    "selected_at": "2026-11-03T00:00:00Z",
                    "reason": "historical grant profile",
                }
            ],
        }
    )
    if not include_mode:
        initial_report.pop("mode")
    if saved_root_device != freeze_root_device:
        prior_reconciliation_sha = "9" * 64
        deadlines = {
            "started_epoch": initial_report["started_epoch"],
            "time_budget_started_epoch": initial_report["time_budget_started_epoch"],
            "deadline_epoch": initial_report["deadline_epoch"],
        }
        initial_report.update(
            {
                "original_system": {
                    "filesystem_device": freeze_root_device,
                    "filesystem_volume_uuid": volume_uuid,
                },
                "accounting_complete": True,
                "accounting_basis": "exact",
                "unfinished_reservation_ids": [],
                "consumption_state": "verified",
                "reboot_recovery_history": [
                    {
                        "previous_boot_stamp": "boot-origin",
                        "boot_stamp": boot_stamp,
                        "previous_filesystem_device": freeze_root_device,
                        "filesystem_device": saved_root_device,
                        "volume_uuid": volume_uuid,
                        "previous_active_process_group": 987_654,
                        "prior_process_group_ignored_after_boot_change": True,
                        "accounting_basis": "exact",
                        "accounting_complete": True,
                        "processing_worker_started": False,
                        "grant_started": False,
                        "wall_deadlines_preserved": deadlines,
                        "accounting_sha256": "a" * 64,
                        "session_before_sha256": "b" * 64,
                        "private_worker_reconciliation_sha256": prior_reconciliation_sha,
                    }
                ],
                "historical_reboot_reconciliation": {
                    "state": "complete",
                    "capture_id": identity["capture_id"],
                    "root_id": identity["root_id"],
                    "boot_stamp": boot_stamp,
                    "accounting_basis": "exact",
                    "processing_worker_started": False,
                    "grant_started": False,
                    "private_worker_reconciliation_sha256": prior_reconciliation_sha,
                },
            }
        )
    production._atomic_report(report_path, initial_report)
    report_path.chmod(0o600)
    session_report_sha256 = grants.sha256_bytes(report_path.read_bytes())

    runtime_manifest_sha256 = "3" * 64
    operational_source_sha256 = "4" * 64
    prior_attempts = {
        "attempt_outcome_sha256": "a" * 64,
        "failed_launch_sha256": "b" * 64,
        "audit_after_sha256": "c" * 64,
        "failed_session_sha256": identity["prior_session_sha256"],
    }
    prior_attempts["bundle_sha256"] = grants.sha256_json(
        {
            "attempt_outcome_sha256": {
                "path": "attempt.json",
                "sha256": prior_attempts["attempt_outcome_sha256"],
            },
            "failed_launch_sha256": {
                "path": "launch.json",
                "sha256": prior_attempts["failed_launch_sha256"],
            },
            "audit_after_sha256": {
                "path": "audit.jsonl",
                "sha256": prior_attempts["audit_after_sha256"],
            },
            "failed_session_sha256": {
                "path": "session.json",
                "sha256": prior_attempts["failed_session_sha256"],
            },
        }
    )
    frozen_identity = dict(
        identity,
        failed_attempts_sha256=prior_attempts["bundle_sha256"],
        runtime_sha256=grants.sha256_json(
            {
                "runtime_manifest_sha256": runtime_manifest_sha256,
                "operational_source_sha256": operational_source_sha256,
            }
        ),
    )
    template_identity = dict(
        frozen_identity,
        state_sha256=None,
        source_sha256=None,
        runtime_sha256=None,
    )
    control = {
        "version": 1,
        "state": "inert",
        "identity": template_identity,
        "prior_attempts": prior_attempts,
        "fresh_pins": {"source": None, "state": None, "runtime": None, "approval": None},
        "phase_seals": [],
        "authorized_census": {
            "population_report_path": str(artifacts / "population.json"),
            "population_report_sha256": "a" * 64,
            "receipt_index_path": str(artifacts / "receipt-index.json"),
            "receipt_index_sha256": "b" * 64,
            "capture_id": frozen_identity["capture_id"],
            "root_id": frozen_identity["root_id"],
            "capture_state_sha256": frozen_identity["state_sha256"],
            "candidate_pages": 3,
            "observations_by_endpoint": {"/series": 2, "/events": 3, "/historical/markets": 5},
            "nested_event_markets": 0,
            "exact_observations": 10,
        },
    }
    control_sha256 = grants.write_once_json(control_root / "control.json", control)
    freeze = {
        "version": 1,
        "control_sha256": control_sha256,
        "identity": frozen_identity,
        "state_sha256": frozen_identity["state_sha256"],
        "source_sha256": frozen_identity["source_sha256"],
        "runtime_sha256": frozen_identity["runtime_sha256"],
        "source_snapshot_sha256": "5" * 64,
        "receipt_evidence_sha256": "6" * 64,
        "scope_sha256": "7" * 64,
        "code_sha256": "8" * 64,
        "runtime_manifest_sha256": runtime_manifest_sha256,
        "operational_source_sha256": operational_source_sha256,
        "root_volume_uuid": volume_uuid,
        "root_device": freeze_root_device,
        "frozen_at_epoch": time.time(),
    }
    grants.write_once_json(control_root / "freeze.json", freeze)

    grant_id = uuid.uuid4().hex
    approval, latch, _old_launch, now_epoch, now_monotonic = _grant(
        frozen_identity,
        grant_id=grant_id,
        phase="readiness",
        phases=["readiness"],
    )
    approval["measurements"]["session_report_sha256"] = session_report_sha256
    baseline = {
        "database_bytes": grants.GIB,
        "temporary_bytes": 4 * grants.GIB,
        "root_bytes": 4 * grants.GIB,
        "aggregate_bytes": 5 * grants.GIB,
        "free_bytes": 200 * grants.GIB,
        "inventory_entries": 20,
        "peak_rss_bytes": grants.GIB,
    }
    measurements = {
        "version": 1,
        "grant_id": grant_id,
        "phases": approval["phases"],
        "identity": frozen_identity,
        "source_snapshot_sha256": freeze["source_snapshot_sha256"],
        "receipt_evidence_sha256": freeze["receipt_evidence_sha256"],
        "scope_sha256": freeze["scope_sha256"],
        "code_sha256": freeze["code_sha256"],
        "root_volume_uuid": volume_uuid,
        "root_device": saved_root_device,
        "session_report_sha256": session_report_sha256,
        "measured_at_epoch": approval["measurements"]["measured_at_epoch"],
        "forecast_seconds": approval["measurements"]["forecast_seconds"],
        "final_verification_seconds": approval["measurements"]["final_verification_seconds"],
        "projected_database_bytes": approval["measurements"]["projected_database_bytes"],
        "projected_temporary_bytes": approval["measurements"]["projected_temporary_bytes"],
        "projected_root_bytes": approval["measurements"]["projected_root_bytes"],
        "projected_aggregate_bytes": approval["measurements"]["projected_aggregate_bytes"],
        "projected_free_bytes": approval["measurements"]["projected_free_bytes"],
        "inventory_entries": approval["measurements"]["inventory_entries"],
        "projected_peak_rss_bytes": approval["measurements"]["projected_peak_rss_bytes"],
        "baseline": baseline,
    }
    measurement_bytes = grants.canonical_bytes(measurements) + b"\n"
    approval["measurements"]["report_sha256"] = grants.sha256_bytes(measurement_bytes)
    latch["approval_sha256"] = grants.sha256_json(approval)
    session_binding = {
        "version": 1,
        "grant_id": grant_id,
        "approval_sha256": grants.sha256_json(approval),
        "capture_id": frozen_identity["capture_id"],
        "session_id": frozen_identity["session_id"],
        "root_id": frozen_identity["root_id"],
        "stage_name": frozen_identity["stage_name"],
        "report_sha256": session_report_sha256,
        "report_path": str(report_path),
        "boot_stamp": boot_stamp,
        "root_volume_uuid": volume_uuid,
        "root_device": saved_root_device,
    }
    launch = grants.create_launch_record(
        approval,
        latch,
        now_epoch=now_epoch,
        now_monotonic=now_monotonic,
        boot_stamp=boot_stamp,
        launch_id=uuid.uuid4().hex,
        session_binding_sha256=grants.sha256_json(session_binding),
    )
    grant_root = control_root / "grants" / grant_id
    grants.write_once_json(grant_root / "approval.json", approval)
    grants.write_once_json(grant_root / "latch.json", latch)
    grants.write_once_json(grant_root / "session-binding.json", session_binding)
    launch_sha256 = grants.write_once_json(grant_root / "launch.json", launch)
    grants.write_once_json(grant_root / "measurements.json", measurements)

    token = "e" * 32
    worker_pid = 987_654
    controller_pid = 987_655
    progress_path = logs / f"invocation-{token}" / "historical-readiness-prepare.storage.json"
    progress_path.parent.mkdir(mode=0o700)
    target_interval = int(production._effective_limits()["storage_sample_seconds"])
    production._atomic_report(
        progress_path,
        {
            "version": 1,
            "invocation_token": token,
            "sample_count": 1,
            "target_interval_seconds": target_interval,
        },
    )
    report = dict(initial_report)
    report.update(
        {
            "phase": "uncertain",
            "error": "simulated unconfirmed worker stop",
            "active_process_group": worker_pid,
            "storage_supervision_invocation": token,
            "storage_supervision_file": str(progress_path),
            "private_historical_grant": {
                "version": 1,
                "grant_id": grant_id,
                "launch_sha256": launch_sha256,
                "launch_id": launch["launch_id"],
                "control_root": str(control_root),
                "phases": approval["phases"],
                "phase": "readiness",
                "substep": "prepare",
                "controller_pid": controller_pid,
                "controller_pgid": controller_pid,
                "worker_pid": worker_pid,
                "worker_pgid": worker_pid,
                "storage_invocation_token": token,
                "storage_progress_path": str(progress_path),
                "state": "stopped",
                "reason": "simulated unconfirmed worker stop",
            },
        }
    )
    production._atomic_report(report_path, report)
    return {
        "control_root": control_root,
        "directory": directory,
        "report_path": report_path,
        "report": report,
        "identity": frozen_identity,
        "freeze": freeze,
        "grant_id": grant_id,
        "launch": launch,
        "launch_sha256": launch_sha256,
        "volume_uuid": volume_uuid,
        "controller_pid": controller_pid,
        "worker_pid": worker_pid,
        "progress_path": progress_path,
    }


def _write_prelaunch_grant(attempt, *, root_device, phases=("source",)):
    grant_id = uuid.uuid4().hex
    approval, latch, _launch, _epoch, _monotonic = _grant(
        attempt["identity"],
        grant_id=grant_id,
        phase=phases[0],
        phases=list(phases),
    )
    report_sha256 = grants.sha256_bytes(
        production._historical_session_report_bytes(attempt["report_path"])
    )
    approval["approved_at_epoch"] = latch["created_at_epoch"]
    approval["measurements"]["session_report_sha256"] = report_sha256
    measurement = {
        "version": 1,
        "grant_id": grant_id,
        "phases": list(phases),
        "identity": attempt["identity"],
        "source_snapshot_sha256": attempt["freeze"]["source_snapshot_sha256"],
        "receipt_evidence_sha256": attempt["freeze"]["receipt_evidence_sha256"],
        "scope_sha256": attempt["freeze"]["scope_sha256"],
        "code_sha256": attempt["freeze"]["code_sha256"],
        "root_volume_uuid": attempt["freeze"]["root_volume_uuid"],
        "root_device": root_device,
        "session_report_sha256": report_sha256,
        "measured_at_epoch": approval["measurements"]["measured_at_epoch"],
        "forecast_seconds": approval["measurements"]["forecast_seconds"],
        "final_verification_seconds": approval["measurements"]["final_verification_seconds"],
        "projected_database_bytes": approval["measurements"]["projected_database_bytes"],
        "projected_temporary_bytes": approval["measurements"]["projected_temporary_bytes"],
        "projected_root_bytes": approval["measurements"]["projected_root_bytes"],
        "projected_aggregate_bytes": approval["measurements"]["projected_aggregate_bytes"],
        "projected_free_bytes": approval["measurements"]["projected_free_bytes"],
        "inventory_entries": approval["measurements"]["inventory_entries"],
        "projected_peak_rss_bytes": approval["measurements"]["projected_peak_rss_bytes"],
        "baseline": {
            "database_bytes": grants.GIB,
            "temporary_bytes": 4 * grants.GIB,
            "root_bytes": 4 * grants.GIB,
            "aggregate_bytes": 5 * grants.GIB,
            "free_bytes": 200 * grants.GIB,
            "inventory_entries": 20,
            "peak_rss_bytes": grants.GIB,
        },
    }
    raw = grants.canonical_bytes(measurement) + b"\n"
    approval["measurements"]["report_sha256"] = grants.sha256_bytes(raw)
    latch["approval_sha256"] = grants.sha256_json(approval)
    grant_root = attempt["control_root"] / "grants" / grant_id
    grants.write_once_json(grant_root / "approval.json", approval)
    grants.write_once_json(grant_root / "latch.json", latch)
    grants.write_once_json(grant_root / "measurements.json", measurement)
    return grant_id, approval


def _launch_test_grant(attempt, grant_id, approval, *, root_device):
    report = production._strict_json(attempt["report_path"])
    report_sha256 = grants.sha256_bytes(
        production._historical_session_report_bytes(attempt["report_path"])
    )
    session_binding = {
        "version": 1,
        "grant_id": grant_id,
        "approval_sha256": grants.sha256_json(approval),
        "capture_id": attempt["identity"]["capture_id"],
        "session_id": attempt["identity"]["session_id"],
        "root_id": attempt["identity"]["root_id"],
        "stage_name": attempt["identity"]["stage_name"],
        "report_sha256": report_sha256,
        "report_path": str(attempt["report_path"]),
        "boot_stamp": report["boot_stamp"],
        "root_volume_uuid": attempt["freeze"]["root_volume_uuid"],
        "root_device": root_device,
    }
    launch, launch_sha256 = grants.issue_launch(
        attempt["control_root"],
        expected_identity=attempt["identity"],
        requested_phase=approval["phase"],
        grant_id=grant_id,
        now_epoch=time.time(),
        now_monotonic=time.monotonic(),
        boot_stamp=report["boot_stamp"],
        session_binding=session_binding,
    )
    report["private_historical_grant"] = {
        "version": 1,
        "grant_id": grant_id,
        "launch_sha256": launch_sha256,
        "launch_id": launch["launch_id"],
        "control_root": str(attempt["control_root"]),
        "phases": approval["phases"],
        "phase": approval["phase"],
        "substep": None,
        "state": "launched",
    }
    production._atomic_report(attempt["report_path"], report)
    return launch_sha256, session_binding


def _seal(phase, identity, previous_sha256=None, *, outputs=None):
    defaults = {
        "source": {
            "startup_maintenance_sha256": "8" * 64,
            "source_snapshot_sha256": "9" * 64,
            "capture_state_sha256": "a" * 64,
            "receipt_evidence_sha256": "b" * 64,
            "source_sha256": "c" * 64,
            "source_wal_sha256": None,
            "receipt_count": 4,
            "population_report_sha256": "f" * 64,
            "receipt_index_sha256": "1" * 64,
            "exact_observations": 10,
            "observations_by_endpoint": {
                "/series": 2,
                "/events": 3,
                "/historical/markets": 5,
            },
            "scope_sha256": "d" * 64,
            "code_sha256": "e" * 64,
        },
        "readiness": {
            "readiness_sha256": "a" * 64,
            "verification_sha256": "9" * 64,
            "capture_state_sha256": "8" * 64,
            "receipt_evidence_sha256": "b" * 64,
            "source_sha256": "7" * 64,
            "source_wal_sha256": None,
            "candidate_sha256": "c" * 64,
            "observation_count": 10,
            "membership_count": 3,
            "eligibility_count": 2,
            "scope_sha256": "d" * 64,
            "code_sha256": "e" * 64,
        },
        "adoption": {
            "adoption_sha256": "a" * 64,
            "verification_receipt_sha256": "b" * 64,
            "candidate_sha256": "c" * 64,
            "source_retired": False,
            "retirement_receipt_sha256": None,
        },
        "models": {
            "build_receipt_sha256": "a" * 64,
            "warehouse_sha256": "a" * 64,
            "dbt_artifacts_sha256": "b" * 64,
            "source_retirement_sha256": None,
        },
        "certified-build": {
            "build_sha256": "a" * 64,
            "build_manifest_sha256": "b" * 64,
            "certified_receipt_sha256": "c" * 64,
        },
        "export": {
            "export_policy_sha256": "a" * 64,
            "parquet_inventory_sha256": "b" * 64,
            "release_metadata_sha256": "c" * 64,
        },
        "verified-publication": {
            "release_id": "a" * 64,
            "release_manifest_sha256": "b" * 64,
            "publication_pointer_sha256": "c" * 64,
        },
        "independent-verification": {
            "verification_receipt_sha256": "a" * 64,
            "release_id": "b" * 64,
            "release_manifest_sha256": "c" * 64,
        },
        "activation": {
            "activation_audit_sha256": "a" * 64,
            "production_database_sha256": "b" * 64,
            "release_id": "c" * 64,
            "activation_readback_sha256": "d" * 64,
            "relation_rows": {
                "series": 1,
                "events": 2,
                "markets": 3,
                "relationships": 4,
                "combo_legs": 5,
                "observation_history": 10,
            },
        },
    }
    return {
        "version": 1,
        "phase": phase,
        "bindings": identity,
        "previous_sha256": previous_sha256,
        "outputs": outputs or defaults[phase],
        "completed_at_epoch": time.time(),
        "grant_id": "f" * 32,
    }


def test_grant_has_fresh_approval_latch_and_new_bounded_clocks(identity):
    approval, latch, launch, now_epoch, now_monotonic = _grant(identity)

    result = grants.validate_grant_bundle(
        approval,
        latch,
        launch,
        expected_bindings=identity,
        now_epoch=now_epoch,
        now_monotonic=now_monotonic,
        boot_stamp="boot-current",
    )

    assert result["deadline_epoch"] == launch["started_epoch"] + 24 * 60 * 60
    assert result["workflow_deadline_monotonic"] == launch["deadline_monotonic"] - 2 * 60 * 60
    assert result["started_epoch"] > 0
    assert result["started_monotonic"] > 0


@pytest.mark.parametrize("phases", [["source"], ["source", "readiness", "adoption", "models"]])
def test_source_grant_projection_remains_at_processing_admission(identity, phases):
    approval, latch, launch, now_epoch, now_monotonic = _grant(
        identity, phase="source", phases=phases
    )
    grants.validate_grant_bundle(
        approval,
        latch,
        launch,
        expected_bindings=identity,
        now_epoch=now_epoch,
        now_monotonic=now_monotonic,
        boot_stamp=launch["boot_stamp"],
    )
    approval["measurements"]["projected_temporary_bytes"] = 145 * grants.GIB
    approval_sha = grants.sha256_json(approval)
    latch["approval_sha256"] = approval_sha
    launch["approval_sha256"] = approval_sha
    launch["latch_sha256"] = grants.sha256_json(latch)
    with pytest.raises(grants.ControlError, match="fixed admission ceilings"):
        grants.validate_grant_bundle(
            approval,
            latch,
            launch,
            expected_bindings=identity,
            now_epoch=now_epoch,
            now_monotonic=now_monotonic,
            boot_stamp=launch["boot_stamp"],
        )


def test_measurement_allows_over_admission_baseline_only_for_source_maintenance(
    artifacts, identity, monkeypatch
):
    monkeypatch.setitem(sys.modules, "historical_grants", grants)
    approval, _latch, _launch, _epoch, _monotonic = _grant(
        identity, phase="source", phases=["source"]
    )
    admission = grants.expected_resource_admission()
    freeze = {
        "source_snapshot_sha256": "5" * 64,
        "receipt_evidence_sha256": "6" * 64,
        "scope_sha256": "7" * 64,
        "code_sha256": "8" * 64,
        "root_volume_uuid": "ACAD66E7-42DA-426B-B20D-FB87A453396B",
        "root_device": 42,
    }
    recorded = {
        "database_bytes": grants.GIB,
        "temporary_bytes": 168 * grants.GIB,
        "root_bytes": 4 * grants.GIB,
        "aggregate_bytes": 5 * grants.GIB,
        "free_bytes": 200 * grants.GIB,
        "inventory_entries": 20,
        "peak_rss_bytes": grants.GIB,
    }
    projected = dict(approval["measurements"])
    projected["projected_temporary_bytes"] = admission["storage_temporary_admission_bytes"]
    approval["measurements"] = projected
    artifact = {
        "version": 1,
        "grant_id": approval["grant_id"],
        "phases": approval["phases"],
        "identity": identity,
        "source_snapshot_sha256": freeze["source_snapshot_sha256"],
        "receipt_evidence_sha256": freeze["receipt_evidence_sha256"],
        "scope_sha256": freeze["scope_sha256"],
        "code_sha256": freeze["code_sha256"],
        "root_volume_uuid": freeze["root_volume_uuid"],
        "root_device": freeze["root_device"],
        "session_report_sha256": projected["session_report_sha256"],
        "measured_at_epoch": projected["measured_at_epoch"],
        "forecast_seconds": projected["forecast_seconds"],
        "final_verification_seconds": projected["final_verification_seconds"],
        "projected_database_bytes": projected["projected_database_bytes"],
        "projected_temporary_bytes": projected["projected_temporary_bytes"],
        "projected_root_bytes": projected["projected_root_bytes"],
        "projected_aggregate_bytes": projected["projected_aggregate_bytes"],
        "projected_free_bytes": projected["projected_free_bytes"],
        "inventory_entries": projected["inventory_entries"],
        "projected_peak_rss_bytes": projected["projected_peak_rss_bytes"],
        "baseline": recorded,
    }
    raw = grants.canonical_bytes(artifact) + b"\n"
    projected["report_sha256"] = grants.sha256_bytes(raw)
    grants.write_once_json(
        artifacts / "grants" / approval["grant_id"] / "measurements.json", artifact
    )
    measurement_path = artifacts / "grants" / approval["grant_id"] / "measurements.json"
    summary = approval["measurements"]

    def validate(current, *, allow, session_device=42):
        return production._historical_measurement_artifact(
            artifacts,
            approval["grant_id"],
            approval,
            identity,
            freeze,
            session_report_sha256=summary["session_report_sha256"],
            session_root_device=session_device,
            baseline=current,
            require_fresh=False,
            allow_initial_maintenance_overage=allow,
        )

    assert measurement_path.is_file()
    with pytest.raises(production.RunnerError, match="temporary storage"):
        validate(recorded, allow=False)
    assert (
        validate(recorded, allow=True)["projected_temporary_bytes"]
        == admission["storage_temporary_admission_bytes"]
    )

    after_cleanup = recorded | {"temporary_bytes": admission["storage_temporary_admission_bytes"]}
    assert (
        validate(after_cleanup, allow=False)["projected_temporary_bytes"]
        == after_cleanup["temporary_bytes"]
    )
    over_after_cleanup = after_cleanup | {"temporary_bytes": after_cleanup["temporary_bytes"] + 1}
    with pytest.raises(production.RunnerError, match="temporary storage"):
        validate(over_after_cleanup, allow=False)
    with pytest.raises(production.RunnerError, match="temporary storage"):
        validate(recorded | {"temporary_bytes": 193 * grants.GIB}, allow=True)
    with pytest.raises(production.RunnerError, match="measured resource artifact"):
        validate(after_cleanup, allow=False, session_device=43)


def _fresh_measurement_case(
    artifacts,
    identity,
    monkeypatch,
    *,
    measured_at=None,
    recorded_overrides=None,
    artifact_identity=None,
):
    monkeypatch.setitem(sys.modules, "historical_grants", grants)
    measured_at = time.time() - 30 if measured_at is None else measured_at
    approval, _latch, _launch, _epoch, _monotonic = _grant(identity)
    freeze = {
        "source_snapshot_sha256": "5" * 64,
        "receipt_evidence_sha256": "6" * 64,
        "scope_sha256": "7" * 64,
        "code_sha256": "8" * 64,
        "root_volume_uuid": "ACAD66E7-42DA-426B-B20D-FB87A453396B",
        "root_device": 42,
    }
    recorded = {
        "database_bytes": grants.GIB,
        "temporary_bytes": 3 * grants.GIB,
        "root_bytes": 4 * grants.GIB,
        "aggregate_bytes": 5 * grants.GIB,
        "free_bytes": 200 * grants.GIB,
        "inventory_entries": 20,
        "peak_rss_bytes": grants.GIB,
    }
    if recorded_overrides:
        recorded.update(recorded_overrides)
    projected = approval["measurements"]
    artifact = {
        "version": 1,
        "grant_id": approval["grant_id"],
        "phases": approval["phases"],
        "identity": identity if artifact_identity is None else artifact_identity,
        "source_snapshot_sha256": freeze["source_snapshot_sha256"],
        "receipt_evidence_sha256": freeze["receipt_evidence_sha256"],
        "scope_sha256": freeze["scope_sha256"],
        "code_sha256": freeze["code_sha256"],
        "root_volume_uuid": freeze["root_volume_uuid"],
        "root_device": freeze["root_device"],
        "session_report_sha256": projected["session_report_sha256"],
        "measured_at_epoch": measured_at,
        "forecast_seconds": projected["forecast_seconds"],
        "final_verification_seconds": projected["final_verification_seconds"],
        "projected_database_bytes": projected["projected_database_bytes"],
        "projected_temporary_bytes": projected["projected_temporary_bytes"],
        "projected_root_bytes": projected["projected_root_bytes"],
        "projected_aggregate_bytes": projected["projected_aggregate_bytes"],
        "projected_free_bytes": projected["projected_free_bytes"],
        "inventory_entries": projected["inventory_entries"],
        "projected_peak_rss_bytes": projected["projected_peak_rss_bytes"],
        "baseline": recorded,
    }
    raw = grants.canonical_bytes(artifact) + b"\n"
    approval["approved_at_epoch"] = measured_at + 5
    approval["measurements"] = {
        "report_sha256": grants.sha256_bytes(raw),
        "session_report_sha256": artifact["session_report_sha256"],
        "measured_at_epoch": measured_at,
        "forecast_seconds": artifact["forecast_seconds"],
        "final_verification_seconds": artifact["final_verification_seconds"],
        "projected_database_bytes": artifact["projected_database_bytes"],
        "projected_temporary_bytes": artifact["projected_temporary_bytes"],
        "projected_root_bytes": artifact["projected_root_bytes"],
        "projected_aggregate_bytes": artifact["projected_aggregate_bytes"],
        "projected_free_bytes": artifact["projected_free_bytes"],
        "inventory_entries": artifact["inventory_entries"],
        "projected_peak_rss_bytes": artifact["projected_peak_rss_bytes"],
    }
    grants.write_once_json(
        artifacts / "grants" / approval["grant_id"] / "measurements.json", artifact
    )
    return approval, freeze, recorded


def _validate_fresh_measurement_case(
    artifacts, identity, approval, freeze, baseline, *, require_fresh
):
    return production._historical_measurement_artifact(
        artifacts,
        approval["grant_id"],
        approval,
        identity,
        freeze,
        session_report_sha256=approval["measurements"]["session_report_sha256"],
        session_root_device=42,
        baseline=baseline,
        require_fresh=require_fresh,
    )


def test_fresh_measurement_allows_free_space_drift_inside_approved_headroom(
    artifacts, identity, monkeypatch
):
    approval, freeze, recorded = _fresh_measurement_case(artifacts, identity, monkeypatch)
    decrease = recorded | {"free_bytes": recorded["free_bytes"] - 4096}
    increase = recorded | {"free_bytes": recorded["free_bytes"] + 4096}
    reserve_boundary = recorded | {
        "free_bytes": grants.expected_resource_admission()["minimum_free_bytes"]
        + recorded["free_bytes"]
        - approval["measurements"]["projected_free_bytes"]
    }

    assert (
        _validate_fresh_measurement_case(
            artifacts, identity, approval, freeze, decrease, require_fresh=True
        )["grant_id"]
        == approval["grant_id"]
    )
    assert (
        _validate_fresh_measurement_case(
            artifacts, identity, approval, freeze, increase, require_fresh=True
        )["grant_id"]
        == approval["grant_id"]
    )
    assert (
        _validate_fresh_measurement_case(
            artifacts, identity, approval, freeze, reserve_boundary, require_fresh=True
        )["grant_id"]
        == approval["grant_id"]
    )


def test_fresh_measurement_rejects_free_space_drop_consuming_projected_growth(
    artifacts, identity, monkeypatch
):
    approval, freeze, recorded = _fresh_measurement_case(artifacts, identity, monkeypatch)
    current_free = 189 * grants.GIB
    assert current_free > grants.expected_resource_admission()["minimum_free_bytes"]
    current = recorded | {"free_bytes": current_free}

    with pytest.raises(production.RunnerError, match="approved growth reserve"):
        _validate_fresh_measurement_case(
            artifacts, identity, approval, freeze, current, require_fresh=True
        )


def test_fresh_measurement_keeps_other_baseline_fields_exact(artifacts, identity, monkeypatch):
    approval, freeze, recorded = _fresh_measurement_case(artifacts, identity, monkeypatch)
    current = recorded | {"database_bytes": recorded["database_bytes"] + 1}

    with pytest.raises(production.RunnerError, match="differs from approved baseline"):
        _validate_fresh_measurement_case(
            artifacts, identity, approval, freeze, current, require_fresh=True
        )


def test_fresh_measurement_rejects_binding_and_stale_timestamp_changes(
    artifacts, identity, monkeypatch
):
    wrong_identity = identity | {"root_id": "9" * 32}
    approval, freeze, recorded = _fresh_measurement_case(
        artifacts, identity, monkeypatch, artifact_identity=wrong_identity
    )
    with pytest.raises(production.RunnerError, match="measured resource artifact"):
        _validate_fresh_measurement_case(
            artifacts, identity, approval, freeze, recorded, require_fresh=True
        )

    stale_artifacts = artifacts / "stale"
    stale_artifacts.mkdir(mode=0o700)
    stale_approval, stale_freeze, stale_recorded = _fresh_measurement_case(
        stale_artifacts,
        identity,
        monkeypatch,
        measured_at=time.time() - 16 * 60,
    )
    with pytest.raises(production.RunnerError, match="baseline is stale at launch"):
        _validate_fresh_measurement_case(
            stale_artifacts,
            identity,
            stale_approval,
            stale_freeze,
            stale_recorded,
            require_fresh=True,
        )


@pytest.mark.parametrize(
    ("location", "bad_value"),
    [
        ("recorded", True),
        ("recorded", 1.0),
        ("recorded", -1),
        ("current", True),
        ("current", 1.0),
        ("current", -1),
    ],
)
def test_measurement_rejects_noninteger_or_negative_free_baseline(
    artifacts, identity, monkeypatch, location, bad_value
):
    overrides = {"free_bytes": bad_value} if location == "recorded" else None
    approval, freeze, recorded = _fresh_measurement_case(
        artifacts, identity, monkeypatch, recorded_overrides=overrides
    )
    current = recorded if location == "recorded" else recorded | {"free_bytes": bad_value}

    with pytest.raises(production.RunnerError, match="measurement baseline is invalid"):
        _validate_fresh_measurement_case(
            artifacts, identity, approval, freeze, current, require_fresh=True
        )


@pytest.mark.parametrize("mutation", ["missing", "extra"])
def test_measurement_rejects_malformed_current_baseline_shape(
    artifacts, identity, monkeypatch, mutation
):
    approval, freeze, recorded = _fresh_measurement_case(artifacts, identity, monkeypatch)
    current = dict(recorded)
    if mutation == "missing":
        current.pop("peak_rss_bytes")
    else:
        current["unexpected"] = 1

    with pytest.raises(
        production.RunnerError, match="current historical measurement baseline is invalid"
    ):
        _validate_fresh_measurement_case(
            artifacts, identity, approval, freeze, current, require_fresh=True
        )


def test_nonfresh_measurement_does_not_recharge_approved_free_growth(
    artifacts, identity, monkeypatch
):
    approval, freeze, recorded = _fresh_measurement_case(artifacts, identity, monkeypatch)
    postprogress = recorded | {"free_bytes": 170 * grants.GIB}

    assert (
        _validate_fresh_measurement_case(
            artifacts, identity, approval, freeze, postprogress, require_fresh=False
        )["grant_id"]
        == approval["grant_id"]
    )


def test_launched_grant_remains_valid_after_six_hours_but_expires_at_deadline(identity):
    approval, latch, launch, now_epoch, now_monotonic = _grant(identity)
    within = grants.validate_grant_bundle(
        approval,
        latch,
        launch,
        expected_bindings=identity,
        now_epoch=now_epoch + 6 * 60 * 60,
        now_monotonic=now_monotonic + 6 * 60 * 60,
        boot_stamp="boot-current",
    )
    assert within["grant_id"] == approval["grant_id"]
    with pytest.raises(grants.ControlError, match="deadline"):
        grants.validate_grant_bundle(
            approval,
            latch,
            launch,
            expected_bindings=identity,
            now_epoch=launch["deadline_epoch"] + 1,
            now_monotonic=launch["deadline_monotonic"] + 1,
            boot_stamp="boot-current",
        )


def test_old_approval_is_rejected_when_consumed(identity):
    approval, latch, launch, now_epoch, now_monotonic = _grant(identity)
    approval["approved_at_epoch"] = now_epoch - grants.FRESH_APPROVAL_SECONDS - 1
    latch["approval_sha256"] = grants.sha256_json(approval)
    launch["approval_sha256"] = grants.sha256_json(approval)
    launch["latch_sha256"] = grants.sha256_json(latch)
    with pytest.raises(grants.ControlError, match="launch deadline"):
        grants.validate_grant_bundle(
            approval,
            latch,
            launch,
            expected_bindings=identity,
            now_epoch=now_epoch,
            now_monotonic=now_monotonic,
            boot_stamp="boot-current",
        )


@pytest.mark.parametrize(
    "changes, expected_error",
    [
        ({"duration_seconds": 24 * 60 * 60 + 1}, "duration"),
        ({"forecast_seconds": 17 * 60 * 60}, "forecast"),
        ({"resource_admission": {"profile": "default"}}, "resource admission"),
        ({"freeze": {"engineering": True, "runtime": False, "controls": True}}, "frozen"),
    ],
)
def test_grant_rejects_unbounded_or_unfrozen_admission(identity, changes, expected_error):
    approval, latch, launch, now_epoch, now_monotonic = _grant(identity, **changes)
    # The latch is refreshed when a new immutable approval is constructed.
    latch["approval_sha256"] = grants.sha256_json(approval)
    launch["approval_sha256"] = grants.sha256_json(approval)
    launch["latch_sha256"] = grants.sha256_json(latch)
    with pytest.raises(grants.ControlError, match=expected_error):
        grants.validate_grant_bundle(
            approval,
            latch,
            launch,
            expected_bindings=identity,
            now_epoch=now_epoch,
            now_monotonic=now_monotonic,
            boot_stamp="boot-current",
        )


def test_grant_rejects_old_boot_stale_approval_and_old_identity(identity):
    approval, latch, launch, now_epoch, now_monotonic = _grant(identity)
    with pytest.raises(grants.ControlError, match="another boot"):
        grants.validate_grant_bundle(
            approval,
            latch,
            launch,
            expected_bindings=identity,
            now_epoch=now_epoch,
            now_monotonic=now_monotonic,
            boot_stamp="boot-after-restart",
        )
    with pytest.raises(grants.ControlError, match="deadline"):
        grants.validate_grant_bundle(
            approval,
            latch,
            launch,
            expected_bindings=identity,
            now_epoch=launch["deadline_epoch"] + 1,
            now_monotonic=launch["deadline_monotonic"] + 1,
            boot_stamp="boot-current",
        )
    changed = dict(identity, prior_session_sha256="0" * 64)
    with pytest.raises(grants.ControlError, match="different capture or failed attempt"):
        grants.validate_grant_bundle(
            approval,
            latch,
            launch,
            expected_bindings=changed,
            now_epoch=now_epoch,
            now_monotonic=now_monotonic,
            boot_stamp="boot-current",
        )


def test_independent_historical_verification_preserves_manifest_source_completeness(
    artifacts, identity, monkeypatch
):
    manifest = {
        "capture_id": identity["capture_id"],
        "source_release_id": "a" * 64,
        "coverage_scope": "historical",
        "historical_contract": "oddsfox.kalshi.historical.v1",
        "source_capture_complete": False,
    }
    current = manifest | {
        "output": str(artifacts / "release"),
        "manifest_sha256": "b" * 64,
    }
    authority = {
        "identity": identity,
        "directory": artifacts,
        "operator": artifacts / "operator",
        "report_path": artifacts / "session.json",
        "report": {},
        "grant": {
            "deadline_epoch": time.time() + 3600,
            "deadline_monotonic": time.monotonic() + 3600,
        },
    }
    monkeypatch.setattr(
        production, "_historical_published_release", lambda _authority: (current, manifest)
    )
    monkeypatch.setattr(production, "_prepare_run_paths", lambda *_args: {})

    def verify(standalone):
        monkeypatch.setattr(production, "_release_verify", lambda *_args, **_kwargs: standalone)
        return production._historical_release_verification(authority, grant_id_for_receipt=None)

    verified = verify(dict(manifest))
    assert verified["receipt"]["source_capture_complete"] is False
    assert verified["receipt"]["scope_complete"] is True

    with pytest.raises(production.RunnerError, match="independent historical publication"):
        verify(manifest | {"source_capture_complete": 0})
    with pytest.raises(production.RunnerError, match="independent historical publication"):
        verify(manifest | {"source_capture_complete": True})
    with pytest.raises(production.RunnerError, match="independent historical publication"):
        verify(manifest | {"source_release_id": "c" * 64})
    with pytest.raises(production.RunnerError, match="independent historical publication"):
        verify(manifest | {"coverage_scope": "catalogue"})


@pytest.mark.parametrize(
    "pointer_changes, expected_error",
    [
        ({}, None),
        ({"source_release_id": "c" * 64}, "manifest does not match its current pointer"),
        ({"manifest_sha256": "d" * 64}, "manifest does not match its current pointer"),
        ({"manifest_sha256": 0}, "manifest does not match its current pointer"),
    ],
)
def test_historical_published_release_binds_actual_pointer_bytes(
    artifacts, identity, monkeypatch, pointer_changes, expected_error
):
    import hashlib

    from kalshi_events import publish

    class Store:
        def __init__(self, root):
            self.root = root

        def path(self, relative):
            return self.root / relative

    store = Store(artifacts / "operator")
    release_id = "a" * 64
    release_dir = store.path(f"releases/{release_id}")
    release_dir.mkdir(parents=True)
    manifest = {
        "source_release_id": release_id,
        "capture_id": identity["capture_id"],
        "contract": "oddsfox.kalshi.historical.v1",
        "historical_export_policy": "historical-arrow-external-merge-v1",
    }
    manifest_raw = (json.dumps(manifest, sort_keys=True) + "\n").encode()
    (release_dir / "manifest.json").write_bytes(manifest_raw)
    pointer = {
        "source_release_id": release_id,
        "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
    }
    pointer.update(pointer_changes)
    pointer_raw = (json.dumps(pointer, sort_keys=True) + "\n").encode()
    store.path("current-historical.json").write_bytes(pointer_raw)
    monkeypatch.setattr(
        publish,
        "current",
        lambda _store, *, catalogue_version: {
            "output": str(release_dir),
            "source_release_id": release_id,
            "capture_id": identity["capture_id"],
        },
    )

    authority = {"store": store, "identity": identity}
    if pointer_changes:
        with pytest.raises(production.RunnerError, match=expected_error):
            production._historical_published_release(authority)
        return

    pointer["manifest_sha256"] = hashlib.sha256(manifest_raw).hexdigest()
    pointer_raw = (json.dumps(pointer, sort_keys=True) + "\n").encode()
    store.path("current-historical.json").write_bytes(pointer_raw)
    current, checked_manifest = production._historical_published_release(authority)
    assert checked_manifest == manifest
    assert current["manifest_sha256"] == hashlib.sha256(manifest_raw).hexdigest()
    assert current["pointer_sha256"] == hashlib.sha256(pointer_raw).hexdigest()


@pytest.mark.parametrize("phase", ["adoption", "models"])
def test_source_retirement_requires_the_exact_policy(identity, phase):
    approval, latch, launch, now_epoch, now_monotonic = _grant(
        identity,
        phase=phase,
        phases=[phase],
        retire_source=True,
        retirement_policy="coexistence-capacity-relief-approved-v1",
    )
    latch["approval_sha256"] = grants.sha256_json(approval)
    launch["approval_sha256"] = grants.sha256_json(approval)
    launch["latch_sha256"] = grants.sha256_json(latch)
    assert (
        grants.validate_grant_bundle(
            approval,
            latch,
            launch,
            expected_bindings=identity,
            now_epoch=now_epoch,
            now_monotonic=now_monotonic,
            boot_stamp="boot-current",
        )["retire_source"]
        is True
    )
    approval["retirement_policy"] = "anything-else"
    latch["approval_sha256"] = grants.sha256_json(approval)
    launch["approval_sha256"] = grants.sha256_json(approval)
    launch["latch_sha256"] = grants.sha256_json(latch)
    with pytest.raises(grants.ControlError, match="explicit approved policy"):
        grants.validate_grant_bundle(
            approval,
            latch,
            launch,
            expected_bindings=identity,
            now_epoch=now_epoch,
            now_monotonic=now_monotonic,
            boot_stamp="boot-current",
        )


def test_adoption_retry_cannot_seal_prior_retirement_under_new_false_grant():
    with pytest.raises(grants.ControlError, match="differs from its linked grant"):
        grants.validate_adoption_retirement({"retire_source": False}, True)
    grants.validate_adoption_retirement({"retire_source": True}, True)
    grants.validate_adoption_retirement({"retire_source": False}, False)


@pytest.mark.parametrize(
    "field, value",
    [
        ("projected_database_bytes", 97 * grants.GIB),
        ("projected_temporary_bytes", 145 * grants.GIB),
        ("projected_root_bytes", 351 * grants.GIB),
        ("projected_aggregate_bytes", 401 * grants.GIB),
        ("projected_free_bytes", 149 * grants.GIB),
        ("inventory_entries", 65_537),
        ("projected_peak_rss_bytes", 25 * grants.GIB),
    ],
)
def test_launch_requires_measured_inventory_below_fixed_admission(identity, field, value):
    approval, latch, launch, now_epoch, now_monotonic = _grant(identity)
    approval["measurements"][field] = value
    latch["approval_sha256"] = grants.sha256_json(approval)
    launch["approval_sha256"] = grants.sha256_json(approval)
    launch["latch_sha256"] = grants.sha256_json(latch)
    with pytest.raises(grants.ControlError, match="measured resource forecast"):
        grants.validate_grant_bundle(
            approval,
            latch,
            launch,
            expected_bindings=identity,
            now_epoch=now_epoch,
            now_monotonic=now_monotonic,
            boot_stamp="boot-current",
        )


def test_phase_chain_rejects_skips_changed_bindings_and_reused_audit(identity):
    source = _seal("source", identity)
    source_hash = grants.validate_phase_seal(
        source,
        expected_phase="source",
        expected_bindings=identity,
        previous_sha256=None,
    )
    readiness = _seal("readiness", identity, source_hash)
    current, digest = grants.validate_phase_chain([source, readiness], expected_bindings=identity)
    assert current == "readiness"
    assert digest == grants.sha256_json(readiness)
    assert grants.next_phase(current) == "adoption"
    with pytest.raises(grants.ControlError, match="out of order"):
        grants.validate_phase_seal(
            _seal("activation", identity),
            expected_phase="adoption",
            expected_bindings=identity,
            previous_sha256=digest,
        )
    activation = _seal("activation", identity, "e" * 64)
    audit = activation["outputs"]["activation_audit_sha256"]
    with pytest.raises(grants.ControlError, match="already used"):
        grants.validate_phase_seal(
            activation,
            expected_phase="activation",
            expected_bindings=identity,
            previous_sha256="e" * 64,
            prior_activation_audits={audit},
        )


def test_phase_seals_are_write_once_and_ordered_under_private_lock(artifacts, identity):
    approval, latch, launch, now_epoch, _now_monotonic = _grant(
        identity,
        phase="source",
        phases=["source", "readiness"],
    )
    grants.write_once_json(artifacts / "grants" / approval["grant_id"] / "approval.json", approval)
    grants.write_once_json(artifacts / "grants" / approval["grant_id"] / "latch.json", latch)
    binding = {
        "version": 1,
        "grant_id": approval["grant_id"],
        "approval_sha256": grants.sha256_json(approval),
        "capture_id": identity["capture_id"],
        "session_id": identity["session_id"],
        "root_id": identity["root_id"],
        "stage_name": identity["stage_name"],
        "report_sha256": approval["measurements"]["session_report_sha256"],
        "report_path": str(artifacts / "runs" / identity["session_id"] / "session.json"),
        "boot_stamp": launch["boot_stamp"],
        "root_volume_uuid": "ACAD66E7-42DA-426B-B20D-FB87A453396B",
        "root_device": 42,
    }
    grants.write_once_json(
        artifacts / "grants" / approval["grant_id"] / "session-binding.json", binding
    )
    launch["session_binding_sha256"] = grants.sha256_json(binding)
    grants.write_once_json(artifacts / "grants" / approval["grant_id"] / "launch.json", launch)
    source = _seal("source", identity)
    source["grant_id"] = approval["grant_id"]
    source["completed_at_epoch"] = launch["started_epoch"] + 1
    grants.write_phase_seal(artifacts, source, bindings=identity, grant_launch=launch)
    with pytest.raises(grants.ControlError, match="already exists"):
        grants.write_once_json(artifacts / "phases" / "source.json", source)
    skipped = _seal("adoption", identity, grants.sha256_json(source))
    with pytest.raises(grants.ControlError, match="not the next phase"):
        grants.write_phase_seal(artifacts, skipped, bindings=identity)


def test_phase_chain_rejects_missing_or_tampered_prior_session_binding(artifacts, identity):
    approval, latch, launch, _epoch, _monotonic = _grant(
        identity, phase="source", phases=["source"]
    )
    grant_root = artifacts / "grants" / approval["grant_id"]
    grants.write_once_json(grant_root / "approval.json", approval)
    grants.write_once_json(grant_root / "latch.json", latch)
    binding = {
        "version": 1,
        "grant_id": approval["grant_id"],
        "approval_sha256": grants.sha256_json(approval),
        "capture_id": identity["capture_id"],
        "session_id": identity["session_id"],
        "root_id": identity["root_id"],
        "stage_name": identity["stage_name"],
        "report_sha256": approval["measurements"]["session_report_sha256"],
        "report_path": str(artifacts / "runs" / identity["session_id"] / "session.json"),
        "boot_stamp": launch["boot_stamp"],
        "root_volume_uuid": "ACAD66E7-42DA-426B-B20D-FB87A453396B",
        "root_device": 42,
    }
    binding_path = grant_root / "session-binding.json"
    grants.write_once_json(binding_path, binding)
    launch["session_binding_sha256"] = grants.sha256_json(binding)
    grants.write_once_json(grant_root / "launch.json", launch)
    source = _seal("source", identity)
    source["grant_id"] = approval["grant_id"]
    source["completed_at_epoch"] = launch["started_epoch"] + 1
    grants.write_phase_seal(artifacts, source, bindings=identity)
    binding_path.unlink()
    with pytest.raises(grants.ControlError, match="missing or unsafe"):
        grants.read_phase_chain(artifacts / "phases", identity)
    binding_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    grants.write_once_json(
        binding_path,
        binding | {"report_sha256": "0" * 64},
    )
    with pytest.raises(grants.ControlError, match="approval does not bind"):
        grants.read_phase_chain(artifacts / "phases", identity)


def test_phase_completion_time_must_fit_linked_launch_cutoff(identity):
    now = time.time()
    launch = {
        "started_epoch": now - 10,
        "deadline_epoch": now + 10,
        "workflow_deadline_epoch": now + 5,
    }
    source = _seal("source", identity)
    source["completed_at_epoch"] = now + 6
    with pytest.raises(grants.ControlError, match="outside its approved launch window"):
        grants.validate_phase_seal(
            source,
            expected_phase="source",
            expected_bindings=identity,
            previous_sha256=None,
            grant_launch=launch,
        )
    source["completed_at_epoch"] = now + 4
    assert grants.validate_phase_seal(
        source,
        expected_phase="source",
        expected_bindings=identity,
        previous_sha256=None,
        grant_launch=launch,
    )
    source["completed_at_epoch"] = now + 11
    source["phase"] = "independent-verification"
    source["outputs"] = _seal("independent-verification", identity)["outputs"]
    with pytest.raises(grants.ControlError, match="outside its approved launch window"):
        grants.validate_phase_seal(
            source,
            expected_phase="independent-verification",
            expected_bindings=identity,
            previous_sha256=None,
            grant_launch=launch,
        )


def test_phase_seal_rechecks_live_monotonic_cutoff_after_output_validation(
    artifacts, identity, monkeypatch
):
    approval, latch, launch, now_epoch, _now_monotonic = _grant(
        identity, phase="source", phases=["source"]
    )
    grant_root = artifacts / "grants" / approval["grant_id"]
    grants.write_once_json(grant_root / "approval.json", approval)
    grants.write_once_json(grant_root / "latch.json", latch)
    binding = {
        "version": 1,
        "grant_id": approval["grant_id"],
        "approval_sha256": grants.sha256_json(approval),
        "capture_id": identity["capture_id"],
        "session_id": identity["session_id"],
        "root_id": identity["root_id"],
        "stage_name": identity["stage_name"],
        "report_sha256": approval["measurements"]["session_report_sha256"],
        "report_path": str(artifacts / "session.json"),
        "boot_stamp": launch["boot_stamp"],
        "root_volume_uuid": "ACAD66E7-42DA-426B-B20D-FB87A453396B",
        "root_device": 42,
    }
    grants.write_once_json(grant_root / "session-binding.json", binding)
    launch["session_binding_sha256"] = grants.sha256_json(binding)
    grants.write_once_json(grant_root / "launch.json", launch)
    seal = _seal("source", identity)
    seal["grant_id"] = approval["grant_id"]
    seal["completed_at_epoch"] = now_epoch + 1
    validate = grants.validate_phase_seal

    def expire_monotonic_after_validation(*args, **kwargs):
        result = validate(*args, **kwargs)
        monkeypatch.setattr(grants.time, "time", lambda: launch["workflow_deadline_epoch"] - 1)
        monkeypatch.setattr(
            grants.time,
            "monotonic",
            lambda: launch["workflow_deadline_monotonic"] + 1,
        )
        return result

    monkeypatch.setattr(grants, "validate_phase_seal", expire_monotonic_after_validation)
    with pytest.raises(grants.ControlError, match="live grant deadline expired"):
        grants.write_phase_seal(artifacts, seal, bindings=identity)
    assert not (artifacts / "phases" / "source.json").exists()


def _temporary_stop_authority(artifacts, *, current_boot, stopped_boot):
    admission = grants.expected_resource_admission()
    stop_reason = (
        f"live temporary storage reached "
        f"the {admission['storage_temporary_stop_bytes'] // 1024**3} GiB stop threshold"
    )
    volume = "ACAD66E7-42DA-426B-B20D-FB87A453396B"
    device = 42
    stop = {
        "mode": "historical",
        "category": "temporary",
        "threshold_bytes": admission["storage_temporary_stop_bytes"],
        "threshold_inclusive": True,
        "observed_bytes": admission["storage_temporary_stop_bytes"] + 1,
        "original_process_group": 303,
    }
    identity = {"capture_id": "a" * 32, "session_id": "b" * 32, "root_id": "c" * 32}
    reconciliation = {
        "event_id": "d" * 64,
        "audit_file": "stopped-worker-reconciliation.jsonl",
        "boot_stamp": stopped_boot,
        "stale_process_group": 303,
        "supervisor_pid": 101,
        "supervisor_pgid": 202,
        "primary_stop_reason": stop_reason,
        "reconciliation_kind": "temporary_storage_stop",
        "storage_stop_evidence": stop,
    }
    operator = artifacts / "operator"
    operator.mkdir(mode=0o700)
    report = {
        "boot_stamp": current_boot,
        "active_process_group": None,
        "phase": "stopped",
        "error": stop_reason if current_boot == stopped_boot else None,
        "system": {"filesystem_device": device, "filesystem_volume_uuid": volume},
        "stopped_worker_reconciliation": reconciliation,
    }
    return {
        "report": report,
        "identity": identity,
        "operator": operator,
        "control_root": artifacts / "controls",
        "freeze": {"root_volume_uuid": volume, "root_device": device},
        "_stop_reason": stop_reason,
        "_stop": stop,
    }


def test_same_boot_startup_maintenance_checks_supervisor_group_not_pid(artifacts, monkeypatch):
    authority = _temporary_stop_authority(
        artifacts, current_boot="boot-current", stopped_boot="boot-current"
    )
    groups = []
    monkeypatch.setattr(production, "_boot_stamp", lambda: "boot-current")
    monkeypatch.setattr(production, "_pid_exists", lambda _pid: False)

    def group_exists(pgid):
        groups.append(pgid)
        return pgid == 202

    monkeypatch.setattr(production, "_process_group_exists", group_exists)
    from kalshi_events import warehouse

    monkeypatch.setattr(
        warehouse,
        "_cleanup_resumed_historical_dbt_spills",
        lambda *_args, **_kwargs: pytest.fail("live supervisor group reached cleanup"),
    )
    with pytest.raises(production.RunnerError, match="live or ambiguous"):
        production._historical_startup_maintenance(authority)
    assert groups == [303, 202]


def test_rebooted_startup_gate_uses_recorded_boot_proof_without_old_pid_queries(
    artifacts, monkeypatch
):
    authority = _temporary_stop_authority(
        artifacts, current_boot="boot-new", stopped_boot="boot-old"
    )
    report = authority["report"]
    identity = authority["identity"]
    remounted_device = authority["freeze"]["root_device"] + 9
    report["system"]["filesystem_device"] = remounted_device
    authority["current_mount"] = {
        "device": remounted_device,
        "volume_uuid": authority["freeze"]["root_volume_uuid"],
    }
    report["historical_reboot_reconciliation"] = {
        "state": "complete",
        "capture_id": identity["capture_id"],
        "root_id": identity["root_id"],
        "boot_stamp": "boot-new",
        "accounting_basis": "exact",
        "processing_worker_started": False,
        "grant_started": False,
    }
    report["reboot_recovery_history"] = [
        {
            "previous_boot_stamp": "boot-old",
            "boot_stamp": "boot-new",
            "volume_uuid": authority["freeze"]["root_volume_uuid"],
            "filesystem_device": remounted_device,
            "previous_active_process_group": 303,
            "prior_process_group_ignored_after_boot_change": True,
            "accounting_basis": "exact",
            "accounting_complete": True,
            "processing_worker_started": False,
            "grant_started": False,
        }
    ]
    record = {
        "version": 1,
        "event_id": "d" * 64,
        "capture_id": identity["capture_id"],
        "session_id": identity["session_id"],
        "root_id": identity["root_id"],
        "boot_stamp": "boot-old",
        "stale_process_group": 303,
        "supervisor_pid": 101,
        "supervisor_pgid": 202,
        "primary_stop_reason": authority["_stop_reason"],
        "reconciliation_kind": "temporary_storage_stop",
        "storage_stop_evidence": authority["_stop"],
    }
    audit_path = authority["operator"] / "stopped-worker-reconciliation.jsonl"
    audit_path.write_text(json.dumps(record) + "\n")
    monkeypatch.setattr(production, "_boot_stamp", lambda: "boot-new")
    monkeypatch.setattr(
        production,
        "_pid_exists",
        lambda _pid: pytest.fail("old-boot PID must not be queried after PID reuse"),
    )
    monkeypatch.setattr(
        production,
        "_process_group_exists",
        lambda _pgid: pytest.fail("old-boot process group must not be queried after reboot"),
    )

    proof = production._historical_reconciliation_audit(authority)
    assert proof["boot_stamp"] == "boot-old"
    assert proof["supervisor_pgid"] == 202


def test_rebooted_startup_gate_rejects_contradictory_old_group_ignored_flag(artifacts, monkeypatch):
    authority = _temporary_stop_authority(
        artifacts, current_boot="boot-new", stopped_boot="boot-old"
    )
    report = authority["report"]
    identity = authority["identity"]
    report["historical_reboot_reconciliation"] = {
        "state": "complete",
        "capture_id": identity["capture_id"],
        "root_id": identity["root_id"],
        "boot_stamp": "boot-new",
        "accounting_basis": "exact",
        "processing_worker_started": False,
        "grant_started": False,
    }
    report["reboot_recovery_history"] = [
        {
            "previous_boot_stamp": "boot-old",
            "boot_stamp": "boot-new",
            "volume_uuid": authority["freeze"]["root_volume_uuid"],
            "filesystem_device": authority["freeze"]["root_device"],
            "previous_active_process_group": 303,
            "prior_process_group_ignored_after_boot_change": False,
            "accounting_basis": "exact",
            "accounting_complete": True,
            "processing_worker_started": False,
            "grant_started": False,
        }
    ]
    monkeypatch.setattr(production, "_boot_stamp", lambda: "boot-new")
    monkeypatch.setattr(
        production,
        "_pid_exists",
        lambda _pid: pytest.fail("old-boot PID must not be queried"),
    )
    monkeypatch.setattr(
        production,
        "_process_group_exists",
        lambda _pgid: pytest.fail("old-boot process group must not be queried"),
    )
    with pytest.raises(production.RunnerError, match="exact reboot accounting proof"):
        production._historical_reconciliation_audit(authority)


def test_inert_template_cannot_issue_a_grant_or_rewrite_a_launch(artifacts, identity):
    template_identity = dict(identity, state_sha256=None, source_sha256=None, runtime_sha256=None)
    attempts = {
        "attempt_outcome_sha256": "a" * 64,
        "failed_launch_sha256": "b" * 64,
        "audit_after_sha256": "c" * 64,
        "failed_session_sha256": template_identity["prior_session_sha256"],
    }
    attempt_bundle = {
        "attempt_outcome_sha256": {
            "path": "attempt.json",
            "sha256": attempts["attempt_outcome_sha256"],
        },
        "failed_launch_sha256": {"path": "launch.json", "sha256": attempts["failed_launch_sha256"]},
        "audit_after_sha256": {"path": "audit.jsonl", "sha256": attempts["audit_after_sha256"]},
        "failed_session_sha256": {
            "path": "session.json",
            "sha256": attempts["failed_session_sha256"],
        },
    }
    attempts["bundle_sha256"] = grants.sha256_json(attempt_bundle)
    template_identity["failed_attempts_sha256"] = attempts["bundle_sha256"]
    template = {
        "version": 1,
        "state": "inert",
        "identity": template_identity,
        "prior_attempts": attempts,
        "fresh_pins": {"source": None, "state": None, "runtime": None, "approval": None},
        "phase_seals": [],
        "authorized_census": {
            "population_report_path": str(ROOT / "data" / "population.json"),
            "population_report_sha256": "a" * 64,
            "receipt_index_path": str(ROOT / "data" / "receipt-index.json"),
            "receipt_index_sha256": "b" * 64,
            "capture_id": identity["capture_id"],
            "root_id": identity["root_id"],
            "capture_state_sha256": identity["state_sha256"],
            "candidate_pages": 3,
            "observations_by_endpoint": {
                "/series": 2,
                "/events": 3,
                "/historical/markets": 5,
            },
            "nested_event_markets": 0,
            "exact_observations": 10,
        },
    }
    grants.write_once_json(artifacts / "control.json", template)
    loaded, _digest = grants.read_control_template(artifacts, expected_identity=template_identity)
    assert loaded["state"] == "inert"
    with pytest.raises(grants.ControlError, match="no frozen approval snapshot"):
        grants.read_frozen_control(artifacts, expected_identity=template_identity)


def test_grant_launch_artifact_cannot_be_reused(artifacts, identity):
    launch = {"version": 1, "grant_id": "4" * 32}
    path = artifacts / "grants" / ("4" * 32) / "launch.json"
    grants.write_once_json(path, launch)
    with pytest.raises(grants.ControlError, match="already exists"):
        grants.write_once_json(path, launch)


def test_frozen_control_rejects_unsafe_root_before_reading(artifacts, identity, monkeypatch):
    alias = artifacts / "alias"
    alias.symlink_to(artifacts, target_is_directory=True)
    monkeypatch.setattr(grants, "read_json", lambda *_: pytest.fail("opened an unsafe root"))
    with pytest.raises(grants.ControlError, match="control root must be owner-only"):
        grants.read_frozen_control(alias, expected_identity=identity)


def test_activated_warehouse_record_requires_the_bound_hard_link(artifacts):
    source = artifacts / "warehouse.duckdb"
    source.write_bytes(b"certified fixture bytes")
    alias = artifacts / "production.duckdb"
    os.link(source, alias)
    with pytest.raises(production.RunnerError, match="unsafe or oversized"):
        production._historical_file_record(alias)
    record = production._historical_file_record(alias, linked_source=source)
    assert record["sha256"] == grants.sha256_bytes(source.read_bytes())
    other = artifacts / "unrelated.duckdb"
    other.write_bytes(source.read_bytes())
    with pytest.raises(production.RunnerError, match="not a hard link"):
        production._historical_file_record(alias, linked_source=other)
    os.link(source, artifacts / "untracked.duckdb")
    with pytest.raises(production.RunnerError, match="unsafe or oversized"):
        production._historical_file_record(alias, linked_source=source)


def test_phase_seal_survives_crash_before_attempt_result_and_next_grant_starts_next_phase(
    artifacts, identity
):
    template_identity = dict(identity, state_sha256=None, source_sha256=None, runtime_sha256=None)
    attempts = {
        "attempt_outcome_sha256": "a" * 64,
        "failed_launch_sha256": "b" * 64,
        "audit_after_sha256": "c" * 64,
        "failed_session_sha256": template_identity["prior_session_sha256"],
    }
    attempts["bundle_sha256"] = grants.sha256_json(
        {
            "attempt_outcome_sha256": {
                "path": "attempt.json",
                "sha256": attempts["attempt_outcome_sha256"],
            },
            "failed_launch_sha256": {
                "path": "launch.json",
                "sha256": attempts["failed_launch_sha256"],
            },
            "audit_after_sha256": {"path": "audit.jsonl", "sha256": attempts["audit_after_sha256"]},
            "failed_session_sha256": {
                "path": "session.json",
                "sha256": attempts["failed_session_sha256"],
            },
        }
    )
    template_identity["failed_attempts_sha256"] = attempts["bundle_sha256"]
    control = {
        "version": 1,
        "state": "inert",
        "identity": template_identity,
        "prior_attempts": attempts,
        "fresh_pins": {"source": None, "state": None, "runtime": None, "approval": None},
        "phase_seals": [],
        "authorized_census": {
            "population_report_path": str(artifacts / "population.json"),
            "population_report_sha256": "a" * 64,
            "receipt_index_path": str(artifacts / "receipt-index.json"),
            "receipt_index_sha256": "b" * 64,
            "capture_id": identity["capture_id"],
            "root_id": identity["root_id"],
            "capture_state_sha256": identity["state_sha256"],
            "candidate_pages": 3,
            "observations_by_endpoint": {
                "/series": 2,
                "/events": 3,
                "/historical/markets": 5,
            },
            "nested_event_markets": 0,
            "exact_observations": 10,
        },
    }
    control_hash = grants.write_once_json(artifacts / "control.json", control)
    runtime_manifest_sha = "3" * 64
    operational_source_sha = "4" * 64
    frozen_identity = dict(
        template_identity,
        state_sha256="1" * 64,
        source_sha256="2" * 64,
        runtime_sha256=grants.sha256_json(
            {
                "runtime_manifest_sha256": runtime_manifest_sha,
                "operational_source_sha256": operational_source_sha,
            }
        ),
    )
    freeze = {
        "version": 1,
        "control_sha256": control_hash,
        "identity": frozen_identity,
        "state_sha256": frozen_identity["state_sha256"],
        "source_sha256": frozen_identity["source_sha256"],
        "runtime_sha256": frozen_identity["runtime_sha256"],
        "source_snapshot_sha256": "5" * 64,
        "receipt_evidence_sha256": "6" * 64,
        "scope_sha256": "7" * 64,
        "code_sha256": "8" * 64,
        "runtime_manifest_sha256": runtime_manifest_sha,
        "operational_source_sha256": operational_source_sha,
        "root_volume_uuid": "ACAD66E7-42DA-426B-B20D-FB87A453396B",
        "root_device": 42,
        "frozen_at_epoch": time.time(),
    }
    grants.write_once_json(artifacts / "freeze.json", freeze)

    template_readback = grants.read_frozen_control(artifacts, expected_identity=template_identity)
    assert (
        grants.read_frozen_control(artifacts, expected_identity=frozen_identity)
        == template_readback
    )
    with pytest.raises(grants.ControlError, match="do not match this session"):
        grants.read_control_template(artifacts, expected_identity=frozen_identity)
    for key in ("capture_id", "state_sha256", "source_sha256", "runtime_sha256"):
        wrong = dict(frozen_identity, **{key: "f" * len(frozen_identity[key])})
        with pytest.raises(grants.ControlError, match="frozen historical control identity changed"):
            grants.read_frozen_control(artifacts, expected_identity=wrong)

    source_approval, source_latch, source_launch, _epoch, _mono = _grant(
        frozen_identity, phase="source", phases=["source"]
    )
    source_binding = {
        "version": 1,
        "grant_id": source_approval["grant_id"],
        "approval_sha256": grants.sha256_json(source_approval),
        "capture_id": frozen_identity["capture_id"],
        "session_id": frozen_identity["session_id"],
        "root_id": frozen_identity["root_id"],
        "stage_name": frozen_identity["stage_name"],
        "report_sha256": source_approval["measurements"]["session_report_sha256"],
        "report_path": str(artifacts / "session.json"),
        "boot_stamp": source_launch["boot_stamp"],
        "root_volume_uuid": freeze["root_volume_uuid"],
        "root_device": freeze["root_device"],
    }
    source_root = artifacts / "grants" / source_approval["grant_id"]
    grants.write_once_json(source_root / "approval.json", source_approval)
    grants.write_once_json(source_root / "latch.json", source_latch)
    grants.write_once_json(source_root / "session-binding.json", source_binding)
    source_launch["session_binding_sha256"] = grants.sha256_json(source_binding)
    grants.write_once_json(source_root / "launch.json", source_launch)
    source_seal = _seal("source", frozen_identity)
    source_seal["grant_id"] = source_approval["grant_id"]
    source_seal["completed_at_epoch"] = source_launch["started_epoch"] + 1
    grants.write_phase_seal(artifacts, source_seal, bindings=frozen_identity)
    source_bytes = (artifacts / "phases" / "source.json").read_bytes()
    assert not (source_root / "attempt-result.json").exists()

    readiness_approval, readiness_latch, _unused_launch, epoch, monotonic = _grant(
        frozen_identity,
        grant_id="7" * 32,
        phase="readiness",
        phases=["readiness"],
    )
    readiness_latch["nonce"] = "9" * 32
    readiness_root = artifacts / "grants" / readiness_approval["grant_id"]
    grants.write_once_json(readiness_root / "approval.json", readiness_approval)
    grants.write_once_json(readiness_root / "latch.json", readiness_latch)
    bundle = grants.read_control_bundle(artifacts, readiness_approval["grant_id"], prelaunch=True)
    assert bundle["current_phase"] == "source"
    assert grants.next_phase(bundle["current_phase"]) == readiness_approval["phase"]
    binding = {
        "version": 1,
        "grant_id": readiness_approval["grant_id"],
        "approval_sha256": grants.sha256_json(readiness_approval),
        "capture_id": frozen_identity["capture_id"],
        "session_id": frozen_identity["session_id"],
        "root_id": frozen_identity["root_id"],
        "stage_name": frozen_identity["stage_name"],
        "report_sha256": readiness_approval["measurements"]["session_report_sha256"],
        "report_path": str(artifacts / "session.json"),
        "boot_stamp": "boot-current",
        "root_volume_uuid": freeze["root_volume_uuid"],
        "root_device": freeze["root_device"],
    }
    launch, _launch_sha = grants.issue_launch(
        artifacts,
        expected_identity=frozen_identity,
        requested_phase="readiness",
        grant_id=readiness_approval["grant_id"],
        now_epoch=epoch,
        now_monotonic=monotonic,
        boot_stamp="boot-current",
        session_binding=binding,
    )
    assert launch["grant_id"] == readiness_approval["grant_id"]
    assert (artifacts / "phases" / "source.json").read_bytes() == source_bytes
    assert not (source_root / "attempt-result.json").exists()
    assert grants.read_phase_chain(artifacts / "phases", frozen_identity)[0] == "source"


def test_frozen_control_binds_runtime_inventory_operational_source_and_root_volume(identity):
    expected = dict(identity, state_sha256=None, source_sha256=None, runtime_sha256=None)
    runtime_manifest_sha = "a" * 64
    operational_source_sha = "b" * 64
    frozen_identity = dict(
        expected,
        state_sha256="1" * 64,
        source_sha256="2" * 64,
        runtime_sha256=grants.sha256_json(
            {
                "runtime_manifest_sha256": runtime_manifest_sha,
                "operational_source_sha256": operational_source_sha,
            }
        ),
    )
    freeze = {
        "version": 1,
        "control_sha256": "3" * 64,
        "identity": frozen_identity,
        "state_sha256": frozen_identity["state_sha256"],
        "source_sha256": frozen_identity["source_sha256"],
        "runtime_sha256": frozen_identity["runtime_sha256"],
        "source_snapshot_sha256": "4" * 64,
        "receipt_evidence_sha256": "5" * 64,
        "scope_sha256": "6" * 64,
        "code_sha256": "7" * 64,
        "runtime_manifest_sha256": runtime_manifest_sha,
        "operational_source_sha256": operational_source_sha,
        "root_volume_uuid": "ACAD66E7-42DA-426B-B20D-FB87A453396B",
        "root_device": 42,
        "frozen_at_epoch": time.time(),
    }
    assert (
        grants.validate_frozen_control(freeze, control_sha256="3" * 64, expected_identity=expected)
        == frozen_identity
    )
    freeze["operational_source_sha256"] = "8" * 64
    with pytest.raises(grants.ControlError, match="runtime digest"):
        grants.validate_frozen_control(freeze, control_sha256="3" * 64, expected_identity=expected)


def test_grant_session_binding_is_approval_and_report_bound(artifacts, identity):
    approval, _latch, _launch, _now_epoch, _now_monotonic = _grant(identity)
    approval["measurements"]["session_report_sha256"] = "a" * 64
    binding = {
        "version": 1,
        "grant_id": approval["grant_id"],
        "approval_sha256": grants.sha256_json(approval),
        "capture_id": identity["capture_id"],
        "session_id": identity["session_id"],
        "root_id": identity["root_id"],
        "stage_name": identity["stage_name"],
        "report_sha256": "a" * 64,
        "report_path": str(artifacts / "session.json"),
        "boot_stamp": "boot-current",
        "root_volume_uuid": "ACAD66E7-42DA-426B-B20D-FB87A453396B",
        "root_device": 42,
    }
    assert (
        grants.validate_session_binding(binding, approval=approval, expected_identity=identity)
        == binding
    )
    binding["report_sha256"] = "b" * 64
    with pytest.raises(grants.ControlError, match="approval does not bind"):
        grants.validate_session_binding(binding, approval=approval, expected_identity=identity)


def test_activation_audit_claim_is_per_grant_but_rejects_replay_and_prior_completion(
    artifacts, identity
):
    audit = "a" * 64
    grant = "b" * 32
    launch = "c" * 32
    grants.claim_activation_audit(
        artifacts,
        audit_sha256=audit,
        grant_id=grant,
        launch_id=launch,
        identity=identity,
        prior_audits=set(),
    )
    first_claim = grants.read_json(artifacts / "activation-audits" / f"{grant}.json")
    assert first_claim == {
        "version": 1,
        "audit_sha256": audit,
        "identity": identity,
        "grant_id": grant,
        "launch_id": launch,
    }
    next_grant = "d" * 32
    next_launch = "e" * 32
    grants.claim_activation_audit(
        artifacts,
        audit_sha256=audit,
        grant_id=next_grant,
        launch_id=next_launch,
        identity=identity,
        prior_audits=set(),
    )
    assert production._historical_activation_claimed(artifacts, grant, launch, identity)
    assert production._historical_activation_claimed(artifacts, next_grant, next_launch, identity)
    assert not production._historical_activation_claimed(artifacts, "f" * 32, "1" * 32, identity)
    with pytest.raises(production.RunnerError, match="not bound to the current grant"):
        production._historical_activation_claimed(artifacts, grant, "2" * 32, identity)
    with pytest.raises(production.RunnerError, match="not bound to the current grant"):
        production._historical_activation_claimed(
            artifacts, grant, launch, dict(identity, root_id="0" * 32)
        )
    with pytest.raises(grants.ControlError, match="already exists"):
        grants.claim_activation_audit(
            artifacts,
            audit_sha256=audit,
            grant_id=grant,
            launch_id=launch,
            identity=identity,
            prior_audits=set(),
        )
    with pytest.raises(grants.ControlError, match="prior attempt"):
        grants.claim_activation_audit(
            artifacts,
            audit_sha256="f" * 64,
            grant_id="6" * 32,
            launch_id="7" * 32,
            identity=identity,
            prior_audits={"f" * 64},
        )


def test_hidden_worker_entry_rejects_inert_controls_before_any_store_or_action(
    artifacts, isolated_control_root, monkeypatch, capsys
):
    inert_root = isolated_control_root
    token = "c" * 32
    report_dir = artifacts / "runs" / "session"
    logs = report_dir / "logs"
    progress_dir = logs / f"invocation-{'d' * 32}"
    progress_dir.mkdir(parents=True, mode=0o700)
    progress_path = progress_dir / "historical-a-readiness-prepare.storage.json"
    report_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    (report_dir / production.REPORT_NAME).write_text(
        json.dumps(
            {
                "active_process_group": os.getpid(),
                "private_historical_grant": {
                    "grant_id": "a" * 32,
                    "launch_sha256": "b" * 64,
                    "phase": "readiness",
                    "substep": "prepare",
                    "controller_pid": os.getppid(),
                    "storage_invocation_token": token,
                    "storage_progress_path": str(progress_path),
                },
                "storage_supervision_invocation": token,
                "storage_supervision_file": str(progress_path),
            }
        )
    )
    progress_path.write_text(
        json.dumps(
            {
                "version": 1,
                "invocation_token": token,
                "sample_count": 1,
                "target_interval_seconds": 5,
            }
        )
    )
    monkeypatch.setattr(
        production,
        "_historical_store",
        lambda *_args, **_kwargs: pytest.fail("inert controls reached the warehouse"),
    )
    result = production._historical_worker_main(
        artifacts,
        inert_root,
        "a" * 32,
        "b" * 64,
        "readiness",
        "prepare",
        token,
        progress_path,
    )
    assert result == 1
    assert "parent-issued supervisor pipe" in capsys.readouterr().out


def test_private_controller_command_requires_exact_argv_shape(artifacts, monkeypatch):
    control_root = artifacts / "controls"
    directory = artifacts / "production"
    grant_id = "a" * 32
    python = Path("/private/pinned-venv/bin/python")
    script = str(ROOT / "scripts" / "production_run.py")
    monkeypatch.setattr(production, "_historical_source_root", lambda _root: ROOT)
    monkeypatch.setattr(production, "_historical_venv_root", lambda _root: python.parent.parent)
    expected = " ".join(
        (
            str(python),
            "-B",
            script,
            "--_historical-control",
            str(directory),
            str(control_root),
            grant_id,
        )
    )
    assert production._historical_private_controller_command_matches(
        expected,
        expected_python=python,
        control_root=control_root,
        directory=directory,
        grant_id=grant_id,
    )
    fake_code = f"print('{expected}')"
    spoofed = f"{python} -c {json.dumps(fake_code)}"
    assert not production._historical_private_controller_command_matches(
        spoofed,
        expected_python=python,
        control_root=control_root,
        directory=directory,
        grant_id=grant_id,
    )


def test_pinned_venv_symlink_preserves_prefix_for_actual_controller_launch(artifacts):
    runtime_root = ROOT / "data" / "runtime"
    runtime_root.mkdir(parents=True, exist_ok=True)
    control_root = runtime_root / (
        "kalshi-historical-completion-" + f"{uuid.uuid4().int % 100_000_000:08d}"
    )
    control_root.mkdir(mode=0o700)
    directory = artifacts / "production"
    directory.mkdir(mode=0o700)
    script = ROOT / "scripts" / "production_run.py"
    pinned_python = ROOT / ".venv" / "bin" / "python"
    grant_id = "a" * 32

    def launch(interpreter: Path) -> subprocess.CompletedProcess[str]:
        site_packages = next((ROOT / ".venv" / "lib").glob("python*/site-packages"))
        child_env = os.environ.copy()
        child_env.pop("PYTHONDONTWRITEBYTECODE", None)
        child_env["PYTHONPATH"] = os.pathsep.join((str(ROOT / "src"), str(site_packages)))
        return subprocess.run(
            [
                str(interpreter),
                "-B",
                str(script),
                "--_historical-control",
                str(directory),
                str(control_root),
                grant_id,
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
            env=child_env,
        )

    try:
        literal = launch(pinned_python)
        assert literal.returncode == 1
        assert "outside the pinned virtual environment" not in literal.stdout
        assert "control" in literal.stdout.lower() or "grant" in literal.stdout.lower()

        resolved = launch(pinned_python.resolve(strict=True))
        assert resolved.returncode == 1
        assert "outside the pinned virtual environment" in (resolved.stdout + resolved.stderr)
    finally:
        shutil.rmtree(control_root, ignore_errors=True)


def test_private_controller_sets_bytecode_environment_before_import(monkeypatch):
    import builtins

    monkeypatch.delenv("PYTHONDONTWRITEBYTECODE", raising=False)
    original_import = builtins.__import__
    observed = []

    def import_guard(name, *args, **kwargs):
        if name == "historical_grants":
            observed.append(os.environ.get("PYTHONDONTWRITEBYTECODE"))
            raise RuntimeError("reached private controller grant import")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_guard)
    with pytest.raises(RuntimeError, match="grant import"):
        production._historical_control_main(Path("unused"), Path("unused"), "a" * 32)
    assert observed == ["1"]


def test_worker_rejects_caller_created_pipe_when_parent_is_not_controller(
    artifacts, isolated_control_root
):
    control_root = isolated_control_root
    token = "d" * 32
    grant_id = "a" * 32
    launch_sha256 = "b" * 64
    progress_path = (
        artifacts
        / "runs"
        / "session"
        / "logs"
        / f"invocation-{'e' * 32}"
        / "historical-a-source-snapshot.storage.json"
    )
    read_fd, write_fd = os.pipe()
    env = os.environ.copy()
    env[production.HISTORICAL_SUPERVISOR_FD_ENV] = str(read_fd)
    bootstrap = (
        "import runpy, subprocess, sys, types\n"
        "subprocess.run = lambda *_args, **_kwargs: types.SimpleNamespace("
        "returncode=0, stdout='python -m pytest\\n', stderr='')\n"
        "script = sys.argv[1]\n"
        "sys.argv = [script, *sys.argv[2:]]\n"
        "runpy.run_path(script, run_name='__main__')\n"
    )
    command = [
        sys.executable,
        "-B",
        "-c",
        bootstrap,
        str(ROOT / "scripts" / "production_run.py"),
        "--_historical-worker",
        str(artifacts),
        str(control_root),
        grant_id,
        launch_sha256,
        "source",
        "snapshot",
        token,
        str(progress_path),
    ]
    process = subprocess.Popen(
        command,
        cwd=ROOT,
        env=env,
        pass_fds=(read_fd,),
        start_new_session=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    os.close(read_fd)
    try:
        handoff = {
            "version": 1,
            "nonce": uuid.uuid4().hex,
            "directory": str(artifacts),
            "control_root": str(control_root),
            "grant_id": grant_id,
            "launch_sha256": launch_sha256,
            "phase": "source",
            "substep": "snapshot",
            "storage_invocation_token": token,
            "storage_progress_path": str(progress_path),
            "supervisor_pid": os.getpid(),
            "supervisor_pgid": os.getpgrp(),
            "worker_pid": process.pid,
            "worker_pgid": process.pid,
        }
        os.write(write_fd, json.dumps(handoff).encode("utf-8"))
    finally:
        os.close(write_fd)
    stdout, _stderr = process.communicate(timeout=10)
    assert process.returncode == 1
    assert "supervised worker parent is not the pinned private controller" in stdout


def test_private_controller_rejects_wrong_venv_before_lock_or_grant_use(
    artifacts, isolated_control_root, monkeypatch, capsys
):
    control_root = isolated_control_root
    monkeypatch.setattr(production.sys, "prefix", str(artifacts))
    monkeypatch.setattr(
        production,
        "_run_lock",
        lambda *_args, **_kwargs: pytest.fail("wrong interpreter acquired the run lock"),
    )
    result = production._historical_control_main(artifacts, control_root, "a" * 32)
    assert result == 1
    assert "outside the pinned virtual environment" in capsys.readouterr().out


def test_post_launch_baseline_mismatch_consumes_launch_without_worker(
    artifacts, identity, monkeypatch, capsys
):
    control_root = artifacts / "controls"
    control_root.mkdir(mode=0o700)
    directory = artifacts / "production"
    directory.mkdir(mode=0o700)
    report_path = directory / "runs" / identity["session_id"] / production.REPORT_NAME
    report_path.parent.mkdir(parents=True, mode=0o700)
    report = {
        "session_id": identity["session_id"],
        "capture_id": identity["capture_id"],
        "root_id": identity["root_id"],
        "mode": "historical",
        "phase": "stopped",
        "active_process_group": None,
        "boot_stamp": "boot-current",
        "system": {"filesystem_device": directory.stat().st_dev},
    }
    production._atomic_report(report_path, report)
    report_path.chmod(0o600)
    grant_id = "a" * 32
    approval = {
        "grant_id": grant_id,
        "phase": "source",
        "phases": ["source"],
        "measurements": {},
    }
    root_volume = "ACAD66E7-42DA-426B-B20D-FB87A453396B"
    freeze = {
        "source_snapshot_sha256": "a" * 64,
        "receipt_evidence_sha256": "b" * 64,
        "scope_sha256": "c" * 64,
        "code_sha256": "d" * 64,
        "root_volume_uuid": root_volume,
        "root_device": directory.stat().st_dev,
    }
    preflight = {
        "approval": approval,
        "identity": identity,
        "report": report,
        "report_path": report_path,
        "freeze": freeze,
    }
    baseline = {
        "database_bytes": grants.GIB,
        "temporary_bytes": 3 * grants.GIB,
        "root_bytes": 4 * grants.GIB,
        "aggregate_bytes": 5 * grants.GIB,
        "free_bytes": 200 * grants.GIB,
        "inventory_entries": 20,
        "peak_rss_bytes": grants.GIB,
    }
    projected = {
        "projected_database_bytes": 100 * grants.GIB,
        "projected_temporary_bytes": 144 * grants.GIB,
        "projected_root_bytes": 300 * grants.GIB,
        "projected_aggregate_bytes": 350 * grants.GIB,
        "projected_free_bytes": 160 * grants.GIB,
        "inventory_entries": 50_000,
        "projected_peak_rss_bytes": 20 * grants.GIB,
    }
    launch_marker = control_root / "grants" / grant_id / "launch.json"
    monkeypatch.setitem(sys.modules, "historical_grants", grants)
    atomic_report = production._atomic_report

    def write_private_report(path, value):
        atomic_report(path, value)
        path.chmod(0o600)

    monkeypatch.setattr(production, "_atomic_report", write_private_report)
    monkeypatch.setattr(production, "_historical_control_root", lambda path: path)
    monkeypatch.setattr(production, "_historical_assert_pinned_python", lambda _root: None)
    monkeypatch.setattr(production, "_historical_authority", lambda *_a, **_kw: preflight)
    monkeypatch.setattr(production, "_volume_uuid", lambda _path: root_volume)
    monkeypatch.setattr(production, "_historical_store", lambda *_a, **_kw: object())
    monkeypatch.setattr(production, "_historical_resource_baseline", lambda *_a: baseline)
    monkeypatch.setattr(
        production,
        "_historical_stop_record",
        lambda *_a, **_kw: None,
    )
    monkeypatch.setattr(
        production,
        "_invoke",
        lambda *_a, **_kw: pytest.fail("baseline mismatch started a private worker"),
    )
    monkeypatch.setattr(production, "_root_id", lambda _store: identity["root_id"])
    monkeypatch.setattr(
        grants,
        "read_frozen_control",
        lambda *_a, **_kw: ({"identity": identity}, "e" * 64),
    )
    current_epoch = time.time()

    def consume_launch(_root, *, requested_phase, grant_id, session_binding, **_kwargs):
        assert requested_phase == "source"
        assert session_binding["report_sha256"] == grants.sha256_bytes(
            grants._private_regular_file(report_path)
        )
        (control_root / "grants").mkdir(mode=0o700)
        launch_marker.parent.mkdir(mode=0o700)
        launch = {"started_epoch": current_epoch, "grant_id": grant_id}
        grants.write_once_json(launch_marker, launch)
        measurements = {
            "version": 1,
            "grant_id": grant_id,
            "phases": ["source"],
            "identity": identity,
            "source_snapshot_sha256": freeze["source_snapshot_sha256"],
            "receipt_evidence_sha256": freeze["receipt_evidence_sha256"],
            "scope_sha256": freeze["scope_sha256"],
            "code_sha256": freeze["code_sha256"],
            "root_volume_uuid": root_volume,
            "root_device": freeze["root_device"],
            "session_report_sha256": session_binding["report_sha256"],
            "measured_at_epoch": current_epoch - 1,
            "forecast_seconds": 16 * 60 * 60,
            "final_verification_seconds": 2 * 60 * 60,
            **projected,
            "baseline": baseline | {"temporary_bytes": baseline["temporary_bytes"] - 1},
        }
        raw = grants.canonical_bytes(measurements) + b"\n"
        summary = {
            "report_sha256": grants.sha256_bytes(raw),
            "session_report_sha256": session_binding["report_sha256"],
            "measured_at_epoch": measurements["measured_at_epoch"],
            "forecast_seconds": measurements["forecast_seconds"],
            "final_verification_seconds": measurements["final_verification_seconds"],
            **projected,
        }
        approval["approved_at_epoch"] = current_epoch
        approval["measurements"] = summary
        grants.write_once_json(
            control_root / "grants" / grant_id / "measurements.json", measurements
        )
        return launch, grants.sha256_bytes(grants._private_regular_file(launch_marker))

    monkeypatch.setattr(grants, "issue_launch", consume_launch)
    result = production._historical_control_main(directory, control_root, grant_id)
    assert result == 1
    assert launch_marker.is_file()
    assert (launch_marker.parent / "measurements.json").is_file()
    assert (
        "current measured resource inventory differs from approved baseline"
        in capsys.readouterr().out
    )


def test_controller_persists_full_private_recovery_handoff_on_unreaped_child(
    artifacts, identity, monkeypatch, capsys
):
    from types import SimpleNamespace

    monkeypatch.setitem(sys.modules, "historical_grants", grants)
    directory = artifacts / "production"
    report_path = directory / "runs" / identity["session_id"] / production.REPORT_NAME
    report_path.parent.mkdir(parents=True, mode=0o700)
    control_root = artifacts / "controls"
    control_root.mkdir(mode=0o700)
    root_volume = "ACAD66E7-42DA-426B-B20D-FB87A453396B"
    report = {
        "version": 1,
        "session_id": identity["session_id"],
        "mode": "historical",
        "capture_id": identity["capture_id"],
        "root_id": identity["root_id"],
        "phase": "stopped",
        "production_state": "not_attempted",
        "active_process_group": None,
        "boot_stamp": "boot-current",
        "system": {"filesystem_device": directory.stat().st_dev},
    }
    production._atomic_report(report_path, report)
    report_path.chmod(0o600)
    operational_source_sha256 = "4" * 64
    runtime_manifest_sha256 = "3" * 64
    identity = dict(
        identity,
        runtime_sha256=grants.sha256_json(
            {
                "runtime_manifest_sha256": runtime_manifest_sha256,
                "operational_source_sha256": operational_source_sha256,
            }
        ),
    )
    approval, latch, _launch, now_epoch, now_monotonic = _grant(
        identity, phase="readiness", phases=["readiness"]
    )
    freeze = {
        "operational_source_sha256": operational_source_sha256,
        "runtime_manifest_sha256": runtime_manifest_sha256,
        "source_snapshot_sha256": "5" * 64,
        "receipt_evidence_sha256": "6" * 64,
        "scope_sha256": "7" * 64,
        "code_sha256": "8" * 64,
        "root_volume_uuid": root_volume,
        "root_device": directory.stat().st_dev,
    }
    preflight = {
        "approval": approval,
        "identity": identity,
        "report": report,
        "report_path": report_path,
        "freeze": freeze,
    }
    baseline = {
        "database_bytes": grants.GIB,
        "temporary_bytes": 3 * grants.GIB,
        "root_bytes": 4 * grants.GIB,
        "aggregate_bytes": 5 * grants.GIB,
        "free_bytes": 200 * grants.GIB,
        "inventory_entries": 20,
        "peak_rss_bytes": grants.GIB,
    }
    monkeypatch.setattr(production, "_historical_control_root", lambda path: path)
    monkeypatch.setattr(production, "_historical_source_root", lambda _root: ROOT)
    monkeypatch.setattr(production, "_historical_venv_root", lambda _root: ROOT / ".venv")
    monkeypatch.setattr(production, "_historical_assert_pinned_python", lambda _root: None)
    monkeypatch.setattr(production, "_historical_authority", lambda *_a, **_k: preflight)
    monkeypatch.setattr(
        production, "_historical_store", lambda *_a, **_k: SimpleNamespace(reconcile=lambda: None)
    )
    monkeypatch.setattr(production, "_historical_resource_baseline", lambda *_a: baseline)
    monkeypatch.setattr(production, "_historical_measurement_artifact", lambda *_a, **_k: {})
    monkeypatch.setattr(production, "_historical_verify_runtime", lambda *_a, **_k: "runtime")
    monkeypatch.setattr(
        production,
        "_historical_operational_source_sha256",
        lambda _root: operational_source_sha256,
    )
    monkeypatch.setattr(production, "_historical_substeps_for_grant", lambda *_a: ("prepare",))
    monkeypatch.setattr(production, "_root_id", lambda _store: identity["root_id"])
    monkeypatch.setattr(production, "_volume_uuid", lambda _path: root_volume)
    monkeypatch.setattr(
        grants,
        "read_frozen_control",
        lambda *_a, **_k: ({"identity": identity, "freeze": freeze}, "f" * 64),
    )

    def issue_launch(
        _root,
        *,
        expected_identity,
        requested_phase,
        grant_id,
        now_epoch,
        now_monotonic,
        boot_stamp,
        session_binding,
        **_kwargs,
    ):
        assert expected_identity == identity and requested_phase == "readiness"
        launch = grants.create_launch_record(
            approval,
            latch,
            now_epoch=now_epoch,
            now_monotonic=now_monotonic,
            boot_stamp=boot_stamp,
            launch_id=uuid.uuid4().hex,
            session_binding_sha256=grants.sha256_json(session_binding),
        )
        return launch, grants.sha256_json(launch)

    monkeypatch.setattr(grants, "issue_launch", issue_launch)
    stop_record = {}
    monkeypatch.setattr(
        production,
        "_historical_stop_record",
        lambda *_a, **kwargs: stop_record.update(kwargs),
    )

    def unconfirmed_child(_command, **kwargs):
        worker_pid = 987_654
        child_report = kwargs["report"]
        token = kwargs["storage_invocation_token"]
        progress_path = kwargs["storage_progress_path"]
        child_report["active_process_group"] = worker_pid
        child_report["storage_supervision_invocation"] = token
        child_report["storage_supervision_file"] = str(progress_path)
        child_report["private_historical_grant"]["worker_pid"] = worker_pid
        child_report["private_historical_grant"]["worker_pgid"] = worker_pid
        production._atomic_report(
            progress_path,
            {
                "version": 1,
                "invocation_token": token,
                "sample_count": 1,
                "target_interval_seconds": int(
                    production._effective_limits()["storage_sample_seconds"]
                ),
            },
        )
        production._atomic_report(report_path, child_report)
        return None, "child outcome unclear", False

    monkeypatch.setattr(production, "_invoke", unconfirmed_child)
    result = production._historical_control_main(directory, control_root, approval["grant_id"])
    assert result == 1
    saved = production._strict_json(report_path)
    marker = saved["private_historical_grant"]
    assert saved["phase"] == "uncertain"
    assert saved["active_process_group"] == 987_654
    assert marker["state"] == "stopped"
    assert marker["control_root"] == str(control_root)
    assert marker["launch_id"]
    assert marker["controller_pid"] == os.getpid()
    assert marker["controller_pgid"] == os.getpgrp()
    assert marker["worker_pid"] == marker["worker_pgid"] == saved["active_process_group"]
    assert marker["storage_invocation_token"] == saved["storage_supervision_invocation"]
    assert marker["storage_progress_path"] == saved["storage_supervision_file"]
    assert stop_record["root_volume_uuid"] == root_volume
    assert "child outcome unclear" in capsys.readouterr().out


def _mock_private_recovery_runtime(monkeypatch, attempt, *, current_boot=None):
    monkeypatch.setitem(sys.modules, "historical_grants", grants)
    boot = current_boot or attempt["report"]["boot_stamp"]
    monkeypatch.setattr(production, "_boot_stamp", lambda: boot)
    monkeypatch.setattr(production, "_volume_uuid", lambda _path: attempt["volume_uuid"])
    monkeypatch.setattr(
        production,
        "_historical_operational_source_sha256",
        lambda _root: attempt["freeze"]["operational_source_sha256"],
    )
    monkeypatch.setattr(production, "_historical_verify_runtime", lambda *_a, **_k: "runtime")


def test_private_same_boot_reconciliation_uses_existing_flag_and_immutable_audit(
    artifacts, identity, monkeypatch
):
    attempt = _private_uncertain_attempt(artifacts, identity)
    _mock_private_recovery_runtime(monkeypatch, attempt)
    monkeypatch.setattr(production, "_pid_exists", lambda _pid: False)
    monkeypatch.setattr(production, "_process_group_exists", lambda _pgid: False)
    monkeypatch.setattr(
        production.os,
        "kill",
        lambda *_a, **_k: pytest.fail("private reconciliation must not signal a PID"),
    )
    monkeypatch.setattr(
        production.os,
        "killpg",
        lambda *_a, **_k: pytest.fail("private reconciliation must not signal a process group"),
    )

    report, code = production.run_production(
        attempt["directory"],
        historical=True,
        capture_id=attempt["identity"]["capture_id"],
        reconcile_stopped_worker=True,
    )
    assert code == 0
    assert report["phase"] == "stopped"
    assert report["active_process_group"] is None
    assert report["private_historical_grant"]["state"] == "reconciled"
    reconciliation = report["private_historical_reconciliation"]
    assert reconciliation["kind"] == "same_boot_worker_absence"
    assert reconciliation["reconciliation_worker_started"] is False
    assert reconciliation["new_grant_started"] is False
    record = grants.read_json(
        attempt["control_root"] / "grants" / attempt["grant_id"] / "private-reconciliation.json"
    )
    assert record["kind"] == "same_boot_worker_absence"
    assert record["processes_proven_absent"] is True
    assert record["sha256"] == grants.sha256_json(
        {key: value for key, value in record.items() if key != "sha256"}
    )


@pytest.mark.parametrize("failure", ["launch", "volume", "live-controller"])
def test_private_same_boot_reconciliation_refusal_preserves_report_and_audit(
    artifacts, identity, monkeypatch, failure
):
    attempt = _private_uncertain_attempt(artifacts, identity)
    _mock_private_recovery_runtime(monkeypatch, attempt)
    if failure == "launch":
        attempt["report"]["private_historical_grant"]["launch_sha256"] = "0" * 64
        production._atomic_report(attempt["report_path"], attempt["report"])
    elif failure == "volume":
        monkeypatch.setattr(
            production, "_volume_uuid", lambda _path: "00000000-0000-0000-0000-000000000001"
        )
    else:
        monkeypatch.setattr(
            production,
            "_pid_exists",
            lambda pid: pid == attempt["controller_pid"],
        )
        monkeypatch.setattr(production, "_process_group_exists", lambda _pgid: False)
    monkeypatch.setattr(
        production.os,
        "kill",
        lambda *_a, **_k: pytest.fail("private reconciliation must not signal a PID"),
    )
    monkeypatch.setattr(
        production.os,
        "killpg",
        lambda *_a, **_k: pytest.fail("private reconciliation must not signal a process group"),
    )
    before = attempt["report_path"].read_bytes()
    with pytest.raises(production.RunnerError):
        production.run_production(
            attempt["directory"],
            historical=True,
            capture_id=attempt["identity"]["capture_id"],
            reconcile_stopped_worker=True,
        )
    assert attempt["report_path"].read_bytes() == before
    assert not (
        attempt["control_root"] / "grants" / attempt["grant_id"] / "private-reconciliation.json"
    ).exists()


def test_private_same_boot_reconciliation_rejects_live_supervisor_group(
    artifacts, identity, monkeypatch
):
    attempt = _private_uncertain_attempt(artifacts, identity)
    marker = attempt["report"]["private_historical_grant"]
    marker["controller_pgid"] = marker["controller_pid"] + 17
    production._atomic_report(attempt["report_path"], attempt["report"])
    _mock_private_recovery_runtime(monkeypatch, attempt)
    monkeypatch.setattr(production, "_pid_exists", lambda _pid: False)
    monkeypatch.setattr(
        production,
        "_process_group_exists",
        lambda pgid: pgid == marker["controller_pgid"],
    )
    before = attempt["report_path"].read_bytes()
    with pytest.raises(production.RunnerError):
        production.run_production(
            attempt["directory"],
            historical=True,
            capture_id=attempt["identity"]["capture_id"],
            reconcile_stopped_worker=True,
        )
    assert attempt["report_path"].read_bytes() == before


@pytest.mark.parametrize("abrupt_controller_exit", [False, True])
def test_private_changed_boot_recovery_uses_existing_flags_and_ignores_old_pids(
    artifacts, identity, monkeypatch, abrupt_controller_exit
):
    attempt = _private_uncertain_attempt(artifacts, identity, boot_stamp="boot-old")
    if abrupt_controller_exit:
        marker = attempt["report"]["private_historical_grant"]
        marker.pop("reason")
        marker["state"] = "supervised-child-pending"
        attempt["report"]["phase"] = "historical-readiness-prepare"
        production._atomic_report(attempt["report_path"], attempt["report"])
    _mock_private_recovery_runtime(monkeypatch, attempt, current_boot="boot-new")
    monkeypatch.delenv("PYTHONDONTWRITEBYTECODE", raising=False)
    monkeypatch.setattr(
        production,
        "_pid_exists",
        lambda _pid: pytest.fail("old-boot PID must not be queried"),
    )
    monkeypatch.setattr(
        production,
        "_process_group_exists",
        lambda _pgid: pytest.fail("old-boot process group must not be queried"),
    )
    snapshot = {
        "root_id": attempt["identity"]["root_id"],
        "accounting_complete": True,
        "unfinished_reservations": [],
        "attempts": 12,
        "downloaded_or_reserved_bytes": 4096,
    }

    def read_accounting(*_args):
        assert os.environ.get("PYTHONDONTWRITEBYTECODE") == "1"
        return snapshot

    monkeypatch.setattr(production, "_read_accounting_worker", read_accounting)

    report, code = production.run_production(
        attempt["directory"],
        historical=True,
        capture_id=attempt["identity"]["capture_id"],
        reconcile_accounting=True,
        recover_reboot=True,
        recovery_volume_uuid=attempt["volume_uuid"],
    )
    assert code == 0
    assert report["boot_stamp"] == "boot-new"
    assert report["phase"] == "stopped"
    assert report["active_process_group"] is None
    assert report["private_historical_grant"]["state"] == "reconciled-after-boot-change"
    assert report["reboot_recovery_history"][-1]["prior_process_group_ignored_after_boot_change"]
    assert report["reboot_recovery_history"][-1]["processing_worker_started"] is False
    assert report["reboot_recovery_history"][-1]["grant_started"] is False
    assert report["historical_reboot_reconciliation"]["processing_worker_started"] is False
    record = grants.read_json(
        attempt["control_root"] / "grants" / attempt["grant_id"] / "private-reconciliation.json"
    )
    assert record["kind"] == "changed_boot_worker_ignored"
    assert record["process_group_interaction"] == "not_queried_after_boot_change"
    assert record["previous_active_process_group"] == attempt["worker_pid"]


def test_reboot_remount_keeps_frozen_origin_and_allows_fresh_grant_preflight(
    artifacts, identity, monkeypatch
):
    from types import SimpleNamespace

    old_device = artifacts.stat().st_dev + 101
    current_device = artifacts.stat().st_dev
    actual_path_device = production._historical_path_device
    attempt = _private_uncertain_attempt(
        artifacts,
        identity,
        boot_stamp="boot-old",
        freeze_root_device=old_device,
        saved_root_device=old_device + 1,
        mode="full",
    )

    def mounted_device(path):
        path = Path(path)
        if path == attempt["directory"] or path == attempt["directory"] / "operator":
            return current_device
        if path == attempt["control_root"]:
            return current_device
        return actual_path_device(path)

    _mock_private_recovery_runtime(monkeypatch, attempt, current_boot="boot-new")
    monkeypatch.setattr(production, "_historical_path_device", mounted_device)
    monkeypatch.setattr(production, "_owner_root_id", lambda _path: identity["root_id"])
    monkeypatch.setattr(
        production,
        "_pid_exists",
        lambda _pid: pytest.fail("old-boot PID must not be queried"),
    )
    monkeypatch.setattr(
        production,
        "_process_group_exists",
        lambda _pgid: pytest.fail("old-boot process group must not be queried"),
    )
    snapshot = {
        "root_id": identity["root_id"],
        "accounting_complete": True,
        "unfinished_reservations": [],
        "attempts": 12,
        "downloaded_or_reserved_bytes": 4096,
    }
    monkeypatch.setattr(production, "_read_accounting_worker", lambda *_a: snapshot)
    report, code = production.run_production(
        attempt["directory"],
        historical=True,
        capture_id=identity["capture_id"],
        reconcile_accounting=True,
        recover_reboot=True,
        recovery_volume_uuid=attempt["volume_uuid"],
    )
    assert code == 0
    assert report["mode"] == "full"
    assert report["system"]["filesystem_device"] == current_device
    assert report["original_system"]["filesystem_device"] == old_device
    assert report["reboot_recovery_history"][-1]["previous_filesystem_device"] == old_device + 1
    assert report["reboot_recovery_history"][-1]["filesystem_device"] == current_device
    assert attempt["freeze"]["root_device"] == old_device

    grant_id, approval = _write_prelaunch_grant(attempt, root_device=current_device)
    baseline = {
        "database_bytes": grants.GIB,
        "temporary_bytes": 4 * grants.GIB,
        "root_bytes": 4 * grants.GIB,
        "aggregate_bytes": 5 * grants.GIB,
        "free_bytes": 200 * grants.GIB,
        "inventory_entries": 20,
        "peak_rss_bytes": grants.GIB,
    }
    source_snapshot = {"phase": "historical-source-snapshot", "sha256": "5" * 64}
    monkeypatch.setattr(production, "_historical_operational_source_sha256", lambda _root: "4" * 64)
    monkeypatch.setattr(production, "_historical_verify_runtime", lambda *_a, **_k: "runtime")
    monkeypatch.setattr(
        production,
        "_historical_store",
        lambda *_a, **_k: SimpleNamespace(root=attempt["directory"] / "operator"),
    )
    monkeypatch.setattr(production, "_root_id", lambda _store: identity["root_id"])
    monkeypatch.setattr(
        production, "_historical_source_snapshot", lambda *_a, **_k: source_snapshot
    )
    monkeypatch.setattr(production, "_historical_resource_baseline", lambda *_a, **_k: baseline)
    launch_sha256, session_binding = _launch_test_grant(
        attempt, grant_id, approval, root_device=current_device
    )
    authority = production._historical_authority(
        attempt["directory"],
        attempt["control_root"],
        grant_id,
        launch_sha256=launch_sha256,
        require_worker=False,
        prelaunch=False,
        expected_phase="source",
    )
    assert authority["freeze"]["root_device"] == old_device
    assert authority["current_mount"]["device"] == current_device
    assert authority["measurement"]["root_device"] == current_device
    assert authority["approval"]["grant_id"] == grant_id
    assert session_binding["root_device"] == current_device
    assert approval["measurements"]["session_report_sha256"] == session_binding["report_sha256"]
    assert authority["supervised_report_phase"] is None

    report = authority["report"]
    worker_pid = os.getpid()
    supervisor_pid = os.getppid()
    storage_token = "b" * 32
    progress = attempt["report_path"].with_name("test-storage.json")
    running = report["private_historical_grant"]
    running.update(
        state="supervised-child-pending",
        substep="maintenance",
        worker_pid=worker_pid,
        worker_pgid=worker_pid,
        controller_pid=supervisor_pid,
        storage_invocation_token=storage_token,
        storage_progress_path=str(progress),
    )
    report.update(
        phase="historical-source-maintenance",
        active_process_group=worker_pid,
        storage_supervision_invocation=storage_token,
        storage_supervision_file=str(progress),
    )
    monkeypatch.setattr(production, "_historical_validate_storage_handoff", lambda *_a, **_k: {})

    def worker_authority():
        production._atomic_report(attempt["report_path"], report)
        return production._historical_authority(
            attempt["directory"],
            attempt["control_root"],
            grant_id,
            launch_sha256=launch_sha256,
            require_worker=True,
            expected_phase="source",
            expected_substep="maintenance",
            storage_invocation_token=storage_token,
            storage_progress_path=progress,
            supervisor_pid=supervisor_pid,
        )

    assert worker_authority()["supervised_report_phase"] == "historical-source-maintenance"
    for record, field, replacement in (
        (report, "phase", "historical-source-snapshot"),
        (report, "active_process_group", worker_pid + 1),
        (running, "state", "stopped"),
        (running, "grant_id", "f" * 32),
        (running, "launch_id", "f" * 32),
        (running, "launch_sha256", "f" * 64),
        (running, "control_root", "/wrong-root"),
        (running, "worker_pid", worker_pid + 1),
        (running, "worker_pgid", worker_pid + 1),
        (running, "controller_pid", supervisor_pid + 1),
    ):
        original, record[field] = record[field], replacement
        with pytest.raises(production.RunnerError, match="not the supervised child"):
            worker_authority()
        record[field] = original


def _reboot_mount_report(attempt, current_device):
    identity = attempt["identity"]
    freeze = attempt["freeze"]
    started_epoch = 1_800_000_000.0
    deadline_epoch = started_epoch + 12 * 60 * 60
    budget_started_epoch = started_epoch + 1
    reconciliation_sha = "a" * 64
    deadlines = {
        "started_epoch": started_epoch,
        "time_budget_started_epoch": budget_started_epoch,
        "deadline_epoch": deadline_epoch,
    }
    event = {
        "previous_boot_stamp": "boot-old",
        "boot_stamp": "boot-new",
        "previous_filesystem_device": freeze["root_device"],
        "filesystem_device": current_device,
        "volume_uuid": freeze["root_volume_uuid"],
        "previous_active_process_group": 987_654,
        "prior_process_group_ignored_after_boot_change": True,
        "accounting_basis": "exact",
        "accounting_complete": True,
        "processing_worker_started": False,
        "grant_started": False,
        "wall_deadlines_preserved": deadlines,
        "accounting_sha256": "b" * 64,
        "session_before_sha256": "c" * 64,
        "private_worker_reconciliation_sha256": reconciliation_sha,
    }
    return {
        "capture_id": identity["capture_id"],
        "root_id": identity["root_id"],
        "boot_stamp": "boot-new",
        "started_epoch": started_epoch,
        "deadline_epoch": deadline_epoch,
        "time_budget_started_epoch": budget_started_epoch,
        "system": {
            "filesystem_device": current_device,
            "filesystem_volume_uuid": freeze["root_volume_uuid"],
        },
        "original_system": {
            "filesystem_device": freeze["root_device"],
            "filesystem_volume_uuid": freeze["root_volume_uuid"],
        },
        "accounting_complete": True,
        "unfinished_reservation_ids": [],
        "consumption_state": "verified",
        "reboot_recovery_history": [event],
        "historical_reboot_reconciliation": {
            "state": "complete",
            "capture_id": identity["capture_id"],
            "root_id": identity["root_id"],
            "boot_stamp": "boot-new",
            "accounting_basis": "exact",
            "processing_worker_started": False,
            "grant_started": False,
            "private_worker_reconciliation_sha256": reconciliation_sha,
        },
    }


@pytest.mark.parametrize(
    "failure",
    ["unexplained-device", "wrong-volume", "split-device", "wrong-root", "wrong-boot"],
)
def test_current_mount_rejects_unproven_or_mixed_mounts(artifacts, identity, monkeypatch, failure):
    attempt = _private_uncertain_attempt(artifacts, identity)
    current_device = artifacts.stat().st_dev + 303
    original_device = production._historical_path_device

    def device(path):
        path = Path(path)
        if path == attempt["directory"]:
            return current_device if failure == "unexplained-device" else original_device(path)
        if path == attempt["directory"] / "operator":
            if failure == "split-device":
                return current_device + 1
            return current_device if failure == "unexplained-device" else original_device(path)
        if path == attempt["control_root"]:
            return current_device if failure == "unexplained-device" else original_device(path)
        return original_device(path)

    monkeypatch.setattr(production, "_historical_path_device", device)
    monkeypatch.setattr(production, "_boot_stamp", lambda: "boot-current")
    monkeypatch.setattr(production, "_volume_uuid", lambda _path: attempt["volume_uuid"])
    monkeypatch.setattr(production, "_owner_root_id", lambda _path: identity["root_id"])
    report = dict(attempt["report"])
    if failure == "wrong-volume":
        monkeypatch.setattr(
            production, "_volume_uuid", lambda _path: "00000000-0000-0000-0000-000000000001"
        )
    elif failure == "wrong-root":
        report["root_id"] = "0" * 32
    elif failure == "wrong-boot":
        report["boot_stamp"] = "boot-old"
    with pytest.raises(production.RunnerError):
        production._historical_current_mount(
            attempt["directory"],
            attempt["directory"] / "operator",
            attempt["control_root"],
            report,
            identity,
            attempt["freeze"],
            current_boot="boot-current",
        )


@pytest.mark.parametrize(
    "failure", ["latest-device", "latest-boot", "incomplete-accounting", "root", "session-device"]
)
def test_current_mount_rejects_tampered_reboot_proof(artifacts, identity, monkeypatch, failure):
    attempt = _private_uncertain_attempt(artifacts, identity)
    current_device = artifacts.stat().st_dev + 404
    original_device = production._historical_path_device

    def device(path):
        path = Path(path)
        if path in {
            attempt["directory"],
            attempt["directory"] / "operator",
            attempt["control_root"],
        }:
            return current_device
        return original_device(path)

    monkeypatch.setattr(production, "_historical_path_device", device)
    monkeypatch.setattr(production, "_boot_stamp", lambda: "boot-new")
    monkeypatch.setattr(production, "_volume_uuid", lambda _path: attempt["volume_uuid"])
    monkeypatch.setattr(production, "_owner_root_id", lambda _path: identity["root_id"])
    report = _reboot_mount_report(attempt, current_device)
    event = report["reboot_recovery_history"][-1]
    if failure == "latest-device":
        event["filesystem_device"] += 1
    elif failure == "latest-boot":
        event["boot_stamp"] = "boot-stale"
    elif failure == "incomplete-accounting":
        report["accounting_complete"] = False
    elif failure == "root":
        report["root_id"] = "0" * 32
    session_device = current_device + 1 if failure == "session-device" else current_device
    with pytest.raises(production.RunnerError):
        production._historical_current_mount(
            attempt["directory"],
            attempt["directory"] / "operator",
            attempt["control_root"],
            report,
            identity,
            attempt["freeze"],
            current_boot="boot-new",
            session_root_device=session_device,
        )


@pytest.mark.parametrize("failure", ["volume", "incomplete-accounting"])
def test_private_changed_boot_recovery_refusal_preserves_report(
    artifacts, identity, monkeypatch, failure
):
    attempt = _private_uncertain_attempt(artifacts, identity, boot_stamp="boot-old")
    _mock_private_recovery_runtime(monkeypatch, attempt, current_boot="boot-new")
    if failure == "volume":
        volume_uuid = "00000000-0000-0000-0000-000000000001"
    else:
        volume_uuid = attempt["volume_uuid"]
        monkeypatch.setattr(
            production,
            "_read_accounting_worker",
            lambda *_a: {
                "root_id": attempt["identity"]["root_id"],
                "accounting_complete": False,
                "unfinished_reservations": ["reservation"],
                "attempts": 12,
                "downloaded_bytes": 4096,
            },
        )
    before = attempt["report_path"].read_bytes()
    with pytest.raises(production.RunnerError):
        production.run_production(
            attempt["directory"],
            historical=True,
            capture_id=attempt["identity"]["capture_id"],
            reconcile_accounting=True,
            recover_reboot=True,
            recovery_volume_uuid=volume_uuid,
        )
    assert attempt["report_path"].read_bytes() == before
    assert not (
        attempt["control_root"] / "grants" / attempt["grant_id"] / "private-reconciliation.json"
    ).exists()


@pytest.mark.parametrize("mode", ["full", "resume", "historical"])
def test_changed_boot_private_authority_accepts_supported_saved_modes_without_pid_queries(
    artifacts, identity, monkeypatch, mode
):
    attempt = _private_uncertain_attempt(artifacts, identity, boot_stamp="boot-old", mode=mode)
    _mock_private_recovery_runtime(monkeypatch, attempt, current_boot="boot-new")
    monkeypatch.setattr(
        production,
        "_pid_exists",
        lambda _pid: pytest.fail("old-boot PID must not be queried"),
    )
    monkeypatch.setattr(
        production,
        "_process_group_exists",
        lambda _pgid: pytest.fail("old-boot process group must not be queried"),
    )
    proof = production._historical_private_uncertain_authority(
        attempt["directory"],
        attempt["report_path"],
        attempt["report"],
        current_boot="boot-new",
        changed_boot=True,
    )
    assert proof["changed_boot"] is True
    assert proof["marker"]["worker_pgid"] == attempt["worker_pid"]
    assert attempt["report"]["mode"] == mode


@pytest.mark.parametrize(
    ("mode", "include_mode"),
    [(None, True), ("activate-current", True), (None, False)],
    ids=["null-mode", "unsupported-mode", "missing-mode"],
)
def test_changed_boot_private_authority_rejects_unsupported_saved_modes(
    artifacts, identity, mode, include_mode
):
    attempt = _private_uncertain_attempt(
        artifacts,
        identity,
        boot_stamp="boot-old",
        mode=mode,
        include_mode=include_mode,
    )
    with pytest.raises(production.RunnerError, match="not an exact private uncertain"):
        production._historical_private_uncertain_authority(
            attempt["directory"],
            attempt["report_path"],
            attempt["report"],
            current_boot="boot-new",
            changed_boot=True,
        )


def test_initial_historical_accounting_accepts_original_resume_mode(tmp_path, monkeypatch):
    directory, operator, store, report_path, capture_id, root_id = _resource_baseline_workspace(
        tmp_path, production
    )
    revision = "f" * 40
    report = {
        "version": 1,
        "mode": "resume",
        "session_id": report_path.parent.name,
        "capture_id": capture_id,
        "root_id": root_id,
        "revision": revision,
        "active_process_group": 1234,
    }
    production._atomic_report(report_path, report)
    monkeypatch.setattr(production.os, "getpgrp", lambda: 1234)
    monkeypatch.setattr(production.os, "getpid", lambda: 1234)
    validated = []
    monkeypatch.setattr(
        production,
        "_validate_offline_renewal_root",
        lambda *args, **kwargs: validated.append((args, kwargs)),
    )

    result = production._validate_initial_historical_accounting_session(
        operator,
        directory,
        report_path,
        capture_id,
        root_id,
        revision,
    )

    assert result["mode"] == "resume"
    assert len(validated) == 1
    assert store.path("writer.lock").is_file()


@pytest.mark.parametrize(
    ("mode", "include_mode"),
    [("activate-current", True), (None, False)],
    ids=["unsupported-mode", "missing-mode"],
)
def test_initial_historical_accounting_rejects_unsupported_modes(
    tmp_path, monkeypatch, mode, include_mode
):
    directory, operator, _store, report_path, capture_id, root_id = _resource_baseline_workspace(
        tmp_path, production
    )
    revision = "f" * 40
    report = {
        "version": 1,
        "mode": mode,
        "session_id": report_path.parent.name,
        "capture_id": capture_id,
        "root_id": root_id,
        "revision": revision,
        "active_process_group": 1234,
    }
    if not include_mode:
        report.pop("mode")
    production._atomic_report(report_path, report)
    monkeypatch.setattr(production.os, "getpgrp", lambda: 1234)
    monkeypatch.setattr(production.os, "getpid", lambda: 1234)
    monkeypatch.setattr(
        production,
        "_validate_offline_renewal_root",
        lambda *_args, **_kwargs: pytest.fail("invalid mode must fail before root validation"),
    )

    with pytest.raises(production.RunnerError, match="session binding changed"):
        production._validate_initial_historical_accounting_session(
            operator,
            directory,
            report_path,
            capture_id,
            root_id,
            revision,
        )


def test_frozen_sibling_controls_layout_and_runner_pin_detect_script_tampering(
    artifacts, monkeypatch
):
    runtime = artifacts / "runtime"
    control_root = runtime / "kalshi-historical-completion-20990101"
    source_root = control_root / "source"
    (source_root / "scripts").mkdir(parents=True, mode=0o700)
    (source_root / "src" / "kalshi_events").mkdir(parents=True, mode=0o700)
    (control_root / "controls").mkdir(parents=True, mode=0o700)
    runtime.chmod(0o700)
    control_root.chmod(0o700)
    for relative, body in (
        ("scripts/production_run.py", "runner frozen one\n"),
        ("scripts/historical_grants.py", "grants frozen\n"),
        ("scripts/historical_admission.py", "authorized census helper frozen\n"),
        ("scripts/production_activation.py", "activation frozen\n"),
        ("src/kalshi_events/__init__.py", "package frozen\n"),
        ("pyproject.toml", "[project]\nname='fixture'\n"),
        ("uv.lock", "version = 1\n"),
    ):
        path = source_root / relative
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.write_text(body)
    helper = control_root / "controls" / "runtime_manifest.py"
    helper.write_text("trusted helper frozen\n")
    helper.chmod(0o400)
    monkeypatch.setattr(production, "__file__", str(source_root / "scripts" / "production_run.py"))
    monkeypatch.setattr(production, "SOURCE_ROOT", source_root)
    monkeypatch.setattr(production, "HISTORICAL_HOST_PYTHON", Path(__file__).resolve())

    assert production._historical_control_root(control_root) == control_root.resolve()
    assert production._historical_venv_root(control_root) == control_root / ".venv"
    before = production._historical_operational_source_sha256(control_root)
    admission = source_root / "scripts" / "historical_admission.py"
    admission.write_text("authorized census helper changed\n")
    after_admission = production._historical_operational_source_sha256(control_root)
    assert before != after_admission
    runner = source_root / "scripts" / "production_run.py"
    runner.write_text("runner frozen two\n")
    after = production._historical_operational_source_sha256(control_root)
    assert after_admission != after


def test_private_controller_lock_excludes_a_concurrent_legacy_runner(artifacts):
    directory = artifacts / "production"
    directory.mkdir(mode=0o700)
    with (
        production._run_lock(directory),
        pytest.raises(production.RunnerError, match="another production runner"),
        production._run_lock(directory),
    ):
        pytest.fail("second production runner entered a held private controller lock")


def test_successful_subset_grant_still_requires_approval_for_next_phase(
    artifacts, identity, monkeypatch
):
    approval, latch, launch, _epoch, _monotonic = _grant(
        identity, phase="source", phases=["source"]
    )
    grant_root = artifacts / "grants" / approval["grant_id"]
    grants.write_once_json(grant_root / "approval.json", approval)
    grants.write_once_json(grant_root / "latch.json", latch)
    binding = {
        "version": 1,
        "grant_id": approval["grant_id"],
        "approval_sha256": grants.sha256_json(approval),
        "capture_id": identity["capture_id"],
        "session_id": identity["session_id"],
        "root_id": identity["root_id"],
        "stage_name": identity["stage_name"],
        "report_sha256": approval["measurements"]["session_report_sha256"],
        "report_path": str(artifacts / "session.json"),
        "boot_stamp": launch["boot_stamp"],
        "root_volume_uuid": "ACAD66E7-42DA-426B-B20D-FB87A453396B",
        "root_device": 42,
    }
    grants.write_once_json(grant_root / "session-binding.json", binding)
    launch["session_binding_sha256"] = grants.sha256_json(binding)
    launch_sha = grants.write_once_json(grant_root / "launch.json", launch)
    source = _seal("source", identity)
    source["grant_id"] = approval["grant_id"]
    source["completed_at_epoch"] = launch["started_epoch"] + 1
    grants.write_phase_seal(artifacts, source, bindings=identity)

    monkeypatch.setattr(production, "_historical_source_root", lambda _root: ROOT)
    monkeypatch.setattr(production, "_historical_venv_root", lambda _root: ROOT / ".venv")
    production._historical_stop_record(
        artifacts / "production",
        artifacts,
        approval["grant_id"],
        launch_sha,
        outcome="complete",
        phase="source",
        substep=None,
        completed_phases=["source"],
        reason=None,
        bindings=identity,
        root_volume_uuid="ACAD66E7-42DA-426B-B20D-FB87A453396B",
    )
    result = grants.read_json(grant_root / "attempt-result.json")
    assert result["outcome"] == "complete"
    assert result["next_phase"] == "readiness"
    assert result["last_durable_phase"] == "source"
    assert result["last_durable_phase_sha256"] == grants.sha256_json(source)
    assert result["new_approval_required"] is True
    assert result["remaining_phases"][0] == "readiness"
    assert result["proposed_grant_status"] == "unapproved"
    assert result["proposed_grant_id"] not in {None, approval["grant_id"]}
    assert result["next_command_argv"] == [
        str(ROOT / ".venv" / "bin" / "python"),
        "-B",
        str(ROOT / "scripts" / "production_run.py"),
        "--_historical-control",
        str((artifacts / "production").resolve()),
        str(artifacts.resolve()),
        result["proposed_grant_id"],
    ]
    assert result["next_command"]
    assert not (artifacts / "grants" / result["proposed_grant_id"]).exists()


def test_uncertain_attempt_records_exact_supported_reconciliation_argv(
    artifacts, identity, monkeypatch
):
    directory = artifacts / "production"
    approval, latch, launch, _epoch, _monotonic = _grant(
        identity, phase="source", phases=["source", "readiness"]
    )
    grant_root = artifacts / "grants" / approval["grant_id"]
    grants.write_once_json(grant_root / "approval.json", approval)
    grants.write_once_json(grant_root / "latch.json", latch)
    binding = {
        "version": 1,
        "grant_id": approval["grant_id"],
        "approval_sha256": grants.sha256_json(approval),
        "capture_id": identity["capture_id"],
        "session_id": identity["session_id"],
        "root_id": identity["root_id"],
        "stage_name": identity["stage_name"],
        "report_sha256": approval["measurements"]["session_report_sha256"],
        "report_path": str((artifacts / "session.json").resolve()),
        "boot_stamp": launch["boot_stamp"],
        "root_volume_uuid": "ACAD66E7-42DA-426B-B20D-FB87A453396B",
        "root_device": 42,
    }
    grants.write_once_json(grant_root / "session-binding.json", binding)
    launch["session_binding_sha256"] = grants.sha256_json(binding)
    launch_sha = grants.write_once_json(grant_root / "launch.json", launch)
    source = _seal("source", identity)
    source["grant_id"] = approval["grant_id"]
    source["completed_at_epoch"] = launch["started_epoch"] + 1
    grants.write_phase_seal(artifacts, source, bindings=identity)

    monkeypatch.setattr(production, "_historical_source_root", lambda _root: ROOT)
    monkeypatch.setattr(production, "_historical_venv_root", lambda _root: ROOT / ".venv")
    production._historical_stop_record(
        directory,
        artifacts,
        approval["grant_id"],
        launch_sha,
        outcome="uncertain",
        phase="readiness",
        substep="prepare",
        completed_phases=["source"],
        reason="child outcome unclear",
        bindings=identity,
        root_volume_uuid="ACAD66E7-42DA-426B-B20D-FB87A453396B",
    )
    result = grants.read_json(grant_root / "attempt-result.json")
    assert result["reconciliation_required"] is True
    assert result["next_phase"] == "readiness"
    assert result["last_durable_phase"] == "source"
    assert isinstance(result["last_durable_phase_sha256"], str)
    assert result["proposed_grant_status"] == "unapproved"
    assert result["next_command_argv"] == [
        str(ROOT / ".venv" / "bin" / "python"),
        "-B",
        str(ROOT / "scripts" / "production_run.py"),
        "--directory",
        str(directory.resolve()),
        "--capture-id",
        identity["capture_id"],
        "--historical",
        "--reconcile-stopped-worker",
    ]
    assert result["changed_boot_reconciliation_command_argv"] == [
        str(ROOT / ".venv" / "bin" / "python"),
        "-B",
        str(ROOT / "scripts" / "production_run.py"),
        "--directory",
        str(directory.resolve()),
        "--capture-id",
        identity["capture_id"],
        "--historical",
        "--reconcile-accounting",
        "--recover-reboot",
        "--recovery-volume-uuid",
        "ACAD66E7-42DA-426B-B20D-FB87A453396B",
    ]


def test_actual_readiness_adoption_and_late_retirement_preserve_phase_origins(
    artifacts, tmp_path, monkeypatch
):
    import duckdb
    from conftest import FakeClient

    from kalshi_events import capture as acquisition
    from kalshi_events import warehouse
    from kalshi_events.common import Store, digest, json_bytes
    from kalshi_events.incremental import root_id as ensure_root_id

    store = Store(tmp_path / "operator")

    def client_factory(active_store, budget, **options):
        return FakeClient(active_store, budget, {}, **options)

    capture_state = acquisition.capture(store, client_factory=client_factory)
    state = warehouse.read_capture(store, capture_state["capture_id"])
    capture_id = state["capture_id"]
    with store.writer_guard():
        ensure_root_id(store, create=True)
    warehouse.build_historical(store, capture_id)
    stage_name = "build-historical-" + uuid.uuid4().hex
    stage_relative = f"tmp/{stage_name}"
    stage = store.path(stage_relative)
    stage.mkdir(parents=True)
    source = stage / "warehouse.duckdb"
    seed_build = store.path(f"builds-historical/{capture_id}")
    shutil.copyfile(seed_build / "warehouse.duckdb", source)

    # Exercise the real source WAL recovery and partial-unlink retry path.
    snapshot_db = store.path("tmp/readiness-origin-source.duckdb")
    shutil.copyfile(source, snapshot_db)
    with duckdb.connect(str(snapshot_db)) as connection:
        connection.execute("begin transaction")
        connection.execute("create table retirement_origin_probe(value bigint)")
        connection.execute("insert into retirement_origin_probe values (1)")
        connection.execute("commit")
        wal_path = snapshot_db.with_name(snapshot_db.name + ".wal")
        assert wal_path.is_file()
        shutil.copyfile(snapshot_db, source)
        shutil.copyfile(wal_path, source.with_name(source.name + ".wal"))
    shutil.rmtree(seed_build)
    store.sync(stage_relative)

    source_snapshot = warehouse.historical_readiness_snapshot(store, capture_id, stage_name)
    current_device = store.root.lstat().st_dev
    origin_device = current_device + 17
    frozen_result = dict(source_snapshot["result"])
    for identity_key in ("source_identity", "source_wal_identity"):
        file_identity = frozen_result.get(identity_key)
        if file_identity is not None:
            frozen_result[identity_key] = [origin_device, *file_identity[1:]]
    frozen_body = {
        "version": source_snapshot["version"],
        "phase": source_snapshot["phase"],
        "binding": source_snapshot["binding"],
        "result": frozen_result,
    }
    source_snapshot = frozen_body | {"sha256": digest(json_bytes(frozen_body))}
    verified_mount = {
        "root_path": str(store.root.resolve()),
        "root_id": production._root_id(store),
        "capture_id": capture_id,
        "current_device": current_device,
        "device_lineage": [origin_device, current_device],
    }
    current_snapshot = warehouse.historical_readiness_snapshot(
        store, capture_id, stage_name, _verified_mount=verified_mount
    )
    assert current_snapshot["result"]["source_identity"][0] == current_device
    assert source_snapshot["result"]["source_identity"][0] == origin_device
    state_sha = digest(json_bytes(dict(state)))
    root_id = "c" * 32
    session_id = "b" * 32
    identity = {
        "capture_id": capture_id,
        "session_id": session_id,
        "root_id": root_id,
        "stage_name": stage_name,
        "failed_attempts_sha256": "e" * 64,
        "prior_session_sha256": "f" * 64,
        "state_sha256": state_sha,
        "source_sha256": source_snapshot["result"]["source_sha256"],
        "runtime_sha256": "3" * 64,
    }
    control_root = artifacts / "actual-controls"
    for relative in ("source", "grants", "phases"):
        (control_root / relative).mkdir(parents=True, mode=0o700)
    control_root.chmod(0o700)
    source_snapshot_path = control_root / "source" / "source-snapshot.json"
    grants.write_once_json(source_snapshot_path, source_snapshot)

    census = {
        "population_report_sha256": "a" * 64,
        "receipt_index_sha256": "b" * 64,
        "candidate_pages": 1,
        "observations_by_endpoint": {"/series": 0, "/events": 0, "/historical/markets": 0},
        "nested_event_markets": 0,
        "exact_observations": 0,
    }
    census_control = census | {
        "population_report_path": str((artifacts / "population.json").resolve()),
        "receipt_index_path": str((artifacts / "candidate-pages.json").resolve()),
        "capture_id": capture_id,
        "root_id": root_id,
        "capture_state_sha256": state_sha,
    }
    control = {"authorized_census": census_control}
    freeze = {
        "state_sha256": state_sha,
        "source_sha256": source_snapshot["result"]["source_sha256"],
        "source_snapshot_sha256": source_snapshot["sha256"],
        "receipt_evidence_sha256": source_snapshot["result"]["receipt_evidence_sha256"],
        "scope_sha256": source_snapshot["binding"]["scope_sha256"],
        "code_sha256": source_snapshot["binding"]["code_sha256"],
        "root_volume_uuid": "ACAD66E7-42DA-426B-B20D-FB87A453396B",
        "root_device": origin_device,
    }

    def create_grant(phase, phases, *, retire_source=False):
        grant_id = uuid.uuid4().hex
        approval, latch, launch, _epoch, _monotonic = _grant(
            identity,
            grant_id=grant_id,
            phase=phase,
            phases=phases,
            retire_source=retire_source,
            retirement_policy=(
                "coexistence-capacity-relief-approved-v1" if retire_source else None
            ),
        )
        grant_root = control_root / "grants" / grant_id
        grant_root.mkdir(parents=True, mode=0o700)
        binding = {
            "version": 1,
            "grant_id": grant_id,
            "approval_sha256": grants.sha256_json(approval),
            "capture_id": capture_id,
            "session_id": session_id,
            "root_id": root_id,
            "stage_name": stage_name,
            "report_sha256": approval["measurements"]["session_report_sha256"],
            "report_path": str((artifacts / "runs" / session_id / "session.json").resolve()),
            "boot_stamp": launch["boot_stamp"],
            "root_volume_uuid": freeze["root_volume_uuid"],
            "root_device": current_device,
        }
        grants.write_once_json(grant_root / "approval.json", approval)
        grants.write_once_json(grant_root / "latch.json", latch)
        grants.write_once_json(grant_root / "session-binding.json", binding)
        launch["session_binding_sha256"] = grants.sha256_json(binding)
        launch_sha = grants.write_once_json(grant_root / "launch.json", launch)
        return {
            "identity": identity,
            "control_root": control_root,
            "store": store,
            "source_snapshot": source_snapshot,
            "freeze": freeze,
            "verified_mount": verified_mount,
            "control": control,
            "approval": approval,
            "grant": {"grant_id": grant_id, "phase": phase},
            "launch": launch,
            "launch_sha256": launch_sha,
        }

    monkeypatch.setattr(
        production,
        "_historical_authorized_census",
        lambda _authority, *, full_revalidation: census,
    )
    readiness_authority = create_grant("source", ["source", "readiness"])
    source_seal = _seal("source", identity)
    source_seal["grant_id"] = readiness_authority["grant"]["grant_id"]
    source_seal["completed_at_epoch"] = readiness_authority["launch"]["started_epoch"] + 1
    grants.write_phase_seal(
        control_root,
        source_seal,
        bindings=identity,
        grant_launch=readiness_authority["launch"],
    )
    initial_prepared = warehouse.prepare_historical_readiness(
        store, capture_id, stage_name, _verified_mount=verified_mount
    )
    exact_observations = initial_prepared["observation_count"]
    census["exact_observations"] = exact_observations
    census["observations_by_endpoint"] = {
        "/series": exact_observations,
        "/events": 0,
        "/historical/markets": 0,
    }
    control["authorized_census"]["exact_observations"] = exact_observations
    control["authorized_census"]["observations_by_endpoint"] = census["observations_by_endpoint"]
    prepared = production._historical_worker_action(readiness_authority, "readiness", "prepare")
    assert prepared["observation_count"] == exact_observations
    control["authorized_census"]["exact_observations"] = prepared["observation_count"]
    census_body = {
        "version": 1,
        "identity": identity,
        "source_snapshot_sha256": source_snapshot["sha256"],
        "census": census,
    }
    grants.write_once_json(
        control_root / "source" / "authorized-census.json",
        census_body | {"sha256": grants.sha256_json(census_body)},
    )
    verified = production._historical_worker_action(readiness_authority, "readiness", "verify")
    assert verified["verification_sha256"]
    readiness_outputs = production._historical_phase_outputs(readiness_authority, "readiness")
    readiness_seal = {
        "version": 1,
        "phase": "readiness",
        "bindings": identity,
        "previous_sha256": grants.read_phase_chain(control_root / "phases", identity)[1],
        "outputs": readiness_outputs,
        "completed_at_epoch": time.time(),
        "grant_id": readiness_authority["grant"]["grant_id"],
    }
    grants.write_phase_seal(
        control_root,
        readiness_seal,
        bindings=identity,
        grant_launch=readiness_authority["launch"],
    )

    adoption_authority = create_grant("adoption", ["adoption"])
    production._historical_worker_action(adoption_authority, "adoption", "verify")
    production._historical_worker_action(adoption_authority, "adoption", "adopt")
    adoption_outputs = production._historical_phase_outputs(adoption_authority, "adoption")
    assert adoption_outputs["source_retired"] is False
    adoption_seal = {
        "version": 1,
        "phase": "adoption",
        "bindings": identity,
        "previous_sha256": grants.read_phase_chain(control_root / "phases", identity)[1],
        "outputs": adoption_outputs,
        "completed_at_epoch": time.time(),
        "grant_id": adoption_authority["grant"]["grant_id"],
    }
    grants.write_phase_seal(
        control_root,
        adoption_seal,
        bindings=identity,
        grant_launch=adoption_authority["launch"],
    )

    candidate = store.path(f"tmp/{stage_name}/resume-candidate/warehouse.duckdb")
    with duckdb.connect(str(candidate)) as connection:
        connection.execute("create schema derived")
        connection.execute("create table derived.marker(value varchar)")
    derived_sha = warehouse.file_digest(candidate, store.limits.max_database_bytes)[0]
    monkeypatch.setattr(
        warehouse,
        "_copy_resume_file",
        lambda *args, **kwargs: pytest.fail("adopted candidate must never be recopied"),
    )

    first_retirement_authority = create_grant("models", ["models"], retire_source=True)
    assert (
        production._historical_source_snapshot(
            store,
            control_root,
            identity,
            freeze,
            grant_id=first_retirement_authority["grant"]["grant_id"],
            approval=first_retirement_authority["approval"],
            control=control,
            verified_mount=verified_mount,
        )["sha256"]
        == source_snapshot["sha256"]
    )
    production._historical_worker_action(first_retirement_authority, "models", "prepare")
    production._historical_worker_action(first_retirement_authority, "models", "verify")

    source_wal = source.with_name(source.name + ".wal")
    real_unlink = Path.unlink

    def interrupt_after_database_unlink(path, *args, **kwargs):
        if Path(path) == source_wal:
            assert not source.exists()
            raise OSError("simulated stop after source database unlink")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", interrupt_after_database_unlink)
    with pytest.raises(OSError, match="source database unlink"):
        production._historical_worker_action(first_retirement_authority, "models", "adopt")
    monkeypatch.setattr(Path, "unlink", real_unlink)
    assert not source.exists() and source_wal.is_file()
    assert (stage / "resume-cleanup-intent.json").is_file()
    assert warehouse.file_digest(candidate, store.limits.max_database_bytes)[0] == derived_sha

    retry_authority = create_grant("models", ["models"], retire_source=True)
    retired_snapshot = production._historical_source_snapshot(
        store,
        control_root,
        identity,
        freeze,
        grant_id=retry_authority["grant"]["grant_id"],
        approval=retry_authority["approval"],
        control=control,
        verified_mount=verified_mount,
    )
    assert retired_snapshot["sha256"] == source_snapshot["sha256"]
    prepare_output = production._historical_worker_action(retry_authority, "models", "prepare")
    verify_output = production._historical_worker_action(retry_authority, "models", "verify")
    verification, _ = production._historical_read_artifact(
        control_root
        / "grants"
        / retry_authority["grant"]["grant_id"]
        / "models-adoption-verification.json",
        4 * production.MIB,
    )
    outputs = production._historical_worker_action(retry_authority, "models", "adopt")
    assert outputs["source_retired"] is True

    def record_substep(authority, phase, substep, output):
        body = {
            "version": 1,
            "identity": identity,
            "grant_id": authority["grant"]["grant_id"],
            "launch_sha256": authority["launch_sha256"],
            "phase": phase,
            "substep": substep,
            "completed_at_epoch": time.time(),
            "output_sha256": grants.sha256_json(output),
        }
        grants.write_once_json(
            production._historical_substep_path(
                control_root, authority["grant"]["grant_id"], phase, substep
            ),
            body,
        )

    record_substep(retry_authority, "models", "prepare", prepare_output)
    record_substep(retry_authority, "models", "verify", verify_output)
    record_substep(retry_authority, "models", "adopt", outputs)

    build_authority = create_grant("models", ["models"], retire_source=False)
    assert (
        production._historical_source_snapshot(
            store,
            control_root,
            identity,
            freeze,
            grant_id=build_authority["grant"]["grant_id"],
            approval=build_authority["approval"],
            control=control,
            verified_mount=verified_mount,
        )["sha256"]
        == source_snapshot["sha256"]
    )
    model_output = production._historical_worker_action(build_authority, "models", "build")
    record_substep(build_authority, "models", "build", model_output)
    models_outputs = production._historical_phase_outputs(build_authority, "models")
    models_seal = {
        "version": 1,
        "phase": "models",
        "bindings": identity,
        "previous_sha256": grants.read_phase_chain(control_root / "phases", identity)[1],
        "outputs": models_outputs,
        "completed_at_epoch": time.time(),
        "grant_id": build_authority["grant"]["grant_id"],
    }
    grants.write_phase_seal(
        control_root,
        models_seal,
        bindings=identity,
        grant_launch=build_authority["launch"],
    )

    post_install_snapshot = production._historical_source_snapshot(
        store,
        control_root,
        identity,
        freeze,
        grant_id=build_authority["grant"]["grant_id"],
        approval=build_authority["approval"],
        control=control,
        verified_mount=verified_mount,
    )
    assert post_install_snapshot["sha256"] == source_snapshot["sha256"]
    build_authority["source_snapshot"] = post_install_snapshot
    assert production._historical_phase_outputs(build_authority, "readiness") == readiness_outputs
    assert production._historical_phase_outputs(build_authority, "adoption") == adoption_outputs
    assert production._historical_phase_outputs(build_authority, "models") == models_outputs


def test_actual_adoption_retirement_seals_pre_origin_and_retry_verifications(
    artifacts, monkeypatch
):
    """A pre-adoption verification and a post-crash retry both seal one real adoption."""
    from conftest import FakeClient

    from kalshi_events import capture as acquisition
    from kalshi_events import warehouse
    from kalshi_events.common import Store, digest, json_bytes

    store = Store(artifacts / "early-retirement-operator")

    def client_factory(active_store, budget, **options):
        return FakeClient(active_store, budget, {}, **options)

    captured = acquisition.capture(store, client_factory=client_factory)
    capture_id = captured["capture_id"]
    state = warehouse.read_capture(store, capture_id)
    warehouse.build_historical(store, capture_id)
    stage_name = "build-historical-" + uuid.uuid4().hex
    stage = store.path(f"tmp/{stage_name}")
    stage.mkdir(parents=True)
    shutil.copyfile(
        store.path(f"builds-historical/{capture_id}/warehouse.duckdb"),
        stage / "warehouse.duckdb",
    )
    source_snapshot = warehouse.historical_readiness_snapshot(store, capture_id, stage_name)
    state_sha = digest(json_bytes(dict(state)))
    identity = {
        "capture_id": capture_id,
        "session_id": uuid.uuid4().hex,
        "root_id": uuid.uuid4().hex,
        "stage_name": stage_name,
        "failed_attempts_sha256": "e" * 64,
        "prior_session_sha256": "f" * 64,
        "state_sha256": state_sha,
        "source_sha256": source_snapshot["result"]["source_sha256"],
        "runtime_sha256": "3" * 64,
    }
    control_root = artifacts / "early-retirement-controls"
    for relative in ("source", "grants", "phases"):
        (control_root / relative).mkdir(parents=True, mode=0o700)
    control_root.chmod(0o700)
    grants.write_once_json(control_root / "source" / "source-snapshot.json", source_snapshot)

    prepared = warehouse.prepare_historical_readiness(store, capture_id, stage_name)
    exact_observations = prepared["observation_count"]
    census = {
        "population_report_sha256": "a" * 64,
        "receipt_index_sha256": "b" * 64,
        "candidate_pages": 1,
        "observations_by_endpoint": {
            "/series": exact_observations,
            "/events": 0,
            "/historical/markets": 0,
        },
        "nested_event_markets": 0,
        "exact_observations": exact_observations,
    }
    census_control = census | {
        "population_report_path": str((artifacts / "population.json").resolve()),
        "receipt_index_path": str((artifacts / "candidate-pages.json").resolve()),
        "capture_id": capture_id,
        "root_id": identity["root_id"],
        "capture_state_sha256": state_sha,
    }
    control = {"authorized_census": census_control}
    freeze = {
        "state_sha256": state_sha,
        "source_sha256": source_snapshot["result"]["source_sha256"],
        "source_snapshot_sha256": source_snapshot["sha256"],
        "receipt_evidence_sha256": source_snapshot["result"]["receipt_evidence_sha256"],
        "scope_sha256": source_snapshot["binding"]["scope_sha256"],
        "code_sha256": source_snapshot["binding"]["code_sha256"],
        "root_volume_uuid": "ACAD66E7-42DA-426B-B20D-FB87A453396B",
        "root_device": store.root.stat().st_dev,
    }

    def make_authority(phase, phases, *, retire_source=False):
        grant_id = uuid.uuid4().hex
        approval, latch, launch, _epoch, _monotonic = _grant(
            identity,
            grant_id=grant_id,
            phase=phase,
            phases=phases,
            retire_source=retire_source,
            retirement_policy=(
                "coexistence-capacity-relief-approved-v1" if retire_source else None
            ),
        )
        grant_root = control_root / "grants" / grant_id
        grant_root.mkdir(parents=True, mode=0o700)
        binding = {
            "version": 1,
            "grant_id": grant_id,
            "approval_sha256": grants.sha256_json(approval),
            "capture_id": capture_id,
            "session_id": identity["session_id"],
            "root_id": identity["root_id"],
            "stage_name": stage_name,
            "report_sha256": approval["measurements"]["session_report_sha256"],
            "report_path": str(
                (artifacts / "runs" / identity["session_id"] / "session.json").resolve()
            ),
            "boot_stamp": launch["boot_stamp"],
            "root_volume_uuid": freeze["root_volume_uuid"],
            "root_device": freeze["root_device"],
        }
        grants.write_once_json(grant_root / "approval.json", approval)
        grants.write_once_json(grant_root / "latch.json", latch)
        grants.write_once_json(grant_root / "session-binding.json", binding)
        launch["session_binding_sha256"] = grants.sha256_json(binding)
        launch_sha = grants.write_once_json(grant_root / "launch.json", launch)
        return {
            "identity": identity,
            "control_root": control_root,
            "store": store,
            "source_snapshot": source_snapshot,
            "freeze": freeze,
            "control": control,
            "approval": approval,
            "grant": {"grant_id": grant_id, "phase": phase},
            "launch": launch,
            "launch_sha256": launch_sha,
        }

    monkeypatch.setattr(
        production, "_historical_authorized_census", lambda _authority, *, full_revalidation: census
    )
    source_authority = make_authority("source", ["source"])
    source_outputs = {
        "startup_maintenance_sha256": "4" * 64,
        "source_snapshot_sha256": source_snapshot["sha256"],
        "capture_state_sha256": state_sha,
        "receipt_evidence_sha256": source_snapshot["result"]["receipt_evidence_sha256"],
        "source_sha256": source_snapshot["result"]["source_sha256"],
        "source_wal_sha256": source_snapshot["result"]["source_wal_sha256"],
        "receipt_count": source_snapshot["result"]["receipt_count"],
        "population_report_sha256": census["population_report_sha256"],
        "receipt_index_sha256": census["receipt_index_sha256"],
        "exact_observations": exact_observations,
        "observations_by_endpoint": census["observations_by_endpoint"],
        "scope_sha256": source_snapshot["binding"]["scope_sha256"],
        "code_sha256": source_snapshot["binding"]["code_sha256"],
    }
    source_seal = _seal("source", identity, outputs=source_outputs)
    source_seal["grant_id"] = source_authority["grant"]["grant_id"]
    source_seal["completed_at_epoch"] = source_authority["launch"]["started_epoch"] + 1
    grants.write_phase_seal(
        control_root,
        source_seal,
        bindings=identity,
        grant_launch=source_authority["launch"],
    )

    census_body = {
        "version": 1,
        "identity": identity,
        "source_snapshot_sha256": source_snapshot["sha256"],
        "census": census,
    }
    grants.write_once_json(
        control_root / "source" / "authorized-census.json",
        census_body | {"sha256": grants.sha256_json(census_body)},
    )
    readiness_authority = make_authority("readiness", ["readiness"])
    production._historical_worker_action(readiness_authority, "readiness", "prepare")
    production._historical_worker_action(readiness_authority, "readiness", "verify")
    readiness_outputs = production._historical_phase_outputs(readiness_authority, "readiness")
    readiness_seal = _seal("readiness", identity, outputs=readiness_outputs)
    readiness_seal["previous_sha256"] = grants.read_phase_chain(control_root / "phases", identity)[
        1
    ]
    readiness_seal["grant_id"] = readiness_authority["grant"]["grant_id"]
    grants.write_phase_seal(
        control_root,
        readiness_seal,
        bindings=identity,
        grant_launch=readiness_authority["launch"],
    )

    interrupted = make_authority("adoption", ["adoption"], retire_source=True)
    production._historical_worker_action(interrupted, "adoption", "verify")
    first_verification, _ = production._historical_read_artifact(
        control_root / "grants" / interrupted["grant"]["grant_id"] / "adoption-verification.json",
        4 * production.MIB,
    )
    assert first_verification["binding"]["adoption_origin_sha256"] is None
    first_adoption = production._historical_worker_action(interrupted, "adoption", "adopt")
    assert first_adoption["source_retired"] is True

    def record_adoption_substep(authority, output):
        body = {
            "version": 1,
            "identity": identity,
            "grant_id": authority["grant"]["grant_id"],
            "launch_sha256": authority["launch_sha256"],
            "phase": "adoption",
            "substep": "adopt",
            "completed_at_epoch": time.time(),
            "output_sha256": grants.sha256_json(output),
        }
        grants.write_once_json(
            production._historical_substep_path(
                control_root, authority["grant"]["grant_id"], "adoption", "adopt"
            ),
            body,
        )

    record_adoption_substep(interrupted, first_adoption)
    first_outputs = production._historical_phase_outputs(interrupted, "adoption")
    assert first_outputs["source_retired"] is True

    # Simulate process death after adoption and source retirement, before phase seal.
    retry = make_authority("adoption", ["adoption"], retire_source=True)
    production._historical_worker_action(retry, "adoption", "verify")
    retry_verification, _ = production._historical_read_artifact(
        control_root / "grants" / retry["grant"]["grant_id"] / "adoption-verification.json",
        4 * production.MIB,
    )
    adoption_proof, _ = warehouse._load_self_proof(
        stage / "resume-adoption.json", "historical-adoption"
    )
    assert retry_verification["binding"]["adoption_origin_sha256"] == adoption_proof["sha256"]
    retry_output = production._historical_worker_action(retry, "adoption", "adopt")
    record_adoption_substep(retry, retry_output)
    outputs = production._historical_phase_outputs(retry, "adoption")
    assert outputs["source_retired"] is True
    assert outputs["retirement_receipt_sha256"]
    seal = _seal("adoption", identity, outputs=outputs)
    seal["previous_sha256"] = grants.read_phase_chain(control_root / "phases", identity)[1]
    seal["grant_id"] = retry["grant"]["grant_id"]
    grants.write_phase_seal(
        control_root,
        seal,
        bindings=identity,
        grant_launch=retry["launch"],
    )
    assert grants.read_phase_chain(control_root / "phases", identity)[0] == "adoption"


def test_models_retry_after_install_certifies_durable_build_without_fresh_landing(
    artifacts, monkeypatch
):
    from kalshi_events import warehouse
    from kalshi_events.common import digest

    capture_id = "a" * 32
    stage_name = "build-historical-" + "b" * 32
    root = artifacts / "operator"
    build_dir = root / "builds-historical" / capture_id
    build_dir.mkdir(parents=True)
    artifact_path = build_dir / "build-artifacts.json"
    artifact_path.write_bytes(b"certified model artifacts\n")
    adoption = {
        "stage": stage_name,
        "sha256": "c" * 64,
        "source_proof_sha256": "d" * 64,
        "candidate_sha256": "e" * 64,
        "source_sha256": "f" * 64,
        "source_wal_sha256": None,
    }
    build = {
        "capture_id": capture_id,
        "catalogue_version": 3,
        "observation_count": 10,
        "warehouse_sha256": "1" * 64,
        "dbt_artifacts_sha256": digest(artifact_path.read_bytes()),
        "historical_adoption": adoption,
    }
    (build_dir / "build.json").write_text(json.dumps(build) + "\n")

    class Store:
        def path(self, relative):
            return root / relative

    assert production._historical_substeps_for_grant(
        "models",
        {"retire_source": True, "bindings": {"stage_name": stage_name}},
        Store(),
        capture_id,
    ) == ("build",)
    monkeypatch.setattr(
        warehouse,
        "build_historical",
        lambda *_args, **_kwargs: pytest.fail("retry entered fresh historical landing"),
    )
    monkeypatch.setattr(
        warehouse,
        "certified",
        lambda *_args, **_kwargs: {
            **build,
            "warehouse_sha256": "1" * 64,
            "dbt_artifacts_sha256": digest(artifact_path.read_bytes()),
        },
    )
    authority = {
        "control_root": artifacts / "controls",
        "grant": {"grant_id": "2" * 32},
        "identity": {"capture_id": capture_id, "stage_name": stage_name},
        "store": Store(),
        "source_snapshot": {"result": {"source_sha256": "f" * 64, "source_wal_sha256": None}},
        "control": {"authorized_census": {"exact_observations": 10}},
    }
    result = production._historical_worker_action(authority, "models", "build")
    assert result == {
        "build_receipt_sha256": grants.sha256_bytes((build_dir / "build.json").read_bytes()),
        "warehouse_sha256": "1" * 64,
        "dbt_artifacts_sha256": digest(artifact_path.read_bytes()),
    }


def _storage_supervision_workspace(artifacts):
    directory = artifacts / "supervised-production"
    operator = directory / "operator"
    session_id = "c" * 32
    capture_id = "b" * 32
    root_id = "a" * 32
    report_path = directory / "runs" / session_id / "session.json"
    log_dir = report_path.parent / "logs" / f"invocation-{'d' * 32}"
    operator.mkdir(parents=True)
    log_dir.mkdir(parents=True)
    production._atomic_report(
        directory / production.OWNER_MARKER, {"version": 1, "root_id": root_id}
    )
    (directory / ".production-run.lock").touch()
    store = production._historical_store(operator, max_duration_seconds=3600)
    production._atomic_report(operator / "root-id.json", {"version": 1, "root_id": root_id})
    store.reconcile()
    report = {
        "session_id": session_id,
        "capture_id": capture_id,
        "root_id": root_id,
        "mode": "historical",
        "resource_profile": "default",
        "system": {"filesystem_device": directory.stat().st_dev},
    }
    usage = store.usage()
    external = production._external_bytes(directory)
    report["storage"] = {
        "operator_retained_bytes": usage["retained_bytes"],
        "operator_temporary_bytes": usage["temporary_bytes"],
        "operator_database_bytes": usage["database_bytes"],
        "external_bytes": external,
        "aggregate_estimate_bytes": usage["retained_bytes"] + external,
    }
    production._atomic_report(report_path, report)
    return directory, operator, report_path, log_dir, report


def _normal_memory_sample(_pid=None, *, timeout=5.0):
    del timeout
    return {
        "system_memory_pressure": "normal",
        "system_memory_pressure_observed_monotonic": time.monotonic(),
        "child_process_group_rss_bytes": 1,
        "system_compressed_bytes": 0,
        "system_compressor_occupied_bytes": 0,
        "child_process_group_physical_footprint_bytes": 1,
        "supervisor_physical_footprint_bytes": 1,
    }


def test_private_child_installs_live_storage_observer_before_native_work(artifacts, monkeypatch):
    directory, operator, report_path, log_dir, report = _storage_supervision_workspace(artifacts)
    monkeypatch.setattr(production, "_sample_memory", _normal_memory_sample)
    monkeypatch.setattr(
        production.shutil,
        "disk_usage",
        lambda _path: type(
            "Usage", (), {"total": 500 * grants.GIB, "used": 1, "free": 500 * grants.GIB}
        )(),
    )
    token = "e" * 32
    progress = log_dir / "historical-test-source-snapshot.storage.json"
    stdout = log_dir / "historical-test.stdout.json"
    stderr = log_dir / "historical-test.stderr.log"
    handoff = {
        "directory": str(directory),
        "control_root": str(artifacts / "controls"),
        "grant_id": "1" * 32,
        "launch_sha256": "2" * 64,
        "phase": "source",
        "substep": "snapshot",
        "storage_invocation_token": token,
        "storage_progress_path": str(progress),
    }
    child_code = (
        "import json,os; "
        f"fd=int(os.environ.pop('{production.HISTORICAL_SUPERVISOR_FD_ENV}')); "
        "data=b''; "
        "\nwhile True:\n"
        " chunk=os.read(fd,4096)\n"
        " if not chunk: break\n"
        " data+=chunk\n"
        "os.close(fd); print(data.decode())"
    )
    code, reason, reaped = production._invoke(
        [sys.executable, "-c", child_code],
        directory=directory,
        operator=operator,
        report=report,
        report_path=report_path,
        deadline=time.monotonic() + 60,
        wall_deadline_epoch_override=time.time() + 60,
        stdout_path=stdout,
        stderr_path=stderr,
        storage_supervision=True,
        storage_invocation_token=token,
        metrics_progress_path=report_path.parent / "metrics.progress.json",
        storage_progress_path=progress,
        supervisor_handoff=handoff,
    )
    assert (code, reason, reaped) == (0, None, True)
    state = json.loads(progress.read_text())
    assert state["invocation_token"] == token
    assert state["sample_count"] >= 1
    assert report["storage_supervision_invocation"] == token
    handed = json.loads(stdout.read_text())
    assert handed["grant_id"] == handoff["grant_id"]
    assert handed["storage_invocation_token"] == token
    assert handed["worker_pid"] > 1
    assert (
        production._historical_validate_storage_handoff(
            directory, report_path, progress, invocation_token=token
        )["sample_count"]
        >= 1
    )
    with pytest.raises(production.RunnerError, match="not bound"):
        production._historical_validate_storage_handoff(
            directory, report_path, progress, invocation_token="a" * 32
        )


def test_private_child_fails_before_start_when_storage_observer_cannot_account(
    artifacts, monkeypatch
):
    directory, operator, report_path, log_dir, report = _storage_supervision_workspace(artifacts)
    monkeypatch.setattr(production, "_sample_memory", _normal_memory_sample)
    token = "f" * 32
    progress = log_dir / "historical-test-source-snapshot.storage.json"
    stdout = log_dir / "historical-test.stdout.json"
    stderr = log_dir / "historical-test.stderr.log"
    report.pop("storage")
    monkeypatch.setattr(
        production.subprocess,
        "Popen",
        lambda *_args, **_kwargs: pytest.fail("child started without storage accounting"),
    )
    with pytest.raises(production.RunnerError, match="no verified baseline"):
        production._invoke(
            [sys.executable, "-c", "raise SystemExit(0)"],
            directory=directory,
            operator=operator,
            report=report,
            report_path=report_path,
            deadline=time.monotonic() + 60,
            wall_deadline_epoch_override=time.time() + 60,
            stdout_path=stdout,
            stderr_path=stderr,
            storage_supervision=True,
            storage_invocation_token=token,
            storage_progress_path=progress,
        )


def test_startup_cleanup_is_only_overstop_observer_allowance(artifacts, monkeypatch):
    directory, operator, report_path, log_dir, report = _storage_supervision_workspace(artifacts)
    monkeypatch.setattr(
        production.shutil,
        "disk_usage",
        lambda _path: type(
            "Usage", (), {"total": 500 * grants.GIB, "used": 1, "free": 500 * grants.GIB}
        )(),
    )
    token = production._ACTIVE_LIMITS.set(production._limits_for_profile("expanded"))
    try:
        stop = int(production._effective_limits()["storage_temporary_stop_bytes"])
        report["storage"]["operator_temporary_bytes"] = stop + 1
        progress = log_dir / "historical-startup-maintenance.storage.json"
        external = {
            "session_report": report_path,
            "stdout": log_dir / "child.stdout.json",
            "stderr": log_dir / "child.stderr.log",
            "storage_progress": progress,
        }
        state = production._begin_storage_supervision(
            directory,
            operator,
            report,
            progress,
            external,
            invocation_token="9" * 32,
            allow_temporary_cleanup=True,
        )
        assert (
            production._sample_live_storage(directory, operator, report, state, progress, external)
            is None
        )

        spill = operator / "tmp" / f"dbt-{'7' * 32}" / "spill"
        spill.mkdir(parents=True)
        with (spill / "unbounded.bin").open("wb") as stream:
            stream.truncate(64 * production.MIB + 1)
        reason = production._sample_live_storage(
            directory, operator, report, state, progress, external
        )
        assert reason == "startup cleanup exceeded its fixed temporary metadata allowance"
    finally:
        production._ACTIVE_LIMITS.reset(token)


def test_authorized_census_revalidates_protocol5_capture_and_writes_proof(artifacts, monkeypatch):
    import collections
    import hashlib
    import json

    from conftest import FakeClient

    from kalshi_events import capture, historical, publish
    from kalshi_events.common import (
        CatalogueError,
        Store,
        atomic_json,
        json_bytes,
        series_root_marker,
        series_scope,
    )
    from kalshi_events.incremental import root_id

    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    import historical_admission

    store = Store(artifacts / "operator")
    with store.writer_guard():
        logical_root_id = root_id(store, create=True)
    captured = capture.capture(
        store,
        client_factory=lambda active_store, budget, **options: FakeClient(
            active_store, budget, {}, **options
        ),
    )
    state = capture.read_capture(store, captured["capture_id"])
    assert state["acquisition_protocol"] == 5
    state_sha = state.verified_snapshot_sha256
    selection = historical.historical_selection(state, allow_legacy_incomplete=True)
    task_indices = historical.historical_task_indices(state, allow_legacy_incomplete=True)
    task_scopes = {int(index): scope for index, scope in state["task_scopes"].items()}
    excluded = [
        {"task_index": index, "scope": scope, "endpoint": state["tasks"][index]["endpoint"]}
        for index, scope in sorted(task_scopes.items())
        if scope in publish.HISTORICAL_OMITTED_SCOPES
    ]

    selected_pages = []
    for order, row in enumerate(state.page_rows):
        if row["index"] not in task_indices:
            continue
        receipt_dir = store.path(row["relative"]).parent
        manifest = json.loads((receipt_dir / "manifest.json").read_bytes())
        selected_pages.append(
            {
                **row,
                "order": order,
                "endpoint": manifest["endpoint"],
                "generation": manifest.get(
                    "task_generation", manifest.get("request_generation", 0)
                ),
                "encoded_bytes": (receipt_dir / "body.bin").stat().st_size,
                "decoded_bytes": manifest.get("decoded_size", manifest["size"]),
                "status": manifest["status"],
                "manifest_bytes": (receipt_dir / "manifest.json").stat().st_size,
            }
        )
    data_pages = [
        page
        for page in selected_pages
        if page["status"] == 200 and page["endpoint"] != "/historical/cutoff"
    ]
    groups = collections.defaultdict(list)
    observations = collections.Counter()
    nested_event_markets = 0
    endpoint_collections = {
        "/series": "series",
        "/events": "events",
        "/historical/markets": "markets",
    }
    for page in data_pages:
        groups[(page["endpoint"], page["generation"])].append(page)
        receipt_dir = store.path(page["relative"]).parent
        payload = json.loads((receipt_dir / "body.bin").read_bytes())
        rows = payload[endpoint_collections[page["endpoint"]]]
        observations[page["endpoint"]] += len(rows)
        if page["endpoint"] == "/events":
            nested_event_markets += sum(len(event.get("markets", [])) for event in rows)
    observations_by_endpoint = dict(sorted(observations.items()))
    exact_observations = sum(observations_by_endpoint.values())
    data_strata = []
    for (endpoint, generation), pages in sorted(groups.items()):
        decoded = sorted(page["decoded_bytes"] for page in pages)
        data_strata.append(
            {
                "endpoint": endpoint,
                "generation": generation,
                "candidate_pages": len(pages),
                "encoded_bytes": sum(page["encoded_bytes"] for page in pages),
                "decoded_bytes": sum(decoded),
                "median_decoded_page_bytes": decoded[(len(decoded) + 1) // 2 - 1],
                "max_decoded_page_bytes": max(decoded),
            }
        )
    index = {
        "capture_id": state["capture_id"],
        "capture_state_sha256": state_sha,
        "historical_selection": selection,
        "selected_task_indices": sorted(task_indices),
        "retired_scopes_excluded": excluded,
        "data_candidate_pages": len(data_pages),
        "manifest_metadata_bytes": sum(page["manifest_bytes"] for page in selected_pages),
        "pages": [
            {key: page[key] for key in historical_admission.INDEX_PAGE_FIELDS}
            for page in data_pages
        ],
    }
    index_raw = json_bytes(index)
    population = {
        "status": "complete",
        "capture_id": state["capture_id"],
        "capture_state_sha256": state_sha,
        "metadata_index_sha256": hashlib.sha256(index_raw).hexdigest(),
        "candidate_pages": len(data_pages),
        "pages_processed": len(data_pages),
        "next_index": len(data_pages),
        "observation_counts": observations_by_endpoint
        | {"nested_event_markets": nested_event_markets},
        "candidate_population": {"data_strata": data_strata, "control_receipts": []},
        "encoded_bytes_verified": sum(page["encoded_bytes"] for page in data_pages),
        "decoded_bytes_verified": sum(page["decoded_bytes"] for page in data_pages),
        "page_counts": dict(
            sorted(collections.Counter(page["endpoint"] for page in data_pages).items())
        ),
    }
    population_raw = json_bytes(population)
    index_path = artifacts / "receipt-index.json"
    population_path = artifacts / "population.json"
    index_path.write_bytes(index_raw)
    population_path.write_bytes(population_raw)
    monkeypatch.setattr(historical_admission, "AUTHORIZED_CAPTURE_ID", state["capture_id"])
    monkeypatch.setattr(historical_admission, "AUTHORIZED_ROOT_ID", logical_root_id)
    monkeypatch.setattr(historical_admission, "AUTHORIZED_STATE_SHA256", state_sha)
    monkeypatch.setattr(
        historical_admission, "AUTHORIZED_INDEX_SHA256", hashlib.sha256(index_raw).hexdigest()
    )
    authorized_population = {
        "candidate_pages": len(data_pages),
        "observations_by_endpoint": observations_by_endpoint,
        "nested_event_markets": nested_event_markets,
        "exact_observations": exact_observations,
    }
    monkeypatch.setattr(historical_admission, "AUTHORIZED_POPULATION", authorized_population)
    census_control = {
        **authorized_population,
        "population_report_path": str(population_path),
        "population_report_sha256": hashlib.sha256(population_raw).hexdigest(),
        "receipt_index_path": str(index_path),
        "receipt_index_sha256": hashlib.sha256(index_raw).hexdigest(),
        "capture_id": state["capture_id"],
        "root_id": logical_root_id,
        "capture_state_sha256": state_sha,
    }
    control_root = artifacts / "controls"
    (control_root / "source").mkdir(parents=True, mode=0o700)
    authority = {
        "directory": artifacts,
        "operator": store.root,
        "control_root": control_root,
        "identity": {"capture_id": state["capture_id"], "session_id": "b" * 32},
        "source_snapshot": {"sha256": "c" * 64},
        "control": {"authorized_census": census_control},
        "grant": {"workflow_deadline_monotonic": time.monotonic() + 60},
    }

    census = production._historical_census_proof(authority, full_revalidation_if_missing=True)
    assert census["candidate_pages"] == len(data_pages)
    proof = grants.read_json(control_root / "source" / "authorized-census.json")
    assert proof["census"] == census

    atomic_json(store.path("root.json"), series_root_marker(series_scope("SERIES")))
    with pytest.raises(CatalogueError, match="capture scope does not match the operator root"):
        production._historical_authorized_census(authority, full_revalidation=True)
