"""Explicit recovery keeps interrupted acquisition accounting conservative."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from kalshi_events import transport as transport_module

pytest_plugins = ("test_production_workflow",)


def _target_versions(**overrides):
    versions = {
        "duckdb": "2.0.0",
        "dlt": "1.31.0",
        "dbt-core": "1.12.5",
        "dbt-duckdb": "1.11.1",
        "pyarrow": "26.0.0",
        "httpx": "0.28.1",
        "httpcore": "1.0.9",
    }
    versions.update(overrides)
    return versions


def _mark_target_runtime(production, report_path):
    report = json.loads(report_path.read_text())
    report["system"].update(
        package_versions=_target_versions(),
        uv_lock_sha256=production.RECONCILIATION_UV_LOCK_SHA256,
        transport_sha256=production.RECONCILIATION_TRANSPORT_SHA256,
    )
    report_path.write_text(json.dumps(report))
    return report


def _stopped_capture_with_unknown_accounting(production, workflow):
    workflow.outcomes = [{"_no_result": True, "returncode": -9}]
    stopped, code = production.run_production(workflow.directory, full=True)
    assert code != 0
    assert stopped["accounting_complete"] is False
    assert stopped["requests"] is None
    assert stopped["downloaded_bytes"] is None
    assert stopped["capture_id"] == workflow.manifest["capture_id"]

    report_path = next((workflow.directory / "runs").glob("*/session.json"))
    report = json.loads(report_path.read_text())
    report["system"]["package_versions"] = {
        "duckdb": "1.5.6",
        "dlt": "1.31.0",
        "dbt-core": "1.12.5",
        "pyarrow": "26.0.0",
        "httpx": "0.28.1",
    }
    report["system"]["uv_lock_sha256"] = production.LEGACY_RECONCILIATION_UV_LOCK_SHA256
    report["system"].pop("transport_sha256", None)
    report_path.write_text(json.dumps(report))
    return stopped, report_path, report


def test_reconcile_unknown_accounting_then_verify_and_activate_completed_capture(
    production, workflow, monkeypatch
):
    stopped, report_path, report = _stopped_capture_with_unknown_accounting(production, workflow)
    original_system = json.loads(report_path.read_text())["system"]

    pending = {
        "capture_id": workflow.manifest["capture_id"],
        "request_id": "b" * 64,
        "reserved_bytes": int(production.RUN_LIMITS["response_bytes"]),
        "status": None,
        "accounting_complete": False,
        "acquisition_protocol": 5,
        "version": 1,
    }
    prior_bound = (
        int(production.RUN_LIMITS["response_bytes"]) + production.HTTPX_MAX_RAW_CHUNK_BYTES
    )
    first = {
        "attempts": 10,
        "downloaded_or_reserved_bytes": prior_bound,
        "accounting_complete": False,
        "unfinished_reservations": [pending],
        "storage": {},
        "root_id": stopped["root_id"],
    }
    second = json.loads(json.dumps(first))
    snapshots = iter((first, second))

    def staged_snapshot(*_args, **_kwargs):
        try:
            return next(snapshots)
        except StopIteration:
            return second

    monkeypatch.setattr(production, "_resource_guard", staged_snapshot)
    monkeypatch.setattr(
        production,
        "_package_versions",
        lambda: _target_versions(),
    )

    workflow.outcomes = [{"accounting_complete": False, "requests": 0, "downloaded_bytes": 0}]
    recovered, code = production.run_production(
        workflow.directory,
        capture_id=workflow.manifest["capture_id"],
        reconcile_accounting=True,
    )

    assert code == 0, recovered
    assert recovered["capture_complete"] is True
    assert recovered["publication_state"] == "verified"
    assert recovered["production_state"] == "verified"
    assert recovered["accounting_complete"] is False
    assert recovered["downloaded_bytes"] is None
    assert recovered["downloaded_bytes_upper_bound"] == prior_bound
    assert recovered["accounting_basis"] == "reserved-upper-bound"
    assert recovered["error"] is None
    assert recovered["recovery"] is None
    assert recovered["original_system"] == original_system
    assert "httpcore" not in recovered["original_system"]["package_versions"]
    assert recovered["accounting_reconciliation"]["runtime_transition"] == {
        "from_uv_lock_sha256": production.LEGACY_RECONCILIATION_UV_LOCK_SHA256,
        "to_uv_lock_sha256": production.RECONCILIATION_UV_LOCK_SHA256,
        "transport_sha256": production.RECONCILIATION_TRANSPORT_SHA256,
    }
    assert recovered["accounting_reconciliation"]["prior_error"] == (
        "refresh failed with unknown child accounting"
    )
    assert workflow.events[-4:] == [
        "refresh",
        "independent-verification",
        "activation",
        "sleep-restored",
    ]
    assert (workflow.directory / "production.duckdb").is_file()


@pytest.mark.parametrize(
    ("mismatch", "current_versions", "saved_httpcore"),
    [
        ("current_httpx", {"httpx": "0.27.2", "httpcore": "1.0.9"}, None),
        ("current_httpcore", {"httpx": "0.28.1", "httpcore": "1.0.7"}, None),
        ("current_duckdb", {"duckdb": "1.5.6"}, None),
        ("missing_httpcore", {"httpx": "0.28.1", "httpcore": None}, None),
        ("source_lock", {"httpx": "0.28.1", "httpcore": "1.0.9"}, None),
        ("saved_httpcore", {"httpx": "0.28.1", "httpcore": "1.0.9"}, "1.0.7"),
    ],
)
def test_reconcile_rejects_unproven_http_runtime_before_report_mutation_or_child_start(
    production,
    workflow,
    monkeypatch,
    tmp_path,
    mismatch,
    current_versions,
    saved_httpcore,
):
    stopped, report_path, report = _stopped_capture_with_unknown_accounting(production, workflow)
    if saved_httpcore is not None:
        report["system"]["package_versions"]["httpcore"] = saved_httpcore
        report_path.write_text(json.dumps(report))

    workflow.events.clear()
    workflow.commands.clear()
    report_before = report_path.read_bytes()
    monkeypatch.setattr(
        production, "_package_versions", lambda: _target_versions(**current_versions)
    )
    if mismatch == "source_lock":
        source = tmp_path / "changed-source"
        source.mkdir()
        (source / "uv.lock").write_text("changed lock\n")
        monkeypatch.setattr(production, "SOURCE_ROOT", source)

    expected_error = (
        "the stopped session's HTTP chunk bound"
        if mismatch == "saved_httpcore"
        else "current HTTP runtime"
    )
    with pytest.raises(production.RunnerError, match=expected_error):
        production._load_resume(
            workflow.directory, stopped["capture_id"], reconcile_accounting=True
        )

    assert report_path.read_bytes() == report_before
    assert workflow.commands == []
    assert workflow.events == []


@pytest.mark.parametrize(
    ("saved_lock", "current_lock", "allowed"),
    [
        ("legacy", "target", True),
        ("target", "target", True),
        ("legacy", "legacy", False),
        ("target", "legacy", False),
        ("unknown", "target", False),
        ("legacy", "unknown", False),
    ],
)
def test_reconciliation_lock_transition_matrix_is_exact(
    production, saved_lock, current_lock, allowed
):
    lock_ids = {
        "legacy": production.LEGACY_RECONCILIATION_UV_LOCK_SHA256,
        "target": production.RECONCILIATION_UV_LOCK_SHA256,
        "unknown": "f" * 64,
    }

    assert (
        production._supported_reconciliation_lock_transition(
            lock_ids[saved_lock], lock_ids[current_lock]
        )
        is allowed
    )


def test_reconcile_rejects_unrecognized_saved_lock_before_report_mutation_or_child_start(
    production, workflow, monkeypatch
):
    stopped, report_path, report = _stopped_capture_with_unknown_accounting(production, workflow)
    report["system"]["uv_lock_sha256"] = "f" * 64
    report_path.write_text(json.dumps(report))
    workflow.commands.clear()
    before = report_path.read_bytes()
    monkeypatch.setattr(production, "_package_versions", lambda: _target_versions())

    with pytest.raises(production.RunnerError, match="locked package stack"):
        production._load_resume(
            workflow.directory, stopped["capture_id"], reconcile_accounting=True
        )

    assert report_path.read_bytes() == before
    assert workflow.commands == []


@pytest.mark.parametrize("missing", ["duckdb", "dlt", "dbt-core", "pyarrow"])
def test_reconcile_only_allows_known_legacy_missing_runtime_fields(
    production, workflow, monkeypatch, missing
):
    stopped, report_path, report = _stopped_capture_with_unknown_accounting(production, workflow)
    del report["system"]["package_versions"][missing]
    report_path.write_text(json.dumps(report))
    workflow.commands.clear()
    before = report_path.read_bytes()
    monkeypatch.setattr(production, "_package_versions", lambda: _target_versions())

    with pytest.raises(production.RunnerError, match="locked package stack"):
        production._load_resume(
            workflow.directory, stopped["capture_id"], reconcile_accounting=True
        )

    assert report_path.read_bytes() == before
    assert workflow.commands == []


def test_reconcile_rejects_wrong_saved_legacy_package_version(production, workflow, monkeypatch):
    stopped, report_path, report = _stopped_capture_with_unknown_accounting(production, workflow)
    report["system"]["package_versions"]["duckdb"] = "1.5.5"
    report_path.write_text(json.dumps(report))
    workflow.commands.clear()
    before = report_path.read_bytes()
    monkeypatch.setattr(production, "_package_versions", lambda: _target_versions())

    with pytest.raises(production.RunnerError, match="locked package stack"):
        production._load_resume(
            workflow.directory, stopped["capture_id"], reconcile_accounting=True
        )

    assert report_path.read_bytes() == before
    assert workflow.commands == []


def test_reconcile_allows_new_lock_to_new_lock_only_with_complete_saved_metadata(
    production, workflow, monkeypatch
):
    stopped, report_path, _report = _stopped_capture_with_unknown_accounting(production, workflow)
    _mark_target_runtime(production, report_path)
    workflow.commands.clear()
    monkeypatch.setattr(production, "_package_versions", lambda: _target_versions())

    _path, resumed, _paths = production._load_resume(
        workflow.directory, stopped["capture_id"], reconcile_accounting=True
    )

    assert resumed["system"]["uv_lock_sha256"] == production.RECONCILIATION_UV_LOCK_SHA256
    assert resumed["system"]["transport_sha256"] == production.RECONCILIATION_TRANSPORT_SHA256
    assert workflow.commands == []


@pytest.mark.parametrize("saved_lock", ["legacy", "target"])
@pytest.mark.parametrize("saved_hash", ["previous", "pre_payload", "current"])
def test_reconcile_accepts_approved_saved_transport_hashes_with_current_source(
    production, workflow, monkeypatch, saved_lock, saved_hash
):
    stopped, report_path, report = _stopped_capture_with_unknown_accounting(production, workflow)
    if saved_lock == "target":
        report = _mark_target_runtime(production, report_path)
    report["system"]["transport_sha256"] = {
        "previous": production.PREVIOUS_RECONCILIATION_TRANSPORT_SHA256,
        "pre_payload": production.PRE_PAYLOAD_RECONCILIATION_TRANSPORT_SHA256,
        "current": production.RECONCILIATION_TRANSPORT_SHA256,
    }[saved_hash]
    report_path.write_text(json.dumps(report))
    workflow.commands.clear()
    monkeypatch.setattr(production, "_package_versions", lambda: _target_versions())

    _path, resumed, _paths = production._load_resume(
        workflow.directory, stopped["capture_id"], reconcile_accounting=True
    )

    assert resumed["system"]["transport_sha256"] == report["system"]["transport_sha256"]
    assert workflow.commands == []


@pytest.mark.parametrize("saved_transport", ["f" * 64, ["not", "a", "hash"]])
def test_reconcile_rejects_unrecognized_saved_transport_before_mutation_or_child_start(
    production, workflow, monkeypatch, saved_transport
):
    stopped, report_path, report = _stopped_capture_with_unknown_accounting(production, workflow)
    report = _mark_target_runtime(production, report_path)
    report["system"]["transport_sha256"] = saved_transport
    report_path.write_text(json.dumps(report))
    workflow.commands.clear()
    before = report_path.read_bytes()
    monkeypatch.setattr(production, "_package_versions", lambda: _target_versions())

    with pytest.raises(production.RunnerError, match="transport source"):
        production._load_resume(
            workflow.directory, stopped["capture_id"], reconcile_accounting=True
        )

    assert report_path.read_bytes() == before
    assert workflow.commands == []


def test_reconcile_rejects_missing_transport_hash_for_target_lock(
    production, workflow, monkeypatch
):
    stopped, report_path, report = _stopped_capture_with_unknown_accounting(production, workflow)
    report = _mark_target_runtime(production, report_path)
    report["system"].pop("transport_sha256")
    report_path.write_text(json.dumps(report))
    workflow.commands.clear()
    before = report_path.read_bytes()
    monkeypatch.setattr(production, "_package_versions", lambda: _target_versions())

    with pytest.raises(production.RunnerError, match="transport source"):
        production._load_resume(
            workflow.directory, stopped["capture_id"], reconcile_accounting=True
        )

    assert report_path.read_bytes() == before
    assert workflow.commands == []


@pytest.mark.parametrize("missing", ["dbt-duckdb", "httpcore"])
def test_reconcile_rejects_missing_metadata_for_new_lock(
    production, workflow, monkeypatch, missing
):
    stopped, report_path, report = _stopped_capture_with_unknown_accounting(production, workflow)
    report = _mark_target_runtime(production, report_path)
    del report["system"]["package_versions"][missing]
    report_path.write_text(json.dumps(report))
    workflow.commands.clear()
    before = report_path.read_bytes()
    monkeypatch.setattr(production, "_package_versions", lambda: _target_versions())

    with pytest.raises(production.RunnerError, match="locked package stack"):
        production._load_resume(
            workflow.directory, stopped["capture_id"], reconcile_accounting=True
        )

    assert report_path.read_bytes() == before
    assert workflow.commands == []


def test_reconcile_rejects_transport_source_drift_before_report_mutation_or_child_start(
    production, workflow, monkeypatch, tmp_path
):
    stopped, report_path, _report = _stopped_capture_with_unknown_accounting(production, workflow)
    monkeypatch.setattr(production, "_package_versions", lambda: _target_versions())
    workflow.commands.clear()
    source = tmp_path / "changed-source"
    transport_path = source / "src" / "kalshi_events" / "transport.py"
    transport_path.parent.mkdir(parents=True)
    transport_path.write_bytes(
        (Path(production.SOURCE_ROOT) / "src" / "kalshi_events" / "transport.py").read_bytes()
        + b"\n"
    )
    monkeypatch.setattr(transport_module, "__file__", str(transport_path))
    before = report_path.read_bytes()

    with pytest.raises(production.RunnerError, match="transport source"):
        production._load_resume(
            workflow.directory, stopped["capture_id"], reconcile_accounting=True
        )

    assert report_path.read_bytes() == before
    assert workflow.commands == []


def test_reconcile_rejects_raw_chunk_bound_drift_before_report_mutation_or_child_start(
    production, workflow, monkeypatch
):
    stopped, report_path, _report = _stopped_capture_with_unknown_accounting(production, workflow)
    monkeypatch.setattr(production, "_package_versions", lambda: _target_versions())
    monkeypatch.setattr(production, "HTTPX_MAX_RAW_CHUNK_BYTES", 32 * 1024)
    workflow.commands.clear()
    before = report_path.read_bytes()

    with pytest.raises(production.RunnerError, match="64 KiB raw chunk bound"):
        production._load_resume(
            workflow.directory, stopped["capture_id"], reconcile_accounting=True
        )

    assert report_path.read_bytes() == before
    assert workflow.commands == []


def test_missing_delta_persists_fresh_cumulative_receipts_for_safe_reconciliation(
    production, workflow, monkeypatch
):
    stopped, report_path, _report = _stopped_capture_with_unknown_accounting(production, workflow)
    capture_id = stopped["capture_id"]
    charge = int(production.RUN_LIMITS["response_bytes"]) + production.HTTPX_MAX_RAW_CHUNK_BYTES

    def receipt(request_id):
        return {
            "capture_id": capture_id,
            "request_id": request_id * 64,
            "reserved_bytes": int(production.RUN_LIMITS["response_bytes"]),
            "status": None,
            "accounting_complete": False,
            "acquisition_protocol": 5,
            "version": 1,
        }

    def snapshot(attempts, pending):
        return {
            "attempts": attempts,
            "downloaded_or_reserved_bytes": charge * len(pending),
            "accounting_complete": False,
            "unfinished_reservations": pending,
            "storage": {},
            "root_id": stopped["root_id"],
        }

    prior = snapshot(10, [receipt("b")])
    after = snapshot(12, [receipt("b"), receipt("c")])
    snapshots = [prior, after]

    def staged_snapshot(*_args, **_kwargs):
        return snapshots.pop(0) if snapshots else after

    monkeypatch.setattr(production, "_resource_guard", staged_snapshot)
    monkeypatch.setattr(
        production,
        "_package_versions",
        lambda: _target_versions(),
    )
    workflow.commands.clear()
    workflow.outcomes = [
        {
            "accounting_complete": False,
            "requests": None,
            "downloaded_bytes": None,
            "error": "malformed native combo side",
        }
    ]

    stopped_again, code = production.run_production(
        workflow.directory, capture_id=capture_id, reconcile_accounting=True
    )

    assert code != 0
    assert stopped_again["error"] == "resumed child did not report exact invocation accounting"
    assert stopped_again["requests"] == 12
    assert stopped_again["downloaded_bytes"] is None
    assert stopped_again["downloaded_bytes_upper_bound"] == charge * 2
    assert stopped_again["accounting_complete"] is False
    first_limits = json.loads(workflow.commands[0][-1])
    assert first_limits["max-requests"] == production.RUN_LIMITS["attempts"] - 10
    assert first_limits["max-download-bytes"] == production.RUN_LIMITS["downloaded_bytes"] - charge

    saved = json.loads(report_path.read_text())
    assert saved["requests"] == 12
    assert saved["downloaded_bytes_upper_bound"] == charge * 2
    saved["system"].update(
        {
            "package_versions": _target_versions(),
            "uv_lock_sha256": production.RECONCILIATION_UV_LOCK_SHA256,
            "transport_sha256": production.RECONCILIATION_TRANSPORT_SHA256,
        }
    )
    report_path.write_text(json.dumps(saved))
    workflow.outcomes = [{"accounting_complete": False, "requests": 0, "downloaded_bytes": 0}]

    recovered, code = production.run_production(
        workflow.directory, capture_id=capture_id, reconcile_accounting=True
    )

    assert code == 0, recovered
    second_limits = json.loads(workflow.commands[1][-1])
    assert second_limits["max-requests"] == production.RUN_LIMITS["attempts"] - 12
    assert (
        second_limits["max-download-bytes"]
        == production.RUN_LIMITS["downloaded_bytes"] - charge * 2
    )
    assert recovered["requests"] == 12
    assert recovered["downloaded_bytes_upper_bound"] == charge * 2
    assert recovered["accounting_reconciliation"]["runtime_transition"] == {
        "from_uv_lock_sha256": production.RECONCILIATION_UV_LOCK_SHA256,
        "to_uv_lock_sha256": production.RECONCILIATION_UV_LOCK_SHA256,
        "transport_sha256": production.RECONCILIATION_TRANSPORT_SHA256,
    }


def test_mismatched_child_delta_remains_blocked_from_ordinary_resume(
    production, workflow, monkeypatch
):
    stopped, _report_path, _report = _stopped_capture_with_unknown_accounting(production, workflow)
    capture_id = stopped["capture_id"]
    snapshots = iter(
        [
            {
                "attempts": 10,
                "downloaded_or_reserved_bytes": 100,
                "accounting_complete": True,
                "unfinished_reservations": [],
                "storage": {},
                "root_id": stopped["root_id"],
            },
            {
                "attempts": 12,
                "downloaded_or_reserved_bytes": 200,
                "accounting_complete": True,
                "unfinished_reservations": [],
                "storage": {},
                "root_id": stopped["root_id"],
            },
        ]
    )
    monkeypatch.setattr(production, "_resource_guard", lambda *_a, **_kw: next(snapshots))
    monkeypatch.setattr(
        production,
        "_package_versions",
        lambda: _target_versions(),
    )
    workflow.outcomes = [{"requests": 1, "downloaded_bytes": 50}]

    failed, code = production.run_production(
        workflow.directory, capture_id=capture_id, reconcile_accounting=True
    )

    assert code != 0
    assert failed["accounting_complete"] is False
    assert failed["requests"] is None
    assert (
        failed["error"]
        == "durable receipt accounting is unknown or differs from the invocation ledger"
    )
    with pytest.raises(production.RunnerError, match="resume accounting is unknown"):
        production.run_production(workflow.directory, capture_id=capture_id)


def test_expired_resume_needs_one_explicit_extension_and_ordinary_resume_keeps_it(
    production, workflow, monkeypatch
):
    stopped, report_path, report = _stopped_capture_with_unknown_accounting(production, workflow)
    original_started_epoch = report["started_epoch"]
    original_started_monotonic = report["started_monotonic"]
    old_deadline_epoch = report["deadline_epoch"]
    old_deadline_monotonic = report["deadline_monotonic"]
    now_epoch = old_deadline_epoch + 10
    now_monotonic = old_deadline_monotonic + 10
    report["last_observed_epoch"] = now_epoch - 1
    report_path.write_text(json.dumps(report))
    monkeypatch.setattr(production.time, "time", lambda: now_epoch)
    monkeypatch.setattr(production.time, "monotonic", lambda: now_monotonic)
    monkeypatch.setattr(
        production,
        "_package_versions",
        lambda: _target_versions(),
    )

    with pytest.raises(production.RunnerError, match="48-hour session expired"):
        production._load_resume(
            workflow.directory, stopped["capture_id"], reconcile_accounting=True
        )

    _path, extended, _paths = production._load_resume(
        workflow.directory,
        stopped["capture_id"],
        reconcile_accounting=True,
        extend_session=True,
    )
    assert extended["started_epoch"] == original_started_epoch
    assert extended["started_monotonic"] == original_started_monotonic
    assert extended["time_budget_started_epoch"] == now_epoch
    assert extended["time_budget_started_monotonic"] == now_monotonic
    assert extended["deadline_epoch"] == now_epoch + production.RUN_LIMITS["session_seconds"]
    assert (
        extended["deadline_monotonic"] == now_monotonic + production.RUN_LIMITS["session_seconds"]
    )
    assert extended["session_extension_history"] == [
        {
            "action": "--extend-session",
            "capture_id": stopped["capture_id"],
            "granted_at": extended["session_extension_history"][0]["granted_at"],
            "source_revision": "a" * 40,
            "prior_deadline_epoch": old_deadline_epoch,
            "prior_deadline_monotonic": old_deadline_monotonic,
            "deadline_epoch": extended["deadline_epoch"],
            "deadline_monotonic": extended["deadline_monotonic"],
        }
    ]
    assert production.RUN_LIMITS["attempts"] == 850_000
    assert production.RUN_LIMITS["downloaded_bytes"] == 256 * 1024**3
    with pytest.raises(production.RunnerError, match="already been used"):
        production._load_resume(
            workflow.directory,
            stopped["capture_id"],
            reconcile_accounting=True,
            extend_session=True,
        )
    _path, resumed, _paths = production._load_resume(
        workflow.directory, stopped["capture_id"], reconcile_accounting=True
    )
    assert resumed["time_budget_started_epoch"] == now_epoch
    assert resumed["deadline_epoch"] == extended["deadline_epoch"]


def test_extension_requires_both_capture_and_reconciliation_flags(production, tmp_path):
    with pytest.raises(production.RunnerError, match="requires --capture-id"):
        production.run_production(
            tmp_path, full=True, reconcile_accounting=True, extend_session=True
        )
    with pytest.raises(SystemExit):
        production.main(
            ["--directory", str(tmp_path), "--capture-id", "a" * 32, "--extend-session"]
        )


def test_invalid_effective_clock_fails_before_reconciliation_or_networking(
    production, workflow, monkeypatch
):
    stopped, report_path, report = _stopped_capture_with_unknown_accounting(production, workflow)
    report["time_budget_started_epoch"] = "invalid"
    report_path.write_text(json.dumps(report))
    monkeypatch.setattr(
        production,
        "_package_versions",
        lambda: _target_versions(),
    )
    workflow.commands.clear()

    with pytest.raises(production.RunnerError, match="time budget start is invalid"):
        production.run_production(
            workflow.directory,
            capture_id=stopped["capture_id"],
            reconcile_accounting=True,
        )

    assert workflow.commands == []


def test_failed_offline_preflight_consumes_extension_without_starting_networking(
    production, workflow, monkeypatch
):
    stopped, report_path, report = _stopped_capture_with_unknown_accounting(production, workflow)
    now_epoch = report["deadline_epoch"] + 10
    now_monotonic = report["deadline_monotonic"] + 10
    report["last_observed_epoch"] = now_epoch - 1
    report_path.write_text(json.dumps(report))
    monkeypatch.setattr(production.time, "time", lambda: now_epoch)
    monkeypatch.setattr(production.time, "monotonic", lambda: now_monotonic)
    monkeypatch.setattr(
        production,
        "_package_versions",
        lambda: _target_versions(),
    )
    monkeypatch.setattr(
        production,
        "_resource_guard",
        lambda *_a, **_kw: (_ for _ in ()).throw(
            production.RunnerError("offline accounting failed")
        ),
    )
    workflow.events.clear()
    workflow.commands.clear()

    result, code = production.run_production(
        workflow.directory,
        capture_id=stopped["capture_id"],
        reconcile_accounting=True,
        extend_session=True,
    )

    assert code != 0
    assert result["error"] == "offline accounting failed"
    assert len(result["session_extension_history"]) == 1
    assert "refresh" not in workflow.events
    assert workflow.commands == []
