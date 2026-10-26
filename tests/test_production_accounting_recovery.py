"""Explicit recovery keeps interrupted acquisition accounting conservative."""

from __future__ import annotations

import json

import pytest

pytest_plugins = ("test_production_workflow",)


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
    report["system"]["package_versions"] = {"httpx": "0.28.1"}
    report["system"]["uv_lock_sha256"] = production.RECONCILIATION_UV_LOCK_SHA256
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
        lambda: {"httpx": "0.28.1", "httpcore": "1.0.9"},
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
    monkeypatch.setattr(production, "_package_versions", lambda: current_versions)
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
        production.run_production(
            workflow.directory,
            capture_id=stopped["capture_id"],
            reconcile_accounting=True,
        )

    assert report_path.read_bytes() == report_before
    assert workflow.commands == []
    assert workflow.events == []
