"""Explicit recovery keeps interrupted acquisition accounting conservative."""

from __future__ import annotations

import json

pytest_plugins = ("test_production_workflow",)


def test_reconcile_unknown_accounting_then_verify_and_activate_completed_capture(
    production, workflow, monkeypatch
):
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
