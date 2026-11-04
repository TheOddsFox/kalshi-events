from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import shutil
import sys
import time
import uuid
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from conftest import FakeClient

from kalshi_events import capture, publish
from kalshi_events.common import CatalogueError, Store
from kalshi_events.historical import historical_task_indices

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"


def _load_script(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


grants = _load_script("historical_grants", SCRIPTS / "historical_grants.py")
activation = _load_script("production_activation", SCRIPTS / "production_activation.py")
production = _load_script(
    "historical_control_release_coverage_runner", SCRIPTS / "production_run.py"
)


@pytest.fixture(autouse=True)
def _loopback_offline_api(monkeypatch):
    monkeypatch.setenv("KALSHI_EVENTS_API_URL", "http://127.0.0.1:9/trade-api/v2")
    monkeypatch.setenv("KALSHI_EVENTS_ALLOW_LOOPBACK", "1")


@pytest.fixture(scope="module")
def incomplete_historical_release():
    evidence_root = (
        ROOT / "data" / "testing" / "historical-control-release-coverage" / uuid.uuid4().hex
    )
    source_directory = evidence_root / "source-production"
    source_root = source_directory / "operator"
    source_root.mkdir(parents=True, mode=0o700)
    store = Store(source_root)
    capture_id: list[str] = []

    def fail_unrelated_combo_after_history(_params):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if capture_id:
                state = capture.read_capture(store, capture_id[0])
                selected = [
                    int(index)
                    for index, scope in state.get("task_scopes", {}).items()
                    if scope in {"series", "events_live", "markets_history"}
                ]
                if selected and all(state["tasks"][index]["done"] for index in selected):
                    return 500, {"message": "synthetic unrelated combo failure"}
            time.sleep(0.01)
        raise RuntimeError("timed out waiting for the historical task plan")

    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("KALSHI_EVENTS_API_URL", "http://127.0.0.1:9/trade-api/v2")
        patch.setenv("KALSHI_EVENTS_ALLOW_LOOPBACK", "1")
        with pytest.raises(CatalogueError, match="incomplete source traversal"):
            capture.capture(
                store,
                client_factory=lambda root, budget, **_options: FakeClient(
                    root,
                    budget,
                    {"/events/multivariate": fail_unrelated_combo_after_history},
                ),
                catalogue_profile="exchange",
                _on_capture_id=capture_id.append,
            )

    source_state = capture.read_capture(store, capture_id[0])
    selected = historical_task_indices(source_state, allow_legacy_incomplete=True)
    assert source_state["complete"] is False
    assert selected
    assert all(source_state["tasks"][index]["done"] for index in selected)
    manifest = publish.publish(store, capture_id[0], catalogue_version=3)
    assert manifest["source_capture_complete"] is False
    assert manifest["coverage_scope"] == "historical"
    assert manifest["historical_contract"] == "oddsfox.kalshi.historical.v1"
    try:
        yield evidence_root, source_directory, manifest
    finally:
        shutil.rmtree(evidence_root, ignore_errors=True)


@pytest.fixture
def verified_candidate(incomplete_historical_release, monkeypatch):
    evidence_root, source_directory, _source_manifest = incomplete_historical_release
    directory = evidence_root / f"candidate-{uuid.uuid4().hex}"
    shutil.copytree(source_directory, directory)
    operator = directory / "operator"
    store = Store(operator)
    manifest = publish.current(store, catalogue_version=3)
    release_output = Path(manifest["output"])
    capture_id = manifest["capture_id"]
    now_epoch = time.time()
    now_monotonic = time.monotonic()

    relation_names = (
        "series",
        "events",
        "markets",
        "relationships",
        "combo_legs",
        "observation_history",
    )
    expected_rows = {
        name: pq.read_metadata(release_output / f"{name}.parquet").num_rows
        for name in relation_names
    }
    endpoint_rows = Counter(
        pq.read_table(release_output / "observation_history.parquet", columns=["source_endpoint"])
        .column("source_endpoint")
        .to_pylist()
    )
    observations_by_endpoint = dict(endpoint_rows)
    assert observations_by_endpoint
    assert sum(observations_by_endpoint.values()) == expected_rows["observation_history"]

    monkeypatch.setattr(production, "_revision", lambda: "d" * 40)
    report_path, report, _paths = production._session_report(directory, "historical", capture_id)
    session_id = report["session_id"]
    report["root_id"] = manifest["root_id"]
    production._bind_report_resource_profile(report, "expanded", transition=True)
    census = {
        "capture_id": capture_id,
        "root_id": manifest["root_id"],
        "capture_state_sha256": capture.read_capture(store, capture_id).verified_snapshot_sha256,
        "receipt_index_sha256": hashlib.sha256(b"synthetic receipt index").hexdigest(),
        "population_report_sha256": hashlib.sha256(b"synthetic population report").hexdigest(),
        "observations_by_endpoint": observations_by_endpoint,
        "exact_observations": expected_rows["observation_history"],
    }
    session_directory = report_path.parent
    audit = {
        "version": 1,
        "session_id": session_id,
        **census,
        "sample_strategy": "verified-parquet-keyed-v1",
    }
    audit_bytes = json.dumps(
        audit,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    audit_sha256 = hashlib.sha256(audit_bytes).hexdigest()
    audit_name = f"activation-audit-{audit_sha256}.json"
    audit_path = session_directory / audit_name
    audit_path.write_bytes(audit_bytes)
    audit_path.chmod(0o400)
    report["activation_audit"] = {"relative_path": audit_name, "sha256": audit_sha256}
    production._atomic_report(report_path, report)

    control_root = evidence_root / f"controls-{uuid.uuid4().hex}"
    control_root.mkdir(mode=0o700)
    for name in ("activations", "activation-audits", "phases", "verifications"):
        (control_root / name).mkdir(mode=0o700)
    verification_grant_id = uuid.uuid4().hex
    activation_grant_id = uuid.uuid4().hex
    identity = {
        "capture_id": capture_id,
        "session_id": session_id,
        "root_id": manifest["root_id"],
        "stage_name": "build-historical-" + uuid.uuid4().hex,
        "failed_attempts_sha256": hashlib.sha256(b"synthetic failed attempts").hexdigest(),
        "prior_session_sha256": hashlib.sha256(b"synthetic prior session").hexdigest(),
        "state_sha256": hashlib.sha256(b"synthetic frozen state").hexdigest(),
        "source_sha256": hashlib.sha256(b"synthetic frozen source").hexdigest(),
        "runtime_sha256": hashlib.sha256(b"synthetic frozen runtime").hexdigest(),
    }
    authority = {
        "identity": identity,
        "directory": directory,
        "operator": operator,
        "report_path": report_path,
        "report": report,
        "grant": {
            "grant_id": activation_grant_id,
            "deadline_epoch": now_epoch + 3600,
            "deadline_monotonic": now_monotonic + 3600,
        },
        "control_root": control_root,
        "control": {"authorized_census": census},
        "approval": {"activation_audit_sha256": audit_sha256},
        "launch": {"launch_id": uuid.uuid4().hex},
        "store": store,
    }
    report_before_verification = report_path.read_bytes()
    verification = production._historical_release_verification(
        authority, grant_id_for_receipt=verification_grant_id
    )
    assert report_path.read_bytes() == report_before_verification
    assert verification["standalone"] == verification["manifest"]
    assert "scope_complete" not in verification["standalone"]
    assert verification["receipt"]["source_capture_complete"] is False
    assert verification["receipt"]["scope_complete"] is True
    phase_path = control_root / "phases" / "independent-verification.json"
    grants.write_once_json(
        phase_path,
        {
            "version": 1,
            "phase": "independent-verification",
            "grant_id": verification_grant_id,
            "outputs": {"verification_receipt_sha256": verification["verification_receipt_sha256"]},
        },
    )
    try:
        yield {
            "authority": authority,
            "directory": directory,
            "operator": operator,
            "store": store,
            "manifest": manifest,
            "expected_rows": expected_rows,
            "verification": verification,
            "verification_grant_id": verification_grant_id,
            "control_root": control_root,
            "phase_path": phase_path,
        }
    finally:
        shutil.rmtree(directory, ignore_errors=True)
        shutil.rmtree(control_root, ignore_errors=True)


def test_private_verification_and_activation_keep_global_and_selected_coverage_separate(
    verified_candidate,
):
    directory = verified_candidate["directory"]
    manifest = verified_candidate["manifest"]
    authority = verified_candidate["authority"]
    operator = verified_candidate["operator"]
    expected_rows = verified_candidate["expected_rows"]
    alias = directory / "production.duckdb"
    previous = directory / "previous-production.duckdb"
    previous.write_bytes(b"previous valid consumer snapshot")
    os.link(previous, alias)
    previous_identity = (alias.stat().st_dev, alias.stat().st_ino)
    source_warehouse = operator / "builds-historical" / manifest["capture_id"] / "warehouse.duckdb"

    result = production._historical_worker_action(authority, "activation", "activate")

    activation_path = (
        verified_candidate["control_root"]
        / "activations"
        / f"{authority['grant']['grant_id']}.json"
    )
    activated = json.loads(activation_path.read_text())
    assert result["relation_rows"] == expected_rows
    assert result["production_database_sha256"] == manifest["warehouse_sha256"]
    assert activated["state"] == "verified"
    assert activated["source_release_id"] == manifest["source_release_id"]
    assert activated["capture_id"] == manifest["capture_id"]
    assert activated["coverage_scope"] == "historical"
    assert type(activated["source_capture_complete"]) is bool
    assert activated["source_capture_complete"] is False
    assert activated["scope_complete"] is True
    assert activated["relation_rows"] == expected_rows
    assert activated["production_readback"]["relation_rows"] == expected_rows
    assert activated["production_readback"]["read_only"] is True
    assert activated["catalogue_coverage"] == {
        "historical_contract": "oddsfox.kalshi.historical.v1",
        "scope": "historical",
        "omitted_scopes": json.loads(manifest["omitted_scopes_json"]),
        "source_capture_id": manifest["capture_id"],
        "source_capture_complete": False,
    }
    assert (alias.stat().st_dev, alias.stat().st_ino) != previous_identity
    assert os.path.samefile(alias, source_warehouse)
    assert not list(directory.glob(".production.duckdb.*.tmp"))


def test_activation_rejects_receipt_binding_and_type_mismatches_before_alias_replace(
    verified_candidate,
):
    authority = verified_candidate["authority"]
    control_root = verified_candidate["control_root"]
    verification_grant_id = verified_candidate["verification_grant_id"]
    phase_path = verified_candidate["phase_path"]
    receipt_path = control_root / "verifications" / f"{verification_grant_id}.json"
    original = verified_candidate["verification"]["receipt"]
    alias = verified_candidate["directory"] / "production.duckdb"
    sentinel = b"existing production database must survive failed receipt checks"
    alias.write_bytes(sentinel)

    mutations = (
        ("source_capture_complete", 0),
        ("source_capture_complete", None),
        ("source_capture_complete", True),
        ("coverage_scope", "catalogue"),
        ("scope_complete", False),
        ("release_id", "0" * 64),
        ("release_manifest_sha256", "1" * 64),
        ("capture_id", "2" * 32),
    )
    for field, value in mutations:
        receipt = dict(original)
        receipt[field] = value
        receipt_path.write_text(json.dumps(receipt, sort_keys=True), encoding="utf-8")
        seal = {
            "version": 1,
            "phase": "independent-verification",
            "grant_id": verification_grant_id,
            "outputs": {"verification_receipt_sha256": grants.sha256_json(receipt)},
        }
        phase_path.write_text(json.dumps(seal, sort_keys=True), encoding="utf-8")

        with pytest.raises(production.RunnerError, match="sealed independent verification receipt"):
            production._historical_worker_action(authority, "activation", "activate")
        assert alias.read_bytes() == sentinel, field
        assert not (
            control_root / "activations" / f"{authority['grant']['grant_id']}.json"
        ).exists()


def test_fresh_grant_reuses_audit_after_post_replace_failure_with_fresh_readback(
    verified_candidate, monkeypatch, capsys
):
    authority = verified_candidate["authority"]
    control_root = verified_candidate["control_root"]
    directory = verified_candidate["directory"]
    operator = verified_candidate["operator"]
    manifest = verified_candidate["manifest"]
    source_warehouse = operator / "builds-historical" / manifest["capture_id"] / "warehouse.duckdb"
    alias = directory / "production.duckdb"
    sentinel = b"prior consumer database"
    alias.write_bytes(sentinel)
    original_grant_id = authority["grant"]["grant_id"]
    original_launch_id = authority["launch"]["launch_id"]
    original_audit_sha256 = authority["approval"]["activation_audit_sha256"]
    claim_path = control_root / "activation-audits" / f"{original_grant_id}.json"
    activation_path = control_root / "activations" / f"{original_grant_id}.json"
    launch_sha256 = "9" * 64
    invocation_token = uuid.uuid4().hex
    storage_progress_path = authority["report_path"].parent / f"storage-{invocation_token}.json"
    authority["report"]["active_process_group"] = os.getpid()
    authority["report"]["private_historical_grant"] = {
        "grant_id": original_grant_id,
        "launch_sha256": launch_sha256,
    }
    production._atomic_report(authority["report_path"], authority["report"])

    real_activate = production.activate_current
    real_os_replace = os.replace
    replace_calls = 0

    def replace_then_fail_after_commit(source, destination):
        nonlocal replace_calls
        real_os_replace(source, destination)
        if Path(destination) == alias:
            replace_calls += 1
            raise OSError("synthetic failure after atomic alias replacement")

    def activate_with_post_replace_failure(*args, **kwargs):
        kwargs["replace_fn"] = replace_then_fail_after_commit
        return real_activate(*args, **kwargs)

    class SyntheticSupervisorHeartbeat:
        """The real pipe and storage observer are covered by the separate handoff test."""

        def __init__(self, *_args, **_kwargs):
            pass

        def start(self):
            pass

        def check(self):
            pass

        def close(self):
            pass

    def run_private_worker(*, fail_after_replace=False):
        # Only supervisor handoff and frozen authority entry are synthetic here.
        # The exception classifier, report I/O, audit claim lookup, and activation stay real.
        with monkeypatch.context() as patch:
            patch.setattr(production, "_historical_control_root", lambda _path: control_root)
            patch.setattr(
                production,
                "_historical_consume_supervisor_handoff",
                lambda *_args, **_kwargs: {"supervisor_pid": os.getpid()},
            )
            patch.setattr(
                grants,
                "read_control_bundle",
                lambda *_args, **_kwargs: {
                    "identity": authority["identity"],
                    "launch": authority["launch"],
                },
            )
            patch.setattr(production, "_historical_authority", lambda *_args, **_kwargs: authority)
            patch.setattr(production, "_historical_validate_phase_chain", lambda _authority: None)
            patch.setattr(production, "_HistoricalStorageHeartbeat", SyntheticSupervisorHeartbeat)
            patch.setattr(production.os, "getpgrp", lambda: os.getpid())
            if fail_after_replace:
                patch.setattr(production, "activate_current", activate_with_post_replace_failure)
            result = production._historical_worker_main(
                directory,
                control_root,
                authority["grant"]["grant_id"],
                launch_sha256,
                "activation",
                "activate",
                invocation_token,
                storage_progress_path,
            )
        output = json.loads(capsys.readouterr().out)
        return result, output

    status, first_failure = run_private_worker(fail_after_replace=True)

    assert status == 1
    assert first_failure["phase"] == "activation"
    assert "verified readback" in first_failure["error"]
    assert replace_calls == 1
    assert os.path.samefile(alias, source_warehouse)
    assert alias.read_bytes() != sentinel
    assert claim_path.is_file()
    original_claim = grants.read_json(claim_path)
    original_claim_bytes = claim_path.read_bytes()
    assert original_claim == {
        "version": 1,
        "audit_sha256": original_audit_sha256,
        "identity": authority["identity"],
        "grant_id": original_grant_id,
        "launch_id": original_launch_id,
    }
    assert production._historical_activation_claimed(
        control_root, original_grant_id, original_launch_id, authority["identity"]
    )
    wrong_identity = {**authority["identity"], "root_id": "0" * 32}
    with pytest.raises(production.RunnerError, match="not bound"):
        production._historical_activation_claimed(
            control_root, original_grant_id, original_launch_id, wrong_identity
        )
    assert json.loads(authority["report_path"].read_text())["production_state"] == "unknown"
    assert not activation_path.exists()

    status, same_grant_replay = run_private_worker()
    assert status == 1
    assert "already exists" in same_grant_replay["error"]
    assert claim_path.read_bytes() == original_claim_bytes
    assert os.path.samefile(alias, source_warehouse)
    assert json.loads(authority["report_path"].read_text())["production_state"] == "unknown"
    assert not activation_path.exists()

    authority["grant"] = {**authority["grant"], "grant_id": uuid.uuid4().hex}
    authority["launch"] = {**authority["launch"], "launch_id": uuid.uuid4().hex}
    fresh_grant_id = authority["grant"]["grant_id"]
    fresh_launch_id = authority["launch"]["launch_id"]
    authority["report"] = json.loads(authority["report_path"].read_text())
    authority["report"]["private_historical_grant"] = {
        "grant_id": fresh_grant_id,
        "launch_sha256": launch_sha256,
    }
    production._atomic_report(authority["report_path"], authority["report"])
    assert not production._historical_activation_claimed(
        control_root, fresh_grant_id, fresh_launch_id, authority["identity"]
    )
    status, fresh_success = run_private_worker()

    fresh_claim_path = control_root / "activation-audits" / f"{fresh_grant_id}.json"
    fresh_activation_path = control_root / "activations" / f"{fresh_grant_id}.json"
    fresh_claim = grants.read_json(fresh_claim_path)
    activated = json.loads(fresh_activation_path.read_text())
    assert fresh_claim["audit_sha256"] == original_audit_sha256
    assert fresh_claim["identity"] == original_claim["identity"]
    assert fresh_claim["launch_id"] == authority["launch"]["launch_id"]
    assert claim_path.read_bytes() == original_claim_bytes
    assert production._historical_activation_claimed(
        control_root, original_grant_id, original_launch_id, authority["identity"]
    )
    assert production._historical_activation_claimed(
        control_root, fresh_grant_id, fresh_launch_id, authority["identity"]
    )
    assert status == 0
    assert fresh_success["ok"] is True
    assert activated["state"] == "verified"
    assert activated["source_release_id"] == manifest["source_release_id"]
    assert activated["capture_id"] == manifest["capture_id"]
    assert activated["source_capture_complete"] is False
    assert activated["scope_complete"] is True
    assert activated["relation_rows"] == verified_candidate["expected_rows"]
    assert activated["activation_audit"]["sha256"] == original_audit_sha256
    assert json.loads(authority["report_path"].read_text())["production_state"] == "attempting"
    assert os.path.samefile(alias, source_warehouse)
    assert not list(directory.glob(".production.duckdb.*.tmp"))
