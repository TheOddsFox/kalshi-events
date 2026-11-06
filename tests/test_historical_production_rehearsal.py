"""Opt-in installed-runtime rehearsal of the real historical controller."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import uuid
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from kalshi_events import publish, warehouse

ROOT = Path(__file__).resolve().parents[1]
pytestmark = [
    pytest.mark.skipif(
        os.environ.get("KALSHI_HISTORICAL_REHEARSAL") != "1",
        reason="set KALSHI_HISTORICAL_REHEARSAL=1 for the host rehearsal",
    ),
    pytest.mark.timeout(3600),
]


def _issue_fixture_grant(
    grants,
    runner,
    directory,
    control,
    report_path,
    identity,
    phases,
    *,
    audit_sha=None,
    wrong_binding=False,
):
    grant_id = uuid.uuid4().hex
    frozen, _ = grants.read_frozen_control(control, expected_identity=identity)
    freeze = frozen["freeze"]
    token = runner._ACTIVE_LIMITS.set(runner._limits_for_profile("expanded"))
    try:
        with runner._run_lock(directory):
            report_raw = runner._historical_session_report_bytes(report_path)
            report = json.loads(report_raw)
            assert report["phase"] == "stopped" and report["active_process_group"] is None
            operator = directory / "operator"
            store = runner._historical_store(operator, max_duration_seconds=600)
            with store.writer_guard():
                mount = runner._historical_current_mount(
                    directory, operator, control, report, identity, freeze
                )
                baseline = runner._historical_resource_baseline(directory, store, report)
    finally:
        runner._ACTIVE_LIMITS.reset(token)
    admission = grants.expected_resource_admission()
    projected = {
        "projected_database_bytes": baseline["database_bytes"] + grants.GIB,
        "projected_temporary_bytes": baseline["temporary_bytes"] + grants.GIB,
        "projected_root_bytes": baseline["root_bytes"] + 2 * grants.GIB,
        "projected_aggregate_bytes": baseline["aggregate_bytes"] + 2 * grants.GIB,
        "projected_free_bytes": baseline["free_bytes"] - 2 * grants.GIB,
        "inventory_entries": baseline["inventory_entries"] + 1024,
        "projected_peak_rss_bytes": admission["peak_rss_bytes"],
    }
    measured = {
        "measured_at_epoch": time.time(),
        "session_report_sha256": grants.sha256_bytes(report_raw),
        "forecast_seconds": 600,
        "final_verification_seconds": grants.MIN_RESERVE_SECONDS,
        **projected,
    }
    destination = control / "grants" / grant_id
    measurement_sha = grants.write_once_json(
        destination / "measurements.json",
        {
            "version": 1,
            "grant_id": grant_id,
            "identity": identity,
            "phases": phases,
            **{
                key: freeze[key]
                for key in (
                    "source_snapshot_sha256",
                    "receipt_evidence_sha256",
                    "scope_sha256",
                    "code_sha256",
                    "root_volume_uuid",
                )
            },
            "root_device": mount["device"],
            "baseline": baseline,
            **measured,
        },
    )
    approval = {
        "version": 1,
        "grant_id": grant_id,
        "phase": phases[0],
        "phases": phases,
        "bindings": dict(identity, root_id=uuid.uuid4().hex) if wrong_binding else identity,
        "approved_at_epoch": time.time(),
        "duration_seconds": grants.MIN_RESERVE_SECONDS + 1200,
        "forecast_seconds": measured["forecast_seconds"],
        "measured_final_verification_seconds": measured["final_verification_seconds"],
        "measurements": {"report_sha256": measurement_sha, **measured},
        "resource_admission": admission,
        "freeze": {"engineering": True, "runtime": True, "controls": True},
        "retire_source": False,
        "retirement_policy": None,
        "activation_audit_sha256": audit_sha,
    }
    grants.write_once_json(destination / "approval.json", approval)
    grants.write_once_json(
        destination / "latch.json",
        {
            "version": 1,
            "grant_id": grant_id,
            "approval_sha256": grants.sha256_json(approval),
            "nonce": uuid.uuid4().hex,
            "created_at_epoch": time.time(),
        },
    )
    return grant_id


def _interrupt_export(controller, command, report_path):
    deadline = time.monotonic() + 180
    while controller.poll() is None and time.monotonic() < deadline:
        report = json.loads(report_path.read_bytes())
        marker = report.get("private_historical_grant", {})
        worker = marker.get("worker_pid")
        if (
            report.get("phase") == "historical-export-publish"
            and marker.get("grant_id") == command[-1]
            and marker.get("controller_pid") == controller.pid
            and marker.get("state") == "supervised-child-pending"
            and type(worker) is int
            and worker > 1
            and marker.get("worker_pgid") == report.get("active_process_group") == worker
        ):
            expected = [
                *command[:3],
                "--_historical-worker",
                *command[4:],
                marker["launch_sha256"],
                "export",
                "publish",
                marker["storage_invocation_token"],
                marker["storage_progress_path"],
            ]
            probe = subprocess.run(
                ["/bin/ps", "-p", str(worker), "-o", "ppid=,pgid=,command="],
                capture_output=True,
                text=True,
                timeout=3,
                check=True,
            )
            assert probe.stdout.strip().split(maxsplit=2) == [
                str(controller.pid),
                str(worker),
                " ".join(expected),
            ]
            os.killpg(worker, signal.SIGTERM)
            return worker
        time.sleep(0.02)
    pytest.fail("did not observe the exact supervised export worker before the timeout")


def _run_fixture_grant(grants, directory, control, report_path, grant_id, *, interrupt=False):
    assert directory.is_relative_to(ROOT / "data/testing/historical-production-rehearsal")
    assert directory.resolve(strict=True) == directory
    assert control.parent == ROOT / "data/runtime"
    command = [
        str(control / ".venv/bin/python"),
        "-B",
        str(control / "source/scripts/production_run.py"),
        "--_historical-control",
        str(directory),
        str(control),
        grant_id,
    ]
    evidence = control / "grants" / grant_id
    env = {
        "PATH": os.environ["PATH"],
        "KALSHI_EVENTS_PROJECT_ROOT": str(ROOT),
        "KALSHI_EVENTS_REVISION": subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            text=True,
        ).strip(),
        "KALSHI_EVENTS_API_URL": "http://127.0.0.1:9/trade-api/v2",
        "KALSHI_EVENTS_ALLOW_LOOPBACK": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
    }
    grants.write_once_json(evidence / "rehearsal-command.json", command)
    log = evidence / "rehearsal-controller.log"
    worker = None
    with log.open("xb") as stream:
        controller = subprocess.Popen(
            command,
            cwd=ROOT,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            umask=0o077,
        )
        try:
            if interrupt:
                worker = _interrupt_export(controller, command, report_path)
            code = controller.wait(timeout=960)
        finally:
            if controller.poll() is None:
                # Leave the sealed grant and its exact live marker for reconciliation.
                controller.terminate()
                try:
                    controller.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    controller.kill()
                    controller.wait(timeout=5)
    return code, log.read_text(), worker


def _check_fixture_release(release, database, fixture):
    census = fixture["census"]
    expected = fixture["expected"]
    manifest = publish.verify_release(release)
    assert manifest["source_capture_complete"] is False
    assert manifest["coverage_scope"] == "historical"
    assert (release / "coverage.parquet").is_file()
    assert not database.with_suffix(".duckdb.wal").exists()
    counts = {}
    with duckdb.connect(str(database), read_only=True) as con:
        for relation, table in warehouse.RELATIONS.items():
            db_rows = con.sql(f"select * from main.{table}").to_arrow_table()
            exported = pq.read_table(release / f"{relation}.parquet")
            assert db_rows.column_names == exported.column_names
            keys = publish.ORDER_KEYS[relation].split(", ")
            ordering = [(key, "ascending") for key in keys]
            assert db_rows.cast(exported.schema).sort_by(ordering).equals(exported), relation
            key_rows = [tuple(row.values()) for row in exported.select(keys).to_pylist()]
            assert len(set(key_rows)) == len(key_rows), relation
            counts[relation] = exported.num_rows
        assert (
            dict(
                con.execute(
                    "select source_endpoint, count(*) from main.observation_history group by 1"
                ).fetchall()
            )
            == census["observations_by_endpoint"]
        )
        assert census["observations_by_endpoint"] == expected["observations_by_endpoint"]
        assert census["exact_observations"] == expected["exact_observations"]
        for relation, key in (
            ("current_series", "series_ticker"),
            ("current_events", "event_ticker"),
            ("current_markets", "market_ticker"),
        ):
            actual_keys = con.execute(f"select {key} from main.{relation} order by 1").fetchall()
            assert [row[0] for row in actual_keys] == expected["relation_keys"][relation]
        for market in expected["source"]["markets"]:
            price, volume, capture_id, endpoint, payload = con.execute(
                "select last_price_dollars, volume_fp, capture_id, source_endpoint, payload_json "
                "from main.current_markets where market_ticker = ?",
                [market["ticker"]],
            ).fetchone()
            assert price == Decimal(market["last_price_dollars"])
            assert volume == Decimal(market["volume_fp"])
            assert (capture_id, endpoint) == (fixture["capture_id"], "/historical/markets")
            assert json.loads(payload)["ticker"] == market["ticker"]
    assert counts["observation_history"] == census["exact_observations"]
    return counts


def test_real_sealed_historical_recovery(monkeypatch):
    if sys.platform != "darwin":
        pytest.fail("the opt-in historical rehearsal requires macOS")
    from historical_rehearsal_fixture import prepare_fixture
    from historical_rehearsal_seal import prepare_source, seal_fixture
    from test_historical_control_handoff_positive import _new_control_root, grants, production

    monkeypatch.setenv(
        "KALSHI_EVENTS_REVISION",
        subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
    )
    assert shutil.disk_usage(ROOT).free >= 152 * grants.GIB
    helper = Path(os.environ["KALSHI_HISTORICAL_REHEARSAL_RUNTIME_HELPER"])
    wheel = Path(os.environ["KALSHI_HISTORICAL_REHEARSAL_WHEEL"])
    for name in set(os.environ) - {
        "PATH",
        "HOME",
        "LANG",
        "LC_ALL",
        "TMPDIR",
        "UV_CACHE_DIR",
        "KALSHI_EVENTS_REVISION",
    }:
        monkeypatch.delenv(name)
    monkeypatch.setenv("KALSHI_EVENTS_API_URL", "http://127.0.0.1:9/trade-api/v2")
    monkeypatch.setenv("KALSHI_EVENTS_ALLOW_LOOPBACK", "1")
    monkeypatch.setenv("PYTHONDONTWRITEBYTECODE", "1")
    monkeypatch.setenv("PYTHONNOUSERSITE", "1")
    namespace = ROOT / "data/testing/historical-production-rehearsal"
    assert namespace.resolve() == namespace
    assert (ROOT / "data/runtime").resolve() == ROOT / "data/runtime"
    directory = namespace / uuid.uuid4().hex
    directory.mkdir(parents=True, mode=0o700)
    control = _new_control_root()
    print(f"historical rehearsal evidence: {directory} and {control}", flush=True)
    prepare_source(control, helper, wheel)
    fixture = prepare_fixture(directory, control)
    report_path = fixture["report_path"]
    identity = seal_fixture(control, directory, fixture)

    def run(phases, **options):
        grant_id = _issue_fixture_grant(
            grants,
            production,
            directory,
            control,
            report_path,
            identity,
            phases,
            audit_sha=options.pop("audit_sha", None),
            wrong_binding=options.pop("wrong_binding", False),
        )
        code, output, worker = _run_fixture_grant(
            grants, directory, control, report_path, grant_id, **options
        )
        return grant_id, code, output, worker

    old_report = report_path.read_bytes()
    unrelated = fixture["unrelated_spill"]
    unrelated_hash = hashlib.sha256(unrelated.read_bytes()).digest()
    survivor = fixture["owned_spill"]
    cleanup_inventory = survivor.parent.parent / "historical-spill-cleanup.json"
    original_cleanup = cleanup_inventory.read_bytes()
    original_files = json.loads(original_cleanup)["result"]["files"]
    assert len(original_files) == 2 and survivor.is_file()
    stage_db = directory / "operator/tmp" / fixture["stage_name"] / "warehouse.duckdb"
    source = {
        path: hashlib.sha256(path.read_bytes()).digest()
        for path in (
            stage_db,
            stage_db.with_suffix(".duckdb.wal"),
        )
    }
    bad_grant, code, output, _ = run(["source"], wrong_binding=True)
    assert code != 0 and "bound to a different capture" in output, output
    assert not (control / "grants" / bad_grant / "launch.json").exists()
    assert report_path.read_bytes() == old_report
    assert survivor.is_file()
    assert not (directory / "production.duckdb").exists()

    _, code, output, _ = run(["source", "readiness", "adoption"])
    assert code == 0, output
    assert not survivor.exists()
    assert cleanup_inventory.read_bytes() == original_cleanup
    maintenance = grants.read_json(control / "source/startup-maintenance.json")
    assert maintenance["removed_files"] == sorted(original_files, key=lambda item: item["path"])
    assert hashlib.sha256(unrelated.read_bytes()).digest() == unrelated_hash
    assert all(
        hashlib.sha256(path.read_bytes()).digest() == digest for path, digest in source.items()
    )
    assert grants.read_phase_chain(control / "phases", identity)[0] == "adoption"

    _, code, output, _ = run(["models", "certified-build"])
    assert code == 0, output
    checkpoint = grants.read_phase_chain(control / "phases", identity)
    assert checkpoint[0] == "certified-build"
    interrupted, code, output, worker = run(["export"], interrupt=True)
    assert code != 0 and worker is not None, output
    assert not production._process_group_exists(worker)
    attempt = grants.read_json(control / "grants" / interrupted / "attempt-result.json")
    assert attempt["outcome"] == "stopped" and attempt["reconciliation_required"] is False
    assert (attempt["phase"], attempt["substep"]) == ("export", "publish")
    assert grants.read_phase_chain(control / "phases", identity) == checkpoint
    stopped_report = json.loads(report_path.read_bytes())
    assert stopped_report["phase"] == "stopped"
    assert stopped_report["active_process_group"] is None
    progress = Path(stopped_report["storage_supervision_file"])
    assert progress.is_relative_to(report_path.parent)
    storage = grants.read_json(progress)
    assert storage["invocation_token"] == stopped_report["storage_supervision_invocation"]
    assert storage["sample_count"] > 0 and storage["last_sample_monotonic"] is not None

    _, code, output, _ = run(["export", "verified-publication", "independent-verification"])
    assert code == 0, output
    assert not (directory / "operator/prune-trash").exists()
    assert not (directory / "production.duckdb").exists()
    publication = grants.read_json(control / "phases/verified-publication.json")["outputs"]
    release = directory / "operator/releases" / publication["release_id"]
    certified = (
        directory / "operator/builds-historical" / fixture["capture_id"] / "warehouse.duckdb"
    )
    counts = _check_fixture_release(release, certified, fixture)
    census_fields = (
        "capture_id",
        "root_id",
        "capture_state_sha256",
        "receipt_index_sha256",
        "population_report_sha256",
        "observations_by_endpoint",
        "exact_observations",
    )
    report = json.loads(report_path.read_bytes())
    assert report["publication_state"] == "verified"
    assert report["production_state"] == "not_attempted"
    audit = {
        "version": 1,
        "session_id": report["session_id"],
        "sample_strategy": "verified-parquet-keyed-v1",
        **{key: fixture["census"][key] for key in census_fields},
    }
    audit_raw = grants.canonical_bytes(audit)
    audit_sha = grants.sha256_bytes(audit_raw)
    audit_path = report_path.with_name(f"activation-audit-{audit_sha}.json")
    with production._run_lock(directory):
        with audit_path.open("xb") as handle:
            handle.write(audit_raw)
            os.fsync(handle.fileno())
        audit_path.chmod(0o400)
        report["activation_audit"] = {"relative_path": audit_path.name, "sha256": audit_sha}
        production._atomic_report(report_path, report)
    activation, code, output, _ = run(["activation"], audit_sha=audit_sha)
    assert code == 0, output
    result = grants.read_json(control / "activations" / f"{activation}.json")
    alias = directory / "production.duckdb"
    assert result["state"] == "verified" and result["relation_rows"] == counts
    assert result["production_readback"]["read_only"] is True
    assert result["scope_complete"] is True and result["source_capture_complete"] is False
    assert alias.samefile(certified)
    alias_sha = hashlib.sha256(alias.read_bytes()).hexdigest()
    assert alias_sha == result["warehouse_sha256"] == result["manifest_warehouse_sha256"]
    assert _check_fixture_release(release, alias, fixture) == counts
    final_report = json.loads(report_path.read_bytes())
    assert final_report["active_process_group"] is None
    assert final_report["publication_state"] == final_report["production_state"] == "verified"
    assert hashlib.sha256(unrelated.read_bytes()).digest() == unrelated_hash
    phase, phase_sha = grants.read_phase_chain(control / "phases", identity)
    assert phase == "activation"
    grants.write_once_json(
        control / "rehearsal-result.json",
        {
            "state": "verified",
            "identity": identity,
            "interrupted_grant_id": interrupted,
            "activation_grant_id": activation,
            "release_id": publication["release_id"],
            "warehouse_sha256": alias_sha,
            "relation_rows": counts,
            "phase_chain_sha256": phase_sha,
        },
    )
