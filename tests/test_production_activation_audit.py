"""Historical production activation is bound to a fresh census and keyed Parquet audit."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import shutil
import stat
import subprocess
import sys
import time
import uuid
from collections import Counter
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest
from conftest import COMBO_EVENTS, EVENTS, HISTORICAL, FakeClient

from kalshi_events import capture, publish
from kalshi_events.common import CatalogueError, Store
from kalshi_events.historical import historical_task_indices

RUNNER = Path(__file__).parents[1] / "scripts" / "production_run.py"
ACTIVATION = RUNNER.with_name("production_activation.py")


@pytest.fixture(scope="module")
def production_activation():
    spec = importlib.util.spec_from_file_location("production_activation", ACTIVATION)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def production(production_activation):
    spec = importlib.util.spec_from_file_location("production_activation_audit_runner", RUNNER)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def certified_incomplete_historical_root(tmp_path_factory):
    """Leave an unrelated exchange task failed after every historical task is durable."""
    root = tmp_path_factory.mktemp("activation-audit-source") / "operator"
    store = Store(root)
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

    with pytest.raises(CatalogueError, match="incomplete source traversal"):
        capture.capture(
            store,
            client_factory=lambda s, b, **_options: FakeClient(
                s, b, {"/events/multivariate": fail_unrelated_combo_after_history}
            ),
            catalogue_profile="exchange",
            _on_capture_id=capture_id.append,
        )

    state = capture.read_capture(store, capture_id[0])
    selected = historical_task_indices(state, allow_legacy_incomplete=True)
    assert state["complete"] is False
    assert selected
    assert all(state["tasks"][index]["done"] for index in selected)
    manifest = publish.publish(store, capture_id[0], catalogue_version=3)
    assert manifest["source_capture_complete"] is False
    return root, manifest


def _audit_market(
    ticker: str,
    event_ticker: str,
    decimal_values: tuple[str | None, str | None, str | None],
    *,
    combo_settlement: str | None = None,
) -> dict:
    row = {
        **HISTORICAL,
        "ticker": ticker,
        "event_ticker": event_ticker,
        "last_price_dollars": decimal_values[0],
        "volume_fp": decimal_values[1],
        "open_interest_fp": decimal_values[2],
    }
    if event_ticker == "COMBO":
        row.update(
            mve_collection_ticker="COLLECTION",
            mve_selected_legs=[
                {
                    "event_ticker": "EVENT",
                    "market_ticker": "A-NULL",
                    "side": "yes",
                    "yes_settlement_value_dollars": combo_settlement,
                }
            ],
        )
    return row


@pytest.fixture(scope="module")
def decimal_historical_roots(tmp_path_factory):
    scenarios = {
        "all_three": [
            _audit_market("A-NULL", "HISTORY", (None, None, None)),
            _audit_market("B-PARTIAL", "HISTORY", (None, "7.000000000000000001", None)),
            _audit_market(
                "C-ALL",
                "HISTORY",
                (
                    "0.000000000000000123",
                    "7.000000000000000001",
                    "9.000000000000000004",
                ),
            ),
            _audit_market("A-COMBO-NULL", "COMBO", (None, None, None)),
            _audit_market(
                "B-COMBO-EXACT",
                "COMBO",
                (None, None, None),
                combo_settlement="0.000000000000000019",
            ),
        ],
        "partial_only": [
            _audit_market("A-NULL", "HISTORY", (None, None, None)),
            _audit_market("B-PARTIAL", "HISTORY", (None, "7.000000000000000001", None)),
            _audit_market("A-COMBO-NULL", "COMBO", (None, None, None)),
            _audit_market(
                "B-COMBO-EXACT",
                "COMBO",
                (None, None, None),
                combo_settlement="0.000000000000000019",
            ),
        ],
        "all_null": [
            _audit_market("A-NULL", "HISTORY", (None, None, None)),
            _audit_market("B-NULL", "HISTORY", (None, None, None)),
            _audit_market("A-COMBO-NULL", "COMBO", (None, None, None)),
            _audit_market("B-COMBO-NULL", "COMBO", (None, None, None)),
        ],
    }
    roots = {}
    for scenario, markets in scenarios.items():
        root = tmp_path_factory.mktemp(f"activation-audit-{scenario}") / "operator"
        store = Store(root)
        state = capture.capture(
            store,
            client_factory=lambda s, b, markets=markets, **_options: FakeClient(
                s,
                b,
                {
                    "/events": (200, {"events": [*EVENTS, *COMBO_EVENTS], "cursor": ""}),
                    "/historical/markets": (200, {"markets": markets, "cursor": ""}),
                },
            ),
        )
        assert state["complete"] is True
        manifest = publish.publish(store, state["capture_id"], catalogue_version=3)
        roots[scenario] = (root, manifest)
    return roots


@pytest.fixture
def candidate(certified_incomplete_historical_root, tmp_path):
    source, manifest = certified_incomplete_historical_root
    directory = tmp_path / "production"
    directory.mkdir()
    root = directory / "operator"
    shutil.copytree(source, root)
    return directory, root, manifest


def _copy_candidate(source: Path, manifest: dict, tmp_path: Path) -> tuple[Path, Path, dict]:
    directory = tmp_path / "production"
    directory.mkdir()
    root = directory / "operator"
    shutil.copytree(source, root)
    return directory, root, manifest


def _endpoint_counts(output: Path) -> dict[str, int]:
    endpoints = pq.read_table(output / "observation_history.parquet", columns=["source_endpoint"])
    return dict(Counter(endpoints.column("source_endpoint").to_pylist()))


def _write_audit(report_path: Path, report: dict, artifact: dict) -> tuple[Path, dict]:
    raw = json.dumps(
        artifact,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    checksum = hashlib.sha256(raw).hexdigest()
    relative_path = f"activation-audit-{checksum}.json"
    artifact_path = report_path.parent / relative_path
    artifact_path.write_bytes(raw)
    artifact_path.chmod(0o400)
    report["activation_audit"] = {"relative_path": relative_path, "sha256": checksum}
    report_path.write_text(json.dumps(report, sort_keys=True, allow_nan=False), encoding="utf-8")
    return artifact_path, {**artifact, "relative_path": relative_path, "sha256": checksum}


def _audit_session(production, candidate) -> tuple[Path, dict, Path, dict]:
    directory, root, manifest = candidate
    store = Store(root)
    capture_id = manifest["capture_id"]
    state = capture.read_capture(store, capture_id)
    current = publish.current(store, catalogue_version=3)
    session_id = uuid.uuid4().hex
    session_directory = directory / "runs" / session_id
    session_directory.mkdir(parents=True)
    now_epoch = time.time()
    now_monotonic = time.monotonic()
    report = {
        "version": 1,
        "session_id": session_id,
        "mode": "historical",
        "resource_profile": "default",
        "capture_id": capture_id,
        "root_id": current["root_id"],
        "started_epoch": now_epoch - 1,
        "deadline_epoch": now_epoch + 3600,
        "started_monotonic": now_monotonic - 1,
        "deadline_monotonic": now_monotonic + 3600,
        "time_budget_started_epoch": now_epoch - 1,
        "time_budget_started_monotonic": now_monotonic - 1,
    }
    report_path = session_directory / "session.json"
    counts = _endpoint_counts(Path(current["output"]))
    artifact = {
        "version": 1,
        "session_id": session_id,
        "capture_id": capture_id,
        "root_id": current["root_id"],
        "capture_state_sha256": state.verified_snapshot_sha256,
        "receipt_index_sha256": hashlib.sha256(b"synthetic receipt index").hexdigest(),
        "population_report_sha256": hashlib.sha256(b"synthetic population report").hexdigest(),
        "observations_by_endpoint": counts,
        "exact_observations": sum(counts.values()),
        "sample_strategy": "verified-parquet-keyed-v1",
    }
    artifact_path, bound_audit = _write_audit(report_path, report, artifact)
    return report_path, artifact, artifact_path, bound_audit


_SAMPLE_KEYS = {
    "series": ("series_ticker",),
    "events": ("event_ticker",),
    "markets": ("market_ticker",),
    "observation_history": ("observation_id",),
    "combo_legs": ("combo_market_ticker", "leg_index"),
    "relationships": ("parent_kind", "parent_ticker", "child_kind", "child_ticker"),
}
_SAMPLE_COLUMNS = {
    "series": ("series_ticker", "observation_id", "capture_id", "received_at", "source_endpoint"),
    "events": (
        "event_ticker",
        "series_ticker",
        "observation_id",
        "capture_id",
        "received_at",
        "source_endpoint",
    ),
    "markets": (
        "market_ticker",
        "event_ticker",
        "series_ticker",
        "usable",
        "observation_id",
        "capture_id",
        "received_at",
        "source_endpoint",
        "last_price_dollars",
        "volume_fp",
        "open_interest_fp",
    ),
    "observation_history": (
        "observation_id",
        "capture_id",
        "entity_kind",
        "entity_ticker",
        "received_at",
        "source_endpoint",
        "source_kind",
    ),
    "combo_legs": (
        "combo_market_ticker",
        "leg_index",
        "event_ticker",
        "market_ticker",
        "side",
        "yes_settlement_value_dollars",
    ),
    "relationships": ("parent_kind", "parent_ticker", "child_kind", "child_ticker", "resolved"),
}
_MARKET_DECIMALS = ("last_price_dollars", "volume_fp", "open_interest_fp")


def _normalized(value):
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (date, datetime)):
        return str(value)
    return value


def _selected_parquet_row(output: Path, relation: str):
    rows = pq.read_table(output / f"{relation}.parquet").to_pylist()
    if relation == "markets" and rows:
        complete = [
            row for row in rows if all(row.get(field) is not None for field in _MARKET_DECIMALS)
        ]
        any_decimal = [
            row for row in rows if any(row.get(field) is not None for field in _MARKET_DECIMALS)
        ]
        rows = complete or any_decimal or rows
    elif relation == "combo_legs" and rows:
        nonnull = [row for row in rows if row.get("yes_settlement_value_dollars") is not None]
        rows = nonnull or rows
    if not rows:
        return None
    return min(
        rows,
        key=lambda row: tuple(
            (row.get(key) is None, _normalized(row.get(key)) or "")
            for key in _SAMPLE_KEYS[relation]
        ),
    )


def _expected_parquet_sample(output: Path, relation: str) -> tuple | None:
    row = _selected_parquet_row(output, relation)
    if row is None:
        return None
    result = [_normalized(row.get(column)) for column in _SAMPLE_COLUMNS[relation]]
    if "payload_json" in row:
        payload = _normalized(row["payload_json"])
        result.append(hashlib.sha256(payload.encode("utf-8")).hexdigest())
        if relation != "combo_legs":
            result.append(payload[:256])
    return tuple(result)


def _expected_decimal_sample(output: Path, relation: str) -> dict:
    row = _selected_parquet_row(output, relation)
    fields = []
    if relation == "markets" and row is not None:
        fields = [field for field in _MARKET_DECIMALS if row.get(field) is not None]
    elif (
        relation == "combo_legs"
        and row is not None
        and row.get("yes_settlement_value_dollars") is not None
    ):
        fields = ["yes_settlement_value_dollars"]
    return {"decimal_fields_sampled": fields, "decimal_sample_absent": not fields}


def _activate(production, candidate, bound_audit, **kwargs):
    directory, root, manifest = candidate
    return production.activate_current(
        root,
        directory,
        expected_release_id=manifest["source_release_id"],
        expected_capture_id=manifest["capture_id"],
        catalogue_version=3,
        activation_audit=bound_audit,
        **kwargs,
    )


def test_worker_forwards_bound_audit_and_accepts_complete_history_in_incomplete_capture(
    production, candidate, capsys
):
    report_path, artifact, artifact_path, bound_audit = _audit_session(production, candidate)
    directory, root, manifest = candidate
    output = Path(manifest["output"])

    code = production._activation_worker(
        root,
        directory,
        f"historical:{manifest['source_release_id']}",
        report_path,
        "default",
    )

    result = json.loads(capsys.readouterr().out)
    assert code == 0, result.get("error")
    assert result["state"] == "verified"
    assert result["source_capture_complete"] is False
    assert result["scope_complete"] is True
    assert result["activation_audit"] == {
        "state": "verified",
        "sha256": bound_audit["sha256"],
        "capture_state_sha256": artifact["capture_state_sha256"],
        "receipt_index_sha256": artifact["receipt_index_sha256"],
        "population_report_sha256": artifact["population_report_sha256"],
        "sample_strategy": "verified-parquet-keyed-v1",
        "sample_matches": {
            name: True
            for name in (
                "combo_legs",
                "events",
                "markets",
                "observation_history",
                "relationships",
                "series",
            )
        },
        "decimal_samples": {
            relation: _expected_decimal_sample(output, relation)
            for relation in ("markets", "combo_legs")
        },
        "observations_by_endpoint": artifact["observations_by_endpoint"],
        "exact_observations": artifact["exact_observations"],
    }
    samples = result["production_readback"]["samples"]
    for relation in (
        "series",
        "events",
        "markets",
        "observation_history",
        "combo_legs",
        "relationships",
    ):
        expected_sample = _expected_parquet_sample(output, relation)
        if expected_sample is None:
            assert samples[relation] is None
        else:
            assert tuple(samples[relation]) == expected_sample
    market_sample = samples["markets"]
    assert all(isinstance(market_sample[index], str) for index in (8, 9, 10))
    assert (directory / "production.duckdb").stat().st_ino == (
        root / "builds-historical" / manifest["capture_id"] / "warehouse.duckdb"
    ).stat().st_ino
    assert artifact_path.is_file()


@pytest.mark.parametrize("scenario", ["all_three", "partial_only", "all_null"])
def test_decimal_samples_skip_null_keys_and_preserve_exact_values(
    production, decimal_historical_roots, tmp_path, scenario
):
    source, manifest = decimal_historical_roots[scenario]
    candidate = _copy_candidate(source, manifest, tmp_path)
    _report_path, _artifact, _artifact_path, bound_audit = _audit_session(production, candidate)
    result = _activate(production, candidate, bound_audit)
    output = Path(manifest["output"])
    samples = result["production_readback"]["samples"]

    for relation in ("markets", "combo_legs"):
        assert tuple(samples[relation]) == _expected_parquet_sample(output, relation)
    assert result["activation_audit"]["decimal_samples"] == {
        relation: _expected_decimal_sample(output, relation)
        for relation in ("markets", "combo_legs")
    }

    market = _selected_parquet_row(output, "markets")
    combo_leg = _selected_parquet_row(output, "combo_legs")
    if scenario == "all_three":
        assert market["market_ticker"] == "C-ALL"
        assert tuple(samples["markets"][8:11]) == (
            "0.000000000000000123",
            "7.000000000000000001",
            "9.000000000000000004",
        )
        assert combo_leg["combo_market_ticker"] == "B-COMBO-EXACT"
        assert samples["combo_legs"][5] == "0.000000000000000019"
    elif scenario == "partial_only":
        assert market["market_ticker"] == "B-PARTIAL"
        assert tuple(samples["markets"][8:11]) == (None, "7.000000000000000001", None)
        assert combo_leg["combo_market_ticker"] == "B-COMBO-EXACT"
        assert samples["combo_legs"][5] == "0.000000000000000019"
    else:
        assert market["market_ticker"] == "A-COMBO-NULL"
        assert tuple(samples["markets"][8:11]) == (None, None, None)
        assert result["activation_audit"]["decimal_samples"]["markets"] == {
            "decimal_fields_sampled": [],
            "decimal_sample_absent": True,
        }
        assert combo_leg["combo_market_ticker"] == "A-COMBO-NULL"
        assert samples["combo_legs"][5] is None
        assert result["activation_audit"]["decimal_samples"]["combo_legs"] == {
            "decimal_fields_sampled": [],
            "decimal_sample_absent": True,
        }


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_file",
        "wrong_file_hash",
        "wrong_filename",
        "directory_path",
        "symlink",
        "hardlink",
        "oversized",
        "writable_artifact",
        "writable_session_directory",
        "foreign_owner",
        "unexpected_field",
        "wrong_capture",
        "wrong_root",
        "boolean_count",
        "inconsistent_total",
        "malformed_proof_hash",
    ],
)
def test_worker_rejects_unsafe_or_unbound_audit_before_activation(
    production, production_activation, candidate, monkeypatch, mutation
):
    report_path, artifact, artifact_path, _bound_audit = _audit_session(production, candidate)
    directory, root, manifest = candidate
    report = json.loads(report_path.read_text(encoding="utf-8"))
    lower_calls = []
    monkeypatch.setattr(
        production_activation,
        "activate_current",
        lambda *_args, **_kwargs: lower_calls.append(True),
    )
    alias = directory / "production.duckdb"
    alias.write_bytes(b"previous production database")
    previous = alias.read_bytes()

    if mutation == "missing_file":
        artifact_path.unlink()
    elif mutation == "wrong_file_hash":
        artifact_path.chmod(0o600)
        artifact_path.write_bytes(artifact_path.read_bytes() + b"tampered")
        artifact_path.chmod(0o400)
    elif mutation == "wrong_filename":
        report["activation_audit"]["relative_path"] = "audit.json"
        report_path.write_text(json.dumps(report), encoding="utf-8")
    elif mutation == "directory_path":
        report["activation_audit"]["relative_path"] = f"nested/{artifact_path.name}"
        report_path.write_text(json.dumps(report), encoding="utf-8")
    elif mutation == "symlink":
        target = report_path.parent / "audit-target"
        target.write_bytes(artifact_path.read_bytes())
        artifact_path.unlink()
        artifact_path.symlink_to(target)
    elif mutation == "hardlink":
        os.link(artifact_path, report_path.parent / "audit-hardlink")
    elif mutation == "oversized":
        artifact_path.chmod(0o600)
        artifact_path.write_bytes(b"x" * (64 * 1024 + 1))
        artifact_path.chmod(0o400)
    elif mutation == "writable_artifact":
        artifact_path.chmod(0o600)
    elif mutation == "writable_session_directory":
        original_session_mode = stat.S_IMODE(artifact_path.parent.stat().st_mode)
        artifact_path.parent.chmod(0o777)
    elif mutation == "foreign_owner":
        real_lstat = Path.lstat

        def foreign_artifact_owner(path, *args, **kwargs):
            info = real_lstat(path, *args, **kwargs)
            if path == artifact_path:
                return SimpleNamespace(
                    st_dev=info.st_dev,
                    st_ino=info.st_ino,
                    st_mode=info.st_mode,
                    st_nlink=info.st_nlink,
                    st_size=info.st_size,
                    st_uid=os.geteuid() + 1,
                )
            return info

        monkeypatch.setattr(Path, "lstat", foreign_artifact_owner)
    else:
        if mutation == "unexpected_field":
            artifact["unexpected"] = True
        elif mutation == "wrong_capture":
            artifact["capture_id"] = "0" * 32 if artifact["capture_id"] != "0" * 32 else "1" * 32
        elif mutation == "wrong_root":
            artifact["root_id"] = "0" * 32 if artifact["root_id"] != "0" * 32 else "1" * 32
        elif mutation == "boolean_count":
            endpoint = next(iter(artifact["observations_by_endpoint"]))
            artifact["observations_by_endpoint"][endpoint] = True
        elif mutation == "inconsistent_total":
            artifact["exact_observations"] += 1
        else:
            artifact["receipt_index_sha256"] = "z" * 64
        _write_audit(report_path, report, artifact)

    code = production._activation_worker(
        root,
        directory,
        f"historical:{manifest['source_release_id']}",
        report_path,
        "default",
    )

    if mutation == "writable_session_directory":
        artifact_path.parent.chmod(original_session_mode)

    assert code == 1, mutation
    assert not lower_calls, mutation
    assert alias.read_bytes() == previous, mutation


@pytest.mark.parametrize("mismatch", ["strata", "total"])
def test_endpoint_census_mismatches_never_replace_previous_alias(
    production, production_activation, candidate, monkeypatch, mismatch
):
    _report_path, _artifact, _artifact_path, bound_audit = _audit_session(production, candidate)
    directory, _root, _manifest = candidate
    alias = directory / "production.duckdb"
    alias.write_bytes(b"previous production database")
    previous = alias.read_bytes()
    original_run = production_activation.subprocess.run
    replace_calls = []

    def alter_census(command, *args, **kwargs):
        result = original_run(command, *args, **kwargs)
        if (
            isinstance(command, (list, tuple))
            and len(command) > 2
            and "observations_by_endpoint" in command[2]
        ):
            value = json.loads(result.stdout)
            rows = value["observations_by_endpoint"]
            if mismatch == "strata":
                assert len(rows) >= 2
                assert rows[1][1] > 0
                rows[0][1] += 1
                rows[1][1] -= 1
            else:
                rows[0][1] += 1
            return subprocess.CompletedProcess(
                command, result.returncode, stdout=json.dumps(value), stderr=result.stderr
            )
        return result

    def forbidden_replace(*args, **kwargs):
        replace_calls.append((args, kwargs))
        raise AssertionError("activation reached replacement after a census mismatch")

    monkeypatch.setattr(production_activation.subprocess, "run", alter_census)
    with pytest.raises(RuntimeError, match="endpoint census"):
        _activate(production, candidate, bound_audit, replace_fn=forbidden_replace)
    assert not replace_calls
    assert alias.read_bytes() == previous


@pytest.mark.parametrize(
    ("failure", "relation"),
    [
        ("missing", "markets"),
        ("payload", "series"),
        ("decimal", "markets"),
        ("provenance", "observation_history"),
    ],
)
def test_missing_or_mismatched_keyed_samples_never_replace_previous_alias(
    production, production_activation, candidate, monkeypatch, failure, relation
):
    _report_path, _artifact, _artifact_path, bound_audit = _audit_session(production, candidate)
    directory, _root, _manifest = candidate
    alias = directory / "production.duckdb"
    alias.write_bytes(b"previous production database")
    previous = alias.read_bytes()
    original_run = production_activation.subprocess.run
    replace_calls = []

    def alter_samples(command, *args, **kwargs):
        result = original_run(command, *args, **kwargs)
        if isinstance(command, (list, tuple)) and len(command) > 2 and "sample_audit" in command[2]:
            value = json.loads(result.stdout)
            if failure == "missing":
                del value["sample_audit"][relation]
            else:
                value["sample_audit"][relation]["matched"] = False
            return subprocess.CompletedProcess(
                command, result.returncode, stdout=json.dumps(value), stderr=result.stderr
            )
        return result

    def forbidden_replace(*args, **kwargs):
        replace_calls.append((args, kwargs))
        raise AssertionError("activation reached replacement after a sample mismatch")

    monkeypatch.setattr(production_activation.subprocess, "run", alter_samples)
    with pytest.raises(RuntimeError, match="keyed sample"):
        _activate(production, candidate, bound_audit, replace_fn=forbidden_replace)
    assert not replace_calls
    assert alias.read_bytes() == previous


def test_capture_state_hash_mismatch_never_replaces_previous_alias(production, candidate):
    _report_path, artifact, _artifact_path, _bound_audit = _audit_session(production, candidate)
    directory, _root, _manifest = candidate
    report = json.loads(
        (_report_path := next((directory / "runs").glob("*/session.json"))).read_text()
    )
    artifact["capture_state_sha256"] = "e" * 64
    _artifact_path, bound_audit = _write_audit(_report_path, report, artifact)
    alias = directory / "production.duckdb"
    alias.write_bytes(b"previous production database")
    previous = alias.read_bytes()
    replace_calls = []

    with pytest.raises(RuntimeError, match="current capture state and root"):
        _activate(
            production,
            candidate,
            bound_audit,
            replace_fn=lambda *args, **kwargs: replace_calls.append((args, kwargs)),
        )
    assert not replace_calls
    assert alias.read_bytes() == previous


@pytest.mark.parametrize("tamper", ["artifact", "session_binding"])
def test_activation_rechecks_artifact_and_report_binding_before_replace(
    production, candidate, monkeypatch, tamper
):
    report_path, _artifact, artifact_path, bound_audit = _audit_session(production, candidate)
    directory, _root, _manifest = candidate
    alias = directory / "production.duckdb"
    alias.write_bytes(b"previous production database")
    previous = alias.read_bytes()
    from kalshi_events import incremental

    original_prune = incremental.prune

    def mutate_after_cleanup(store, *args, **kwargs):
        result = original_prune(store, *args, **kwargs)
        if tamper == "artifact":
            artifact_path.chmod(0o600)
            artifact_path.write_bytes(artifact_path.read_bytes() + b"tampered")
            artifact_path.chmod(0o400)
        else:
            report = json.loads(report_path.read_text(encoding="utf-8"))
            report["activation_audit"]["sha256"] = "f" * 64
            report_path.write_text(json.dumps(report, sort_keys=True), encoding="utf-8")
        return result

    monkeypatch.setattr(incremental, "prune", mutate_after_cleanup)
    replace_calls = []

    def forbidden_replace(*args, **kwargs):
        replace_calls.append((args, kwargs))
        raise AssertionError("activation replaced after its session audit binding changed")

    with pytest.raises(RuntimeError, match="activation (audit file digest|session report)"):
        _activate(production, candidate, bound_audit, replace_fn=forbidden_replace)
    assert not replace_calls
    assert alias.read_bytes() == previous
