from __future__ import annotations

import importlib
import json
import os
import subprocess
import uuid
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from kalshi_events import incremental, warehouse

pytest_plugins = ("test_production_activation",)


def _activation_module(production):
    return importlib.import_module("production_activation")


def _footer_counts(directory: Path, manifest: dict) -> dict[str, int]:
    release = directory / "operator" / "releases" / manifest["source_release_id"]
    return {
        name: pq.ParquetFile(release / f"{name}.parquet").metadata.num_rows
        for name in warehouse.RELATIONS
    }


def _preserve_previous_alias(directory: Path) -> tuple[Path, bytes]:
    alias = directory / "production.duckdb"
    before = b"previous production generation"
    alias.write_bytes(before)
    return alias, before


def _protect_prune(root: Path, monkeypatch) -> Path:
    sentinel = root / "tmp" / "activation-prune-sentinel"
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    sentinel.write_bytes(b"retained until readback acceptance")
    monkeypatch.setattr(
        incremental,
        "prune",
        lambda *_args, **_kwargs: pytest.fail("activation pruned before acceptance completed"),
    )
    return sentinel


def test_readback_timeout_preserves_previous_alias_and_cleans_temporary_files(
    production, historical_candidate, monkeypatch
):
    activation = _activation_module(production)
    directory, root, _warehouse, manifest = historical_candidate
    alias, before = _preserve_previous_alias(directory)
    sentinel = _protect_prune(root, monkeypatch)
    observed = {}

    def timeout(command, **kwargs):
        observed["command"] = command
        observed["timeout"] = kwargs["timeout"]
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr(activation.subprocess, "run", timeout)
    with pytest.raises(subprocess.TimeoutExpired):
        activation.activate_current(
            root,
            directory,
            expected_release_id=manifest["source_release_id"],
            expected_capture_id=manifest["capture_id"],
            catalogue_version=3,
            max_duration_seconds=900,
        )

    assert alias.read_bytes() == before
    assert 0 < observed["timeout"] <= 600
    assert '"memory_limit": "4GB"' in observed["command"][2]
    assert '"threads": "4"' in observed["command"][2]
    assert not list(directory.glob(".production.duckdb.*.tmp"))
    assert not list((root / "tmp").glob("activation-readback-*"))
    assert sentinel.read_bytes() == b"retained until readback acceptance"


def test_readback_count_mismatch_preserves_previous_alias(
    production, historical_candidate, monkeypatch
):
    activation = _activation_module(production)
    directory, root, _warehouse, manifest = historical_candidate
    alias, before = _preserve_previous_alias(directory)
    sentinel = _protect_prune(root, monkeypatch)
    counts = _footer_counts(directory, manifest)
    counts["observation_history"] += 1
    samples = {name: ["representative"] for name in counts}

    monkeypatch.setattr(
        activation.subprocess,
        "run",
        lambda command, **kwargs: SimpleNamespace(
            stdout=json.dumps({"relation_rows": counts, "samples": samples}),
            stderr="",
            returncode=0,
        ),
    )
    with pytest.raises(RuntimeError, match="row counts differ"):
        _activation_module(production).activate_current(
            root,
            directory,
            expected_release_id=manifest["source_release_id"],
            expected_capture_id=manifest["capture_id"],
            catalogue_version=3,
        )

    assert alias.read_bytes() == before
    assert sentinel.read_bytes() == b"retained until readback acceptance"


def test_expected_observation_census_is_optional_but_enforced_when_supplied(
    production, historical_candidate, monkeypatch
):
    directory, root, _warehouse, manifest = historical_candidate
    alias, before = _preserve_previous_alias(directory)
    sentinel = _protect_prune(root, monkeypatch)

    with pytest.raises(RuntimeError, match="expected observation census"):
        _activation_module(production).activate_current(
            root,
            directory,
            expected_release_id=manifest["source_release_id"],
            expected_capture_id=manifest["capture_id"],
            catalogue_version=3,
            expected_observations=_footer_counts(directory, manifest)["observation_history"] + 1,
        )

    assert alias.read_bytes() == before
    assert sentinel.read_bytes() == b"retained until readback acceptance"


def test_readback_matches_footers_and_preserves_public_sample_values(
    production, historical_candidate
):
    directory, root, _warehouse, manifest = historical_candidate
    expected_counts = _footer_counts(directory, manifest)

    result = _activation_module(production).activate_current(
        root,
        directory,
        expected_release_id=manifest["source_release_id"],
        expected_capture_id=manifest["capture_id"],
        catalogue_version=3,
        expected_observations=expected_counts["observation_history"],
    )

    assert result["production_readback"]["relation_rows"] == expected_counts
    assert result["relation_rows"] == expected_counts
    assert result["production_readback"]["read_only"] is True
    market = result["production_readback"]["samples"]["markets"]
    history = result["production_readback"]["samples"]["observation_history"]
    assert market[8:11] == ["0.001230000000000000", "12.345678900000000000", "2.125000000000000000"]
    assert len(market[11]) == 64 and market[12].startswith("{")
    assert len(history[7]) == 64 and history[8].startswith("{")
    assert result["production_readback"]["catalogue_coverage"]["scope"] == "historical"


@pytest.mark.parametrize("mutation", ["pointer", "source", "wal", "temporary_symlink"])
def test_post_readback_mutation_never_replaces_previous_alias(
    production, historical_candidate, monkeypatch, mutation
):
    activation = _activation_module(production)
    directory, root, warehouse_path, manifest = historical_candidate
    alias, before = _preserve_previous_alias(directory)
    run = activation.subprocess.run

    def mutate_after_readback(command, **kwargs):
        result = run(command, **kwargs)
        if mutation == "pointer":
            pointer = root / "current-historical.json"
            value = json.loads(pointer.read_text())
            value["manifest_sha256"] = "0" * 64
            pointer.write_text(json.dumps(value))
        elif mutation == "source":
            with warehouse_path.open("ab") as stream:
                stream.write(b"changed after readback")
        elif mutation == "wal":
            warehouse_path.with_name(warehouse_path.name + ".wal").write_bytes(b"uncheckpointed")
        else:
            temporary = Path(command[3])
            temporary.unlink()
            temporary.symlink_to(warehouse_path)
        return result

    monkeypatch.setattr(activation.subprocess, "run", mutate_after_readback)
    with pytest.raises((OSError, RuntimeError, ValueError)):
        production.activate_current(
            root,
            directory,
            expected_release_id=manifest["source_release_id"],
            expected_capture_id=manifest["capture_id"],
            catalogue_version=3,
        )

    assert alias.read_bytes() == before


def test_stale_temporary_recovery_removes_only_current_certified_hardlink(
    production, historical_candidate
):
    activation = _activation_module(production)
    directory, root, warehouse_path, manifest = historical_candidate
    temporary = directory / f".production.duckdb.{uuid.uuid4().hex}.tmp"
    os.link(warehouse_path, temporary)

    removed = activation.recover_activation_temporaries(
        root,
        directory,
        expected_release_id=manifest["source_release_id"],
        expected_capture_id=manifest["capture_id"],
        catalogue_version=3,
    )

    assert removed == 1
    assert not temporary.exists()
    assert warehouse_path.exists()


@pytest.mark.parametrize("kind", ["unrelated_file", "symlink"])
def test_stale_temporary_recovery_rejects_unowned_entries(production, historical_candidate, kind):
    activation = _activation_module(production)
    directory, root, warehouse_path, manifest = historical_candidate
    temporary = directory / f".production.duckdb.{uuid.uuid4().hex}.tmp"
    if kind == "symlink":
        temporary.symlink_to(warehouse_path)
    else:
        temporary.write_bytes(b"not the certified warehouse")

    with pytest.raises(RuntimeError, match="certified source link"):
        activation.recover_activation_temporaries(
            root,
            directory,
            expected_release_id=manifest["source_release_id"],
            expected_capture_id=manifest["capture_id"],
            catalogue_version=3,
        )

    assert temporary.is_symlink() == (kind == "symlink")
    assert temporary.exists()
    assert warehouse_path.exists()
