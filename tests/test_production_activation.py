"""The consumer entry point must refer only to independently certified data."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest
from conftest import FakeClient

from kalshi_events import capture, incremental, publish
from kalshi_events.common import CatalogueError, Store, digest, file_digest, regular_bytes

SCRIPT = Path(__file__).parents[1] / "scripts" / "production_run.py"


@pytest.fixture(scope="module")
def production():
    spec = importlib.util.spec_from_file_location("production_activation_test", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def certified_production_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("production-source") / "operator"
    store = Store(root)
    state = capture.capture(store, client_factory=FakeClient, catalogue_profile="exchange")
    assert state["complete"] is True
    manifest = publish.publish(store, state["capture_id"], catalogue_version=2)
    assert not store.path("current.json").exists()
    return root, manifest


@pytest.fixture
def candidate(certified_production_root, tmp_path):
    source, manifest = certified_production_root
    directory = tmp_path / "production"
    root = directory / "operator"
    shutil.copytree(source, root)
    warehouse = root / "builds-v2" / manifest["capture_id"] / "warehouse.duckdb"
    return directory, root, warehouse, manifest


@pytest.fixture(scope="module")
def certified_historical_production_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("historical-production-source") / "operator"
    store = Store(root)
    state = capture.capture(store, client_factory=FakeClient)
    assert state["complete"] is True
    manifest = publish.publish(store, state["capture_id"], catalogue_version=3)
    return root, manifest


@pytest.fixture
def historical_candidate(certified_historical_production_root, tmp_path):
    source, manifest = certified_historical_production_root
    directory = tmp_path / "production"
    root = directory / "operator"
    shutil.copytree(source, root)
    warehouse = root / "builds-historical" / manifest["capture_id"] / "warehouse.duckdb"
    return directory, root, warehouse, manifest


@pytest.fixture(scope="module")
def rebased_production_root(certified_production_root, tmp_path_factory):
    source, old_manifest = certified_production_root
    directory = tmp_path_factory.mktemp("production-rebase")
    root = directory / "operator"
    shutil.copytree(source, root)
    old_database = root / "builds-v2" / old_manifest["capture_id"] / "warehouse.duckdb"
    old_alias = directory / "previous.duckdb"
    os.link(old_database, old_alias)
    store = Store(root)
    state = capture.capture(store, client_factory=FakeClient, catalogue_profile="exchange")
    assert state["complete"]
    manifest = publish.publish(store, state["capture_id"], catalogue_version=2)
    assert manifest["warehouse_sha256"] != old_manifest["warehouse_sha256"]
    return root, manifest, old_alias


@pytest.fixture
def prior_production_candidate(rebased_production_root, tmp_path):
    source, manifest, old_alias = rebased_production_root
    directory = tmp_path / "production"
    root = directory / "operator"
    shutil.copytree(source, root)
    shutil.copyfile(old_alias, directory / "production.duckdb")
    warehouse = root / "builds-v2" / manifest["capture_id"] / "warehouse.duckdb"
    return directory, root, warehouse, manifest


def activate(production, candidate, **kwargs):
    directory, root, _warehouse, manifest = candidate
    return production.activate_current(
        root, directory, expected_release_id=manifest["source_release_id"], **kwargs
    )


def test_activation_rejects_repository_code_paths_before_opening_store(production):
    source = SCRIPT.parents[1] / "src" / "kalshi_events"
    with pytest.raises((CatalogueError, ValueError, RuntimeError), match="strict descendant"):
        production.activate_current(
            source / "operator",
            source,
            expected_release_id="a" * 64,
        )
    assert not (source / "production.duckdb").exists()


def test_expanded_activation_accepts_a_mocked_sparse_warehouse_over_64_gib(
    production, historical_candidate, monkeypatch
):
    from kalshi_events import common

    directory, _root, warehouse_path, manifest = historical_candidate
    alias = directory / "production.duckdb"
    real_lstat = Path.lstat
    real_file_digest = common.file_digest
    observed_limits = []
    sparse_size = 70 * 1024**3

    def sparse_lstat(path):
        details = real_lstat(path)
        if path == warehouse_path:
            return SimpleNamespace(
                st_dev=details.st_dev,
                st_ino=details.st_ino,
                st_mode=details.st_mode,
                st_nlink=details.st_nlink,
                st_size=sparse_size,
            )
        return details

    def sparse_digest(path, max_bytes=None):
        if path in {warehouse_path, alias}:
            observed_limits.append(max_bytes)
            return manifest["warehouse_sha256"], sparse_size
        return real_file_digest(path, max_bytes)

    monkeypatch.setattr(Path, "lstat", sparse_lstat)
    monkeypatch.setattr(common, "file_digest", sparse_digest)
    result = production.activate_current(
        historical_candidate[1],
        directory,
        expected_release_id=manifest["source_release_id"],
        catalogue_version=3,
        max_database_bytes=128 * 1024**3,
        max_temp_bytes=192 * 1024**3,
    )

    assert result["state"] == "verified"
    assert observed_limits == [128 * 1024**3, 128 * 1024**3]


def test_historical_activation_only_rejects_over_default_database_limit(
    production, historical_candidate, monkeypatch, capsys
):
    directory, root, warehouse_path, manifest = historical_candidate
    monkeypatch.setattr(production, "_revision", lambda: "d" * 40)
    report_path, _report, _paths = production._session_report(
        directory, "activate-historical", manifest["capture_id"]
    )
    real_lstat = Path.lstat
    real_activate = production.activate_current

    def sparse_lstat(path):
        details = real_lstat(path)
        if path == warehouse_path:
            return SimpleNamespace(
                st_dev=details.st_dev,
                st_ino=details.st_ino,
                st_mode=details.st_mode,
                st_nlink=details.st_nlink,
                st_size=65 * 1024**3,
            )
        return details

    monkeypatch.setattr(Path, "lstat", sparse_lstat)
    monkeypatch.setattr(
        production,
        "activate_current",
        lambda *_args, **_kwargs: pytest.fail("oversized activation should be stopped"),
    )
    code = production._activation_worker(
        root,
        directory,
        f"historical:{manifest['source_release_id']}",
        report_path,
        "default",
    )

    assert code == 1
    assert (
        "resume the exact capture with --historical-resource-profile expanded"
        in capsys.readouterr().out
    )
    assert not (directory / "production.duckdb").exists()
    monkeypatch.setattr(production, "activate_current", real_activate)


def test_activation_worker_propagates_sealed_expanded_limits(
    production, historical_candidate, monkeypatch, capsys
):
    directory, root, _warehouse, manifest = historical_candidate
    monkeypatch.setattr(production, "_revision", lambda: "d" * 40)
    report_path, report, _paths = production._session_report(
        directory, "historical", manifest["capture_id"]
    )
    production._bind_report_resource_profile(report, "expanded", transition=True)
    production._atomic_report(report_path, report)
    captured = {}

    def activate(*_args, **kwargs):
        captured.update(kwargs)
        return {"state": "verified"}

    monkeypatch.setattr(production, "activate_current", activate)
    code = production._activation_worker(
        root,
        directory,
        f"historical:{manifest['source_release_id']}",
        report_path,
        "expanded",
    )

    assert code == 0
    assert captured["max_database_bytes"] == 128 * 1024**3
    assert captured["max_temp_bytes"] == 192 * 1024**3
    assert captured["expected_capture_id"] == manifest["capture_id"]
    assert json.loads(capsys.readouterr().out)["state"] == "verified"


def test_activation_rejects_expanded_session_capture_mismatch_before_linking(
    production, historical_candidate
):
    directory, root, _warehouse, manifest = historical_candidate
    with pytest.raises(RuntimeError, match="different capture"):
        production.activate_current(
            root,
            directory,
            expected_release_id=manifest["source_release_id"],
            expected_capture_id="a" * 32,
            catalogue_version=3,
            max_database_bytes=128 * 1024**3,
            max_temp_bytes=192 * 1024**3,
        )
    assert not (directory / "production.duckdb").exists()


def update_manifest(candidate, change):
    _directory, root, _warehouse, manifest = candidate
    store = Store(root)
    relative = f"releases/{manifest['source_release_id']}/manifest.json"
    value = json.loads(regular_bytes(store.path(relative), 1024**2))
    change(value)
    store.write(relative, value)
    pointer = json.loads(store.path("current-v2.json").read_text())
    pointer["manifest_sha256"] = digest(regular_bytes(store.path(relative), 1024**2))
    store.write("current-v2.json", pointer)
    return value, regular_bytes(store.path(relative), 1024**2)


def rewrite_warehouse(candidate, statement):
    _directory, root, warehouse_path, manifest = candidate
    with duckdb.connect(str(warehouse_path)) as connection:
        connection.execute(statement)
        connection.execute("checkpoint")
    store = Store(root)
    build = f"builds-v2/{manifest['capture_id']}/build.json"
    receipt = json.loads(store.path(build).read_text())
    receipt["warehouse_sha256"] = file_digest(warehouse_path, store.limits.max_database_bytes)[0]
    store.write(build, receipt)


def test_verified_activation_is_a_shared_inode_and_queries_read_only(production, candidate):
    directory, _root, warehouse, _manifest = candidate
    result = activate(production, candidate)
    assert result["verification_mode"] == "warehouse_hash"
    assert result["warehouse_sha256"] == result["manifest_warehouse_sha256"]
    alias = directory / "production.duckdb"
    assert alias.is_file() and not alias.is_symlink()
    assert alias.stat().st_ino == warehouse.stat().st_ino
    assert alias.stat().st_dev == warehouse.stat().st_dev
    with (
        duckdb.connect(str(warehouse), read_only=True) as source,
        duckdb.connect(str(alias), read_only=True) as consumer,
    ):
        expected = source.execute("select count(*) from main.current_markets").fetchone()
        assert consumer.execute("select count(*) from main.current_markets").fetchone() == expected
        with pytest.raises(duckdb.InvalidInputException):
            consumer.execute("create table unverified_write as select 1")
    assert not alias.with_name(alias.name + ".wal").exists()


@pytest.mark.parametrize("hint", ["stale", "missing"])
def test_semantic_activation_accepts_an_equivalent_rebuilt_warehouse_without_changing_manifest(
    production, candidate, hint
):
    _directory, _root, warehouse_path, manifest = candidate
    if hint == "missing":
        _, frozen_manifest = update_manifest(candidate, lambda value: value.pop("warehouse_sha256"))
        expected_manifest_hash = None
    else:
        expected_manifest_hash = manifest["warehouse_sha256"]
        frozen_manifest = regular_bytes(
            _root / "releases" / manifest["source_release_id"] / "manifest.json", 1024**2
        )
    rewrite_warehouse(candidate, "create table main.physical_rebuild_marker as select 123 as value")

    result = activate(production, candidate)

    assert result["verification_mode"] == "semantic"
    assert result["warehouse_sha256"] != expected_manifest_hash
    assert result["manifest_warehouse_sha256"] == expected_manifest_hash
    assert (
        _root / "releases" / manifest["source_release_id"] / "manifest.json"
    ).read_bytes() == frozen_manifest
    assert (candidate[0] / "production.duckdb").stat().st_ino == warehouse_path.stat().st_ino


@pytest.mark.parametrize(
    ("mutation", "statement"),
    [
        ("schema", "alter table main.current_markets add column activation_extra integer"),
        (
            "rows",
            "update main.current_markets set last_price_dollars = 0.999 where market_ticker = 'MARKET'",
        ),
        ("membership", "delete from landing.membership"),
    ],
)
def test_semantic_activation_rejects_schema_rows_or_membership_changes(
    production, candidate, mutation, statement
):
    directory, _root, _warehouse, _manifest = candidate
    rewrite_warehouse(candidate, statement)

    with pytest.raises((CatalogueError, ValueError, RuntimeError)):
        activate(production, candidate)

    assert not (directory / "production.duckdb").exists(), mutation


@pytest.mark.parametrize("field", ["capture_id", "root_id"])
def test_activation_rejects_a_manifest_bound_to_another_capture_or_root(
    production, candidate, field
):
    directory, _root, _warehouse, manifest = candidate
    other_identity = "0" * 32 if manifest[field] != "0" * 32 else "f" * 32
    update_manifest(candidate, lambda value: value.update({field: other_identity}))

    with pytest.raises((CatalogueError, OSError, ValueError, RuntimeError)):
        activate(production, candidate)

    assert not (directory / "production.duckdb").exists()


def test_activation_rechecks_the_current_manifest_pointer_after_cleanup(
    production, candidate, monkeypatch
):
    directory, _root, _warehouse, _manifest = candidate
    rewrite_warehouse(candidate, "create table main.physical_rebuild_marker as select 123 as value")
    alias = directory / "production.duckdb"
    alias.write_bytes(b"previous production bytes")
    before = alias.read_bytes()
    original_prune = incremental.prune

    def change_pointer_after_prune(store, *args, **kwargs):
        result = original_prune(store, *args, **kwargs)
        pointer = json.loads(store.path("current-v2.json").read_text())
        pointer["manifest_sha256"] = "0" * 64
        store.write("current-v2.json", pointer)
        return result

    monkeypatch.setattr(incremental, "prune", change_pointer_after_prune)
    with pytest.raises((CatalogueError, ValueError, RuntimeError)):
        activate(production, candidate)

    assert alias.read_bytes() == before


def test_activation_rehashes_the_source_after_cleanup_before_link(
    production, candidate, monkeypatch
):
    directory, _root, warehouse_path, _manifest = candidate
    rewrite_warehouse(candidate, "create table main.physical_rebuild_marker as select 123 as value")
    alias = directory / "production.duckdb"
    alias.write_bytes(b"previous production bytes")
    before = alias.read_bytes()
    original_prune = incremental.prune

    def change_source_after_prune(store, *args, **kwargs):
        result = original_prune(store, *args, **kwargs)
        with warehouse_path.open("ab") as stream:
            stream.write(b"changed after semantic verification")
        return result

    monkeypatch.setattr(incremental, "prune", change_source_after_prune)
    with pytest.raises((CatalogueError, ValueError, RuntimeError)):
        activate(production, candidate)

    assert alias.read_bytes() == before


def test_active_database_survives_pruning_of_the_original_path(production, candidate):
    directory, _root, warehouse, _manifest = candidate
    activate(production, candidate)
    alias = directory / "production.duckdb"
    warehouse.unlink()
    with duckdb.connect(str(alias), read_only=True) as connection:
        assert connection.execute("select count(*) from main.current_markets").fetchone()[0] > 0


def test_unknown_expected_release_never_activates(production, candidate):
    directory, root, _warehouse, _manifest = candidate
    with pytest.raises((CatalogueError, ValueError, RuntimeError)):
        production.activate_current(root, directory, expected_release_id="f" * 64)
    assert not (directory / "production.duckdb").exists()


@pytest.mark.parametrize("mutation", ["raw", "warehouse", "source_wal", "alias_wal"])
def test_tampered_or_uncheckpointed_data_never_activates(production, candidate, mutation):
    directory, root, warehouse, _manifest = candidate
    if mutation == "raw":
        next((root / "raw").glob("*/body.bin")).write_bytes(b"tampered")
    elif mutation == "warehouse":
        with warehouse.open("ab") as stream:
            stream.write(b"tampered")
    elif mutation == "source_wal":
        warehouse.with_name(warehouse.name + ".wal").write_bytes(b"uncheckpointed")
    else:
        (directory / "production.duckdb.wal").write_bytes(b"uncheckpointed")
    with pytest.raises((CatalogueError, ValueError, RuntimeError)):
        activate(production, candidate)
    assert not (directory / "production.duckdb").exists()


def test_standalone_verification_failure_never_creates_production(
    production, candidate, monkeypatch
):
    directory, _root, _warehouse, _manifest = candidate
    original = publish.verify_release
    calls = []

    def failed_verifier(*args, **kwargs):
        calls.append(args[0])
        if len(calls) == 2:
            raise CatalogueError("independent verification failed")
        return original(*args, **kwargs)

    monkeypatch.setattr(publish, "verify_release", failed_verifier)
    with pytest.raises(CatalogueError, match="independent verification failed"):
        activate(production, candidate)
    assert len(calls) == 2
    assert not (directory / "production.duckdb").exists()


def test_corrupt_current_pointer_cannot_fall_back_to_another_database(production, candidate):
    directory, root, _warehouse, _manifest = candidate
    pointer = root / "current-v2.json"
    value = json.loads(pointer.read_text())
    value["manifest_sha256"] = "0" * 64
    pointer.write_text(json.dumps(value))
    with pytest.raises((CatalogueError, ValueError, RuntimeError)):
        activate(production, candidate)
    assert not (directory / "production.duckdb").exists()


@pytest.mark.parametrize("operation", ["link", "replace"])
def test_failed_pre_activation_preserves_previous_production_bytes(
    production, prior_production_candidate, monkeypatch, operation
):
    directory, _root, _warehouse, _manifest = prior_production_candidate
    alias = directory / "production.duckdb"
    before = alias.read_bytes()

    def fail(*_args, **_kwargs):
        raise OSError(f"injected {operation} failure")

    monkeypatch.setattr(production.os, operation, fail)
    with pytest.raises(OSError, match=f"injected {operation} failure"):
        activate(production, prior_production_candidate)
    assert alias.read_bytes() == before


def test_successful_rebase_atomically_exposes_new_database_without_mutating_prior_readers(
    production, prior_production_candidate
):
    directory, _root, warehouse, _manifest = prior_production_candidate
    alias = directory / "production.duckdb"
    query = "select observation_id from main.observation_history order by observation_id"
    with duckdb.connect(str(alias), read_only=True) as previous_reader:
        old_rows = previous_reader.execute(query).fetchall()
        activate(production, prior_production_candidate)
        assert previous_reader.execute(query).fetchall() == old_rows
        with duckdb.connect(str(alias), read_only=True) as cached_reader:
            assert cached_reader.execute(query).fetchall() == old_rows
        fresh_rows = json.loads(
            subprocess.check_output(
                [
                    sys.executable,
                    "-c",
                    "import duckdb,json,sys; "
                    "c=duckdb.connect(sys.argv[1],read_only=True); "
                    "print(json.dumps(c.execute(sys.argv[2]).fetchall()))",
                    str(alias),
                    query,
                ],
                text=True,
                timeout=20,
            )
        )
        with duckdb.connect(str(warehouse), read_only=True) as source:
            assert fresh_rows == [list(row) for row in source.execute(query).fetchall()]
        assert fresh_rows != [list(row) for row in old_rows]
    assert alias.stat().st_ino == warehouse.stat().st_ino
    with (
        duckdb.connect(str(alias), read_only=True) as new_reader,
        duckdb.connect(str(warehouse), read_only=True) as source,
    ):
        assert new_reader.execute(query).fetchall() == source.execute(query).fetchall()
        assert new_reader.execute(query).fetchall() != old_rows


def test_readback_hash_rejects_bytes_changed_during_replacement(production, candidate):
    original = os.replace

    def replace_then_change_bytes(source, destination):
        original(source, destination)
        with Path(destination).open("ab") as stream:
            stream.write(b"changed after certification")

    with pytest.raises((CatalogueError, ValueError, RuntimeError)):
        activate(production, candidate, replace_fn=replace_then_change_bytes)


def test_directory_sync_failure_is_not_success_and_activation_can_be_reconciled(
    production, candidate
):
    directory, _root, warehouse, _manifest = candidate

    def fail_sync(_path):
        raise OSError("injected directory synchronization failure")

    with pytest.raises(OSError, match="directory synchronization failure"):
        activate(production, candidate, sync_fn=fail_sync)
    alias = directory / "production.duckdb"
    assert alias.stat().st_ino == warehouse.stat().st_ino
    reconciled = activate(production, candidate)
    assert reconciled["state"] == "verified"


def test_external_accounting_does_not_count_active_warehouse_inode_twice(production, candidate):
    directory, _root, warehouse, _manifest = candidate
    runs = directory / "runs"
    runs.mkdir()
    report = runs / "report.json"
    report.write_bytes(b"{}")
    os.link(warehouse, directory / "production.duckdb")
    assert production._external_bytes(directory) == report.stat().st_size


def test_external_accounting_counts_production_inode_after_source_is_pruned(production, candidate):
    directory, _root, warehouse, _manifest = candidate
    os.link(warehouse, directory / "production.duckdb")
    expected = warehouse.stat().st_size
    warehouse.unlink()
    assert production._external_bytes(directory) == expected


def test_external_accounting_deduplicates_hard_linked_run_artifacts(production, tmp_path):
    runs = tmp_path / "runs"
    runs.mkdir()
    original = runs / "original.log"
    original.write_bytes(b"only one physical inode")
    os.link(original, runs / "linked.log")
    assert production._external_bytes(tmp_path) == original.stat().st_size


def test_private_activation_worker_runs_outside_checkout(candidate, tmp_path):
    directory, root, warehouse, manifest = candidate
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--_activate-current",
            str(root),
            str(directory),
            manifest["source_release_id"],
        ],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(SCRIPT.parent.parent / "src")},
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    value = json.loads(result.stdout)
    assert value["state"] == "verified"
    assert value["source_release_id"] == manifest["source_release_id"]
    assert (directory / "production.duckdb").stat().st_ino == warehouse.stat().st_ino


def test_external_accounting_deduplicates_a_still_retained_older_warehouse(production, tmp_path):
    root = tmp_path / "operator"
    previous = root / "builds-v2" / ("a" * 32) / "warehouse.duckdb"
    previous.parent.mkdir(parents=True)
    previous.write_bytes(b"previous production database")
    os.link(previous, tmp_path / "production.duckdb")
    assert production._external_bytes(tmp_path) == 0
    previous.unlink()
    assert production._external_bytes(tmp_path) == len(b"previous production database")


def test_interrupted_post_promotion_retirement_blocks_activation_until_cleanup_finishes(
    production, candidate, monkeypatch
):
    directory, root, _warehouse, old_manifest = candidate
    activate(production, candidate)
    alias = directory / "production.duckdb"
    previous_bytes = alias.read_bytes()
    next_state = capture.capture(
        Store(root), client_factory=FakeClient, catalogue_profile="exchange"
    )
    assert next_state["complete"]
    original_finish = incremental._finish_retirement_plan

    def interrupted_retirement(store):
        if store.path("prune-plan.json").exists():
            raise OSError("injected durable retirement interruption")
        return original_finish(store)

    monkeypatch.setattr(incremental, "_finish_retirement_plan", interrupted_retirement)
    with pytest.raises(OSError, match="durable retirement interruption"):
        publish.publish(Store(root), next_state["capture_id"], catalogue_version=2)
    promoted = publish.current(Store(root), catalogue_version=2)
    assert promoted["capture_id"] == next_state["capture_id"]
    assert promoted["source_release_id"] != old_manifest["source_release_id"]
    assert (root / "prune-plan.json").is_file()
    assert alias.read_bytes() == previous_bytes

    with pytest.raises(OSError, match="durable retirement interruption"):
        production.activate_current(
            root, directory, expected_release_id=promoted["source_release_id"]
        )
    assert alias.read_bytes() == previous_bytes
    assert (root / "prune-plan.json").is_file()

    monkeypatch.setattr(incremental, "_finish_retirement_plan", original_finish)
    monkeypatch.setattr(
        FakeClient, "get", lambda *_a, **_kw: pytest.fail("activation recovery used networking")
    )
    recovered = production.activate_current(
        root, directory, expected_release_id=promoted["source_release_id"]
    )
    assert recovered["state"] == "verified"
    assert recovered["capture_id"] == next_state["capture_id"]
    assert not (root / "prune-plan.json").exists()
    assert not list((root / "prune-trash").iterdir())
    assert not (root / "captures" / old_manifest["capture_id"]).exists()
    warehouse = root / "builds-v2" / next_state["capture_id"] / "warehouse.duckdb"
    assert alias.stat().st_ino == warehouse.stat().st_ino


def test_activation_prune_failure_preserves_previous_production(
    production, prior_production_candidate, monkeypatch
):
    directory, _root, _warehouse, _manifest = prior_production_candidate
    alias = directory / "production.duckdb"
    previous_bytes = alias.read_bytes()

    def failed_prune(*_args, **_kwargs):
        raise OSError("injected activation prune failure")

    monkeypatch.setattr(incremental, "prune", failed_prune)
    with pytest.raises(OSError, match="activation prune failure"):
        activate(production, prior_production_candidate)
    assert alias.read_bytes() == previous_bytes
