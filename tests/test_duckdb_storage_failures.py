from __future__ import annotations

import hashlib
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest
from test_incremental_catalogue import baseline as baseline
from test_incremental_catalogue import certified_baseline as certified_baseline

from kalshi_events import warehouse
from kalshi_events.common import BudgetExceeded, CatalogueError, Limits, Store


def _create_database(path: Path, *, compatibility_version: str | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    config = (
        {"storage_compatibility_version": compatibility_version}
        if compatibility_version is not None
        else {}
    )
    with duckdb.connect(str(path), config=config) as connection:
        connection.execute("create schema ledger")
        connection.execute(
            """create table ledger.records (
                   record_id integer primary key,
                   amount decimal(38, 9),
                   note varchar,
                   active boolean not null
               )"""
        )
        connection.execute(
            "insert into ledger.records values "
            "(1, '123456789012345678.123456789', 'Unicode 🌉', true), "
            "(2, '-0.000000001', null, false)"
        )
        connection.execute(
            "create view ledger.active_records as "
            "select record_id, amount, note from ledger.records where active"
        )


def _source_store(tmp_path: Path, *, compatibility_version: str | None = None):
    store = Store(tmp_path / "root")
    relative = "builds/source/warehouse.duckdb"
    source = store.path(relative)
    _create_database(source, compatibility_version=compatibility_version)
    store.sync(relative)
    return store, relative, source


def _copy(store: Store, source_relative: str, source: Path, *, expected_sha256: str | None = None):
    staging = store.path("tmp/storage-copy")
    staging.mkdir(parents=True)
    warehouse._copy_closed_database(
        store,
        source_relative,
        staging,
        expected_sha256=expected_sha256 or hashlib.sha256(source.read_bytes()).hexdigest(),
    )
    return staging / "warehouse.duckdb"


def _assert_catalogue(path: Path) -> None:
    with duckdb.connect(str(path), read_only=True) as connection:
        catalog = connection.execute(
            "select table_name, table_type from information_schema.tables "
            "where table_schema = 'ledger' order by table_name"
        ).fetchall()
        assert catalog == [("active_records", "VIEW"), ("records", "BASE TABLE")]
        assert connection.execute(
            "select record_id, amount, note, active from ledger.records order by record_id"
        ).fetchall() == [
            (1, Decimal("123456789012345678.123456789"), "Unicode 🌉", True),
            (2, Decimal("-0.000000001"), None, False),
        ]
        assert connection.execute("select * from ledger.active_records").fetchall() == [
            (1, Decimal("123456789012345678.123456789"), "Unicode 🌉")
        ]


def test_native_storage_copy_keeps_certified_bytes_and_catalogue(tmp_path):
    store, relative, source = _source_store(tmp_path)
    source_bytes = source.read_bytes()
    expected_sha256 = hashlib.sha256(source_bytes).hexdigest()

    destination = _copy(store, relative, source, expected_sha256=expected_sha256)

    assert destination.read_bytes() == source_bytes
    assert hashlib.sha256(destination.read_bytes()).hexdigest() == expected_sha256
    assert source.read_bytes() == source_bytes
    assert not source.with_name(source.name + ".wal").exists()
    _assert_catalogue(destination)
    assert store._storage_reservations == 0


@pytest.mark.parametrize("compatibility_version", ["v1.0.0", "v1.2.0", "v1.4.0"])
def test_legacy_storage_copy_migrates_without_changing_source(tmp_path, compatibility_version):
    store, relative, source = _source_store(tmp_path, compatibility_version=compatibility_version)
    source_bytes = source.read_bytes()
    expected_sha256 = hashlib.sha256(source_bytes).hexdigest()

    destination = _copy(store, relative, source, expected_sha256=expected_sha256)

    assert source.read_bytes() == source_bytes
    assert hashlib.sha256(destination.read_bytes()).hexdigest() != expected_sha256
    assert not source.with_name(source.name + ".wal").exists()
    _assert_catalogue(destination)
    assert store._storage_reservations == 0


def test_failed_legacy_reservation_preserves_existing_reservations(tmp_path, monkeypatch):
    store, relative, source = _source_store(tmp_path, compatibility_version="v1.0.0")
    source_bytes = source.read_bytes()
    preexisting = 17
    store.reserve_storage(preexisting)

    def fail_reservation(_amount):
        raise BudgetExceeded("injected conversion reservation failure")

    monkeypatch.setattr(store, "reserve_storage", fail_reservation)
    try:
        with pytest.raises(BudgetExceeded, match="injected conversion reservation failure"):
            _copy(store, relative, source)
        assert store._storage_reservations == preexisting
    finally:
        if store._storage_reservations == preexisting:
            store.release_storage(preexisting)

    assert store._storage_reservations == 0
    assert source.read_bytes() == source_bytes
    assert not store.path("tmp/storage-copy/warehouse.duckdb").exists()


def test_copy_refuses_a_source_with_an_uncheckpointed_wal(tmp_path):
    store, relative, source = _source_store(tmp_path)
    source_bytes = source.read_bytes()
    wal = source.with_name(source.name + ".wal")
    wal.write_bytes(b"uncheckpointed changes")
    store.sync(str(Path(relative + ".wal")))

    with pytest.raises(CatalogueError, match="WAL|wal"):
        _copy(store, relative, source)

    assert source.read_bytes() == source_bytes
    assert not store.path("tmp/storage-copy/warehouse.duckdb").exists()
    assert store._storage_reservations == 0


@pytest.mark.parametrize("link", ["source-wal", "staged-database", "staged-wal"])
def test_copy_rejects_dangling_storage_symlinks_without_creating_targets(tmp_path, link):
    store, relative, source = _source_store(tmp_path, compatibility_version="v1.0.0")
    source_bytes = source.read_bytes()
    staging = store.path("tmp/symlink-copy")
    staging.mkdir(parents=True)
    outside = tmp_path / f"outside-{link}.duckdb"
    assert not outside.exists()
    if link == "source-wal":
        source.with_name(source.name + ".wal").symlink_to(outside)
    else:
        staged_name = "warehouse.duckdb" if link == "staged-database" else "warehouse.duckdb.wal"
        (staging / staged_name).symlink_to(outside)
    reservations_before = store._storage_reservations

    with pytest.raises(CatalogueError, match="symlink|unsafe|staged|WAL|wal"):
        warehouse._copy_closed_database(
            store,
            relative,
            staging,
            expected_sha256=hashlib.sha256(source_bytes).hexdigest(),
        )

    assert not outside.exists()
    assert source.read_bytes() == source_bytes
    assert store._storage_reservations == reservations_before


@pytest.mark.parametrize("tamper", [False, True], ids=["wrong-receipt-hash", "changed-source"])
def test_copy_rejects_unverified_source_identity(tmp_path, tamper):
    store, relative, source = _source_store(tmp_path)
    certified_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    if tamper:
        with source.open("ab") as stream:
            stream.write(b"changed after certification")
        store.sync(relative)
    source_bytes = source.read_bytes()
    expected_sha256 = certified_hash if tamper else "0" * 64

    with pytest.raises(CatalogueError):
        _copy(store, relative, source, expected_sha256=expected_sha256)

    assert source.read_bytes() == source_bytes
    assert store._storage_reservations == 0


def test_copy_rejects_source_changed_after_its_initial_hash(tmp_path, monkeypatch):
    store, relative, source = _source_store(tmp_path)
    certified_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    original_open = Path.open
    changed = False

    class MutateAfterRead:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            self.stream.__enter__()
            return self

        def __exit__(self, *args):
            nonlocal changed
            result = self.stream.__exit__(*args)
            if not changed:
                changed = True
                with original_open(source, "ab") as stream:
                    stream.write(b"changed during copy")
            return result

        def __getattr__(self, name):
            return getattr(self.stream, name)

    def mutate_after_source_read(path, mode="r", *args, **kwargs):
        stream = original_open(path, mode, *args, **kwargs)
        if path == source and "r" in mode and "b" in mode and not changed:
            return MutateAfterRead(stream)
        return stream

    monkeypatch.setattr(Path, "open", mutate_after_source_read)
    with pytest.raises(CatalogueError):
        _copy(store, relative, source, expected_sha256=certified_hash)

    assert changed
    assert hashlib.sha256(source.read_bytes()).hexdigest() != certified_hash
    assert store._storage_reservations == 0


@pytest.mark.parametrize("ceiling", ["database", "temporary"])
def test_copy_respects_database_and_temporary_storage_budgets(tmp_path, ceiling):
    store, relative, source = _source_store(tmp_path)
    size = source.stat().st_size
    store.limits = (
        Limits(max_database_bytes=2 * size - 1)
        if ceiling == "database"
        else Limits(max_temp_bytes=size - 1)
    )

    with pytest.raises(CatalogueError, match="database|storage|limit"):
        _copy(store, relative, source)

    assert not store.path("tmp/storage-copy/warehouse.duckdb").exists()
    assert store._storage_reservations == 0


def test_copy_obeys_an_expired_operation_deadline(tmp_path):
    store, relative, source = _source_store(tmp_path)
    store.started -= store.limits.max_duration_seconds + 1

    with pytest.raises(BudgetExceeded, match="deadline"):
        _copy(store, relative, source)

    assert store._storage_reservations == 0


def test_copy_releases_reservation_when_destination_sync_fails(tmp_path, monkeypatch):
    store, relative, source = _source_store(tmp_path)
    source_bytes = source.read_bytes()

    def fail_sync(_path):
        raise OSError("injected destination sync failure")

    monkeypatch.setattr(warehouse, "fsync_file", fail_sync)
    with pytest.raises(OSError, match="destination sync failure"):
        _copy(store, relative, source)

    assert source.read_bytes() == source_bytes
    assert store._storage_reservations == 0


def test_copy_detects_deadline_expiry_during_durability_sync(tmp_path, monkeypatch):
    store, relative, source = _source_store(tmp_path, compatibility_version="v1.0.0")
    source_bytes = source.read_bytes()
    original_fsync_file = warehouse.fsync_file

    def expire_after_file_sync(path):
        original_fsync_file(path)
        store.started -= store.limits.max_duration_seconds + 1

    monkeypatch.setattr(warehouse, "fsync_file", expire_after_file_sync)
    with pytest.raises(BudgetExceeded, match="deadline"):
        _copy(store, relative, source)

    destination = store.path("tmp/storage-copy/warehouse.duckdb")
    assert source.read_bytes() == source_bytes
    assert not destination.exists()
    assert not destination.with_name(destination.name + ".wal").exists()
    assert store._storage_reservations == 0


def test_copy_failure_cannot_replace_certified_baselines_or_publication(baseline, monkeypatch):
    store, capture_id, _legacy_release, _version_two_release = baseline
    v1 = store.path(f"builds/{capture_id}/warehouse.duckdb")
    v2 = store.path(f"builds-v2/{capture_id}/warehouse.duckdb")
    v1_before = hashlib.sha256(v1.read_bytes()).hexdigest()
    v2_before = hashlib.sha256(v2.read_bytes()).hexdigest()
    publication_before = store.path("current.json").read_bytes()
    publication_v2_before = store.path("current-v2.json").read_bytes()

    def fail_sync(_path):
        raise OSError("injected destination sync failure")

    monkeypatch.setattr(warehouse, "fsync_file", fail_sync)
    with pytest.raises(OSError, match="destination sync failure"):
        warehouse.build_v2_full(store, capture_id)

    assert hashlib.sha256(v1.read_bytes()).hexdigest() == v1_before
    assert hashlib.sha256(v2.read_bytes()).hexdigest() == v2_before
    assert store.path("current.json").read_bytes() == publication_before
    assert store.path("current-v2.json").read_bytes() == publication_v2_before
    assert not list(store.path("tmp").glob("build-v2-*"))
