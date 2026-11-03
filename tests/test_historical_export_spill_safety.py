from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

from kalshi_events import historical_export, publish
from kalshi_events.common import CatalogueError, Store

_BINDING = "a" * 64
_NATIVE_SPILL = "duckdb_temp_12345_0_storage_DEFAULT-0.tmp"


def _setup(tmp_path: Path):
    store = Store(tmp_path / "root")
    database = store.path("builds/export/warehouse.duckdb")
    database.parent.mkdir(parents=True)
    with duckdb.connect(str(database), config={}) as connection:
        connection.execute("create table source_rows (value bigint)")
        connection.execute("insert into source_rows values (1), (2), (3)")
    store.sync(database.relative_to(store.root).as_posix())
    temporary = store.path("tmp/publish-" + "b" * 32)
    temporary.mkdir(parents=True)
    return store, database, temporary


def _owner(store, database, temporary):
    return publish._spill_owner_record(
        store,
        database,
        temporary,
        historical_export=True,
        export_binding_sha256=_BINDING,
    )


def _write_owned_leftover(store, database, temporary):
    owner = _owner(store, database, temporary)
    marker = temporary / historical_export._SPILL_OWNER
    store.write(marker.relative_to(store.root).as_posix(), owner)
    spill = temporary / "duckdb-spill"
    spill.mkdir()
    leftover = spill / _NATIVE_SPILL
    leftover.write_bytes(b"native DuckDB spill")
    store.sync(owner["spill"])
    return owner, marker, spill, leftover


def test_owned_native_spill_is_cleaned_under_writer_lock(tmp_path):
    store, database, temporary = _setup(tmp_path)
    owner, marker, spill, leftover = _write_owned_leftover(store, database, temporary)

    with (
        store.writer_guard(),
        publish._export_connection(
            store,
            database,
            temporary,
            historical_export=True,
            export_binding_sha256=_BINDING,
        ) as connection,
    ):
        assert connection.execute("select sum(value) from main.source_rows").fetchone() == (6,)

    assert not spill.exists()
    assert not marker.exists()
    assert not leftover.exists()


def test_owned_spill_recovery_requires_writer_lock_and_preserves_files(tmp_path):
    store, database, temporary = _setup(tmp_path)
    _owner_record, marker, spill, leftover = _write_owned_leftover(store, database, temporary)

    with (
        pytest.raises(CatalogueError, match="writer lock"),
        publish._export_connection(
            store,
            database,
            temporary,
            historical_export=True,
            export_binding_sha256=_BINDING,
        ),
    ):
        pytest.fail("stale spill cleanup ran without the writer lock")

    assert marker.is_file() and leftover.read_bytes() == b"native DuckDB spill"
    assert spill.is_dir()


def test_remove_work_syncs_spill_removal_before_unlinking_owner(tmp_path, monkeypatch):
    store, database, temporary = _setup(tmp_path)
    _owner_record, marker, spill, _leftover = _write_owned_leftover(store, database, temporary)
    events = []
    original_rmtree = historical_export.shutil.rmtree
    original_remove = historical_export._remove_owned_file

    def record_rmtree(path, *args, **kwargs):
        if path == spill:
            assert marker.is_file()
            events.append("spill")
        elif path == temporary:
            assert not spill.exists() and not marker.exists()
            events.append("work")
        return original_rmtree(path, *args, **kwargs)

    def record_remove(store_arg, path):
        if path == marker:
            assert not spill.exists()
            events.append("owner")
        return original_remove(store_arg, path)

    monkeypatch.setattr(historical_export.shutil, "rmtree", record_rmtree)
    monkeypatch.setattr(historical_export, "_remove_owned_file", record_remove)
    with store.writer_guard():
        historical_export.remove_work(
            store,
            temporary.relative_to(store.root).as_posix(),
            binding_sha256=_BINDING,
        )

    assert events == ["spill", "owner", "work"]
    assert not temporary.exists()


def test_foreign_spill_entry_is_rejected_and_preserved(tmp_path):
    store, database, temporary = _setup(tmp_path)
    _owner_record, marker, spill, _leftover = _write_owned_leftover(store, database, temporary)
    foreign = spill / "foreign.bin"
    foreign.write_bytes(b"keep me")

    with (
        store.writer_guard(),
        pytest.raises(CatalogueError, match="foreign"),
        publish._export_connection(
            store,
            database,
            temporary,
            historical_export=True,
            export_binding_sha256=_BINDING,
        ),
    ):
        pytest.fail("foreign spill content was accepted")

    assert marker.is_file()
    assert foreign.read_bytes() == b"keep me"


def test_spill_without_owner_intent_is_rejected_and_preserved(tmp_path):
    store, database, temporary = _setup(tmp_path)
    spill = temporary / "duckdb-spill"
    spill.mkdir()
    leftover = spill / _NATIVE_SPILL
    leftover.write_bytes(b"native DuckDB spill")

    with (
        store.writer_guard(),
        pytest.raises(CatalogueError, match="no durable owner"),
        publish._export_connection(
            store,
            database,
            temporary,
            historical_export=True,
            export_binding_sha256=_BINDING,
        ),
    ):
        pytest.fail("spill without an owner intent was accepted")

    assert spill.is_dir() and leftover.read_bytes() == b"native DuckDB spill"


@pytest.mark.parametrize("identity_change", ["export_binding", "source_database"])
def test_mismatched_spill_identity_preserves_owned_files(tmp_path, identity_change):
    store, database, temporary = _setup(tmp_path)
    _owner_record, marker, spill, leftover = _write_owned_leftover(store, database, temporary)
    changed_binding = _BINDING
    if identity_change == "export_binding":
        changed_binding = "c" * 64
    else:
        database = store.path("builds/export/other.duckdb")
        with duckdb.connect(str(database), config={}) as connection:
            connection.execute("create table source_rows (value bigint)")
        store.sync(database.relative_to(store.root).as_posix())

    with (
        store.writer_guard(),
        pytest.raises(CatalogueError, match="binding"),
        publish._export_connection(
            store,
            database,
            temporary,
            historical_export=True,
            export_binding_sha256=changed_binding,
        ),
    ):
        pytest.fail("spill from a different export identity was accepted")

    assert marker.is_file()
    assert spill.is_dir() and leftover.read_bytes() == b"native DuckDB spill"


@pytest.mark.parametrize("unsafe", ["symlink", "hardlink", "device", "subdirectory"])
def test_spill_inventory_rejects_unsafe_entries(tmp_path, monkeypatch, unsafe):
    store, _database, temporary = _setup(tmp_path)
    spill = temporary / "duckdb-spill"
    spill.mkdir()
    artifact = spill / _NATIVE_SPILL
    artifact.write_bytes(b"native DuckDB spill")
    if unsafe == "symlink":
        artifact.unlink()
        target = tmp_path / "foreign.spill"
        target.write_bytes(b"foreign")
        artifact.symlink_to(target)
    elif unsafe == "hardlink":
        target = tmp_path / "hardlink-target.spill"
        target.write_bytes(b"foreign")
        artifact.unlink()
        artifact.hardlink_to(target)
    elif unsafe == "device":
        original_lstat = Path.lstat

        def other_device(path):
            result = original_lstat(path)
            if path == artifact:
                return SimpleNamespace(
                    st_mode=result.st_mode,
                    st_dev=result.st_dev + 1,
                    st_nlink=result.st_nlink,
                    st_size=result.st_size,
                )
            return result

        monkeypatch.setattr(Path, "lstat", other_device)
    else:
        (spill / "nested").mkdir()

    with pytest.raises(CatalogueError):
        historical_export._validate_safe_tree(store, spill)


def test_work_tree_requires_owned_spill_directory_and_marker(tmp_path):
    store, database, temporary = _setup(tmp_path)
    spill = temporary / "duckdb-spill"
    spill.mkdir()
    (spill / _NATIVE_SPILL).write_bytes(b"native DuckDB spill")

    with pytest.raises(CatalogueError, match="owner"):
        historical_export._validate_work_tree(store, temporary)

    owner = _owner(store, database, temporary)
    marker = temporary / historical_export._SPILL_OWNER
    store.write(marker.relative_to(store.root).as_posix(), owner)
    assert owner["export_binding_sha256"] == _BINDING
    historical_export._validate_work_tree(store, temporary)
    assert marker.is_file()
