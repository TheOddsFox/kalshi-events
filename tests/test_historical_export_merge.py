from __future__ import annotations

import shutil

import duckdb
import pyarrow.parquet as pq
import pytest
from test_historical_compact_views import CAPTURE_ID, _seed_warehouse
from test_historical_publication import (  # noqa: F401
    historical_publication as historical_publication_fixture,
)

from kalshi_events import historical_export, publish, warehouse
from kalshi_events.common import CatalogueError, Store, decode, digest, regular_bytes


def _source(rows):
    connection = duckdb.connect(
        ":memory:",
        config={
            "threads": "1",
            "disabled_optimizers": "statistics_propagation",
            "preserve_insertion_order": "false",
        },
    )
    connection.execute(
        """
        create table source_rows (
            ticker varchar,
            ordinal bigint,
            payload_json json,
            note varchar
        )
        """
    )
    connection.executemany("insert into source_rows values (?, ?, cast(? as json), ?)", rows)
    return connection


def _reference_schemas(connection, path, table="source_rows"):
    connection.execute(
        f"copy (select * from main.{table} limit 0) to ? "
        f"(format parquet, {historical_export.COPY_OPTIONS})",
        [str(path)],
    ).fetchall()
    try:
        parquet = pq.ParquetFile(path)
        return parquet.schema, parquet.schema_arrow
    finally:
        path.unlink()


def _export(tmp_path, monkeypatch, rows, *, batch_rows=4096):
    monkeypatch.setattr(historical_export, "_BATCH_ROWS", batch_rows)
    store = Store(tmp_path / "root")
    connection = _source(rows)
    reference_physical, reference_arrow = _reference_schemas(
        connection, tmp_path / "reference.parquet"
    )
    _, work_relative = historical_export.binding(
        "a" * 32, "b" * 64, {}, {}, {}, {"source_rows": "ticker, ordinal"}
    )
    historical_export.prepare(store, work_relative)
    try:
        result = historical_export.export_relation(
            store,
            connection,
            work_relative=work_relative,
            binding_sha256="c" * 64,
            name="source_rows",
            table="source_rows",
            keys=("ticker", "ordinal"),
        )
    except BaseException:
        connection.close()
        raise
    return store, connection, work_relative, result, reference_physical, reference_arrow


def test_external_merge_preserves_reference_schema_and_binary_nulls_last_order(
    tmp_path, monkeypatch
):
    rows = [
        ("é", 3, '{"value":3}', "accent"),
        ("a", None, '{"value":2}', None),
        (None, 8, '{"value":8}', "null ticker"),
        ("z", 0, '{"value":0}', "last"),
        ("a", 2, '{"value":1}', "first"),
        ("A", 7, '{"value":7}', "upper"),
    ]
    store, connection, work_relative, checkpoint, reference_physical, reference_arrow = _export(
        tmp_path, monkeypatch, rows, batch_rows=2
    )
    try:
        output = store.path(f"{work_relative}/output/source_rows.parquet")
        parquet = pq.ParquetFile(output)
        assert checkpoint["complete"] is True
        assert checkpoint["row_count"] == len(rows)
        assert parquet.schema.equals(reference_physical)
        assert parquet.schema_arrow.equals(reference_arrow, check_metadata=True)
        values = parquet.read().select(["ticker", "ordinal", "note"]).to_pylist()
        assert [(row["ticker"], row["ordinal"]) for row in values] == [
            ("A", 7),
            ("a", 2),
            ("a", None),
            ("z", 0),
            ("é", 3),
            (None, 8),
        ]
        assert values[2]["note"] is None
    finally:
        connection.close()


def test_external_merge_uses_multiple_bounded_passes_and_reuses_sealed_export(
    tmp_path, monkeypatch
):
    rows = [
        (f"key-{index:02}", index, f'{{"ordinal":{index}}}', f"row-{index}")
        for index in reversed(range(19))
    ]
    store, connection, work_relative, first, _, _ = _export(
        tmp_path, monkeypatch, rows, batch_rows=2
    )
    try:
        assert first["row_count"] == 19
        output = store.path(f"{work_relative}/output/source_rows.parquet")
        assert pq.read_table(output, columns=["ordinal"])["ordinal"].to_pylist() == list(range(19))

        def unexpected_respool(*args, **kwargs):
            raise AssertionError("sealed relation should be reused")

        monkeypatch.setattr(historical_export, "_spool_runs", unexpected_respool)
        second = historical_export.export_relation(
            store,
            connection,
            work_relative=work_relative,
            binding_sha256="c" * 64,
            name="source_rows",
            table="source_rows",
            keys=("ticker", "ordinal"),
        )
        assert second == first
    finally:
        connection.close()


def test_external_merge_rejects_duplicate_keys_without_sealing_output(tmp_path, monkeypatch):
    rows = [
        (f"key-{index % 9:02}", index % 9, f'{{"ordinal":{index}}}', f"row-{index}")
        for index in range(20)
    ]
    store = Store(tmp_path / "root")
    connection = _source(rows)
    _, work_relative = historical_export.binding(
        "a" * 32, "b" * 64, {}, {}, {}, {"source_rows": "ticker, ordinal"}
    )
    historical_export.prepare(store, work_relative)
    monkeypatch.setattr(historical_export, "_BATCH_ROWS", 2)
    try:
        with pytest.raises(CatalogueError, match="not unique"):
            historical_export.export_relation(
                store,
                connection,
                work_relative=work_relative,
                binding_sha256="c" * 64,
                name="source_rows",
                table="source_rows",
                keys=("ticker", "ordinal"),
            )
        assert not store.path(f"{work_relative}/output/source_rows.parquet").exists()
        checkpoint = store.path(f"{work_relative}/checkpoints/source_rows.json")
        assert decode(regular_bytes(checkpoint, 1024 * 1024))["complete"] is False
    finally:
        connection.close()


def test_corrupt_publication_recovery_marker_falls_back_to_export(tmp_path):
    store = Store(tmp_path / "root")
    _, work_relative = historical_export.binding(
        "a" * 32, "b" * 64, {}, {}, {}, {"source_rows": "ticker, ordinal"}
    )
    historical_export.prepare(store, work_relative)
    store.write_bytes(f"{work_relative}/checkpoints/publication.json", b"{")

    assert (
        publish._recover_historical_release(
            store,
            work_relative,
            "c" * 64,
            "a" * 32,
            {},
            {},
        )
        is None
    )


def test_external_merge_matches_compact_view_schemas_for_sparse_and_empty_results(tmp_path):
    store = Store(tmp_path / "root")
    database_relative = "builds/compact/warehouse.duckdb"
    database = store.path(database_relative)
    database.parent.mkdir(parents=True)
    _seed_warehouse(database)
    warehouse.dbt(
        store,
        ["build"],
        database=database_relative,
        variables={
            "catalogue_version": 2,
            "incremental_update": False,
            "current_capture_id": CAPTURE_ID,
            "delta_capture_id": CAPTURE_ID,
            "historical_compact": True,
        },
    )
    with duckdb.connect(str(database)) as connection:
        connection.execute(
            "create view main.empty_compact as select * from main.current_markets where false"
        )
    store.sync(database_relative)

    relation_keys = {
        "current_markets": ("market_ticker",),
        "combo_legs": ("combo_market_ticker", "leg_index"),
        "relationships": ("parent_kind", "parent_ticker", "child_kind", "child_ticker"),
        "empty_compact": ("market_ticker",),
    }
    _, work_relative = historical_export.binding(
        CAPTURE_ID,
        "b" * 64,
        {},
        {},
        {},
        {name: ", ".join(keys) for name, keys in relation_keys.items()},
    )
    work = historical_export.prepare(store, work_relative)
    baselines = tmp_path / "baseline"
    baselines.mkdir()
    with publish._export_connection(store, database, work, historical_export=True) as connection:
        for table, keys in relation_keys.items():
            baseline_schema_path = baselines / f"{table}-schema.parquet"
            reference_physical, reference_arrow = _reference_schemas(
                connection, baseline_schema_path, table
            )
            baseline_path = baselines / f"{table}.parquet"
            order = ", ".join(keys)
            connection.execute(
                f"copy (select * from main.{table} order by {order}) to ? "
                f"(format parquet, {historical_export.COPY_OPTIONS})",
                [str(baseline_path)],
            ).fetchall()
            result = historical_export.export_relation(
                store,
                connection,
                work_relative=work_relative,
                binding_sha256="c" * 64,
                name=table,
                table=table,
                keys=keys,
            )
            output = store.path(f"{work_relative}/output/{table}.parquet")
            parquet = pq.ParquetFile(output)
            assert parquet.schema.equals(reference_physical), table
            assert parquet.schema_arrow.equals(reference_arrow, check_metadata=True), table
            assert pq.read_table(output).equals(pq.read_table(baseline_path)), table
            assert result["row_count"] == pq.ParquetFile(baseline_path).metadata.num_rows, table

            if table == "current_markets":
                rows = pq.read_table(output).to_pylist()
                assert any(
                    row["market_ticker"] == "MISSING"
                    and row["usable"] is False
                    and row["rules_secondary"] is None
                    for row in rows
                )
            elif table == "combo_legs":
                assert any(not row["resolved"] for row in pq.read_table(output).to_pylist())
            elif table == "empty_compact":
                assert result["row_count"] == 0


def test_legacy_current_release_reuses_only_after_exact_export_binding(
    tmp_path,
    monkeypatch,
    historical_publication_fixture,  # noqa: F811
):
    root = tmp_path / "legacy-root"
    shutil.copytree(historical_publication_fixture["root"], root)
    store = Store(root)
    release_id = historical_publication_fixture["historical_release"]["source_release_id"]
    manifest_relative = f"releases/{release_id}/manifest.json"
    manifest_path = store.path(manifest_relative)
    manifest = decode(regular_bytes(manifest_path, 1024 * 1024))
    manifest.pop("historical_export_policy")
    store.write(manifest_relative, manifest)
    pointer = decode(regular_bytes(store.path("current-historical.json"), 1024 * 1024))
    pointer["manifest_sha256"] = digest(regular_bytes(manifest_path, 1024 * 1024))
    store.write("current-historical.json", pointer)

    export_calls = 0
    original_export = historical_export.export_relation

    def record_export(*args, **kwargs):
        nonlocal export_calls
        export_calls += 1
        return original_export(*args, **kwargs)

    monkeypatch.setattr(historical_export, "export_relation", record_export)
    first = publish.publish(
        store, historical_publication_fixture["capture_id"], catalogue_version=3
    )
    assert first["source_release_id"] == release_id
    assert first.get("historical_export_policy") is None
    assert export_calls == len(warehouse.RELATIONS)
    updated_pointer = decode(regular_bytes(store.path("current-historical.json"), 1024 * 1024))
    assert len(updated_pointer["historical_export_binding_sha256"]) == 64

    def unexpected_export(*_args, **_kwargs):
        raise AssertionError("matching legacy release should be reused")

    monkeypatch.setattr(historical_export, "export_relation", unexpected_export)
    second = publish.publish(
        store, historical_publication_fixture["capture_id"], catalogue_version=3
    )
    assert second["source_release_id"] == release_id
    assert second["received_at"] == first["received_at"]
