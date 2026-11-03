from __future__ import annotations

import json
import os
from decimal import Decimal

import duckdb
import pyarrow.parquet as pq
import pytest

from kalshi_events import historical_export
from kalshi_events.common import BudgetExceeded, CatalogueError, Limits, Store


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
            amount decimal(14, 2),
            enabled boolean,
            payload_json json,
            note varchar
        )
        """
    )
    if rows:
        connection.executemany(
            "insert into source_rows values (?, ?, ?, ?, cast(? as json), ?)", rows
        )
    return connection


def _row(index, *, ticker=None, ordinal=None, note=None):
    return (
        ticker if ticker is not None else f"key-{index:03d}",
        index if ordinal is None else ordinal,
        Decimal(f"{index}.25"),
        index % 2 == 0,
        json.dumps({"ordinal": index}, separators=(",", ":")),
        note,
    )


def _export(tmp_path, monkeypatch, rows, *, batch_rows=1):
    monkeypatch.setattr(historical_export, "_BATCH_ROWS", batch_rows)
    store = Store(tmp_path / "root")
    connection = _source(rows)
    binding_sha256, work_relative = historical_export.binding(
        "a" * 32,
        "b" * 64,
        {},
        {},
        {},
        {"source_rows": "ticker, ordinal"},
    )
    historical_export.prepare(
        store,
        work_relative,
        binding_sha256=binding_sha256,
        tables={"source_rows": "source_rows"},
    )
    result = historical_export.export_relation(
        store,
        connection,
        work_relative=work_relative,
        binding_sha256=binding_sha256,
        name="source_rows",
        table="source_rows",
        keys=("ticker", "ordinal"),
    )
    return store, connection, work_relative, result


def test_online_frontier_carries_and_final_collapse_preserve_every_value(tmp_path, monkeypatch):
    rows = [
        _row(index, note=None if index % 7 == 0 else f"note-{index}")
        for index in reversed(range(63))
    ]
    original_commit = historical_export._commit_frontier
    committed = []

    def record_commit(*args, **kwargs):
        result = original_commit(*args, **kwargs)
        committed.append(result)
        assert all(len(level) <= historical_export._MERGE_FAN_IN - 1 for level in result["levels"])
        return result

    monkeypatch.setattr(historical_export, "_commit_frontier", record_commit)
    store, connection, work_relative, result = _export(tmp_path, monkeypatch, rows)
    try:
        assert historical_export.POLICY == "historical-arrow-external-merge-v1"
        assert committed and max(item["run_count"] for item in committed) > 8
        assert result["run_count"] <= 8
        assert all(len(level) <= 7 for level in result["levels"])
        output = pq.read_table(store.path(f"{work_relative}/output/source_rows.parquet"))
        actual = output.to_pylist()
        expected = sorted(rows, key=lambda row: (row[0].encode("utf-8"), row[1]))
        assert [
            (
                row["ticker"],
                row["ordinal"],
                row["amount"],
                row["enabled"],
                row["payload_json"],
                row["note"],
            )
            for row in actual
        ] == expected
    finally:
        connection.close()


def test_empty_relation_keeps_its_reference_schema(tmp_path, monkeypatch):
    store, connection, work_relative, result = _export(tmp_path, monkeypatch, [])
    try:
        output = pq.ParquetFile(store.path(f"{work_relative}/output/source_rows.parquet"))
        assert result["row_count"] == 0
        assert result["run_count"] == 1
        assert output.metadata.num_rows == 0
        assert output.schema_arrow.names == [
            "ticker",
            "ordinal",
            "amount",
            "enabled",
            "payload_json",
            "note",
        ]
    finally:
        connection.close()


def test_oversized_singletons_survive_eight_head_merge(tmp_path, monkeypatch):
    monkeypatch.setattr(historical_export, "_BATCH_BYTES", 64)
    rows = [_row(index, note="x" * 1024) for index in reversed(range(8))]
    store, connection, work_relative, result = _export(tmp_path, monkeypatch, rows)
    try:
        assert result["row_count"] == 8
        assert result["input_max_singleton_bytes"] > historical_export._BATCH_BYTES
        output = pq.read_table(
            store.path(f"{work_relative}/output/source_rows.parquet"), columns=["ordinal"]
        )
        assert output["ordinal"].to_pylist() == list(range(8))
    finally:
        connection.close()


def test_cross_run_duplicate_key_is_rejected_without_output(tmp_path, monkeypatch):
    rows = [_row(index) for index in range(7)]
    rows.append(_row(7, ticker=rows[0][0], ordinal=rows[0][1]))
    store = Store(tmp_path / "root")
    connection = _source(rows)
    binding_sha256, work_relative = historical_export.binding(
        "a" * 32, "b" * 64, {}, {}, {}, {"source_rows": "ticker, ordinal"}
    )
    historical_export.prepare(
        store,
        work_relative,
        binding_sha256=binding_sha256,
        tables={"source_rows": "source_rows"},
    )
    monkeypatch.setattr(historical_export, "_BATCH_ROWS", 1)
    try:
        with pytest.raises(CatalogueError, match="not unique"):
            historical_export.export_relation(
                store,
                connection,
                work_relative=work_relative,
                binding_sha256=binding_sha256,
                name="source_rows",
                table="source_rows",
                keys=("ticker", "ordinal"),
            )
        assert not store.path(f"{work_relative}/output/source_rows.parquet").exists()
        checkpoint = json.loads(
            store.path(f"{work_relative}/checkpoints/source_rows.json").read_text()
        )
        assert checkpoint["next_rowid"] == 7
        assert checkpoint["run_count"] == 7
    finally:
        connection.close()


def test_retry_discards_uncommitted_carry_and_reuses_sealed_input(tmp_path, monkeypatch):
    rows = [_row(index) for index in range(8)]
    store = Store(tmp_path / "root")
    connection = _source(rows)
    binding_sha256, work_relative = historical_export.binding(
        "a" * 32, "b" * 64, {}, {}, {}, {"source_rows": "ticker, ordinal"}
    )
    historical_export.prepare(
        store,
        work_relative,
        binding_sha256=binding_sha256,
        tables={"source_rows": "source_rows"},
    )
    monkeypatch.setattr(historical_export, "_BATCH_ROWS", 1)
    original_commit = historical_export._commit_frontier
    failed = False

    def fail_before_carry_commit(*args, **kwargs):
        nonlocal failed
        created = args[7]
        if len(created) > 1 and not failed:
            failed = True
            raise RuntimeError("simulated stop before frontier commit")
        return original_commit(*args, **kwargs)

    monkeypatch.setattr(historical_export, "_commit_frontier", fail_before_carry_commit)
    try:
        with pytest.raises(RuntimeError, match="before frontier commit"):
            historical_export.export_relation(
                store,
                connection,
                work_relative=work_relative,
                binding_sha256=binding_sha256,
                name="source_rows",
                table="source_rows",
                keys=("ticker", "ordinal"),
            )
        checkpoint_path = store.path(f"{work_relative}/checkpoints/source_rows.json")
        checkpoint = json.loads(checkpoint_path.read_text())
        assert failed and checkpoint["next_rowid"] == 7
        assert store.path(f"{work_relative}/inputs/source_rows/input.duckdb").exists()
        monkeypatch.setattr(historical_export, "_commit_frontier", original_commit)

        def unexpected_materialize(*args, **kwargs):
            raise AssertionError("clean resume must reuse the sealed scratch database")

        monkeypatch.setattr(historical_export, "_materialize_input", unexpected_materialize)
        result = historical_export.export_relation(
            store,
            connection,
            work_relative=work_relative,
            binding_sha256=binding_sha256,
            name="source_rows",
            table="source_rows",
            keys=("ticker", "ordinal"),
        )
        assert result["row_count"] == 8
        assert not store.path(f"{work_relative}/inputs/source_rows").exists()
        output = pq.read_table(
            store.path(f"{work_relative}/output/source_rows.parquet"), columns=["ordinal"]
        )
        assert output["ordinal"].to_pylist() == list(range(8))
    finally:
        connection.close()


def test_retry_discards_sealed_run_before_its_intent_record(tmp_path, monkeypatch):
    rows = [_row(index) for index in range(3)]
    store = Store(tmp_path / "root")
    connection = _source(rows)
    binding_sha256, work_relative = historical_export.binding(
        "a" * 32, "b" * 64, {}, {}, {}, {"source_rows": "ticker, ordinal"}
    )
    historical_export.prepare(
        store,
        work_relative,
        binding_sha256=binding_sha256,
        tables={"source_rows": "source_rows"},
    )
    monkeypatch.setattr(historical_export, "_BATCH_ROWS", 1)
    checkpoint_path = store.path(f"{work_relative}/checkpoints/source_rows.json")
    run_directory = store.path(f"{work_relative}/runs/source_rows")
    original_record = historical_export._record_frontier_write
    failed = False

    def fail_after_seal(*args, **kwargs):
        nonlocal failed
        if not failed:
            failed = True
            raise RuntimeError("simulated stop after sealed output")
        return original_record(*args, **kwargs)

    monkeypatch.setattr(historical_export, "_record_frontier_write", fail_after_seal)
    try:
        with pytest.raises(RuntimeError, match="after sealed output"):
            historical_export.export_relation(
                store,
                connection,
                work_relative=work_relative,
                binding_sha256=binding_sha256,
                name="source_rows",
                table="source_rows",
                keys=("ticker", "ordinal"),
            )
        checkpoint = json.loads(checkpoint_path.read_text())
        sealed_run = run_directory / checkpoint["pending_write"]["planned"][0]["name"]
        assert failed and checkpoint["pending_write"]["completed"] == []
        assert checkpoint["next_rowid"] == 0 and sealed_run.is_file()

        monkeypatch.setattr(historical_export, "_record_frontier_write", original_record)
        original_remove_artifact = historical_export._remove_frontier_artifact
        recovered_paths = []

        def record_recovery(store_arg, path):
            if path == sealed_run:
                recovered_paths.append(path)
            return original_remove_artifact(store_arg, path)

        monkeypatch.setattr(historical_export, "_remove_frontier_artifact", record_recovery)
        result = historical_export.export_relation(
            store,
            connection,
            work_relative=work_relative,
            binding_sha256=binding_sha256,
            name="source_rows",
            table="source_rows",
            keys=("ticker", "ordinal"),
        )
        assert result["row_count"] == len(rows)
        assert recovered_paths == [sealed_run]
        final_checkpoint = json.loads(checkpoint_path.read_text())
        assert any(
            descriptor["name"] == sealed_run.name
            for level in final_checkpoint["levels"]
            for descriptor in level
        )
        output = pq.read_table(
            store.path(f"{work_relative}/output/source_rows.parquet"), columns=["ordinal"]
        )
        assert output["ordinal"].to_pylist() == list(range(len(rows)))
    finally:
        connection.close()


def test_foreign_future_run_id_is_rejected_without_deletion(tmp_path, monkeypatch):
    store, connection, work_relative, _ = _export(
        tmp_path, monkeypatch, [_row(index) for index in range(4)]
    )
    try:
        run_directory = store.path(f"{work_relative}/runs/source_rows")
        foreign = run_directory / "run-000000000999.parquet"
        foreign.write_bytes(b"foreign run")
        checkpoint = json.loads(
            store.path(f"{work_relative}/checkpoints/source_rows.json").read_text()
        )
        assert checkpoint["next_run_id"] < 999
        with pytest.raises(CatalogueError, match="unowned|foreign|unreferenced"):
            historical_export.prepare(
                store,
                work_relative,
                binding_sha256=checkpoint["binding_sha256"],
                tables={"source_rows": "source_rows"},
            )
        assert foreign.read_bytes() == b"foreign run"
    finally:
        connection.close()


@pytest.mark.parametrize("tamper", ["owner", "run", "symlink", "foreign_file"])
def test_frontier_rejects_tampered_or_unsafe_owned_inventory(tmp_path, monkeypatch, tamper):
    store, connection, work_relative, _ = _export(
        tmp_path, monkeypatch, [_row(index) for index in range(5)]
    )
    try:
        checkpoint_path = store.path(f"{work_relative}/checkpoints/source_rows.json")
        run_directory = store.path(f"{work_relative}/runs/source_rows")
        checkpoint = json.loads(checkpoint_path.read_text())
        if tamper == "owner":
            owner_path = run_directory / "owner.json"
            owner = json.loads(owner_path.read_text())
            owner["binding_sha256"] = "f" * 64
            owner_path.write_text(json.dumps(owner))
        elif tamper == "run":
            descriptor = checkpoint["levels"][0][0]
            with (run_directory / descriptor["name"]).open("ab") as stream:
                stream.write(b"tampered")
        elif tamper == "symlink":
            descriptor = checkpoint["levels"][0][0]
            run_path = run_directory / descriptor["name"]
            run_path.unlink()
            target = tmp_path / "foreign.parquet"
            target.write_bytes(b"foreign")
            run_path.symlink_to(target)
        else:
            (run_directory / "foreign.bin").write_bytes(b"foreign")

        with pytest.raises(CatalogueError):
            historical_export.prepare(
                store,
                work_relative,
                binding_sha256=checkpoint["binding_sha256"],
                tables={"source_rows": "source_rows"},
            )
    finally:
        connection.close()


@pytest.mark.parametrize("failure_point", ["before_intent", "after_intent"])
def test_uncommitted_batch_recovers_at_its_source_cursor(tmp_path, monkeypatch, failure_point):
    rows = [_row(index) for index in range(3)]
    store = Store(tmp_path / "root")
    connection = _source(rows)
    binding_sha256, work_relative = historical_export.binding(
        "a" * 32, "b" * 64, {}, {}, {}, {"source_rows": "ticker, ordinal"}
    )
    historical_export.prepare(
        store,
        work_relative,
        binding_sha256=binding_sha256,
        tables={"source_rows": "source_rows"},
    )
    monkeypatch.setattr(historical_export, "_BATCH_ROWS", 1)
    checkpoint_path = store.path(f"{work_relative}/checkpoints/source_rows.json")
    run_directory = store.path(f"{work_relative}/runs/source_rows")
    target = "_begin_frontier_write" if failure_point == "before_intent" else "_write_frontier_run"
    original = getattr(historical_export, target)

    def fail_once(*args, **kwargs):
        raise RuntimeError(f"simulated stop {failure_point}")

    monkeypatch.setattr(historical_export, target, fail_once)
    try:
        with pytest.raises(RuntimeError, match=failure_point):
            historical_export.export_relation(
                store,
                connection,
                work_relative=work_relative,
                binding_sha256=binding_sha256,
                name="source_rows",
                table="source_rows",
                keys=("ticker", "ordinal"),
            )
        checkpoint = json.loads(checkpoint_path.read_text())
        assert checkpoint["next_rowid"] == 0
        if failure_point == "before_intent":
            assert checkpoint["pending_write"] is None
        else:
            assert checkpoint["pending_write"]["cursor_start"] == 0
            assert checkpoint["pending_write"]["cursor_end"] == 1
        assert {path.name for path in run_directory.iterdir()} == {"owner.json"}

        monkeypatch.setattr(historical_export, target, original)
        result = historical_export.export_relation(
            store,
            connection,
            work_relative=work_relative,
            binding_sha256=binding_sha256,
            name="source_rows",
            table="source_rows",
            keys=("ticker", "ordinal"),
        )
        assert result["row_count"] == len(rows)
        output = pq.read_table(
            store.path(f"{work_relative}/output/source_rows.parquet"), columns=["ordinal"]
        )
        assert output["ordinal"].to_pylist() == list(range(len(rows)))
    finally:
        connection.close()


def test_retry_finishes_committed_delete_list_after_partial_unlink(tmp_path, monkeypatch):
    rows = [_row(index) for index in range(8)]
    store = Store(tmp_path / "root")
    connection = _source(rows)
    binding_sha256, work_relative = historical_export.binding(
        "a" * 32, "b" * 64, {}, {}, {}, {"source_rows": "ticker, ordinal"}
    )
    historical_export.prepare(
        store,
        work_relative,
        binding_sha256=binding_sha256,
        tables={"source_rows": "source_rows"},
    )
    monkeypatch.setattr(historical_export, "_BATCH_ROWS", 1)
    run_directory = store.path(f"{work_relative}/runs/source_rows")
    original_remove = historical_export._remove_owned_file
    removed_one = False
    failed = False

    def remove_one_then_fail(store_arg, path):
        nonlocal removed_one, failed
        if path.parent == run_directory and historical_export._FRONTIER_RUN.fullmatch(path.name):
            if not removed_one:
                removed_one = True
                return original_remove(store_arg, path)
            if not failed:
                failed = True
                raise RuntimeError("simulated stop during frontier deletion")
        return original_remove(store_arg, path)

    monkeypatch.setattr(historical_export, "_remove_owned_file", remove_one_then_fail)
    try:
        with pytest.raises(RuntimeError, match="during frontier deletion"):
            historical_export.export_relation(
                store,
                connection,
                work_relative=work_relative,
                binding_sha256=binding_sha256,
                name="source_rows",
                table="source_rows",
                keys=("ticker", "ordinal"),
            )
        checkpoint_path = store.path(f"{work_relative}/checkpoints/source_rows.json")
        checkpoint = json.loads(checkpoint_path.read_text())
        assert failed and checkpoint["next_rowid"] == 8
        assert checkpoint["pending_delete"]
        deleted_name = checkpoint["pending_delete"][0]["name"]
        assert not (run_directory / deleted_name).exists()
        monkeypatch.setattr(historical_export, "_remove_owned_file", original_remove)
        result = historical_export.export_relation(
            store,
            connection,
            work_relative=work_relative,
            binding_sha256=binding_sha256,
            name="source_rows",
            table="source_rows",
            keys=("ticker", "ordinal"),
        )
        assert result["row_count"] == 8
        assert not result["pending_delete"]
        active = {item["name"] for level in result["levels"] for item in level}
        assert active == {
            path.name for path in run_directory.iterdir() if path.name != "owner.json"
        }
    finally:
        connection.close()


def test_frontier_rejects_wrong_binding_and_tampered_run(tmp_path, monkeypatch):
    store, connection, work_relative, _ = _export(
        tmp_path, monkeypatch, [_row(index) for index in range(4)]
    )
    run_directory = store.path(f"{work_relative}/runs/source_rows")
    run_path = next(path for path in run_directory.iterdir() if path.name.startswith("run-"))
    try:
        with pytest.raises(CatalogueError, match="owner binding changed"):
            historical_export.export_relation(
                store,
                connection,
                work_relative=work_relative,
                binding_sha256="d" * 64,
                name="source_rows",
                table="source_rows",
                keys=("ticker", "ordinal"),
            )
        run_path.write_bytes(run_path.read_bytes() + b"tamper")
        with pytest.raises(CatalogueError, match="frontier run changed"):
            historical_export.export_relation(
                store,
                connection,
                work_relative=work_relative,
                binding_sha256=json.loads(
                    store.path(f"{work_relative}/checkpoints/source_rows.json").read_text()
                )["binding_sha256"],
                name="source_rows",
                table="source_rows",
                keys=("ticker", "ordinal"),
            )
    finally:
        connection.close()


@pytest.mark.parametrize("unowned", ["foreign", "symlink", "hardlink"])
def test_prepare_rejects_foreign_or_unsafe_frontier_inventory(tmp_path, monkeypatch, unowned):
    store, connection, work_relative, _ = _export(
        tmp_path, monkeypatch, [_row(index) for index in range(2)]
    )
    try:
        run_directory = store.path(f"{work_relative}/runs/source_rows")
        target = tmp_path / "outside.bin"
        target.write_bytes(b"foreign")
        if unowned == "foreign":
            (run_directory / "foreign.bin").write_bytes(b"foreign")
        else:
            entry = run_directory / "run-999999999999.parquet"
            if unowned == "symlink":
                entry.symlink_to(target)
            else:
                os.link(target, entry)
        checkpoint = json.loads(
            store.path(f"{work_relative}/checkpoints/source_rows.json").read_text()
        )
        with pytest.raises(CatalogueError):
            historical_export.prepare(
                store,
                work_relative,
                binding_sha256=checkpoint["binding_sha256"],
                tables={"source_rows": "source_rows"},
            )
    finally:
        connection.close()


def test_frontier_run_reservation_obeys_lower_storage_limit(tmp_path):
    store = Store(
        tmp_path / "root",
        limits=Limits(max_root_bytes=1024**2, max_temp_bytes=1024**2),
    )
    run_directory = store.path("tmp/frontier")
    run_directory.mkdir(parents=True)
    import pyarrow as pa

    schema = pa.schema([("ticker", pa.string()), ("ordinal", pa.int64())])
    batch = pa.RecordBatch.from_arrays(
        [pa.array(["key"], type=pa.string()), pa.array([1], type=pa.int64())],
        schema=schema,
    )
    with pytest.raises(BudgetExceeded, match="storage exhausted"):
        historical_export._write_frontier_run(store, run_directory, 0, 0, batch, schema, (0, 1))
    assert not list(run_directory.iterdir())
