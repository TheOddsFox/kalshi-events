from __future__ import annotations

from decimal import Decimal

import duckdb
import pytest

from kalshi_events import metrics, normalize, warehouse
from kalshi_events.common import BudgetExceeded, CatalogueError


def _page(page_id, generation, native):
    return (
        {
            "capture_id": "0" * 32,
            "page_id": page_id,
            "received_at": f"2026-10-28T12:0{generation}:00Z",
            "endpoint": "/series",
            "sha256": f"{generation + 1:x}" * 64,
            "status": 200,
            "entity_kind": "series",
            "task_scope": "series",
            "task_generation": generation,
        },
        {"series": [native]},
    )


def _state():
    return {
        "capture_id": "0" * 32,
        "tasks": [{}, {}],
        "task_scopes": {"0": "series", "1": "series"},
    }


def _directory(store, name):
    directory = store.path(f"tmp/{name}")
    directory.mkdir(parents=True)
    return directory


def _landing_rows(path):
    with duckdb.connect(str(path), read_only=True) as connection:
        columns = connection.execute("describe landing.observations").fetchall()
        names = [column[0] for column in columns if column[0] in warehouse._LANDING_COLUMNS]
        rows = connection.execute(
            "select "
            + ", ".join(warehouse._LANDING_COLUMNS)
            + " from landing.observations order by observation_id"
        ).fetchall()
        return names, columns, rows


def test_native_landing_progress_uses_supported_counters(store, monkeypatch):
    import json

    page = _page("page-progress", 0, {"ticker": "SERIES-1"})
    monkeypatch.setattr(warehouse, "capture_pages", lambda *_args, **_kwargs: iter([page]))
    directory = _directory(store, "historical-progress")
    progress_path = directory / "progress.json"
    with metrics.collect(progress_path) as measured:
        assert (
            warehouse._land_historical_rows(
                store,
                _state(),
                directory,
                verified_pages={},
                parent_index=normalize.ParentIndex(),
                task_indices={0},
            )
            == 1
        )
    counters = json.loads(progress_path.read_text())["counters"]
    assert counters["receipts_verified"] == 1
    assert counters["observations_emitted"] == 1
    assert counters["batches_yielded"] == 1
    assert measured.report()["counters"].get("progress_write_failures", 0) == 0


def test_native_historical_landing_matches_legacy_rows_and_keeps_generations(store, monkeypatch):
    pages = [
        _page(
            "page-a",
            0,
            {
                "ticker": "SERIES-1",
                "title": "older generation",
                "updated_time": Decimal("1.2300"),
                "price": "0.000000000000000001",
            },
        ),
        _page(
            "page-b",
            1,
            {
                "ticker": "SERIES-1",
                "title": "final generation 🌉",
                "updated_time": {"at": "noon"},
                "price": "0.000000000000000002",
            },
        ),
    ]
    monkeypatch.setattr(warehouse, "capture_pages", lambda *_args, **_kwargs: iter(pages))

    legacy_directory = _directory(store, "historical-legacy")
    native_directory = _directory(store, "historical-native")
    assert warehouse._land_rows(store, _state(), legacy_directory) == 2
    index = normalize.ParentIndex()
    assert (
        warehouse._land_historical_rows(
            store,
            _state(),
            native_directory,
            verified_pages={},
            parent_index=index,
            task_indices={0, 1},
        )
        == 2
    )

    legacy_names, legacy_schema, legacy_rows = _landing_rows(legacy_directory / "warehouse.duckdb")
    native_names, native_schema, native_rows = _landing_rows(native_directory / "warehouse.duckdb")
    assert native_names == legacy_names == list(warehouse._LANDING_COLUMNS)
    assert [row[:3] for row in native_schema if row[0] in warehouse._LANDING_COLUMNS] == [
        row[:3] for row in legacy_schema if row[0] in warehouse._LANDING_COLUMNS
    ]
    assert native_rows == legacy_rows
    assert len(native_rows) == 2
    assert {row[12] for row in native_rows} == {"0", "1"}
    assert {row[5] for row in native_rows} == {"1.2300", None}
    assert all("0.00000000000000000" in row[8] for row in native_rows)
    assert index.scope_membership["series"] == {0: {"SERIES-1"}, 1: {"SERIES-1"}}


def test_native_landing_verifies_selected_receipts_and_skips_identical_page_references(
    store, monkeypatch
):
    page = _page("page-a", 0, {"ticker": "SERIES-1"})
    reads = []

    def capture_pages(_store, _state, *, evidence, task_indices):
        assert task_indices == {0}
        for _ in range(2):
            reads.append(page[0]["page_id"])
            if evidence is not None:
                relative = "raw/page-a/manifest.json"
                evidence[relative] = "a" * 64
                evidence["raw/page-a/body.bin"] = "b" * 64
            yield page

    monkeypatch.setattr(warehouse, "capture_pages", capture_pages)
    directory = _directory(store, "historical-deduplicated")
    verified_pages = {}
    index = normalize.ParentIndex()

    assert (
        warehouse._land_historical_rows(
            store,
            _state(),
            directory,
            verified_pages=verified_pages,
            parent_index=index,
            task_indices={0},
        )
        == 1
    )
    names, _schema, rows = _landing_rows(directory / "warehouse.duckdb")
    assert names == list(warehouse._LANDING_COLUMNS)
    assert len(reads) == 2
    assert len(rows) == 1
    assert verified_pages == {
        "raw/page-a/manifest.json": "a" * 64,
        "raw/page-a/body.bin": "b" * 64,
    }
    assert index.scope_membership["series"] == {0: {"SERIES-1"}}


def test_native_landing_rejects_contradictory_context_for_page_id(store, monkeypatch):
    first = _page("page-a", 0, {"ticker": "SERIES-1"})
    second = (_page("page-a", 0, {"ticker": "SERIES-1"})[0] | {"received_at": "changed"}, first[1])
    monkeypatch.setattr(warehouse, "capture_pages", lambda *_args, **_kwargs: iter((first, second)))
    directory = _directory(store, "historical-contradictory")

    with pytest.raises(CatalogueError, match="contradictory context"):
        warehouse._land_historical_rows(
            store,
            _state(),
            directory,
            verified_pages={},
            parent_index=normalize.ParentIndex(),
            task_indices={0},
        )


def test_native_landing_creates_empty_table_and_accepts_oversized_singleton(store, monkeypatch):
    monkeypatch.setattr(warehouse, "capture_pages", lambda *_args, **_kwargs: iter(()))
    empty_directory = _directory(store, "historical-empty")
    assert (
        warehouse._land_historical_rows(
            store,
            _state(),
            empty_directory,
            verified_pages={},
            parent_index=normalize.ParentIndex(),
            task_indices={0},
        )
        == 0
    )
    names, _schema, rows = _landing_rows(empty_directory / "warehouse.duckdb")
    assert names == list(warehouse._LANDING_COLUMNS)
    assert rows == []

    page = _page(
        "page-wide",
        0,
        {"ticker": "SERIES-WIDE", "rules": "x" * (warehouse._LANDING_BATCH_BYTES + 1024)},
    )
    monkeypatch.setattr(warehouse, "capture_pages", lambda *_args, **_kwargs: iter((page,)))
    wide_directory = _directory(store, "historical-wide")
    assert (
        warehouse._land_historical_rows(
            store,
            _state(),
            wide_directory,
            verified_pages={},
            parent_index=normalize.ParentIndex(),
            task_indices={0},
        )
        == 1
    )
    _names, _schema, rows = _landing_rows(wide_directory / "warehouse.duckdb")
    assert len(rows) == 1
    assert "x" * (warehouse._LANDING_BATCH_BYTES + 1024) in rows[0][8]


def test_native_landing_refuses_existing_database_or_wal(store):
    for name, filename in (
        ("historical-existing-db", "warehouse.duckdb"),
        ("historical-existing-wal", "warehouse.duckdb.wal"),
    ):
        directory = _directory(store, name)
        (directory / filename).write_bytes(b"occupied")
        with pytest.raises(CatalogueError, match="fresh database"):
            warehouse._land_historical_rows(
                store,
                _state(),
                directory,
                verified_pages={},
                parent_index=normalize.ParentIndex(),
                task_indices={0},
            )


def test_native_landing_checks_storage_after_insert_batches(store, monkeypatch):
    page = _page("page-a", 0, {"ticker": "SERIES-1"})
    monkeypatch.setattr(warehouse, "capture_pages", lambda *_args, **_kwargs: iter((page,)))
    original_check_storage = store.check_storage
    checks = []

    def fail_after_first_insert(additional=0):
        checks.append(additional)
        if len(checks) == 3:
            raise BudgetExceeded("synthetic storage limit")
        return original_check_storage(additional)

    monkeypatch.setattr(store, "check_storage", fail_after_first_insert)
    directory = _directory(store, "historical-storage-limit")

    with pytest.raises(BudgetExceeded, match="synthetic storage limit"):
        warehouse._land_historical_rows(
            store,
            _state(),
            directory,
            verified_pages={},
            parent_index=normalize.ParentIndex(),
            task_indices={0},
        )
    assert len(checks) == 3


def test_native_landing_stops_when_deadline_expires_during_normalization(store, monkeypatch):
    page = _page("page-a", 0, {"ticker": "SERIES-1"})
    monkeypatch.setattr(warehouse, "capture_pages", lambda *_args, **_kwargs: iter((page,)))
    original_check_deadline = store.check_deadline
    checks = []

    def expire_during_observations():
        checks.append(None)
        if len(checks) == 3:
            raise BudgetExceeded("synthetic deadline")
        original_check_deadline()

    monkeypatch.setattr(store, "check_deadline", expire_during_observations)
    directory = _directory(store, "historical-deadline")

    with pytest.raises(BudgetExceeded, match="synthetic deadline"):
        warehouse._land_historical_rows(
            store,
            _state(),
            directory,
            verified_pages={},
            parent_index=normalize.ParentIndex(),
            task_indices={0},
        )
    assert len(checks) == 3
