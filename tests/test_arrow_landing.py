from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal

import dlt
import duckdb
import pytest
from dlt.common.schema import Schema

from kalshi_events import normalize, warehouse


def _row(**updates):
    row = {name: "" for name in warehouse._LANDING_COLUMNS}
    row.update(
        {
            **{
                "observation_id": "observation-0",
                "capture_id": "0" * 32,
                "entity_kind": "series",
                "entity_ticker": "SERIES-0",
                "received_at": "2026-10-18T00:00:00Z",
                "provider_updated_at": None,
                "payload_json": "{}",
                "event_ticker": None,
                "series_ticker": None,
                "task_scope": None,
            },
            **updates,
        }
    )
    return row


def _duckdb_settings(connection):
    return connection.execute(
        """select current_setting('memory_limit'), current_setting('threads'),
                  current_setting('temp_directory'), current_setting('max_temp_directory_size')"""
    ).fetchone()


def test_landing_arrow_batches_bound_rows_and_utf8_bytes():
    rows = (_row(observation_id=str(index)) for index in range(4097))
    batches = list(warehouse._landing_arrow_batches(rows))
    assert [batch.num_rows for batch in batches] == [4096, 1]
    assert all(batch.schema == warehouse._LANDING_SCHEMA for batch in batches)

    batches = list(
        warehouse._landing_arrow_batches(
            [
                _row(payload_json="界" * (warehouse._LANDING_BATCH_BYTES // 3)),
                _row(payload_json="😀"),
            ]
        )
    )
    assert [batch.num_rows for batch in batches] == [1, 1]

    oversized = _row(payload_json="😀" * (warehouse._LANDING_BATCH_BYTES // 4 + 1))
    batches = list(warehouse._landing_arrow_batches([oversized]))
    assert [batch.num_rows for batch in batches] == [1]


def test_arrow_merge_replays_dict_landed_database_with_text_coercion(store, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    capture_id = "0" * 32
    directory = store.path("tmp/arrow-landing")
    directory.mkdir(parents=True)
    database = directory / "warehouse.duckdb"
    pipeline_args = {
        "pipeline_name": "kalshi_catalogue",
        "pipelines_dir": str(directory / "landing"),
        "destination": dlt.destinations.duckdb(str(database)),
        "dataset_name": "landing",
    }
    logical_columns = {
        name: {
            "data_type": "text",
            "nullable": name
            in {"provider_updated_at", "event_ticker", "series_ticker", "task_scope"},
        }
        for name in warehouse._LANDING_COLUMNS
    }
    seed = _row(provider_updated_at="2026-10-18T00:00:00Z")
    dlt.pipeline(**pipeline_args).run(
        dlt.resource(
            [seed],
            name="observations",
            primary_key="observation_id",
            columns=logical_columns,
            write_disposition="merge",
        ),
        loader_file_format="parquet",
    )

    values = [
        "2026-10-18T00:00:00Z",
        None,
        42,
        Decimal("1.230"),
        True,
        {"at": "noon"},
        ["later", 2],
        dt.datetime(2026, 10, 18, 12, 30, tzinfo=dt.UTC),
    ]
    source_rows = [
        {
            "entity_kind": "series",
            "series_ticker": f"SERIES-{index}",
            "observation_id": f"observation-{index}",
            "received_at": "2026-10-18T00:00:00Z",
            "provider_updated_at": value,
            "source_endpoint": "/series",
            "source_kind": "direct",
            "payload_json": json.dumps(
                {"updated_time": value if isinstance(value, (dict, list)) else str(value)}
            ),
        }
        for index, value in enumerate(values)
    ]
    legacy_rows = [
        {
            "observation_id": row["observation_id"],
            "capture_id": capture_id,
            "entity_kind": "series",
            "entity_ticker": row["series_ticker"],
            "received_at": row["received_at"],
            "provider_updated_at": row["provider_updated_at"],
            "source_endpoint": row["source_endpoint"],
            "source_kind": row["source_kind"],
            "payload_json": row["payload_json"],
            "event_ticker": None,
            "series_ticker": None,
            "task_scope": None,
            "task_generation": "",
        }
        for row in source_rows
    ]
    legacy_database = directory / "legacy.duckdb"
    dlt.pipeline(
        pipeline_name="legacy_catalogue",
        pipelines_dir=str(directory / "legacy-landing"),
        destination=dlt.destinations.duckdb(str(legacy_database)),
        dataset_name="landing",
    ).run(
        dlt.resource(
            legacy_rows,
            name="observations",
            primary_key="observation_id",
            columns=logical_columns,
            write_disposition="merge",
        ),
        loader_file_format="parquet",
    )
    baseline_connection = duckdb.connect(str(database))
    baseline_settings = _duckdb_settings(baseline_connection)
    baseline_connection.close()
    store.sync("tmp/arrow-landing")
    pages = [
        (
            {
                "capture_id": capture_id,
                "page_id": f"page-{index}",
                "received_at": "2026-10-18T00:00:00Z",
                "endpoint": "/series",
                "sha256": "f" * 64,
                "status": 200,
                "entity_kind": "series",
                "task_scope": "series",
                "task_generation": 0,
            },
            {"series": [{"ticker": f"SERIES-PAGE-{index}"}]},
        )
        for index in range(2)
    ]
    monkeypatch.setattr(warehouse, "capture_pages", lambda *_args, **_kwargs: iter(pages))

    def page_observations(page_rows):
        manifest, _payload = next(iter(page_rows))
        return iter(source_rows if manifest["page_id"] == "page-0" else ())

    monkeypatch.setattr(warehouse, "observations", page_observations)
    from dlt.common.configuration.container import Container
    from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
    from dlt.common.storages.data_item_storage import BufferedDataWriter

    original_init = BufferedDataWriter.__init__
    writer_limits = []

    def record_writer_limit(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        writer_limits.append((self.buffer_max_items, Container()[ConfigSectionContext].sections))

    monkeypatch.setattr(BufferedDataWriter, "__init__", record_writer_limit)

    from dlt.destinations.impl.duckdb.configuration import DuckDbConnectionPool

    original_factory = dlt.destinations.duckdb
    destination_configs = []

    def record_destination(**kwargs):
        usage = store.usage()
        spill_budget = min(
            store.limits.max_temp_bytes - usage["temporary_bytes"],
            store.limits.max_root_bytes - usage["retained_bytes"],
        )
        destination_configs.append((kwargs["credentials"]["global_config"], spill_budget))
        return original_factory(**kwargs)

    monkeypatch.setattr(warehouse.dlt.destinations, "duckdb", record_destination)
    original_borrow = DuckDbConnectionPool.borrow_conn
    worker_settings = []

    def record_worker_settings(pool, *args, **kwargs):
        connection = original_borrow(pool, *args, **kwargs)
        worker_settings.append((dict(pool.credentials.global_config), _duckdb_settings(connection)))
        return connection

    monkeypatch.setattr(DuckDbConnectionPool, "borrow_conn", record_worker_settings)
    monkeypatch.setattr(warehouse, "_LANDING_BATCH_ROWS", 1)

    parent_index = normalize.DiskParentIndex(store, collect_winners=False)
    try:
        with dlt.config.values({"load.workers": 4}):
            assert warehouse._land_rows(
                store,
                {"capture_id": capture_id},
                directory,
                parent_index=parent_index,
            ) == len(values)
            assert warehouse._land_rows(
                store,
                {"capture_id": capture_id},
                directory,
                parent_index=parent_index,
            ) == len(values)
        assert parent_index.db.execute("select count(*) from membership").fetchone() == (2,)
    finally:
        parent_index.close()

    assert len(writer_limits) >= 4
    assert all(limit == 1 for limit, _sections in writer_limits), writer_limits
    assert len(destination_configs) == 2
    for config, spill_budget in destination_configs:
        assert spill_budget > 0
        assert config == {
            "memory_limit": warehouse.DBT_MEMORY_LIMIT,
            "threads": str(warehouse.DBT_THREADS),
            "temp_directory": str(directory / "duckdb-spill"),
            "max_temp_directory_size": f"{spill_budget}B",
        }
    assert len(worker_settings) >= 2
    for config, actual_settings in worker_settings:
        expected_connection = duckdb.connect(config=config)
        try:
            assert actual_settings == _duckdb_settings(expected_connection)
        finally:
            expected_connection.close()
    reopened = duckdb.connect(str(database))
    assert _duckdb_settings(reopened) == baseline_settings
    reopened.close()
    with duckdb.connect(str(database), read_only=True) as connection:
        arrow_schema = {
            row[0]: (row[1], row[2])
            for row in connection.execute("describe landing.observations").fetchall()
            if row[0] in warehouse._LANDING_COLUMNS
        }
        assert connection.execute("select count(*) from landing.observations").fetchone() == (
            len(values),
        )
        actual = connection.execute(
            "select provider_updated_at, payload_json from landing.observations order by observation_id"
        ).fetchall()
    with duckdb.connect(str(legacy_database), read_only=True) as connection:
        legacy_schema = {
            row[0]: (row[1], row[2])
            for row in connection.execute("describe landing.observations").fetchall()
            if row[0] in warehouse._LANDING_COLUMNS
        }
        legacy = connection.execute(
            "select provider_updated_at, payload_json from landing.observations order by observation_id"
        ).fetchall()
    assert set(arrow_schema) == set(warehouse._LANDING_COLUMNS)
    assert arrow_schema == legacy_schema
    assert actual == legacy
    assert actual[5][0] is None
    assert actual[6][0] is None
    assert json.loads(actual[5][1]) == {"updated_time": {"at": "noon"}}
    assert json.loads(actual[6][1]) == {"updated_time": ["later", 2]}


def test_arrow_pipeline_failure_restores_scoped_dlt_config(store, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    directory = store.path("tmp/arrow-failure")
    directory.mkdir(parents=True)
    manifest = {
        "capture_id": "0" * 32,
        "page_id": "page-0",
        "received_at": "2026-10-18T00:00:00Z",
        "endpoint": "/series",
        "sha256": "f" * 64,
        "status": 200,
    }
    monkeypatch.setattr(warehouse, "capture_pages", lambda *_args, **_kwargs: [(manifest, {})])
    monkeypatch.setattr(
        warehouse,
        "observations",
        lambda _pages: iter(
            [
                {
                    "entity_kind": "series",
                    "series_ticker": "SERIES-0",
                    "observation_id": "observation-0",
                    "received_at": "2026-10-18T00:00:00Z",
                    "provider_updated_at": None,
                    "source_endpoint": "/series",
                    "source_kind": "direct",
                    "payload_json": "{}",
                }
            ]
        ),
    )
    original_pipeline = dlt.pipeline
    config_key = (
        "kalshi_catalogue.sources.kalshi_catalogue.kalshi_catalogue.data_writer.buffer_max_items"
    )
    initial = dlt.config.get(config_key)

    def fail(**_kwargs):
        assert dlt.config.get(config_key) == 1
        assert dlt.config.get("kalshi_catalogue.normalize.data_writer.buffer_max_items") == 1
        raise RuntimeError("synthetic pipeline failure")

    monkeypatch.setattr(warehouse.dlt, "pipeline", fail)
    with pytest.raises(RuntimeError, match="synthetic"):
        warehouse._land_rows(store, {"capture_id": "0" * 32}, directory)
    assert dlt.config.get(config_key) == initial
    assert dlt.config.get("kalshi_catalogue.normalize.data_writer.buffer_max_items") is None
    assert not list(tmp_path.glob(".dlt/*.toml"))
    monkeypatch.setattr(warehouse.dlt, "pipeline", original_pipeline)


def test_duckdb_global_config_failure_closes_pool_and_restores_settings(store):
    directory = store.path("tmp/dlt-config-failure")
    directory.mkdir(parents=True)
    database = directory / "warehouse.duckdb"
    connection = duckdb.connect(str(database))
    baseline_settings = _duckdb_settings(connection)
    connection.close()

    destination = dlt.destinations.duckdb(
        credentials={
            "database": str(database),
            "global_config": {"memory_limit": "80MB", "not_a_duckdb_setting": "1"},
        }
    )
    initial = destination.spec()
    initial.dataset_name = "failure"
    initial.local_dir = str(directory)
    client = destination.client(Schema("failure"), initial)

    with pytest.raises(Exception, match="not_a_duckdb_setting"):
        client.sql_client.open_connection()
    assert client.config.credentials.conn_pool._conn is None

    reopened = duckdb.connect(str(database))
    assert _duckdb_settings(reopened) == baseline_settings
    reopened.close()


def test_empty_arrow_landing_keeps_legacy_empty_table(store, monkeypatch):
    directory = store.path("tmp/arrow-empty")
    directory.mkdir(parents=True)
    usage = store.usage()
    spill_budget = min(
        store.limits.max_temp_bytes - usage["temporary_bytes"],
        store.limits.max_root_bytes - usage["retained_bytes"],
    )
    manifest = {
        "capture_id": "0" * 32,
        "page_id": "page-0",
        "received_at": "2026-10-18T00:00:00Z",
        "endpoint": "/series",
        "sha256": "f" * 64,
        "status": 200,
    }
    monkeypatch.setattr(warehouse, "capture_pages", lambda *_args, **_kwargs: [(manifest, {})])
    monkeypatch.setattr(warehouse, "observations", lambda _pages: iter(()))
    monkeypatch.setattr(warehouse.dlt, "pipeline", lambda **_kwargs: pytest.fail("empty landing"))
    original_connect = warehouse.duckdb.connect
    observed = []

    def record_connect(*args, **kwargs):
        connection = original_connect(*args, **kwargs)
        if not kwargs.get("read_only"):
            observed.append((kwargs["config"], _duckdb_settings(connection)))
        return connection

    monkeypatch.setattr(warehouse.duckdb, "connect", record_connect)

    assert warehouse._land_rows(store, {"capture_id": "0" * 32}, directory) == 0
    assert len(observed) == 1
    config, actual_settings = observed[0]
    assert config == {
        "memory_limit": warehouse.DBT_MEMORY_LIMIT,
        "threads": str(warehouse.DBT_THREADS),
        "temp_directory": str(directory / "duckdb-spill"),
        "max_temp_directory_size": f"{spill_budget}B",
    }
    expected_connection = original_connect(config=config)
    assert actual_settings == _duckdb_settings(expected_connection)
    expected_connection.close()
    with duckdb.connect(str(directory / "warehouse.duckdb"), read_only=True) as connection:
        schema = connection.execute("describe landing.observations").fetchall()
        assert [row[0] for row in schema] == list(warehouse._LANDING_COLUMNS)
        assert connection.execute("select count(*) from landing.observations").fetchone() == (0,)


def test_falsey_optional_series_ticker_matches_dictionary_landing(store, monkeypatch):
    directory = store.path("tmp/arrow-falsey-series")
    directory.mkdir(parents=True)
    capture_id = "0" * 32
    optional_series_tickers = [0, False, {}, [], "", None]
    source_rows = [
        {
            "entity_kind": "events",
            "event_ticker": f"EVENT-{index}",
            "series_ticker": value,
            "observation_id": f"observation-{index}",
            "received_at": "2026-10-18T00:00:00Z",
            "provider_updated_at": None,
            "source_endpoint": "/events",
            "source_kind": "direct",
            "payload_json": json.dumps({"series_ticker": value}),
        }
        for index, value in enumerate(optional_series_tickers)
    ]
    logical_columns = {
        name: {
            "data_type": "text",
            "nullable": name
            in {"provider_updated_at", "event_ticker", "series_ticker", "task_scope"},
        }
        for name in warehouse._LANDING_COLUMNS
    }
    legacy_rows = [
        {
            "observation_id": row["observation_id"],
            "capture_id": capture_id,
            "entity_kind": "events",
            "entity_ticker": row["event_ticker"],
            "received_at": row["received_at"],
            "provider_updated_at": row["provider_updated_at"],
            "source_endpoint": row["source_endpoint"],
            "source_kind": row["source_kind"],
            "payload_json": row["payload_json"],
            "event_ticker": row["event_ticker"],
            "series_ticker": row["series_ticker"],
            "task_scope": None,
            "task_generation": "",
        }
        for row in source_rows
    ]
    legacy_database = directory / "legacy.duckdb"
    dlt.pipeline(
        pipeline_name="legacy_falsey_catalogue",
        pipelines_dir=str(directory / "legacy-landing"),
        destination=dlt.destinations.duckdb(str(legacy_database)),
        dataset_name="landing",
    ).run(
        dlt.resource(
            legacy_rows,
            name="observations",
            primary_key="observation_id",
            columns=logical_columns,
            write_disposition="merge",
        ),
        loader_file_format="parquet",
    )
    manifest = {
        "capture_id": capture_id,
        "page_id": "page-falsey",
        "received_at": "2026-10-18T00:00:00Z",
        "endpoint": "/events",
        "sha256": "f" * 64,
        "status": 200,
    }
    monkeypatch.setattr(warehouse, "capture_pages", lambda *_args, **_kwargs: [(manifest, {})])
    monkeypatch.setattr(warehouse, "observations", lambda _pages: iter(source_rows))
    assert warehouse._land_rows(store, {"capture_id": capture_id}, directory) == len(source_rows)

    with duckdb.connect(str(directory / "warehouse.duckdb"), read_only=True) as connection:
        arrow_values = connection.execute(
            "select series_ticker, payload_json from landing.observations order by observation_id"
        ).fetchall()
    with duckdb.connect(str(legacy_database), read_only=True) as connection:
        legacy_values = connection.execute(
            "select series_ticker, payload_json from landing.observations order by observation_id"
        ).fetchall()
    assert arrow_values == legacy_values
    assert [row[0] for row in arrow_values] == ["0", "False", None, None, "", None]
    assert json.loads(arrow_values[2][1]) == {"series_ticker": {}}
    assert json.loads(arrow_values[3][1]) == {"series_ticker": []}


def test_duplicate_ids_across_arrow_batches_match_legacy_and_replay(store, monkeypatch):
    directory = store.path("tmp/arrow-duplicate-boundary")
    directory.mkdir(parents=True)
    capture_id = "0" * 32
    source_rows = [
        {
            "entity_kind": "series",
            "series_ticker": f"SERIES-{index}",
            "observation_id": (
                "observation-boundary" if index in {4095, 4096} else f"observation-{index}"
            ),
            "received_at": "2026-10-18T00:00:00Z",
            "provider_updated_at": None,
            "source_endpoint": "/series",
            "source_kind": "direct",
            "payload_json": json.dumps({"position": index}),
        }
        for index in range(4097)
    ]
    logical_columns = {
        name: {
            "data_type": "text",
            "nullable": name
            in {"provider_updated_at", "event_ticker", "series_ticker", "task_scope"},
        }
        for name in warehouse._LANDING_COLUMNS
    }
    legacy_rows = [
        {
            "observation_id": row["observation_id"],
            "capture_id": capture_id,
            "entity_kind": row["entity_kind"],
            "entity_ticker": row["series_ticker"],
            "received_at": row["received_at"],
            "provider_updated_at": row["provider_updated_at"],
            "source_endpoint": row["source_endpoint"],
            "source_kind": row["source_kind"],
            "payload_json": row["payload_json"],
            "event_ticker": None,
            "series_ticker": row["series_ticker"],
            "task_scope": None,
            "task_generation": "",
        }
        for row in source_rows
    ]
    pipeline_args = {
        "pipeline_name": "kalshi_catalogue",
        "pipelines_dir": str(directory / "landing"),
        "destination": dlt.destinations.duckdb(str(directory / "warehouse.duckdb")),
        "dataset_name": "landing",
    }
    dlt.pipeline(**pipeline_args).run(
        dlt.resource(
            legacy_rows,
            name="observations",
            primary_key="observation_id",
            columns=logical_columns,
            write_disposition="merge",
        ),
        loader_file_format="parquet",
    )
    reference_database = directory / "reference.duckdb"
    dlt.pipeline(
        pipeline_name="legacy_reference",
        pipelines_dir=str(directory / "reference-landing"),
        destination=dlt.destinations.duckdb(str(reference_database)),
        dataset_name="landing",
    ).run(
        dlt.resource(
            legacy_rows,
            name="observations",
            primary_key="observation_id",
            columns=logical_columns,
            write_disposition="merge",
        ),
        loader_file_format="parquet",
    )
    manifest = {
        "capture_id": capture_id,
        "page_id": "page-duplicate-boundary",
        "received_at": "2026-10-18T00:00:00Z",
        "endpoint": "/series",
        "sha256": "f" * 64,
        "status": 200,
    }
    monkeypatch.setattr(warehouse, "capture_pages", lambda *_args, **_kwargs: [(manifest, {})])
    monkeypatch.setattr(warehouse, "observations", lambda _pages: iter(source_rows))

    from dlt.extract.storage import ExtractStorage

    original_closed_files = ExtractStorage.closed_files
    extract_file_counts = []

    def count_extract_files(self, load_id):
        files = original_closed_files(self, load_id)
        extract_file_counts.append(
            len([file for file in files if file.file_path.endswith(".parquet")])
        )
        return files

    monkeypatch.setattr(ExtractStorage, "closed_files", count_extract_files)
    config_keys = (
        "kalshi_catalogue.sources.kalshi_catalogue.kalshi_catalogue.data_writer.buffer_max_items",
        "kalshi_catalogue.normalize.data_writer.buffer_max_items",
        "kalshi_catalogue.normalize.parquet_normalizer.add_dlt_load_id",
        "kalshi_catalogue.normalize.parquet_normalizer.add_dlt_id",
    )
    config_before = {key: dlt.config.get(key) for key in config_keys}

    def logical_rows(database):
        with duckdb.connect(str(database), read_only=True) as connection:
            columns = ", ".join(warehouse._LANDING_COLUMNS)
            return connection.execute(
                f"select {columns} from landing.observations order by observation_id"
            ).fetchall()

    assert warehouse._land_rows(store, {"capture_id": capture_id}, directory) == len(source_rows)
    first_result = logical_rows(directory / "warehouse.duckdb")
    assert first_result == logical_rows(reference_database)
    assert len(first_result) == 4096
    assert {key: dlt.config.get(key) for key in config_keys} == config_before

    assert warehouse._land_rows(store, {"capture_id": capture_id}, directory) == len(source_rows)
    assert logical_rows(directory / "warehouse.duckdb") == first_result
    assert extract_file_counts == [1, 1]
    assert {key: dlt.config.get(key) for key in config_keys} == config_before
