from __future__ import annotations

import hashlib
import json
import re
import shutil
import sqlite3
from contextlib import contextmanager
from decimal import Decimal

import duckdb
import pytest
from conftest import FakeClient
from test_incremental_catalogue import Snapshot, capture_at
from test_incremental_catalogue import affected_baseline as affected_baseline
from test_incremental_catalogue import baseline as baseline
from test_incremental_catalogue import certified_baseline as certified_baseline
from test_manifest_warehouse_hash import semantic_queries

from kalshi_events import capture, incremental, normalize, publish, recovery_index, warehouse
from kalshi_events.common import (
    CatalogueError,
    Limits,
    Store,
    digest,
    file_digest,
    json_bytes,
    now,
    regular_bytes,
    timestamp_order_key,
)
from kalshi_events.recovery_index import RecoveryParentIndex, sealed_settings_sha256

EARLIER = "2026-01-01T01:00:00+01:00"
LATER = "2026-01-01T00:00:00.500000Z"


def _event_page(page_id: str, received_at: str, series: str):
    return (
        {
            "entity_kind": "events",
            "capture_id": "c" * 32,
            "endpoint": "/events",
            "singular": False,
            "status": 200,
            "page_id": page_id,
            "request_id": page_id + "0" * 32,
            "sha256": "d" * 64,
            "received_at": received_at,
        },
        {"events": [{"event_ticker": "E", "series_ticker": series}], "cursor": ""},
    )


def test_project_pages_orders_by_utc_instant_then_observation_id():
    old = _event_page("1" * 32, EARLIER, "OLD")
    new = _event_page("2" * 32, LATER, "NEW")

    assert normalize.project_pages([old, new])["events"][0]["series_ticker"] == "NEW"
    assert normalize.project_pages([new, old])["events"][0]["series_ticker"] == "NEW"

    equivalent = _event_page("3" * 32, "2026-01-01T00:00:00.5Z", "OFFSET")
    tied = normalize.project_pages([new, equivalent])["events"][0]
    expected_id = max(
        normalize.observation(
            "events",
            page[1]["events"][0],
            {
                "capture_id": "c" * 32,
                "page_id": page[0]["page_id"],
                "received_at": page[0]["received_at"],
                "source_endpoint": "/events",
                "payload_sha256": "d" * 64,
                "source_kind": "direct",
            },
            "/events/0",
        )["observation_id"]
        for page in (new, equivalent)
    )
    assert tied["observation_id"] == expected_id


def test_timestamp_order_key_uses_utc_microseconds_and_new_times_are_fixed_precision():
    assert timestamp_order_key(EARLIER, "a") < timestamp_order_key(LATER, "z")
    assert timestamp_order_key("2026-01-01T01:00:00.5+01:00", "a") == timestamp_order_key(
        "2026-01-01T00:00:00.500000Z", "a"
    )
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z", now())
    with pytest.raises(CatalogueError, match="UTC offset"):
        timestamp_order_key("2026-01-01T00:00:00", "id")


def test_baseline_snapshot_sql_uses_utc_instant_and_keeps_raw_timestamp(tmp_path, monkeypatch):
    connection = duckdb.connect(":memory:")
    connection.execute("create schema landing")
    connection.execute("create table landing.membership(scope varchar,entity_ticker varchar)")
    connection.execute(
        "create table landing.observations(entity_ticker varchar,event_ticker varchar,"
        "series_ticker varchar,received_at varchar,observation_id varchar,entity_kind varchar,source_kind varchar)"
    )
    connection.executemany(
        "insert into landing.membership values (?, ?)",
        [("markets_live", "M"), ("events_live", "E")],
    )
    connection.executemany(
        "insert into landing.observations values (?, ?, ?, ?, ?, ?, 'direct')",
        [
            ("E", None, "OLD-SERIES", EARLIER, "e-old", "events"),
            ("E", None, "NEW-SERIES", LATER, "e-new", "events"),
            ("M", "OLD-EVENT", None, EARLIER, "m-old", "markets"),
            ("M", "E", None, LATER, "m-new", "markets"),
        ],
    )

    @contextmanager
    def baseline_data(*_args, **_kwargs):
        yield connection

    monkeypatch.setattr(incremental, "_baseline_data", baseline_data)
    try:
        snapshot = incremental._baseline_snapshot(
            Store(tmp_path / "root"),
            {"capture_id": "c" * 32, "catalogue_version": 2},
        )
    finally:
        connection.close()
    assert snapshot["events"]["E"] == (LATER, "e-new", "NEW-SERIES")
    assert snapshot["markets"]["M"] == (LATER, "m-new", "E")


def test_v1_baseline_without_membership_evidence_fails_with_offline_rebase_path(
    tmp_path, monkeypatch
):
    connection = duckdb.connect(":memory:")

    @contextmanager
    def baseline_data(*_args, **_kwargs):
        yield connection

    monkeypatch.setattr(incremental, "_baseline_data", baseline_data)
    try:
        with pytest.raises(
            CatalogueError, match="complete saved full capture.*catalogue-version 2"
        ):
            incremental._baseline_snapshot(
                Store(tmp_path / "root"),
                {"capture_id": "c" * 32, "catalogue_version": 1},
            )
    finally:
        connection.close()


def test_memory_and_sqlite_parent_winners_use_instants_and_preserve_raw_timestamp(store):
    work = {
        "endpoint": "/events",
        "params": {},
        "kind": "events",
        "singular": False,
        "requested": ["E"],
        "parent_batch": "ordinary",
        "done": True,
        "restarts": 0,
    }
    batch_id = digest(
        json_bytes(
            [
                work["endpoint"],
                work["params"],
                work["kind"],
                work["singular"],
                work["requested"],
            ]
        )
    )

    def batch_page(page_id, received_at, series):
        manifest, payload = _event_page(page_id, received_at, series)
        manifest.update(
            parent_batch="ordinary",
            parent_batch_id=batch_id,
            parent_batch_tickers="E",
            task_generation=0,
        )
        return manifest, payload

    older = batch_page("4" * 32, EARLIER, "OLD")
    newer = batch_page("5" * 32, LATER, "NEW")
    state = {"tasks": [work]}
    memory = normalize.ParentIndex()
    for manifest, payload in (newer, older):
        memory.add_page(manifest, payload)
    assert memory.events["E"][2] == "NEW"
    assert memory.effective_parent_evidence(state)[0]["E"][2] == "NEW"

    disk = normalize.DiskParentIndex(store)
    try:
        disk.add_page(*newer)
        disk.add_page(*older)
        assert list(disk.stream_winners("events")) == [("E", LATER, memory.events["E"][1], "NEW")]
        assert disk.db.execute(
            "select received,parent from candidates where batch_id=? and ticker='E'",
            [batch_id],
        ).fetchall() == [(LATER, "NEW")]
    finally:
        disk.close()


@pytest.mark.parametrize("historical_compact", [False, True])
def test_dbt_current_winners_order_by_utc_instant(tmp_path, historical_compact):
    store = Store(tmp_path / "root")
    database = "timestamp-order.duckdb"
    path = store.path(database)
    with duckdb.connect(str(path)) as connection:
        connection.execute("create schema landing")
        connection.execute(
            "create table landing.observations (observation_id varchar,capture_id varchar,"
            "entity_kind varchar,entity_ticker varchar,received_at varchar,"
            "provider_updated_at varchar,source_endpoint varchar,source_kind varchar,"
            "payload_json varchar,event_ticker varchar,series_ticker varchar,"
            "task_scope varchar,task_generation varchar)"
        )
        connection.execute(
            "create table landing.membership (scope varchar,entity_kind varchar,entity_ticker varchar)"
        )
        connection.execute(
            "create table landing.parent_eligibility (capture_id varchar,entity_kind varchar,"
            "entity_ticker varchar,status integer,receipt varchar)"
        )
        for table, column in (
            ("affected_series", "series_ticker"),
            ("affected_events", "event_ticker"),
            ("affected_markets", "market_ticker"),
        ):
            connection.execute(f"create table landing.{table} ({column} varchar)")
        observations = [
            ("s1", "series", "S1", "2026-01-01T00:00:00Z", "/series", None, None, {"ticker": "S1"}),
            ("s2", "series", "S2", "2026-01-01T00:00:00Z", "/series", None, None, {"ticker": "S2"}),
            (
                "e-old",
                "events",
                "E",
                EARLIER,
                "/events",
                "E",
                "S1",
                {"event_ticker": "E", "series_ticker": "S1"},
            ),
            (
                "e-new",
                "events",
                "E",
                LATER,
                "/events",
                "E",
                "S2",
                {"event_ticker": "E", "series_ticker": "S2"},
            ),
            (
                "m",
                "markets",
                "M",
                "2026-01-01T00:00:00Z",
                "/markets",
                "E",
                None,
                {"ticker": "M", "event_ticker": "E", "mve_selected_legs": []},
            ),
        ]
        connection.executemany(
            "insert into landing.observations values (?, 'c', ?, ?, ?, null, ?, 'direct', ?, ?, ?, null, '0')",
            [
                (obs, kind, entity, received, endpoint, json.dumps(payload), event, series)
                for obs, kind, entity, received, endpoint, event, series, payload in observations
            ],
        )
        connection.executemany(
            "insert into landing.membership values (?, ?, ?)",
            [
                ("series", "series", "S1"),
                ("series", "series", "S2"),
                ("events_live", "events", "E"),
                ("markets_live", "markets", "M"),
            ],
        )

    warehouse.dbt(
        store,
        ["build"],
        database=database,
        artifacts="build-artifacts.json",
        variables={
            "catalogue_version": 2,
            "incremental_update": False,
            "current_capture_id": "c",
            "delta_capture_id": "c",
            "historical_compact": historical_compact,
        },
    )
    with duckdb.connect(str(path), read_only=True) as connection:
        event = connection.execute(
            "select series_ticker, observation_id, received_at from main.current_events where event_ticker='E'"
        ).fetchone()
        market = connection.execute(
            "select series_ticker, usable, received_at from main.current_markets where market_ticker='M'"
        ).fetchone()
        event_columns = {
            row[0] for row in connection.execute("describe main.current_events").fetchall()
        }
    assert event == ("S2", "e-new", LATER)
    assert market == ("S2", True, "2026-01-01T00:00:00Z")
    assert "received_key" not in event_columns


def test_timestamp_change_invalidates_persistent_recovery_normalizer():
    assert recovery_index.NORMALIZER_VERSION == 2


def test_persistent_recovery_index_with_old_normalizer_is_reset(tmp_path):
    root = tmp_path / "root"
    initial_store = Store(root)
    with initial_store.writer_guard():
        incremental.root_id(initial_store, create=True)
    partial = capture.capture(
        Store(root, Limits(max_requests=3)),
        client_factory=FakeClient,
        catalogue_profile="exchange",
    )
    cache = root / "tmp" / "recovery-index" / partial["capture_id"] / "index.sqlite"
    with sqlite3.connect(cache) as connection:
        connection.execute("update recovery_meta set normalizer_version=1 where id=1")

    store = Store(root)
    with store.writer_guard():
        state = capture.read_capture(store, partial["capture_id"], _writer_resume=True)
        index = RecoveryParentIndex(
            store,
            partial["capture_id"],
            incremental.root_id(store),
            sealed_settings_sha256(state),
            page_rows=state.page_rows,
            page_anchors=state.page_anchors,
            genesis_anchor=state.genesis_anchor,
        )
        try:
            assert index.reused_pages == 0
            assert list(index.stream_winners("series")) == []
            columns = {row[1] for row in index.db.execute("pragma table_info(winners)").fetchall()}
            assert "received_key" in columns
        finally:
            index.close(flush=False)


def test_unmarked_v2_baseline_reprojects_every_key_once_then_prunes_deltas(
    affected_baseline, tmp_path, monkeypatch
):
    source, baseline_id = affected_baseline
    shutil.copytree(source, tmp_path / "root")
    store = Store(tmp_path / "root")
    first = publish.current(store, catalogue_version=2)
    first_manifest_path = store.path(f"releases/{first['source_release_id']}/manifest.json")
    first_manifest = first_manifest_path.read_bytes()

    database = store.path(f"builds-v2/{baseline_id}/warehouse.duckdb")
    with duckdb.connect(str(database)) as connection:
        connection.execute(
            "update main.current_markets set last_price_dollars=0.999 where market_ticker='M39'"
        )
        connection.execute("checkpoint")
    receipt_path = store.path(f"builds-v2/{baseline_id}/build.json")
    receipt = json.loads(receipt_path.read_text())
    receipt["warehouse_sha256"] = file_digest(database, store.limits.max_database_bytes)[0]
    store.write(f"builds-v2/{baseline_id}/build.json", receipt)
    # Preserve an old published snapshot whose current table contains the old wrong winner.
    publish.publish(store, baseline_id, catalogue_version=2)
    assert first_manifest_path.read_bytes() == first_manifest

    with duckdb.connect(str(database)) as connection:
        connection.execute("drop table kalshi_private.ordering_policy")
        connection.execute("checkpoint")
    receipt.pop("warehouse_ordering_policy")
    receipt["warehouse_sha256"] = file_digest(database, store.limits.max_database_bytes)[0]
    store.write(f"builds-v2/{baseline_id}/build.json", receipt)
    current = publish.current(store, catalogue_version=2)
    manifest_path = store.path(f"releases/{current['source_release_id']}/manifest.json")
    manifest = json.loads(manifest_path.read_text())
    manifest["warehouse_sha256"] = receipt["warehouse_sha256"]
    store.write(f"releases/{current['source_release_id']}/manifest.json", manifest)
    pointer = json.loads(store.path("current-v2.json").read_text())
    pointer["manifest_sha256"] = hashlib.sha256(regular_bytes(manifest_path)).hexdigest()
    store.write("current-v2.json", pointer)

    delta = capture_at(store, Snapshot(), "2026-01-02T12:00:00Z", incremental=True)
    comparisons = semantic_queries(monkeypatch)
    warehouse.build(store, delta["capture_id"])
    assert comparisons, "an unmarked receipt must not use the warehouse-hash fast path"
    with duckdb.connect(
        str(store.path(f"builds-v2/{delta['capture_id']}/warehouse.duckdb")), read_only=True
    ) as connection:
        assert connection.execute(
            "select last_price_dollars from main.current_markets where market_ticker='M39'"
        ).fetchone()[0] == Decimal("0.00123")
    migrated = json.loads(store.path(f"builds-v2/{delta['capture_id']}/build.json").read_text())
    assert migrated["warehouse_ordering_policy"] == warehouse.ORDERING_POLICY
    publish.publish(store, delta["capture_id"], catalogue_version=2)

    snapshot = Snapshot()
    snapshot.market_updates = [
        {**snapshot.live[0], "ticker": "M20", "last_price_dollars": "0.23456789"}
    ]
    next_delta = capture_at(store, snapshot, "2026-01-03T12:00:00Z", incremental=True)
    from kalshi_events import metrics

    with metrics.collect() as measured:
        warehouse.build(store, next_delta["capture_id"])
    assert measured.report()["counters"]["affected_markets"] < 4
    assert (
        file_digest(first_manifest_path, store.limits.max_root_bytes)[0]
        == hashlib.sha256(first_manifest).hexdigest()
    )


def test_certified_rejects_mismatched_private_ordering_marker(tmp_path):
    store = Store(tmp_path / "root")
    from test_incremental_catalogue import BASE_TIME, Snapshot, capture_at

    state = capture_at(store, Snapshot(), BASE_TIME)
    warehouse.build_v2_full(store, state["capture_id"])
    database = store.path(f"builds-v2/{state['capture_id']}/warehouse.duckdb")
    with duckdb.connect(str(database)) as connection:
        connection.execute("update kalshi_private.ordering_policy set policy='wrong-policy'")
        connection.execute("checkpoint")
    receipt_path = store.path(f"builds-v2/{state['capture_id']}/build.json")
    receipt = json.loads(receipt_path.read_text())
    receipt["warehouse_sha256"] = file_digest(database, store.limits.max_database_bytes)[0]
    store.write(f"builds-v2/{state['capture_id']}/build.json", receipt)
    with pytest.raises(CatalogueError, match="ordering policy"):
        warehouse.certified(store, state["capture_id"], catalogue_version=2)

    with duckdb.connect(str(database)) as connection:
        connection.execute(
            "update kalshi_private.ordering_policy set policy=?", [warehouse.ORDERING_POLICY]
        )
        connection.execute("checkpoint")
    receipt["warehouse_sha256"] = file_digest(database, store.limits.max_database_bytes)[0]
    store.write(f"builds-v2/{state['capture_id']}/build.json", receipt)
    with duckdb.connect(str(database)) as connection:
        connection.execute("drop table kalshi_private.ordering_policy")
        connection.execute("checkpoint")
    receipt["warehouse_sha256"] = file_digest(database, store.limits.max_database_bytes)[0]
    store.write(f"builds-v2/{state['capture_id']}/build.json", receipt)
    with pytest.raises(CatalogueError, match="markers differ"):
        warehouse.certified(store, state["capture_id"], catalogue_version=2)

    with duckdb.connect(str(database)) as connection:
        connection.execute("create table kalshi_private.ordering_policy(policy varchar not null)")
        connection.execute(
            "insert into kalshi_private.ordering_policy values (?)", [warehouse.ORDERING_POLICY]
        )
        connection.execute("checkpoint")
    receipt.pop("warehouse_ordering_policy")
    receipt["warehouse_sha256"] = file_digest(database, store.limits.max_database_bytes)[0]
    store.write(f"builds-v2/{state['capture_id']}/build.json", receipt)
    with pytest.raises(CatalogueError, match="markers differ"):
        warehouse.certified(store, state["capture_id"], catalogue_version=2)

    with duckdb.connect(str(database)) as connection:
        connection.execute("drop table kalshi_private.ordering_policy")
        connection.execute("checkpoint")
    receipt["warehouse_sha256"] = file_digest(database, store.limits.max_database_bytes)[0]
    store.write(f"builds-v2/{state['capture_id']}/build.json", receipt)
    assert warehouse.certified(store, state["capture_id"], catalogue_version=2) == receipt


def test_publish_rebuilds_unmarked_full_build_without_rewriting_old_release(baseline, tmp_path):
    store, capture_id, _, old_release = baseline
    old_manifest_path = store.path(f"releases/{old_release['source_release_id']}/manifest.json")
    old_manifest = old_manifest_path.read_bytes()
    database = store.path(f"builds-v2/{capture_id}/warehouse.duckdb")
    with duckdb.connect(str(database)) as connection:
        connection.execute(
            "update main.current_markets set last_price_dollars=0.999 where market_ticker='M1'"
        )
        connection.execute("drop table kalshi_private.ordering_policy")
        connection.execute("checkpoint")
    receipt_path = store.path(f"builds-v2/{capture_id}/build.json")
    receipt = json.loads(receipt_path.read_text())
    receipt.pop("warehouse_ordering_policy")
    receipt["warehouse_sha256"] = file_digest(database, store.limits.max_database_bytes)[0]
    store.write(f"builds-v2/{capture_id}/build.json", receipt)

    publish.publish(store, capture_id, catalogue_version=2)

    rebuilt = json.loads(store.path(f"builds-v2/{capture_id}/build.json").read_text())
    assert rebuilt["warehouse_ordering_policy"] == warehouse.ORDERING_POLICY
    with duckdb.connect(str(database), read_only=True) as connection:
        assert connection.execute(
            "select last_price_dollars from main.current_markets where market_ticker='M1'"
        ).fetchone()[0] == Decimal("0.00123")
    assert old_manifest_path.read_bytes() == old_manifest
