from __future__ import annotations

import copy
import hashlib
import json
import shutil
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import conftest as fixtures
import duckdb
import pytest
from conftest import CUTOFF, MARKET, FakeClient

from kalshi_events import capture as acquisition
from kalshi_events import metadata, metrics, recovery, warehouse
from kalshi_events import publish as publishing
from kalshi_events.cli import main
from kalshi_events.common import CatalogueError, Limits, Store, json_bytes

BASE_TIME = "2026-01-01T12:00:00+00:00"
DELTA_TIME = "2026-01-02T12:00:00+00:00"
NEXT_TIME = "2026-01-03T12:00:00+00:00"
PROVENANCE = (
    "observation_id",
    "capture_id",
    "received_at",
    "provider_updated_at",
    "source_endpoint",
    "payload_json",
)


class Snapshot:
    """A provider state with separate metadata deltas and complete reconciliation scopes."""

    def __init__(self):
        self.series = [{"ticker": "S1", "title": "One"}, {"ticker": "S2", "title": "Two"}]
        self.events = [{"event_ticker": "E1", "series_ticker": "S1", "title": "Ordinary"}]
        self.combo_events = [{"event_ticker": "E2", "series_ticker": "S2", "title": "Combo"}]
        self.live = [{**MARKET, "ticker": "M1", "event_ticker": "E1"}]
        self.history = [
            {
                **MARKET,
                "ticker": "H",
                "event_ticker": "E1",
                "status": "finalized",
                "result": "no",
                "settlement_ts": "2025-12-01T00:00:00Z",
                "settlement_value_dollars": "0.00000",
            }
        ]
        self.combos = [
            {
                **MARKET,
                "ticker": "C",
                "event_ticker": "E2",
                "mve_collection_ticker": "NATIVE-COLLECTION",
                "mve_selected_legs": [{"event_ticker": "E1", "market_ticker": "M1", "side": "yes"}],
            }
        ]
        self.event_updates = []
        self.market_updates = []
        self.lookups = {}
        self.overrides = {}
        self.requests = []

    def response(self, endpoint, params):
        self.requests.append((endpoint, copy.deepcopy(params)))
        if endpoint in self.overrides:
            value = self.overrides[endpoint]
            return value(params) if callable(value) else value
        if endpoint in self.lookups:
            return self.lookups[endpoint]
        if endpoint == "/historical/cutoff":
            return 200, CUTOFF
        if endpoint == "/series":
            return 200, {"series": self.series}
        if endpoint == "/events":
            rows = self.event_updates if "min_updated_ts" in params else self.events
            return 200, {"events": rows, "cursor": ""}
        if endpoint == "/events/multivariate":
            return 200, {"events": self.combo_events, "cursor": ""}
        if endpoint == "/markets":
            if params.get("mve_filter") == "only":
                rows = self.combos
            elif "min_updated_ts" in params:
                assert params.get("mve_filter") == "exclude"
                rows = self.market_updates
            else:
                rows = [*self.live, *self.combos]
            return 200, {"markets": rows, "cursor": ""}
        if endpoint == "/historical/markets":
            return 200, {"markets": self.history, "cursor": ""}
        for prefix, rows, envelope, key in (
            ("/series/", self.series, "series", "ticker"),
            ("/events/", [*self.events, *self.combo_events], "event", "event_ticker"),
            ("/markets/", [*self.live, *self.combos], "market", "ticker"),
            ("/historical/markets/", self.history, "market", "ticker"),
        ):
            if endpoint.startswith(prefix):
                identity = endpoint.removeprefix(prefix)
                row = next((row for row in rows if row[key] == identity), None)
                return (200, {envelope: row}) if row is not None else (404, {})
        raise AssertionError(f"unexpected offline endpoint: {endpoint}")

    def factory(self, store, budget):
        snapshot = self

        class Client(FakeClient):
            def response(self, endpoint, params):
                return snapshot.response(endpoint, params)

        return Client(store, budget)


def capture_at(store, snapshot, timestamp=DELTA_TIME, *, client_factory=None, **kwargs):
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(fixtures, "now", lambda: timestamp)
        patch.setattr(acquisition, "now", lambda: timestamp)
        return acquisition.capture(
            store, client_factory=client_factory or snapshot.factory, **kwargs
        )


def controlled_receipt_client(snapshot, target_endpoint, page_id, received_at):
    class Client(FakeClient):
        def response(self, endpoint, params):
            return snapshot.response(endpoint, params)

        def get(self, endpoint, params=None, context=None):
            params = params or {}
            if endpoint != target_endpoint or (
                endpoint == "/markets" and params.get("mve_filter") != "exclude"
            ):
                return super().get(endpoint, params, context)
            allowance = self.budget.reserve()
            try:
                status, payload = self.response(endpoint, params)
                body = json_bytes(payload)
                self.budget.add_download(len(body))
                self.store.write_bytes(f"raw/{page_id}/body.bin", body)
                relative = f"raw/{page_id}/manifest.json"
                self.store.write(
                    relative,
                    {
                        **(context or {}),
                        "page_id": page_id,
                        "version": 1,
                        "endpoint": endpoint,
                        "params": params,
                        "received_at": received_at,
                        "status": status,
                        "size": len(body),
                        "sha256": hashlib.sha256(body).hexdigest(),
                        "downloaded_bytes": len(body),
                        "accounting_complete": True,
                    },
                )
                return relative
            finally:
                self.budget.settle(allowance)

    return Client


def lower_observation_page_id(store, observation_id, pointer):
    return next(
        f"{number:032x}"
        for number in range(100_000)
        if hashlib.sha256(json_bytes([f"{number:032x}", pointer])).hexdigest() < observation_id
        and not store.path(f"raw/{number:032x}").exists()
    )


def relation(store, capture_id, table, *, catalogue_version=None):
    if catalogue_version is None:
        catalogue_version = (
            2 if store.path(f"builds-v2/{capture_id}/warehouse.duckdb").exists() else 1
        )
    directory = "builds-v2" if catalogue_version == 2 else "builds"
    database = store.path(f"{directory}/{capture_id}/warehouse.duckdb")
    with duckdb.connect(str(database), read_only=True) as connection:
        cursor = connection.execute(f"select * from main.{table}")
        names = [field[0] for field in cursor.description]
        return [dict(zip(names, values, strict=True)) for values in cursor.fetchall()]


def keyed(store, capture_id, table, key, *, catalogue_version=None):
    rows = relation(store, capture_id, table, catalogue_version=catalogue_version)
    result = {row[key]: row for row in rows}
    assert len(result) == len(rows), f"duplicate current identity in {table}"
    return result


def assert_provenance(store, capture_id):
    history = keyed(store, capture_id, "observation_history", "observation_id")
    current_count = 0
    for table in ("current_series", "current_events", "current_markets"):
        rows = relation(store, capture_id, table)
        key = {"current_series": "series_ticker", "current_events": "event_ticker"}.get(
            table, "market_ticker"
        )
        assert len({row[key] for row in rows}) == len(rows)
        if table != "current_series":
            current_count += len(rows)
        for current in rows:
            original = history[current["observation_id"]]
            assert tuple(current[field] for field in PROVENANCE) == tuple(
                original[field] for field in PROVENANCE
            )
    edges = relation(store, capture_id, "relationships")
    assert len(edges) == current_count
    assert len(
        {
            (row["parent_kind"], row["parent_ticker"], row["child_kind"], row["child_ticker"])
            for row in edges
        }
    ) == len(edges)


def tree_inventory(root):
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file()
    }


@pytest.fixture(scope="module")
def certified_baseline(tmp_path_factory):
    store = Store(tmp_path_factory.mktemp("composed-baseline") / "root")
    state = capture_at(store, Snapshot(), BASE_TIME)
    assert state["complete"]
    warehouse.build(store, state["capture_id"])
    legacy = publishing.publish(store, state["capture_id"])
    version_two = publishing.publish(store, state["capture_id"], catalogue_version=2)
    frozen = tree_inventory(store.root)
    yield store.root, state["capture_id"], legacy, version_two
    assert tree_inventory(store.root) == frozen, "a test mutated the shared certified baseline"


@pytest.fixture
def baseline(certified_baseline, tmp_path):
    source, capture_id, legacy, version_two = certified_baseline
    root = tmp_path / "root"
    shutil.copytree(source, root)
    return Store(root), capture_id, legacy, version_two


@pytest.fixture(scope="module")
def affected_baseline(tmp_path_factory):
    store = Store(tmp_path_factory.mktemp("affected-key-baseline") / "root")
    snapshot = Snapshot()
    snapshot.live = [{**snapshot.live[0], "ticker": f"M{number}"} for number in range(40)]
    state = capture_at(store, snapshot, BASE_TIME)
    warehouse.build(store, state["capture_id"])
    publishing.publish(store, state["capture_id"])
    publishing.publish(store, state["capture_id"], catalogue_version=2)
    frozen = tree_inventory(store.root)
    yield store.root, state["capture_id"]
    assert tree_inventory(store.root) == frozen


def build_delta(store, snapshot, timestamp=DELTA_TIME):
    state = capture_at(store, snapshot, timestamp, incremental=True)
    assert state["complete"]
    assert state["incremental"] is True
    warehouse.build(store, state["capture_id"])
    assert store.path(f"builds-v2/{state['capture_id']}/warehouse.duckdb").is_file()
    assert not store.path(f"builds/{state['capture_id']}").exists()
    assert_provenance(store, state["capture_id"])
    return state


@pytest.mark.parametrize("sparse_change", [False, True])
def test_no_change_and_sparse_refresh_report_bounded_affected_key_counts(
    affected_baseline, tmp_path, sparse_change
):
    source, baseline_id = affected_baseline
    shutil.copytree(source, tmp_path / "root")
    store = Store(tmp_path / "root")
    before = keyed(store, baseline_id, "current_markets", "market_ticker")
    snapshot = Snapshot()
    if sparse_change:
        snapshot.market_updates = [
            {**snapshot.live[0], "ticker": "M20", "last_price_dollars": "0.23456789"}
        ]
    delta = capture_at(store, snapshot, incremental=True)
    with metrics.collect() as measured:
        receipt = warehouse.build(store, delta["capture_id"])
    counters = measured.report()["counters"]
    assert counters["affected_series"] == 2
    assert counters["affected_events"] == 2
    assert counters["affected_markets"] == 2 + int(sparse_change)
    assert counters["affected_markets"] < len(before) // 10
    assert not any(
        name in receipt for name in ("affected_series", "affected_events", "affected_markets")
    )
    after = keyed(store, delta["capture_id"], "current_markets", "market_ticker")
    for number in range(40):
        identity = f"M{number}"
        if sparse_change and identity == "M20":
            assert after[identity]["last_price_dollars"] == Decimal("0.23456789")
        else:
            assert after[identity] == before[identity]


def test_empty_metadata_delta_preserves_carried_times_and_distinct_observations(baseline):
    store, base_id, _, _ = baseline
    before_markets = keyed(store, base_id, "current_markets", "market_ticker")
    before_events = keyed(store, base_id, "current_events", "event_ticker")
    delta = build_delta(store, Snapshot())
    capture_id = delta["capture_id"]
    markets = keyed(store, capture_id, "current_markets", "market_ticker")
    events = keyed(store, capture_id, "current_events", "event_ticker")
    assert markets["M1"] == before_markets["M1"]
    assert events["E1"] == before_events["E1"]
    assert markets["M1"]["last_price_dollars"] == Decimal("0.00123")
    assert markets["M1"]["volume_fp"] == Decimal("12.3456789")
    assert markets["C"]["capture_id"] == capture_id
    history = relation(store, capture_id, "observation_history")
    assert len({row["observation_id"] for row in history}) == len(history)
    for kind, identity, count in (
        ("markets", "M1", 1),
        ("events", "E1", 1),
        ("markets", "C", 2),
        ("markets", "H", 2),
        ("events", "E2", 2),
        ("series", "S1", 2),
    ):
        assert (
            sum(row["entity_kind"] == kind and row["entity_ticker"] == identity for row in history)
            == count
        )
    warehouse.build(store, capture_id)
    assert {
        row["observation_id"] for row in relation(store, capture_id, "observation_history")
    } == {row["observation_id"] for row in history}


def test_history_removal_retains_observation_and_reappearance_accepts_corrected_settlement(
    baseline,
):
    store, _, _, _ = baseline
    removed = Snapshot()
    removed.history = []
    first = build_delta(store, removed)
    assert "H" not in keyed(store, first["capture_id"], "current_markets", "market_ticker")
    history = relation(store, first["capture_id"], "observation_history")
    assert sum(row["entity_ticker"] == "H" for row in history) == 1
    publishing.publish(store, first["capture_id"], catalogue_version=2)
    reappeared = Snapshot()
    reappeared.history[0].update(result="yes", settlement_value_dollars="1.000000000000000001")
    second = build_delta(store, reappeared, NEXT_TIME)
    market = keyed(store, second["capture_id"], "current_markets", "market_ticker")["H"]
    assert market["result"] == "yes"
    assert market["settlement_value_dollars"] == Decimal("1.000000000000000001")
    assert (
        sum(
            row["entity_ticker"] == "H"
            for row in relation(store, second["capture_id"], "observation_history")
        )
        == 2
    )


def test_event_reassignment_updates_carried_market_and_removes_old_relationship(baseline):
    store, base_id, _, _ = baseline
    old = keyed(store, base_id, "current_markets", "market_ticker")["M1"]
    snapshot = Snapshot()
    snapshot.event_updates = [{**snapshot.events[0], "series_ticker": "S2"}]
    delta = build_delta(store, snapshot)
    market = keyed(store, delta["capture_id"], "current_markets", "market_ticker")["M1"]
    assert market["series_ticker"] == "S2"
    assert market["usable"]
    assert tuple(market[field] for field in PROVENANCE) == tuple(old[field] for field in PROVENANCE)
    edges = relation(store, delta["capture_id"], "relationships")
    assert [
        (row["parent_ticker"], row["resolved"])
        for row in edges
        if row["child_kind"] == "events" and row["child_ticker"] == "E1"
    ] == [("S2", True)]


@pytest.mark.parametrize("lookup_status", [200, 404])
def test_series_omission_requires_a_fresh_parent_lookup_in_each_capture(baseline, lookup_status):
    store, _, _, _ = baseline
    snapshot = Snapshot()
    parent = snapshot.series.pop(0)
    snapshot.lookups["/series/S1"] = (
        (200, {"series": parent}) if lookup_status == 200 else (404, {})
    )
    first = build_delta(store, snapshot)
    assert ("/series/S1", {}) in snapshot.requests
    series = keyed(store, first["capture_id"], "current_series", "series_ticker")
    markets = keyed(store, first["capture_id"], "current_markets", "market_ticker")
    assert ("S1" in series) == (lookup_status == 200)
    assert markets["M1"]["usable"] == (lookup_status == 200)
    legs = relation(store, first["capture_id"], "combo_legs")
    assert legs[0]["resolved"], (
        "a missing target series does not invalidate native event/market legs"
    )
    publishing.publish(store, first["capture_id"], catalogue_version=2)

    absent = Snapshot()
    absent.series.pop(0)
    absent.lookups["/series/S1"] = (404, {})
    second = build_delta(store, absent, NEXT_TIME)
    assert ("/series/S1", {}) in absent.requests
    assert "S1" not in keyed(store, second["capture_id"], "current_series", "series_ticker")
    assert not keyed(store, second["capture_id"], "current_markets", "market_ticker")["M1"][
        "usable"
    ]


def test_removed_combo_event_does_not_remain_eligible_from_old_lookup(baseline):
    store, _, _, _ = baseline
    snapshot = Snapshot()
    snapshot.combo_events = []
    snapshot.combos = []
    snapshot.history = [{**snapshot.history[0], "event_ticker": "E2"}]
    snapshot.lookups["/events/E2"] = (404, {})
    delta = build_delta(store, snapshot)
    capture_id = delta["capture_id"]
    assert ("/events/E2", {"exclude_markets": "true"}) in snapshot.requests
    assert "E2" not in keyed(store, capture_id, "current_events", "event_ticker")
    market = keyed(store, capture_id, "current_markets", "market_ticker")["H"]
    assert market["event_ticker"] == "E2"
    assert not market["usable"]
    assert market["series_ticker"] is None
    assert any(
        row["entity_kind"] == "events" and row["entity_ticker"] == "E2"
        for row in relation(store, capture_id, "observation_history")
    )


@pytest.mark.parametrize("new_event", ["E1", "E2"])
def test_combo_leg_resolution_tracks_native_market_parent_changes(baseline, new_event):
    store, _, _, _ = baseline
    snapshot = Snapshot()
    snapshot.market_updates = [{**snapshot.live[0], "event_ticker": new_event}]
    delta = build_delta(store, snapshot)
    leg = relation(store, delta["capture_id"], "combo_legs")[0]
    assert leg["event_ticker"] == "E1"
    assert leg["market_ticker"] == "M1"
    assert leg["resolved"] == (new_event == "E1")


def test_live_and_historical_membership_are_combined_before_removal(baseline):
    store, _, _, _ = baseline
    snapshot = Snapshot()
    moved = snapshot.history.pop()
    snapshot.market_updates = [{**moved, "status": "active"}]
    snapshot.history = [{**snapshot.live[0], "status": "finalized", "result": "yes"}]
    first = build_delta(store, snapshot)
    markets = keyed(store, first["capture_id"], "current_markets", "market_ticker")
    assert {"M1", "H"}.issubset(markets)
    assert markets["H"]["status"] == "active"
    assert markets["M1"]["status"] == "finalized"
    publishing.publish(store, first["capture_id"], catalogue_version=2)
    omitted = Snapshot()
    omitted.history = []
    second = build_delta(store, omitted, NEXT_TIME)
    # Omission from a metadata delta cannot revoke the retained live membership of either ticker.
    assert {"M1", "H"}.issubset(
        keyed(store, second["capture_id"], "current_markets", "market_ticker")
    )


def test_later_receipt_wins_despite_older_provider_timestamp_and_nested_market(baseline):
    store, _, _, _ = baseline
    snapshot = Snapshot()
    direct = {
        **snapshot.live[0],
        "updated_time": "2000-01-01T00:00:00Z",
        "last_price_dollars": "0.987654321000000001",
    }
    snapshot.market_updates = [direct]
    snapshot.event_updates = [
        {
            **snapshot.events[0],
            "updated_time": "2099-01-01T00:00:00Z",
            "markets": [{**direct, "last_price_dollars": "0.111"}],
        }
    ]
    delta = build_delta(store, snapshot)
    market = keyed(store, delta["capture_id"], "current_markets", "market_ticker")["M1"]
    assert market["last_price_dollars"] == Decimal("0.987654321000000001")
    assert market["provider_updated_at"] == "2000-01-01T00:00:00Z"
    assert market["source_endpoint"] == "/markets"
    assert market["capture_id"] == delta["capture_id"]


def test_equal_received_timestamps_use_observation_id_to_break_ties(baseline):
    store, _, _, _ = baseline
    snapshot = Snapshot()
    snapshot.market_updates = [
        {**snapshot.live[0], "last_price_dollars": price}
        for price in ("0.222222222222222222", "0.777777777777777777")
    ]
    delta = build_delta(store, snapshot)
    candidates = [
        row
        for row in relation(store, delta["capture_id"], "observation_history")
        if row["entity_kind"] == "markets"
        and row["entity_ticker"] == "M1"
        and row["source_kind"] == "direct"
    ]
    assert len(candidates) == 3
    expected = max(candidates, key=lambda row: (row["received_at"], row["observation_id"]))
    market = keyed(store, delta["capture_id"], "current_markets", "market_ticker")["M1"]
    assert market["observation_id"] == expected["observation_id"]
    assert market["last_price_dollars"] == Decimal(
        json.loads(expected["payload_json"])["last_price_dollars"]
    )


@pytest.mark.parametrize("receipt_at", [BASE_TIME, "2025-12-31T12:00:00+00:00"])
def test_hydration_uses_effective_baseline_winner_when_incoming_parent_loses(tmp_path, receipt_at):
    store = Store(tmp_path / "root")
    snapshot = Snapshot()
    old_parent = snapshot.events.pop()
    snapshot.lookups["/events/E1"] = (200, {"event": old_parent})
    full = capture_at(store, snapshot, BASE_TIME)
    warehouse.build(store, full["capture_id"])
    publishing.publish(store, full["capture_id"])
    publishing.publish(store, full["capture_id"], catalogue_version=2)
    before = keyed(store, full["capture_id"], "current_markets", "market_ticker")["M1"]
    lower_page_id = lower_observation_page_id(store, before["observation_id"], "/markets/0")
    updated = Snapshot()
    updated.events = []
    updated.history = []
    updated.market_updates = [{**updated.live[0], "event_ticker": "INCOMING-PARENT"}]
    updated.lookups["/events/E1"] = (200, {"event": old_parent})
    updated.lookups["/events/INCOMING-PARENT"] = (404, {})

    client = controlled_receipt_client(updated, "/markets", lower_page_id, receipt_at)
    delta = capture_at(store, updated, incremental=True, client_factory=client)
    assert delta["complete"]
    assert ("/events/E1", {"exclude_markets": "true"}) in updated.requests
    warehouse.build(store, delta["capture_id"])
    winner = keyed(store, delta["capture_id"], "current_markets", "market_ticker")["M1"]
    assert winner["observation_id"] == before["observation_id"]
    assert winner["event_ticker"] == "E1"
    assert winner["usable"]
    assert_provenance(store, delta["capture_id"])


@pytest.mark.parametrize("receipt_at", [BASE_TIME, "2025-12-31T12:00:00+00:00"])
def test_event_hydration_uses_winning_series_when_incoming_event_loses(tmp_path, receipt_at):
    store = Store(tmp_path / "root")
    snapshot = Snapshot()
    old_series = snapshot.series.pop(0)
    snapshot.lookups["/series/S1"] = (200, {"series": old_series})
    full = capture_at(store, snapshot, BASE_TIME)
    warehouse.build(store, full["capture_id"])
    publishing.publish(store, full["capture_id"])
    publishing.publish(store, full["capture_id"], catalogue_version=2)
    before = keyed(store, full["capture_id"], "current_events", "event_ticker")["E1"]
    page_id = lower_observation_page_id(store, before["observation_id"], "/events/0")
    updated = Snapshot()
    updated.series.pop(0)
    updated.event_updates = [{**updated.events[0], "series_ticker": "INCOMING-SERIES"}]
    updated.lookups["/series/S1"] = (200, {"series": old_series})
    updated.lookups["/series/INCOMING-SERIES"] = (404, {})
    client = controlled_receipt_client(updated, "/events", page_id, receipt_at)
    delta = capture_at(store, updated, incremental=True, client_factory=client)
    assert delta["complete"]
    assert ("/series/S1", {}) in updated.requests
    warehouse.build(store, delta["capture_id"])
    winner = keyed(store, delta["capture_id"], "current_events", "event_ticker")["E1"]
    assert winner["observation_id"] == before["observation_id"]
    assert winner["series_ticker"] == "S1"
    assert keyed(store, delta["capture_id"], "current_markets", "market_ticker")["M1"]["usable"]
    assert_provenance(store, delta["capture_id"])


def test_reappearing_entity_uses_retained_history_winner_and_hydrates_its_old_parent(tmp_path):
    store = Store(tmp_path / "root")
    snapshot = Snapshot()
    snapshot.history[0]["event_ticker"] = "OLD-EVENT"
    old_event = {"event_ticker": "OLD-EVENT", "series_ticker": "OLD-SERIES"}
    old_series = {"ticker": "OLD-SERIES"}
    snapshot.lookups["/events/OLD-EVENT"] = (200, {"event": old_event})
    snapshot.lookups["/series/OLD-SERIES"] = (200, {"series": old_series})
    full = capture_at(store, snapshot, BASE_TIME)
    warehouse.build(store, full["capture_id"])
    publishing.publish(store, full["capture_id"])
    publishing.publish(store, full["capture_id"], catalogue_version=2)
    old_market = keyed(store, full["capture_id"], "current_markets", "market_ticker")["H"]

    removed = Snapshot()
    removed.history = []
    first = build_delta(store, removed)
    assert "H" not in keyed(store, first["capture_id"], "current_markets", "market_ticker")
    assert "OLD-EVENT" not in keyed(store, first["capture_id"], "current_events", "event_ticker")
    assert "OLD-SERIES" not in keyed(store, first["capture_id"], "current_series", "series_ticker")
    publishing.publish(store, first["capture_id"], catalogue_version=2)

    returned = Snapshot()
    returned.history[0]["event_ticker"] = "LOSING-EVENT"
    returned.lookups["/events/OLD-EVENT"] = (200, {"event": old_event})
    returned.lookups["/series/OLD-SERIES"] = (200, {"series": old_series})
    returned.lookups["/events/LOSING-EVENT"] = (404, {})
    page_id = lower_observation_page_id(store, old_market["observation_id"], "/markets/0")
    client = controlled_receipt_client(
        returned, "/historical/markets", page_id, "2025-12-31T12:00:00+00:00"
    )
    second = capture_at(store, returned, NEXT_TIME, incremental=True, client_factory=client)
    assert second["complete"]
    assert ("/events/OLD-EVENT", {"exclude_markets": "true"}) in returned.requests
    assert ("/series/OLD-SERIES", {}) in returned.requests
    warehouse.build(store, second["capture_id"])
    winner = keyed(store, second["capture_id"], "current_markets", "market_ticker")["H"]
    assert winner["observation_id"] == old_market["observation_id"]
    assert winner["capture_id"] == old_market["capture_id"]
    assert winner["event_ticker"] == "OLD-EVENT"
    assert winner["series_ticker"] == "OLD-SERIES"
    assert winner["usable"]
    assert_provenance(store, second["capture_id"])


def test_lookup_only_event_and_series_are_hydrated_together_and_leave_with_last_child(baseline):
    store, _, _, _ = baseline
    snapshot = Snapshot()
    snapshot.history = [{**snapshot.history[0], "event_ticker": "LOOKUP-EVENT"}]
    snapshot.lookups["/events/LOOKUP-EVENT"] = (
        200,
        {"event": {"event_ticker": "LOOKUP-EVENT", "series_ticker": "LOOKUP-SERIES"}},
    )
    snapshot.lookups["/series/LOOKUP-SERIES"] = (200, {"series": {"ticker": "LOOKUP-SERIES"}})
    delta = build_delta(store, snapshot)
    assert ("/events/LOOKUP-EVENT", {"exclude_markets": "true"}) in snapshot.requests
    assert ("/series/LOOKUP-SERIES", {}) in snapshot.requests
    market = keyed(store, delta["capture_id"], "current_markets", "market_ticker")["H"]
    assert market["usable"] and market["series_ticker"] == "LOOKUP-SERIES"
    publishing.publish(store, delta["capture_id"], catalogue_version=2)

    removed = Snapshot()
    removed.history = []
    next_delta = build_delta(store, removed, NEXT_TIME)
    capture_id = next_delta["capture_id"]
    assert "H" not in keyed(store, capture_id, "current_markets", "market_ticker")
    assert "LOOKUP-EVENT" not in keyed(store, capture_id, "current_events", "event_ticker")
    assert "LOOKUP-SERIES" not in keyed(store, capture_id, "current_series", "series_ticker")
    assert not any(
        row["child_ticker"] in {"H", "LOOKUP-EVENT"}
        for row in relation(store, capture_id, "relationships")
    )
    assert {"H", "LOOKUP-EVENT", "LOOKUP-SERIES"}.issubset(
        {row["entity_ticker"] for row in relation(store, capture_id, "observation_history")}
    )


def test_cursor_restart_reconciles_only_final_generation_and_retains_abandoned_history(baseline):
    store, _, _, _ = baseline
    snapshot = Snapshot()
    first_page = True

    def historical(params):
        nonlocal first_page
        if params.get("cursor"):
            return 400, {"message": "invalid cursor"}
        if first_page:
            first_page = False
            return 200, {
                "markets": [
                    snapshot.history[0],
                    {**snapshot.history[0], "ticker": "ABANDONED"},
                ],
                "cursor": "invalidated",
            }
        return 200, {"markets": [], "cursor": ""}

    snapshot.overrides["/historical/markets"] = historical
    delta = build_delta(store, snapshot)
    markets = keyed(store, delta["capture_id"], "current_markets", "market_ticker")
    assert "H" not in markets
    assert "ABANDONED" not in markets
    history = relation(store, delta["capture_id"], "observation_history")
    assert sum(row["entity_ticker"] == "H" for row in history) == 2
    assert sum(row["entity_ticker"] == "ABANDONED" for row in history) == 1


def test_delta_requires_explicit_ids_and_legacy_defaults_keep_full_capture(
    baseline, tmp_path, capsys
):
    store, base_id, legacy, _ = baseline
    delta = build_delta(store, Snapshot())
    assert acquisition.latest_catalogue_capture(store) == base_id
    assert acquisition.latest_complete_capture(store) == base_id
    assert main(["--root", str(store.root), "build"]) == 0
    assert json.loads(capsys.readouterr().out)["capture_id"] == base_id
    assert publishing.publish(store)["source_release_id"] == legacy["source_release_id"]
    assert publishing.current(store)["capture_id"] == base_id
    assert publishing.publish(store, catalogue_version=2)["capture_id"] == base_id
    with pytest.raises(CatalogueError, match="incremental|composed|version"):
        publishing.publish(store, delta["capture_id"])
    with pytest.raises(CatalogueError, match="incremental|composed"):
        metadata.export(store, ["M1"], tmp_path / "invalid", capture_id=delta["capture_id"])
    assert not (tmp_path / "invalid").exists()
    metadata.export(store, ["M1"], tmp_path / "legacy-default")
    exported = json.loads((tmp_path / "legacy-default" / "markets.json").read_text())
    assert exported[0]["capture_id"] == base_id


def test_version_two_publication_leaves_legacy_pointer_and_dispatches_verification(baseline):
    store, base_id, _, _ = baseline
    legacy_pointer = store.path("current.json").read_bytes()
    delta = build_delta(store, Snapshot())
    release = publishing.publish(store, delta["capture_id"], catalogue_version=2)
    assert release["contract"] == "oddsfox.kalshi.catalogue.v2"
    assert store.path("current.json").read_bytes() == legacy_pointer
    assert publishing.current(store)["capture_id"] == base_id
    assert publishing.current(store, catalogue_version=2)["capture_id"] == delta["capture_id"]
    verified = publishing.verify_release(Path(release["output"]))
    assert verified["source_release_id"] == release["source_release_id"]
    database = store.path(f"builds-v2/{delta['capture_id']}/warehouse.duckdb")
    with duckdb.connect(str(database), read_only=True) as connection:
        for name, table in warehouse.RELATIONS.items():
            parquet = str(Path(release["output"]) / f"{name}.parquet")
            assert not connection.execute(
                f"select * from main.{table} except all select * from read_parquet(?)", [parquet]
            ).fetchall()
            assert not connection.execute(
                f"select * from read_parquet(?) except all select * from main.{table}", [parquet]
            ).fetchall()


def test_full_v2_build_is_separate_and_does_not_change_legacy_build(baseline):
    store, capture_id, legacy, version_two = baseline
    legacy_database = store.path(f"builds/{capture_id}/warehouse.duckdb")
    v2_database = store.path(f"builds-v2/{capture_id}/warehouse.duckdb")
    assert legacy_database.is_file() and v2_database.is_file()
    legacy_before = hashlib.sha256(legacy_database.read_bytes()).hexdigest()
    publishing.publish(store, capture_id, catalogue_version=2)
    assert hashlib.sha256(legacy_database.read_bytes()).hexdigest() == legacy_before
    for table in warehouse.RELATIONS.values():
        first = relation(store, capture_id, table, catalogue_version=1)
        second = relation(store, capture_id, table, catalogue_version=2)
        assert sorted(json.dumps(row, sort_keys=True, default=str) for row in first) == sorted(
            json.dumps(row, sort_keys=True, default=str) for row in second
        )
    assert publishing.current(store)["source_release_id"] == legacy["source_release_id"]
    assert (
        publishing.current(store, catalogue_version=2)["source_release_id"]
        == version_two["source_release_id"]
    )


def test_watermark_window_is_sealed_before_requests_and_resume_does_not_repin(baseline):
    store, _, _, baseline_release = baseline
    previous_pointer = store.path("current-v2.json").read_bytes()
    first_snapshot = Snapshot()
    bounded = Store(store.root, Limits(max_requests=2))
    first = capture_at(bounded, first_snapshot, DELTA_TIME, incremental=True)
    assert not first["complete"]
    assert store.path("current-v2.json").read_bytes() == previous_pointer
    with pytest.raises(CatalogueError, match="incomplete"):
        warehouse.build(store, first["capture_id"])
    with pytest.raises(CatalogueError, match="incomplete"):
        publishing.publish(store, first["capture_id"], catalogue_version=2)

    resumed_snapshot = Snapshot()
    resumed_snapshot.market_updates = [
        {**resumed_snapshot.live[0], "updated_time": "2099-01-01T00:00:00Z"}
    ]
    resumed = capture_at(store, resumed_snapshot, NEXT_TIME, resume=first["capture_id"])
    assert resumed["complete"]
    assert resumed["incremental"] is True
    assert resumed["started_at"] == DELTA_TIME
    expected = int((datetime.fromisoformat(BASE_TIME) - timedelta(minutes=5)).timestamp())
    updates = [
        params
        for endpoint, params in [*first_snapshot.requests, *resumed_snapshot.requests]
        if endpoint in {"/events", "/markets"} and "min_updated_ts" in params
    ]
    assert len(updates) == 2
    assert all(int(params["min_updated_ts"]) == expected for params in updates)
    assert (
        publishing.current(store, catalogue_version=2)["source_release_id"]
        == baseline_release["source_release_id"]
    )
    warehouse.build(store, resumed["capture_id"])
    publishing.publish(store, resumed["capture_id"], catalogue_version=2)
    third_snapshot = Snapshot()
    capture_at(store, third_snapshot, NEXT_TIME, incremental=True)
    next_expected = int((datetime.fromisoformat(DELTA_TIME) - timedelta(minutes=5)).timestamp())
    assert all(
        int(params["min_updated_ts"]) == next_expected
        for endpoint, params in third_snapshot.requests
        if endpoint in {"/events", "/markets"} and "min_updated_ts" in params
    )


def test_rebuilt_baseline_remains_eligible_for_an_incremental_capture(baseline):
    store, base_id, _, _ = baseline
    old_pointer = store.path("current-v2.json").read_bytes()
    assert recovery.rebuild(store, catalogue_version=2)["capture_id"] == base_id
    assert store.path("current-v2.json").read_bytes() == old_pointer
    delta = build_delta(store, Snapshot())
    assert {"M1", "H", "C"} == set(
        keyed(store, delta["capture_id"], "current_markets", "market_ticker")
    )


def test_first_incremental_capture_can_pin_a_certified_v1_publication(baseline):
    store, base_id, _, _ = baseline
    store.path("current-v2.json").unlink()
    legacy_pointer = store.path("current.json").read_bytes()
    snapshot = Snapshot()
    delta = build_delta(store, snapshot)
    publishing.publish(store, delta["capture_id"], catalogue_version=2)
    assert store.path("current.json").read_bytes() == legacy_pointer
    assert publishing.current(store)["capture_id"] == base_id
    assert publishing.current(store, catalogue_version=2)["capture_id"] == delta["capture_id"]


def test_v1_bootstrap_uses_pinned_current_with_any_list_proof_not_cached_v2_membership(tmp_path):
    store = Store(tmp_path / "root")
    snapshot = Snapshot()
    ghost_event = {"event_ticker": "ABANDONED-EVENT", "series_ticker": "S1"}
    ghost_market = {**snapshot.live[0], "ticker": "ABANDONED-MARKET"}

    def restarting(envelope, final, abandoned):
        first = True

        def page(params):
            nonlocal first
            if params.get("cursor"):
                return 400, {"message": "invalid cursor"}
            if first:
                first = False
                return 200, {envelope: [*final, abandoned], "cursor": "expired"}
            return 200, {envelope: final, "cursor": ""}

        return page

    snapshot.overrides["/events"] = restarting("events", snapshot.events, ghost_event)
    snapshot.overrides["/markets"] = restarting(
        "markets", [*snapshot.live, *snapshot.combos], ghost_market
    )
    full = capture_at(store, snapshot, BASE_TIME)
    capture_id = full["capture_id"]
    warehouse.build(store, capture_id)
    publishing.publish(store, capture_id)
    legacy_pointer = store.path("current.json").read_bytes()
    assert "ABANDONED-EVENT" in keyed(
        store, capture_id, "current_events", "event_ticker", catalogue_version=1
    )
    assert "ABANDONED-MARKET" in keyed(
        store, capture_id, "current_markets", "market_ticker", catalogue_version=1
    )

    publishing.publish(store, capture_id, catalogue_version=2)
    assert "ABANDONED-EVENT" not in keyed(
        store, capture_id, "current_events", "event_ticker", catalogue_version=2
    )
    assert "ABANDONED-MARKET" not in keyed(
        store, capture_id, "current_markets", "market_ticker", catalogue_version=2
    )
    history = relation(store, capture_id, "observation_history", catalogue_version=2)
    assert {"ABANDONED-EVENT", "ABANDONED-MARKET"}.issubset(
        {row["entity_ticker"] for row in history}
    )
    # A cached full-v2 projection has different membership and cannot bootstrap the v1 pointer.
    store.path("current-v2.json").unlink()
    delta = build_delta(store, Snapshot())
    assert "ABANDONED-EVENT" in keyed(store, delta["capture_id"], "current_events", "event_ticker")
    assert "ABANDONED-MARKET" in keyed(
        store, delta["capture_id"], "current_markets", "market_ticker"
    )
    assert store.path("current.json").read_bytes() == legacy_pointer


def test_v1_bootstrap_never_turns_a_hydrated_parent_into_permanent_list_membership(tmp_path):
    store = Store(tmp_path / "root")
    snapshot = Snapshot()
    snapshot.history[0]["event_ticker"] = "LOOKED-UP"
    snapshot.lookups["/events/LOOKED-UP"] = (
        200,
        {"event": {"event_ticker": "LOOKED-UP", "series_ticker": "S1"}},
    )
    full = capture_at(store, snapshot, BASE_TIME)
    warehouse.build(store, full["capture_id"])
    publishing.publish(store, full["capture_id"])
    assert "LOOKED-UP" in keyed(store, full["capture_id"], "current_events", "event_ticker")
    snapshot = Snapshot()
    snapshot.history = []
    delta = build_delta(store, snapshot)
    assert "LOOKED-UP" not in keyed(store, delta["capture_id"], "current_events", "event_ticker")
    assert any(
        row["entity_ticker"] == "LOOKED-UP"
        for row in relation(store, delta["capture_id"], "observation_history")
    )


def test_legacy_full_without_fresh_final_scope_parent_proof_requires_a_new_capture(
    tmp_path, monkeypatch
):
    store = Store(tmp_path / "root")
    snapshot = Snapshot()
    parent = {"event_ticker": "LEGACY-PARENT", "series_ticker": "S1"}
    snapshot.history[0]["event_ticker"] = parent["event_ticker"]
    first = True

    def events(params):
        nonlocal first
        if params.get("cursor"):
            return 400, {"message": "invalid cursor"}
        if first:
            first = False
            return 200, {"events": [*snapshot.events, parent], "cursor": "expired"}
        return 200, {"events": snapshot.events, "cursor": ""}

    def legacy_hydrate(store, state, identities):
        # Legacy acquisition used every successful generation for parent discovery.
        events = {identity: value[2] for identity, value in identities.events.items()}
        markets = {identity: value[2] for identity, value in identities.markets.items()}
        existing = {work["endpoint"] for work in state["tasks"]}
        for kind, missing, prefix in (
            ("events", set(markets.values()) - events.keys(), "/events"),
            (
                "series",
                {value for value in events.values() if value} - identities.series.keys(),
                "/series",
            ),
        ):
            for identity in sorted(missing):
                endpoint = f"{prefix}/{identity}"
                if endpoint not in existing:
                    state["tasks"].append(acquisition.task(endpoint, kind, singular=True))
        more = any(not work["done"] for work in state["tasks"])
        if more:
            acquisition.save(store, state)
        return more

    snapshot.overrides["/events"] = events
    with monkeypatch.context() as patch:
        patch.setattr(acquisition, "now", lambda: BASE_TIME)
        initial = acquisition.new_capture(store, None)
        old_fields = {
            "version",
            "capture_id",
            "selected",
            "started_at",
            "completed_at",
            "complete",
            "max_response_bytes",
            "max_decoded_response_bytes",
            "compression",
            "tasks",
            "cutoff",
            "coverage",
            "error",
        }
        legacy = {key: value for key, value in initial.items() if key in old_fields}
        store.remove(f"captures/{initial['capture_id']}/pages.jsonl")
        store.write(f"captures/{initial['capture_id']}/state.json", legacy)
        patch.setattr(acquisition, "_hydrate_pages", legacy_hydrate)
        state = capture_at(store, snapshot, BASE_TIME, resume=initial["capture_id"])
    assert state["complete"]
    assert not any(endpoint == "/events/LEGACY-PARENT" for endpoint, _ in snapshot.requests)
    warehouse.build(store, state["capture_id"])
    publishing.publish(store, state["capture_id"])
    pointer = store.path("current.json").read_bytes()
    database = store.path(f"builds/{state['capture_id']}/warehouse.duckdb")
    database_digest = hashlib.sha256(database.read_bytes()).hexdigest()
    assert keyed(store, state["capture_id"], "current_markets", "market_ticker")["H"]["usable"]
    with pytest.raises(CatalogueError, match="new full|fresh|parent|reconcil"):
        publishing.publish(store, state["capture_id"], catalogue_version=2)
    assert store.path("current.json").read_bytes() == pointer
    assert hashlib.sha256(database.read_bytes()).hexdigest() == database_digest
    assert not store.path("current-v2.json").exists()


def test_new_full_refreshes_parent_seen_only_in_abandoned_generation_before_v2_rebase(tmp_path):
    store = Store(tmp_path / "root")
    snapshot = Snapshot()
    parent = {"event_ticker": "ABANDONED-PARENT", "series_ticker": "S1"}
    snapshot.history[0]["event_ticker"] = parent["event_ticker"]
    snapshot.lookups["/events/ABANDONED-PARENT"] = (404, {})
    first = True

    def events(params):
        nonlocal first
        if params.get("cursor"):
            return 400, {"message": "invalid cursor"}
        if first:
            first = False
            return 200, {"events": [*snapshot.events, parent], "cursor": "expired"}
        return 200, {"events": snapshot.events, "cursor": ""}

    snapshot.overrides["/events"] = events
    state = capture_at(store, snapshot, BASE_TIME)
    capture_id = state["capture_id"]
    assert state["complete"]
    assert ("/events/ABANDONED-PARENT", {"exclude_markets": "true"}) in snapshot.requests
    warehouse.build(store, capture_id)
    publishing.publish(store, capture_id)
    pointer = store.path("current.json").read_bytes()
    assert "ABANDONED-PARENT" in keyed(
        store, capture_id, "current_events", "event_ticker", catalogue_version=1
    )
    assert keyed(store, capture_id, "current_markets", "market_ticker", catalogue_version=1)["H"][
        "usable"
    ]
    publishing.publish(store, capture_id, catalogue_version=2)
    assert store.path("current.json").read_bytes() == pointer
    assert "ABANDONED-PARENT" not in keyed(
        store, capture_id, "current_events", "event_ticker", catalogue_version=2
    )
    assert not keyed(store, capture_id, "current_markets", "market_ticker", catalogue_version=2)[
        "H"
    ]["usable"]
    assert (
        sum(
            row["entity_kind"] == "events" and row["entity_ticker"] == "ABANDONED-PARENT"
            for row in relation(store, capture_id, "observation_history", catalogue_version=2)
        )
        == 1
    )


def test_historical_combo_survives_empty_live_and_combo_reconciliation_scopes(tmp_path):
    store = Store(tmp_path / "root")
    snapshot = Snapshot()
    historical_combo = {
        **snapshot.combos.pop(),
        "ticker": "HISTORICAL-COMBO",
        "status": "finalized",
        "result": "yes",
    }
    snapshot.live = []
    snapshot.history = [historical_combo]
    full = capture_at(store, snapshot, BASE_TIME)
    warehouse.build(store, full["capture_id"])
    publishing.publish(store, full["capture_id"])
    publishing.publish(store, full["capture_id"], catalogue_version=2)

    refreshed = Snapshot()
    refreshed.live = []
    refreshed.combos = []
    refreshed.history = [historical_combo]
    delta = build_delta(store, refreshed)
    markets = keyed(store, delta["capture_id"], "current_markets", "market_ticker")
    assert set(markets) == {"HISTORICAL-COMBO"}
    assert markets["HISTORICAL-COMBO"]["usable"]
    assert markets["HISTORICAL-COMBO"]["capture_id"] == delta["capture_id"]
    history = relation(store, delta["capture_id"], "observation_history")
    assert sum(row["entity_ticker"] == "HISTORICAL-COMBO" for row in history) == 2
    legs = relation(store, delta["capture_id"], "combo_legs")
    assert len(legs) == 1
    assert legs[0]["combo_market_ticker"] == "HISTORICAL-COMBO"
    assert not legs[0]["resolved"]


def test_stale_pinned_parent_cannot_replace_a_newer_published_branch(baseline):
    store, _, _, _ = baseline
    bounded = Store(store.root, Limits(max_requests=2))
    paused = capture_at(bounded, Snapshot(), DELTA_TIME, incremental=True)
    assert not paused["complete"]
    other = build_delta(store, Snapshot(), NEXT_TIME)
    published = publishing.publish(store, other["capture_id"], catalogue_version=2)
    resumed = capture_at(
        store,
        Snapshot(),
        "2026-01-04T12:00:00+00:00",
        resume=paused["capture_id"],
    )
    assert resumed["started_at"] == DELTA_TIME
    warehouse.build(store, resumed["capture_id"])
    pointer = store.path("current-v2.json").read_bytes()
    with pytest.raises(CatalogueError, match="stale|parent|baseline"):
        publishing.publish(store, resumed["capture_id"], catalogue_version=2)
    assert store.path("current-v2.json").read_bytes() == pointer
    assert (
        publishing.current(store, catalogue_version=2)["source_release_id"]
        == published["source_release_id"]
    )


def test_cli_versions_default_to_one_and_require_explicit_composed_publication():
    from kalshi_events.cli import parser

    cli = parser()
    for command in ("current", "replay", "rebuild", "publish"):
        assert cli.parse_args([command]).catalogue_version == 1
        assert cli.parse_args([command, "--catalogue-version", "2"]).catalogue_version == 2
    assert cli.parse_args(["capture", "--incremental"]).incremental


def test_tampered_ancestor_evidence_blocks_composed_build_and_pointer_promotion(baseline):
    store, base_id, _, _ = baseline
    delta = capture_at(store, Snapshot(), incremental=True)
    state = acquisition.read_capture(store, base_id)
    receipt = acquisition.page_receipts(store, state)[0]
    body = store.path(receipt["relative"]).with_name("body.bin")
    body.write_bytes(body.read_bytes() + b" ")
    pointer = store.path("current-v2.json").read_bytes()
    with pytest.raises(CatalogueError, match="evidence|integrity|ancestor|baseline"):
        warehouse.build(store, delta["capture_id"])
    with pytest.raises(CatalogueError):
        publishing.publish(store, delta["capture_id"], catalogue_version=2)
    assert store.path("current-v2.json").read_bytes() == pointer


@pytest.mark.parametrize("acquire_before_tampering", [False, True])
def test_grandparent_raw_tampering_is_checked_beyond_valid_immediate_baseline(
    baseline, acquire_before_tampering
):
    store, base_id, _, _ = baseline
    first = build_delta(store, Snapshot())
    publishing.publish(store, first["capture_id"], catalogue_version=2)
    next_snapshot = Snapshot()
    second = (
        capture_at(store, next_snapshot, NEXT_TIME, incremental=True)
        if acquire_before_tampering
        else None
    )
    original = acquisition.read_capture(store, base_id)
    receipt = acquisition.page_receipts(store, original)[0]
    body = store.path(receipt["relative"]).with_name("body.bin")
    body.write_bytes(body.read_bytes() + b"altered grandparent evidence")
    pointers = {name: store.path(name).read_bytes() for name in ("current.json", "current-v2.json")}
    if second is None:
        with pytest.raises(CatalogueError):
            capture_at(store, next_snapshot, NEXT_TIME, incremental=True)
        assert next_snapshot.requests == []
    else:
        with pytest.raises(CatalogueError):
            warehouse.build(store, second["capture_id"])
        with pytest.raises(CatalogueError):
            publishing.publish(store, second["capture_id"], catalogue_version=2)
    assert all(store.path(name).read_bytes() == value for name, value in pointers.items())


@pytest.mark.parametrize("missing", ["capture", "release"])
def test_missing_pinned_ancestor_fails_closed_without_replacing_publication(baseline, missing):
    store, base_id, _, version_two = baseline
    delta = capture_at(store, Snapshot(), incremental=True)
    directory = (
        f"captures/{base_id}"
        if missing == "capture"
        else f"releases/{version_two['source_release_id']}"
    )
    shutil.rmtree(store.path(directory))
    pointer = store.path("current-v2.json").read_bytes()
    with pytest.raises(CatalogueError):
        warehouse.build(store, delta["capture_id"])
    assert store.path("current-v2.json").read_bytes() == pointer
    assert not store.path(f"builds-v2/{delta['capture_id']}").exists()


@pytest.mark.parametrize(
    "damage",
    ["missing_capture", "missing_build", "corrupt_build", "corrupt_raw", "corrupt_release"],
)
def test_new_incremental_capture_verifies_baseline_before_any_request(baseline, damage):
    store, base_id, _, version_two = baseline
    if damage == "missing_capture":
        shutil.rmtree(store.path(f"captures/{base_id}"))
    elif damage == "missing_build":
        shutil.rmtree(store.path(f"builds-v2/{base_id}"))
    elif damage == "corrupt_build":
        database = store.path(f"builds-v2/{base_id}/warehouse.duckdb")
        database.write_bytes(database.read_bytes() + b"altered")
    elif damage == "corrupt_raw":
        state = acquisition.read_capture(store, base_id)
        first = acquisition.page_receipts(store, state)[0]
        body = store.path(first["relative"]).with_name("body.bin")
        body.write_bytes(body.read_bytes() + b"altered")
    else:
        parquet = store.path(f"releases/{version_two['source_release_id']}/markets.parquet")
        parquet.write_bytes(parquet.read_bytes() + b"altered")
    snapshot = Snapshot()
    with pytest.raises(CatalogueError):
        capture_at(store, snapshot, incremental=True)
    assert snapshot.requests == []


def test_incremental_targeted_capture_rejects_before_baseline_or_requests(store, monkeypatch):
    snapshot = Snapshot()
    with pytest.raises(CatalogueError, match="incremental|targeted|selected"):
        capture_at(store, snapshot, selected=["M1"], incremental=True)
    assert snapshot.requests == []
    monkeypatch.setattr(acquisition, "PublicClient", snapshot.factory)
    assert (
        main(["--root", str(store.root), "capture", "--incremental", "--market-ticker", "M1"]) == 1
    )
    assert snapshot.requests == []


def test_cross_root_ancestry_cannot_be_built_or_published(baseline, tmp_path):
    from kalshi_events import incremental

    store, _, _, _ = baseline
    delta = capture_at(store, Snapshot(), incremental=True)
    independent = Store(tmp_path / "independent-root")
    with independent.lock():
        other_identity = incremental.root_id(independent, create=True)
    assert other_identity != json.loads(store.path("root-id.json").read_text())["root_id"]
    store.write("root-id.json", json.loads(independent.path("root-id.json").read_text()))
    pointer = store.path("current-v2.json").read_bytes()
    with pytest.raises(CatalogueError, match="root|ancestr|baseline"):
        warehouse.build(store, delta["capture_id"])
    with pytest.raises(CatalogueError, match="root|ancestr|baseline|build"):
        publishing.publish(store, delta["capture_id"], catalogue_version=2)
    assert store.path("current-v2.json").read_bytes() == pointer


def test_corrupt_present_v2_pointer_never_falls_back_to_a_valid_v1_baseline(baseline):
    store, _, _, _ = baseline
    path = store.path("current-v2.json")
    pointer = json.loads(path.read_text())
    pointer["source_release_id"] = "0" * 64
    path.write_text(json.dumps(pointer))
    snapshot = Snapshot()
    with pytest.raises(CatalogueError):
        capture_at(store, snapshot, incremental=True)
    assert snapshot.requests == []


def test_eight_deltas_are_allowed_and_ninth_requires_a_new_full_rebase(baseline):
    store, base_id, _, _ = baseline
    for number in range(1, 9):
        timestamp = (datetime.fromisoformat(BASE_TIME) + timedelta(days=number)).isoformat()
        delta = build_delta(store, Snapshot(), timestamp)
        publishing.publish(store, delta["capture_id"], catalogue_version=2)
    pointer = store.path("current-v2.json").read_bytes()
    with pytest.raises(CatalogueError, match="older|watermark|rebase|baseline|stale"):
        publishing.publish(store, base_id, catalogue_version=2)
    assert store.path("current-v2.json").read_bytes() == pointer
    snapshot = Snapshot()
    with pytest.raises(CatalogueError, match="eight|8|depth|rebase"):
        capture_at(store, snapshot, "2026-01-10T12:00:00+00:00", incremental=True)
    assert snapshot.requests == []

    full = capture_at(store, Snapshot(), "2026-01-10T12:00:00+00:00")
    warehouse.build(store, full["capture_id"])
    publishing.publish(store, full["capture_id"], catalogue_version=2)
    assert capture_at(store, Snapshot(), "2026-01-11T12:00:00+00:00", incremental=True)["complete"]


def test_discard_accepts_only_unreferenced_incomplete_captures(baseline, capsys):
    store, _, _, _ = baseline
    completed = capture_at(store, Snapshot(), incremental=True)
    before = store.path("current-v2.json").read_bytes()
    assert (
        main(["--root", str(store.root), "discard", "--capture-id", completed["capture_id"]]) == 1
    )
    capsys.readouterr()
    assert store.path(f"captures/{completed['capture_id']}").is_dir()

    paused = capture_at(
        Store(store.root, Limits(max_requests=2)), Snapshot(), NEXT_TIME, incremental=True
    )
    assert not paused["complete"]
    assert main(["--root", str(store.root), "discard", "--capture-id", paused["capture_id"]]) == 0
    capsys.readouterr()
    assert not store.path(f"captures/{paused['capture_id']}").exists()
    assert all(
        json.loads(path.read_text())["capture_id"] != paused["capture_id"]
        for path in store.path("raw").glob("*/manifest.json")
    )
    assert store.path("current-v2.json").read_bytes() == before


def test_catalogue_pruning_preserves_old_targeted_capture_and_metadata_handoff(baseline, tmp_path):
    store, _, _, _ = baseline
    targeted = capture_at(store, Snapshot(), "2025-12-31T12:00:00+00:00", selected=["M1"])
    assert targeted["complete"]
    state_path = store.path(f"captures/{targeted['capture_id']}/state.json")
    frozen = state_path.read_bytes()
    receipts = acquisition.page_receipts(store, targeted)
    raw = [store.path(row["relative"]).parent for row in receipts]
    full = capture_at(store, Snapshot(), NEXT_TIME)
    warehouse.build(store, full["capture_id"])
    publishing.publish(store, full["capture_id"])
    publishing.publish(store, full["capture_id"], catalogue_version=2)
    assert state_path.read_bytes() == frozen
    assert all(path.is_dir() for path in raw)
    output = tmp_path / "retained-targeted-bundle"
    metadata.export(store, ["M1"], output, capture_id=targeted["capture_id"])
    assert metadata.verify(output)["contract"] == "oddsfox.kalshi.metadata.v1"
    market = json.loads((output / "markets.json").read_text())[0]
    assert market["capture_id"] == targeted["capture_id"]


def test_incomplete_pin_keeps_ancestry_across_rebase_until_discard_and_later_promotion(
    baseline, capsys
):
    store, base_id, legacy, _ = baseline
    first = build_delta(store, Snapshot())
    old_release = publishing.publish(store, first["capture_id"], catalogue_version=2)
    paused = capture_at(
        Store(store.root, Limits(max_requests=2)), Snapshot(), NEXT_TIME, incremental=True
    )
    assert not paused["complete"]
    old_state = acquisition.read_capture(store, first["capture_id"])
    old_raw = [
        store.path(row["relative"]).parent for row in acquisition.page_receipts(store, old_state)
    ]
    rebased = capture_at(store, Snapshot(), "2026-01-04T12:00:00+00:00")
    warehouse.build(store, rebased["capture_id"])
    publishing.publish(store, rebased["capture_id"], catalogue_version=2)
    assert publishing.current(store)["source_release_id"] == legacy["source_release_id"]
    assert store.path(f"captures/{base_id}").is_dir()
    assert store.path(f"captures/{first['capture_id']}").is_dir()
    assert store.path(f"builds-v2/{first['capture_id']}").is_dir()
    assert store.path(f"releases/{old_release['source_release_id']}").is_dir()
    assert all(path.is_dir() for path in old_raw)
    assert store.path(f"captures/{paused['capture_id']}").is_dir()

    assert main(["--root", str(store.root), "discard", "--capture-id", paused["capture_id"]]) == 0
    capsys.readouterr()
    latest = capture_at(store, Snapshot(), "2026-01-05T12:00:00+00:00")
    warehouse.build(store, latest["capture_id"])
    publishing.publish(store, latest["capture_id"], catalogue_version=2)
    assert not store.path(f"captures/{first['capture_id']}").exists()
    assert not store.path(f"builds-v2/{first['capture_id']}").exists()
    assert not store.path(f"releases/{old_release['source_release_id']}").exists()
    assert all(not path.exists() for path in old_raw)
    assert store.path(f"captures/{base_id}").is_dir()
    assert publishing.current(store)["source_release_id"] == legacy["source_release_id"]


def test_cli_rejects_unknown_catalogue_version_before_opening_root(tmp_path):
    root = tmp_path / "unopened-root"
    with pytest.raises(SystemExit):
        main(["--root", str(root), "current", "--catalogue-version", "3"])
    assert not root.exists()
