from __future__ import annotations

from decimal import Decimal

import duckdb
import pytest
from conftest import CUTOFF, FakeClient

from kalshi_events import capture, warehouse
from kalshi_events.common import CatalogueError, Store

SERIES = {"ticker": "KXCPI", "title": "Consumer Price Index"}
EVENT = {
    "event_ticker": "KXCPI-26JAN",
    "series_ticker": "KXCPI",
    "title": "January 2026 CPI",
}
MARKET = {
    "ticker": "KXCPI-26JAN-CPI",
    "event_ticker": EVENT["event_ticker"],
    "status": "active",
    "last_price_dollars": "0.00123",
    "volume_fp": "12.3456789",
    "open_interest_fp": "2.125",
    "floor_strike": "3.123456789123456789",
}


def _factory(responses):
    def create(store, budget, **kwargs):
        return FakeClient(store, budget, responses=responses)

    return create


def test_scoped_full_build_is_certified_as_internal_v2_with_exact_values(tmp_path):
    store = Store(tmp_path / "series", series_ticker="KXCPI")
    responses = {
        "/historical/cutoff": (200, CUTOFF),
        "/series/KXCPI": (200, {"series": SERIES}),
        "/events": (200, {"events": [EVENT], "cursor": ""}),
        "/markets": (200, {"markets": [MARKET], "cursor": ""}),
        "/historical/markets": (200, {"markets": [], "cursor": ""}),
    }

    acquired = capture.capture(store, client_factory=_factory(responses))
    receipt = warehouse.build_v2_full(store, acquired["capture_id"])

    assert receipt["catalogue_version"] == 2
    assert receipt["scope"] == "series"
    assert receipt["series_scope"] == {
        "kind": "series",
        "series_ticker": "KXCPI",
        "include_multivariate": False,
    }
    assert receipt["root_id"] == acquired["root_id"]
    assert store.path(f"builds-v2/{acquired['capture_id']}/build.json").is_file()
    assert not store.path(f"builds/{acquired['capture_id']}").exists()
    assert warehouse.certified(store, acquired["capture_id"], catalogue_version=2) == receipt
    with pytest.raises(CatalogueError, match="only internal v2"):
        warehouse.certified(store, acquired["capture_id"], catalogue_version=1)

    database = store.path(f"builds-v2/{acquired['capture_id']}/warehouse.duckdb")
    with duckdb.connect(str(database), read_only=True) as connection:
        values = connection.execute(
            "select last_price_dollars, volume_fp, open_interest_fp "
            "from main.current_markets where market_ticker = ?",
            [MARKET["ticker"]],
        ).fetchone()
        assert values == (
            Decimal("0.00123"),
            Decimal("12.3456789"),
            Decimal("2.125"),
        )
        payload = connection.execute(
            "select payload_json from main.observation_history "
            "where entity_ticker = ? order by received_at desc limit 1",
            [MARKET["ticker"]],
        ).fetchone()[0]
        assert '"floor_strike":"3.123456789123456789"' in payload
        assert (
            connection.execute("select count(*) from main.observation_history").fetchone()[0] == 3
        )


def test_scoped_empty_capture_builds_valid_empty_v2_warehouse(tmp_path):
    store = Store(tmp_path / "series", series_ticker="KXCPI")
    responses = {
        "/historical/cutoff": (200, CUTOFF),
        "/series/KXCPI": (200, {"series": SERIES}),
        "/events": (200, {"events": [], "cursor": ""}),
        "/markets": (200, {"markets": [], "cursor": ""}),
        "/historical/markets": (200, {"markets": [], "cursor": ""}),
    }

    acquired = capture.capture(store, client_factory=_factory(responses))
    receipt = warehouse.build(store, acquired["capture_id"])

    assert receipt["scope"] == "series"
    with duckdb.connect(
        str(store.path(f"builds-v2/{acquired['capture_id']}/warehouse.duckdb")),
        read_only=True,
    ) as connection:
        assert connection.execute("select count(*) from main.current_events").fetchone()[0] == 0
        assert connection.execute("select count(*) from main.current_markets").fetchone()[0] == 0


def test_scoped_build_routes_incremental_captures_to_delta_builder(tmp_path, monkeypatch):
    store = Store(tmp_path / "series", series_ticker="KXCPI")
    state = {"capture_id": "capture-id", "incremental": True}
    from kalshi_events import incremental

    monkeypatch.setattr(incremental, "finish_pending_retirement", lambda _store: None)
    monkeypatch.setattr(warehouse, "catalogue_capture", lambda _store, _id: state)
    monkeypatch.setattr(warehouse, "_build_delta", lambda _store, value: value)

    with store.writer_guard():
        assert warehouse._build_locked(store, "capture-id") is state
