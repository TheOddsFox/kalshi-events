from __future__ import annotations

import json

import conftest
import duckdb
import pyarrow.parquet as pq
import pytest
from conftest import CUTOFF, FakeClient

from kalshi_events import capture, incremental, metrics, publish, refresh
from kalshi_events.capture import _receipt_matches_context, _receipt_matches_state_protocol
from kalshi_events.common import CatalogueError, Limits, Store
from kalshi_events.publish import _validate_v2_manifest

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
    "last_price_dollars": "0.01234",
    "volume_fp": "12.3456789",
}


def _factory(responses):
    def create(store, budget, **kwargs):
        return FakeClient(store, budget, responses=responses)

    return create


def _capture(store, *, events, markets, history=(), incremental_mode=False):
    responses = {
        "/historical/cutoff": (200, CUTOFF),
        "/series/KXCPI": (200, {"series": SERIES}),
        "/events": (200, {"events": list(events), "cursor": ""}),
        "/markets": (200, {"markets": list(markets), "cursor": ""}),
        "/historical/markets": (200, {"markets": list(history), "cursor": ""}),
    }
    return capture.capture(
        store,
        client_factory=_factory(responses),
        incremental=incremental_mode,
    )


def test_series_baseline_delta_removal_and_reappearance(tmp_path):
    store = Store(tmp_path / "series", series_ticker="KXCPI")
    baseline = _capture(store, events=[EVENT], markets=[MARKET])
    first_release = publish.publish(store, baseline["capture_id"], catalogue_version=2)

    assert first_release["contract"] == "oddsfox.kalshi.series.v1"
    assert first_release["coverage_scope"] == "series"
    assert first_release["series_ticker"] == "KXCPI"
    assert first_release["include_multivariate"] is False
    assert first_release["series_scope"] == store.scope
    assert store.path("current-series.json").is_file()
    assert not store.path("current-v2.json").exists()
    assert not store.path("current.json").exists()
    assert publish.current(store, catalogue_version=2)["source_release_id"] == first_release[
        "source_release_id"
    ]
    assert pq.read_table(
        store.path(f"releases/{first_release['source_release_id']}/coverage.parquet")
    ).to_pylist()[0]["coverage_scope"] == "series"

    removed = _capture(store, events=[], markets=[], incremental_mode=True)
    assert removed["complete"] is True
    assert "query_from" not in removed
    assert removed["baseline"]["release_contract"] == "oddsfox.kalshi.series.v1"
    assert removed["baseline"]["series_scope"] == store.scope
    second_release = publish.publish(store, removed["capture_id"], catalogue_version=2)
    assert second_release["delta_depth"] == 1
    assert second_release["baseline_release_id"] == first_release["source_release_id"]
    assert publish.current(store, catalogue_version=2)["capture_id"] == removed["capture_id"]
    with duckdb.connect(
        str(store.path(f"builds-v2/{removed['capture_id']}/warehouse.duckdb")), read_only=True
    ) as connection:
        assert connection.execute("select count(*) from main.current_events").fetchone()[0] == 0
        assert connection.execute("select count(*) from main.current_markets").fetchone()[0] == 0
        assert connection.execute("select count(*) from main.observation_history").fetchone()[0] == 4

    reappeared = _capture(store, events=[EVENT], markets=[MARKET], incremental_mode=True)
    third_release = publish.publish(store, reappeared["capture_id"], catalogue_version=2)
    assert third_release["delta_depth"] == 2
    with duckdb.connect(
        str(store.path(f"builds-v2/{reappeared['capture_id']}/warehouse.duckdb")), read_only=True
    ) as connection:
        assert connection.execute("select count(*) from main.current_events").fetchone()[0] == 1
        assert connection.execute("select count(*) from main.current_markets").fetchone()[0] == 1
        assert connection.execute("select count(*) from main.observation_history").fetchone()[0] == 7


def test_series_baseline_does_not_fall_back_to_catalogue_pointer(tmp_path):
    store = Store(tmp_path / "series", series_ticker="KXCPI")
    store.path("current.json").parent.mkdir(parents=True, exist_ok=True)
    store.write("current.json", {"source_release_id": "0" * 64, "manifest_sha256": "0" * 64})

    with store.writer_guard(), pytest.raises(CatalogueError, match="series baseline"):
        incremental.baseline(store)


def test_series_manifest_rejects_non_boolean_nested_scope_flag():
    manifest = {
        "root_id": "0" * 32,
        "capture_id": "1" * 32,
        "delta_depth": 0,
        "membership_sha256": "2" * 64,
        "baseline_release_id": None,
        "baseline_manifest_sha256": None,
        "baseline_capture_id": None,
        "previous_watermark": None,
        "watermark": None,
        "watermark_started_at": "2026-01-01T00:00:00Z",
        "coverage_scope": "series",
        "series_ticker": "KXCPI",
        "include_multivariate": False,
        "series_scope": {
            "kind": "series",
            "series_ticker": "KXCPI",
            "include_multivariate": 0,
        },
    }

    with pytest.raises(CatalogueError, match="operator series scope"):
        _validate_v2_manifest(manifest, series=True)


def test_receipt_matchers_reject_non_boolean_scoped_flag():
    scope = {"kind": "series", "series_ticker": "KXCPI", "include_multivariate": False}
    malformed = {**scope, "include_multivariate": 0}
    manifest = {"acquisition_protocol": 4, "series_scope": malformed}

    assert not _receipt_matches_context(
        manifest, {"acquisition_protocol": 4, "series_scope": scope}
    )
    assert not _receipt_matches_state_protocol(
        manifest, {"acquisition_protocol": 4, "scope": scope}
    )


def test_series_pin_rejects_non_boolean_scope_flag_before_release_lookup(tmp_path):
    store = Store(tmp_path / "series", series_ticker="KXCPI")
    with store.writer_guard():
        identity = incremental.root_id(store, create=True)
    pin = {
        "root_id": identity,
        "series_scope": {**store.scope, "include_multivariate": 0},
    }

    with pytest.raises(CatalogueError, match="operator series scope"):
        incremental.verify_pin(store, {"baseline": pin})


def test_series_resume_rejects_non_boolean_receipt_scope_before_network(tmp_path):
    store = Store(tmp_path / "series", Limits(max_requests=2), series_ticker="KXCPI")
    receipt_paths = []

    class TrackingClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            relative = super().get(endpoint, params, context)
            receipt_paths.append(relative)
            return relative

    def factory(root, budget, **kwargs):
        return TrackingClient(root, budget, responses={"/historical/cutoff": (200, CUTOFF)})

    stopped = capture.capture(store, client_factory=factory)
    assert stopped["complete"] is False
    assert receipt_paths
    receipt = store.path(receipt_paths[0])
    manifest = json.loads(receipt.read_text())
    manifest["series_scope"]["include_multivariate"] = 0
    receipt.write_text(json.dumps(manifest))

    class NoNetworking(FakeClient):
        def get(self, *_args, **_kwargs):
            pytest.fail("malformed receipt must fail before another request")

    with pytest.raises(CatalogueError):
        capture.capture(
            Store(store.root),
            resume=stopped["capture_id"],
            client_factory=lambda root, budget, **kwargs: NoNetworking(root, budget),
        )


def test_series_refresh_depth_error_recommends_series_command(tmp_path, monkeypatch):
    store = Store(tmp_path / "series", series_ticker="KXCPI")
    store.path("current-series.json").touch()

    def depth_failure(*_args, **_kwargs):
        raise CatalogueError("eight deltas already depend on this baseline")

    monkeypatch.setattr(capture, "capture", depth_failure)
    with store.writer_guard():
        result, code = refresh.run(store)

    assert code == 1
    assert "run series refresh --full" in result["error"]


def test_refresh_metrics_include_acquisition_phase(tmp_path, monkeypatch):
    store = Store(tmp_path / "series", series_ticker="KXCPI")
    store.path("current-series.json").touch()
    monkeypatch.setattr(
        capture,
        "capture",
        lambda *_args, **_kwargs: {
            "capture_id": "a" * 32,
            "complete": True,
            "requests": 1,
            "downloaded_bytes": 12,
            "accounting_complete": True,
        },
    )
    monkeypatch.setattr(
        publish,
        "publish",
        lambda *_args, **_kwargs: {"output": "release", "source_release_id": "b" * 64},
    )

    with metrics.collect() as measurements, store.writer_guard():
        result, code = refresh.run(store)

    assert code == 0
    assert result["complete"] is True
    assert "capture" in measurements.report()["phases_seconds"]


def test_incremental_winner_with_404_parent_cannot_complete(tmp_path, monkeypatch):
    store = Store(tmp_path / "series", series_ticker="KXCPI")
    event_one = {**EVENT, "event_ticker": "KXCPI-26JAN"}
    event_two = {**EVENT, "event_ticker": "KXCPI-26FEB", "title": "February CPI"}
    market_one = {**MARKET, "event_ticker": event_one["event_ticker"]}
    market_two = {**MARKET, "event_ticker": event_two["event_ticker"]}
    started_at = ["2026-01-01T12:00:00Z"]
    received_at = ["2026-01-01T12:00:02Z"]
    monkeypatch.setattr(capture, "now", lambda: started_at[0])
    monkeypatch.setattr(conftest, "now", lambda: received_at[0])

    baseline = _capture(store, events=[event_one], markets=[market_one])
    publish.publish(store, baseline["capture_id"], catalogue_version=2)

    started_at[0] = "2026-01-01T12:00:01Z"
    received_at[0] = "2026-01-01T12:00:01Z"
    responses = {
        "/historical/cutoff": (200, CUTOFF),
        "/series/KXCPI": (200, {"series": SERIES}),
        "/events": (200, {"events": [event_two], "cursor": ""}),
        "/markets": (200, {"markets": [market_two], "cursor": ""}),
        "/historical/markets": (200, {"markets": [], "cursor": ""}),
        "/events/KXCPI-26JAN": (404, {}),
    }

    with pytest.raises(CatalogueError, match="authoritative event parent"):
        capture.capture(
            store,
            client_factory=_factory(responses),
            incremental=True,
        )
