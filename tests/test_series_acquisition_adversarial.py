"""Series captures must not certify foreign data or lose committed work on resume."""

from __future__ import annotations

import copy
import json
import threading

import pytest
from conftest import FakeClient

from kalshi_events import capture
from kalshi_events.common import CatalogueError, Limits, Store

SERIES = {"ticker": "KXCPI", "title": "Monthly inflation"}
LIVE_EVENT = {"event_ticker": "KXCPI-26SEP", "series_ticker": "KXCPI"}
OLD_EVENT = {"event_ticker": "KXCPI-25DEC", "series_ticker": "KXCPI"}
LIVE_MARKET = {"ticker": "KXCPI-26SEP-T0.1", "event_ticker": "KXCPI-26SEP"}
OLD_MARKET = {"ticker": "KXCPI-25DEC-T0.1", "event_ticker": "KXCPI-25DEC"}


def _responses():
    return {
        "/series/KXCPI": (200, {"series": copy.deepcopy(SERIES)}),
        "/events": (200, {"events": [LIVE_EVENT, OLD_EVENT], "cursor": ""}),
        "/markets": (200, {"markets": [LIVE_MARKET], "cursor": ""}),
        "/historical/markets": (200, {"markets": [OLD_MARKET], "cursor": ""}),
        "/events/KXCPI-26SEP": (200, {"event": LIVE_EVENT}),
        "/events/KXCPI-25DEC": (200, {"event": OLD_EVENT}),
    }


def _factory(responses=None, *, receipts=None):
    selected = _responses() if responses is None else responses

    class Client(FakeClient):
        def __init__(self, store, budget, **options):
            super().__init__(store, budget, selected, **options)

        def get(self, endpoint, params=None, context=None):
            relative = super().get(endpoint, params, context)
            if receipts is not None:
                receipts.append((context["request_id"], relative))
            return relative

    return Client


@pytest.mark.parametrize(
    "failure",
    [
        "missing-series",
        "wrong-series-definition",
        "foreign-event",
        "event-without-series",
        "live-combo",
        "live-combo-legs",
        "historical-combo",
        "missing-live-parent",
        "foreign-live-parent",
        "missing-historical-parent",
    ],
)
def test_invalid_series_responses_leave_incomplete_capture_with_evidence(tmp_path, failure):
    store = Store(tmp_path / "series", series_ticker="KXCPI")
    responses = _responses()
    if failure == "missing-series":
        responses["/series/KXCPI"] = (404, {})
    elif failure == "wrong-series-definition":
        responses["/series/KXCPI"] = (200, {"series": {"ticker": "KXFED"}})
    elif failure == "foreign-event":
        responses["/events"] = (
            200,
            {"events": [{**LIVE_EVENT, "series_ticker": "KXFED"}], "cursor": ""},
        )
    elif failure == "event-without-series":
        responses["/events"] = (
            200,
            {"events": [{"event_ticker": LIVE_EVENT["event_ticker"]}], "cursor": ""},
        )
    elif failure in {"live-combo", "live-combo-legs", "historical-combo"}:
        endpoint = "/historical/markets" if failure == "historical-combo" else "/markets"
        market = OLD_MARKET if failure == "historical-combo" else LIVE_MARKET
        combo_fields = {"mve_collection_ticker": "COMBO-COLLECTION"}
        if failure == "live-combo-legs":
            combo_fields = {
                "mve_selected_legs": [
                    {
                        "event_ticker": OLD_EVENT["event_ticker"],
                        "market_ticker": OLD_MARKET["ticker"],
                        "side": "yes",
                    }
                ]
            }
        responses[endpoint] = (
            200,
            {"markets": [{**market, **combo_fields}], "cursor": ""},
        )
    elif failure in {"missing-live-parent", "foreign-live-parent"}:
        responses["/events"] = (200, {"events": [OLD_EVENT], "cursor": ""})
        responses["/events/KXCPI-26SEP"] = (
            (404, {})
            if failure == "missing-live-parent"
            else (200, {"event": {**LIVE_EVENT, "series_ticker": "KXFED"}})
        )
    else:
        responses["/events"] = (200, {"events": [LIVE_EVENT], "cursor": ""})
        responses["/events/KXCPI-25DEC"] = (404, {})
    allocated = []

    with pytest.raises(CatalogueError):
        capture.capture(store, client_factory=_factory(responses), _on_capture_id=allocated.append)

    assert len(allocated) == 1
    recovered = capture.read_capture(Store(store.root), allocated[0])
    assert recovered["complete"] is False
    assert list(store.path("raw").glob("*/manifest.json"))
    with pytest.raises(CatalogueError, match="incomplete"):
        capture.complete_capture(Store(store.root), allocated[0])
    assert not store.path("current-series.json").exists()
    assert not store.path("current.json").exists()
    assert not store.path("current-v2.json").exists()


@pytest.mark.parametrize("allowance", ["requests", "downloads"])
def test_series_budget_stop_reuses_every_committed_receipt(tmp_path, allowance):
    limits = (
        Limits(max_requests=2)
        if allowance == "requests"
        else Limits(
            max_download_bytes=256,
            max_response_bytes=256,
            max_decoded_response_bytes=256,
        )
    )
    store = Store(tmp_path / "series", limits, series_ticker="KXCPI")
    receipts = []
    factory = _factory(receipts=receipts)
    stopped = capture.capture(store, client_factory=factory)
    assert stopped["complete"] is False
    assert stopped["budget_exhausted"] is True
    assert receipts
    committed = {relative: store.path(relative).read_bytes() for _, relative in receipts}

    resumed = capture.capture(
        Store(store.root), resume=stopped["capture_id"], client_factory=factory
    )

    assert resumed["complete"] is True
    request_ids = [request_id for request_id, _ in receipts]
    assert len(request_ids) == len(set(request_ids))
    assert all(store.path(relative).read_bytes() == body for relative, body in committed.items())
    assert len(list(capture.capture_pages(Store(store.root), resumed))) == len(receipts)


def test_series_deadline_after_committed_response_retains_resumable_receipts(tmp_path):
    store = Store(tmp_path / "series", Limits(max_duration_seconds=30), series_ticker="KXCPI")
    receipts = []
    lock = threading.Lock()

    class ExpiringClient(_factory(receipts=receipts)):
        def get(self, endpoint, params=None, context=None):
            relative = super().get(endpoint, params, context)
            with lock:
                self.store.started -= 60
            return relative

    stopped = capture.capture(store, client_factory=ExpiringClient)
    assert stopped["complete"] is False
    assert stopped["budget_exhausted"] is True
    assert "deadline" in stopped["error"]
    assert receipts

    resumed = capture.capture(
        Store(store.root), resume=stopped["capture_id"], client_factory=_factory(receipts=receipts)
    )

    assert resumed["complete"] is True
    request_ids = [request_id for request_id, _ in receipts]
    assert len(request_ids) == len(set(request_ids))


def test_resuming_series_capture_rejects_changed_receipt_scope_before_network(tmp_path):
    store = Store(tmp_path / "series", Limits(max_requests=2), series_ticker="KXCPI")
    receipts = []
    stopped = capture.capture(store, client_factory=_factory(receipts=receipts))
    assert stopped["complete"] is False
    assert receipts
    manifest_path = store.path(receipts[0][1])
    manifest = json.loads(manifest_path.read_text())
    manifest["series_scope"]["series_ticker"] = "KXFED"
    manifest_path.write_text(json.dumps(manifest))
    retained_receipts = set(store.path("raw").glob("*/manifest.json"))

    class NoNetworking(_factory()):
        def get(self, *_args, **_kwargs):
            pytest.fail("tampered receipt must fail before another request")

    with pytest.raises(CatalogueError):
        capture.capture(
            Store(store.root), resume=stopped["capture_id"], client_factory=NoNetworking
        )

    assert set(store.path("raw").glob("*/manifest.json")) == retained_receipts
    assert not store.path("current-series.json").exists()


def test_series_recovers_finalized_orphan_receipts_after_worker_interruption(tmp_path):
    class PowerLoss(BaseException):
        pass

    store = Store(tmp_path / "series", series_ticker="KXCPI")
    receipts = []
    allocated = []
    lock = threading.Lock()
    interrupted = False

    class InterruptedClient(_factory(receipts=receipts)):
        def get(self, endpoint, params=None, context=None):
            nonlocal interrupted
            relative = super().get(endpoint, params, context)
            with lock:
                if not interrupted:
                    interrupted = True
                    raise PowerLoss("receipt finalized before page journal")
            return relative

    with pytest.raises(PowerLoss):
        capture.capture(store, client_factory=InterruptedClient, _on_capture_id=allocated.append)
    assert len(allocated) == 1
    committed = {relative: store.path(relative).read_bytes() for _, relative in receipts}
    assert committed

    resumed = capture.capture(
        Store(store.root), resume=allocated[0], client_factory=_factory(receipts=receipts)
    )

    assert resumed["complete"] is True
    request_ids = [request_id for request_id, _ in receipts]
    assert len(request_ids) == len(set(request_ids))
    assert all(store.path(relative).read_bytes() == body for relative, body in committed.items())
    manifests = [
        json.loads(Store(store.root).path(relative).read_bytes()) for _, relative in receipts
    ]
    assert all(manifest["series_scope"]["series_ticker"] == "KXCPI" for manifest in manifests)
