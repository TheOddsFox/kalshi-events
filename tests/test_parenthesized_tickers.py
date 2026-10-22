"""Observed provider-native punctuation must survive ingestion without weakening boundaries."""

from __future__ import annotations

import json
from collections import Counter
from decimal import Decimal
from pathlib import Path

import duckdb
import httpx
import pytest
from conftest import CUTOFF, FakeClient

from kalshi_events import capture, metrics
from kalshi_events.common import (
    Budget,
    CatalogueError,
    Store,
    digest,
    json_bytes,
    selections,
    series_scope,
    ticker,
)
from kalshi_events.normalize import ParentIndex, observations
from kalshi_events.publish import publish, verify_release
from kalshi_events.transport import PublicClient, read_page, source_url

EVENT = "KXCITIESWEATHER-25MAR04(NY)(PHIL)"
GDP_EVENT = "GDP-232022 Q4"
GDP_EVENT_ROW = {"event_ticker": GDP_EVENT, "series_ticker": "KXGDP"}
GDP_MARKET = {
    "ticker": "GDP-MARKET-1",
    "event_ticker": GDP_EVENT,
    "last_price_dollars": "0.123456789",
}
WEATHER_EVENT = {"event_ticker": EVENT, "series_ticker": "KXCITIESWEATHER"}
WEATHER_MARKET = {
    "ticker": "WEATHER-MARKET(NY)",
    "event_ticker": EVENT,
    "last_price_dollars": "0.123456789",
}
COMBO_MARKET = {
    "ticker": "COMBO-MARKET",
    "event_ticker": "COMBO-EVENT",
    "mve_collection_ticker": "COLLECTION",
    "mve_selected_legs": [
        {
            "event_ticker": EVENT,
            "market_ticker": WEATHER_MARKET["ticker"],
            "side": "yes",
            "yes_settlement_value_dollars": "0.00001",
        }
    ],
}


def _responses():
    return {
        "/historical/cutoff": (200, CUTOFF),
        "/markets": (200, {"markets": [WEATHER_MARKET, COMBO_MARKET], "cursor": ""}),
        f"/events/{EVENT}": (200, {"event": WEATHER_EVENT}),
        "/events/COMBO-EVENT": (
            200,
            {"event": {"event_ticker": "COMBO-EVENT", "series_ticker": "KXCITIESWEATHER"}},
        ),
        "/series/KXCITIESWEATHER": (200, {"series": {"ticker": "KXCITIESWEATHER"}}),
    }


def _gdp_responses(*, full=False):
    responses = {
        "/historical/cutoff": (200, CUTOFF),
        "/markets": (200, {"markets": [GDP_MARKET], "cursor": ""}),
        f"/events/{GDP_EVENT}": (200, {"event": GDP_EVENT_ROW}),
        "/series/KXGDP": (200, {"series": {"ticker": "KXGDP"}}),
    }
    if full:
        responses.update(
            {
                "/series": (200, {"series": [{"ticker": "KXGDP"}], "cursor": ""}),
                "/events": (200, {"events": [GDP_EVENT_ROW], "cursor": ""}),
                "/events/multivariate": (200, {"events": [], "cursor": ""}),
                "/historical/markets": (200, {"markets": [], "cursor": ""}),
            }
        )
    return responses


@pytest.mark.parametrize("native", [EVENT, GDP_EVENT])
def test_observed_native_ticker_is_preserved(native):
    assert ticker(native) == native


def test_native_ticker_accepts_exact_length_boundary_and_terminal_punctuation():
    longest = "A" + "B" * 254 + "-"
    assert len(longest) == 256
    assert ticker(longest) == longest
    spaced_longest = "A" + "Q4 " * 84 + "Q4-"
    assert len(spaced_longest) == 256
    assert ticker(spaced_longest) == spaced_longest
    with pytest.raises(CatalogueError, match="invalid native ticker"):
        ticker(spaced_longest + "A")
    assert ticker("A") == "A"
    assert ticker("A1") == "A1"
    assert ticker("A-") == "A-"
    assert ticker("A.") == "A."
    assert ticker("A_") == "A_"
    assert ticker("A(") == "A("
    assert ticker("A)") == "A)"
    with pytest.raises(CatalogueError, match="invalid native ticker"):
        ticker(longest + "A")


def test_operator_selections_preserve_native_spaces_exactly():
    values = ["GDP-232022Q4", GDP_EVENT, "GDP-232022  Q4"]
    assert selections(values) == values


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        False,
        123,
        b"EVENT(NY)",
        "",
        "(EVENT)",
        ".EVENT(NY)",
        "EVENT(NY)/OTHER",
        "EVENT(NY)\\OTHER",
        "EVENT(NY),OTHER",
        "EVENT(NY)%2FOTHER",
        "EVENT(NY)?x=1",
        "EVENT(NY)#OTHER",
        "EVENT(NY)\x00",
        "EVENT(NY)\n",
        "EVENT(NY)\r",
        "EVENT(NY)\t",
        " EVENT(NY)",
        "EVENT(NY) ",
        " GDP-232022 Q4",
        "GDP-232022 Q4 ",
        "GDP-232022\tQ4",
        "GDP-232022\r\nQ4",
        "GDP-232022\u00a0Q4",
        "EVENT(NY)+OTHER",
        "EVENT(NY);OTHER",
        "EVENT(NY)'OTHER",
        'EVENT(NY)"OTHER',
        "EVENT(NY):OTHER",
        "EVENT(NY)@OTHER",
        "EVENT(NY)[OTHER]",
        "EVENT(NY){OTHER}",
        "ÉVENT(NY)",
    ],
)
def test_parentheses_do_not_admit_other_invalid_native_values(value):
    with pytest.raises(CatalogueError, match="invalid native ticker"):
        ticker(value)


@pytest.mark.parametrize(
    "prefix",
    ["/events", "/series", "/markets", "/historical/markets", "/multivariate_event_collections"],
)
@pytest.mark.parametrize("native", [EVENT, GDP_EVENT])
def test_native_singular_endpoint_uses_fixed_metrics_family(prefix, native):
    family = f"{prefix}/{{ticker}}"
    assert {metrics.endpoint_family(f"{prefix}/{native}-{index}") for index in range(100)} == {
        family
    }


@pytest.mark.parametrize(
    ("native", "payload", "expected_path"),
    [
        (EVENT, {"event": WEATHER_EVENT}, f"/events/{EVENT}"),
        (GDP_EVENT, {"event": GDP_EVENT_ROW}, "/events/GDP-232022%20Q4"),
    ],
)
def test_native_singular_request_preserves_identity_and_encodes_path_and_query(
    store, native, payload, expected_path
):
    requests = []

    def source(request):
        requests.append(request)
        return httpx.Response(200, stream=httpx.ByteStream(json_bytes(payload)))

    with PublicClient(store, Budget(store)) as client:
        client.client.close()
        client.client = httpx.Client(transport=httpx.MockTransport(source), trust_env=False)
        manifest, body = read_page(
            store,
            client.get(
                f"/events/{ticker(native)}",
                params={"series_ticker": native},
            ),
        )
    assert len(requests) == 1
    assert requests[0].url.host == httpx.URL(source_url()).host
    assert requests[0].url.raw_path.split(b"?", 1)[0].endswith(expected_path.encode("ascii"))
    assert requests[0].url.params["series_ticker"] == native
    if " " in native:
        assert b"+" in requests[0].url.query
    assert manifest["endpoint"] == f"/events/{native}"
    assert manifest["params"] == {"series_ticker": native}
    assert body["event"]["event_ticker"] == native


def test_parenthesized_singular_request_retains_approved_origin_and_exact_identity(store):
    requests = []

    def source(request):
        requests.append(request)
        return httpx.Response(200, stream=httpx.ByteStream(json_bytes({"event": WEATHER_EVENT})))

    with metrics.collect() as measured, PublicClient(store, Budget(store)) as client:
        client.client.close()
        client.client = httpx.Client(transport=httpx.MockTransport(source), trust_env=False)
        manifest, payload = read_page(store, client.get(f"/events/{ticker(EVENT)}"))
    assert len(requests) == 1
    expected = httpx.URL(source_url())
    assert requests[0].url.scheme == expected.scheme
    assert requests[0].url.host == expected.host
    assert requests[0].url.path == f"{expected.path}/events/{EVENT}"
    assert requests[0].url.query == b""
    assert manifest["endpoint"] == f"/events/{EVENT}"
    assert payload["event"]["event_ticker"] == EVENT
    assert set(measured.report()["endpoint_families"]) == {"/events/{ticker}"}


@pytest.mark.parametrize("multivariate", [False, True])
def test_parentheses_heavy_parent_batches_measure_encoded_urls(multivariate):
    selected = [f"E{index:03d}-" + "(NY)" * 30 for index in range(100)]
    groups = capture._event_parent_batches(list(reversed(selected)), multivariate=multivariate)
    assert [value for group in groups for value in group] == selected
    assert any(len(group) >= 20 for group in groups)
    assert any(len(group) == 1 for group in groups)
    endpoint = "/events/multivariate" if multivariate else "/events"
    for group in groups:
        request = httpx.Request(
            "GET",
            source_url() + endpoint,
            params={"tickers": ",".join(group), "limit": 200, "with_nested_markets": "false"},
        )
        assert request.url.params["tickers"].split(",") == group
        assert b"%28" in request.url.raw_path and b"%29" in request.url.raw_path
        assert len(str(request.url).encode("ascii")) <= 6144
        assert capture._event_batch_url_size(group, multivariate=multivariate) == len(
            str(request.url).encode("ascii")
        )


def test_spaced_parent_batch_roundtrips_through_httpx_url_bound():
    selected = [f"GDP-{index:03d}-" + "Q4 " * 17 + "Q4" for index in range(100)]
    groups = capture._event_parent_batches(list(reversed(selected)), multivariate=False)
    assert len(groups) > 1
    assert [value for group in groups for value in group] == selected
    for group in groups:
        request = httpx.Request(
            "GET",
            source_url() + "/events",
            params={"tickers": ",".join(group), "limit": 200, "with_nested_markets": "false"},
        )
        assert request.url.params["tickers"].split(",") == group
        assert b"+" in request.url.raw_path
        encoded_size = len(str(request.url).encode("ascii"))
        assert encoded_size <= 6144
        assert capture._event_batch_url_size(group, multivariate=False) == encoded_size


def test_parentheses_heavy_market_selection_batches_preserve_operator_order():
    selected = [f"M{index:03d}-" + "(NY)" * 30 for index in reversed(range(100))]
    groups = capture._selected_market_batches(selections(selected))
    assert len(groups) > 1
    assert [value for group in groups for value in group] == selected
    for group in groups:
        request = httpx.Request(
            "GET", source_url() + "/markets", params={"limit": 1000, "tickers": ",".join(group)}
        )
        assert request.url.params["tickers"].split(",") == group
        assert len(str(request.url).encode("ascii")) <= 6144


def test_spaced_market_selection_batches_preserve_operator_order_and_url_limit():
    selected = [f"M{index:03d}-" + "Q4 " * 17 + "Q4" for index in reversed(range(100))]
    groups = capture._selected_market_batches(selections(selected))
    assert len(groups) > 1
    assert [value for group in groups for value in group] == selected
    for group in groups:
        request = httpx.Request(
            "GET", source_url() + "/markets", params={"limit": 1000, "tickers": ",".join(group)}
        )
        assert request.url.params["tickers"].split(",") == group
        assert b"+" in request.url.raw_path
        assert len(str(request.url).encode("ascii")) <= 6144


@pytest.mark.parametrize("spaced", [False, True], ids=["parentheses", "interior-space"])
def test_native_parent_hydration_preserves_market_leg_and_receipt_provenance(store, spaced):
    event, series = (GDP_EVENT, "KXGDP") if spaced else (EVENT, "KXCITIESWEATHER")
    markets = [GDP_MARKET] if spaced else [WEATHER_MARKET, COMBO_MARKET]
    state = capture.capture(
        store,
        selected=[market["ticker"] for market in markets],
        client_factory=lambda s, b, **options: FakeClient(
            s, b, _gdp_responses() if spaced else _responses(), **options
        ),
    )
    assert state["complete"]
    pages = list(capture.capture_pages(store, state))
    rows = list(observations(pages))
    parent = next(
        row for row in rows if row.get("event_ticker") == event and row["entity_kind"] == "events"
    )
    manifest, _ = next(
        (manifest, payload)
        for manifest, payload in pages
        if manifest["endpoint"] == f"/events/{event}"
    )
    assert manifest["params"] == {"exclude_markets": "true"}
    assert parent["capture_id"] == state["capture_id"]
    assert parent["page_id"] == manifest["page_id"]
    assert parent["payload_sha256"] == manifest["sha256"]
    assert parent["observation_id"] == digest(json_bytes([manifest["page_id"], "/event/0"]))
    assert parent["series_ticker"] == series
    expected_event = GDP_EVENT_ROW if spaced else WEATHER_EVENT
    assert json.loads(parent["payload_json"]) == expected_event
    index = ParentIndex()
    for page, payload in pages:
        index.add_page(page, payload)
    assert index.events[event][2] == series
    assert index.markets[markets[0]["ticker"]][2] == event
    assert index.effective_parent_evidence(state)[1][("events", event)][0] == 200
    if not spaced:
        assert index.markets[WEATHER_MARKET["ticker"]][2] == EVENT
    for native in markets:
        row = next(row for row in rows if row.get("market_ticker") == native["ticker"])
        assert json.loads(row["payload_json"]) == native


@pytest.mark.parametrize(("event", "series"), [(EVENT, "KXCITIESWEATHER"), (GDP_EVENT, "KXGDP")])
def test_native_event_keeps_series_scope_enforcement(event, series):
    manifest = {
        "entity_kind": "events",
        "status": 200,
        "endpoint": "/events",
        "page_id": "a" * 32,
        "received_at": "2026-01-01T00:00:00Z",
        "series_scope": series_scope(series),
    }
    index = ParentIndex()
    native = {"event_ticker": event, "series_ticker": series}
    index.add_page(manifest, {"events": [native]})
    assert index.events[event][2] == series
    with pytest.raises(CatalogueError, match="outside the scoped series"):
        index.add_page(
            {**manifest, "page_id": "b" * 32},
            {"events": [{**native, "series_ticker": "OTHER"}]},
        )


@pytest.mark.parametrize(("event", "series"), [(EVENT, "KXCITIESWEATHER"), (GDP_EVENT, "KXGDP")])
def test_native_event_winner_uses_receipt_time_then_observation_id(event, series):
    pages = []
    for page_id, received_at, series, updated_time in [
        ("a", "2026-01-02T00:00:00Z", "SERIES-A", "2025-01-01T00:00:00Z"),
        ("b", "2026-01-02T00:00:00Z", "SERIES-B", "2025-01-01T00:00:00Z"),
        ("c", "2026-01-01T00:00:00Z", "OLDER-RECEIPT", "2099-01-01T00:00:00Z"),
    ]:
        pages.append(
            (
                {
                    "entity_kind": "events",
                    "status": 200,
                    "endpoint": "/events",
                    "capture_id": "d" * 32,
                    "page_id": page_id * 32,
                    "sha256": "e" * 64,
                    "received_at": received_at,
                },
                {
                    "events": [
                        {
                            "event_ticker": event,
                            "series_ticker": series,
                            "updated_time": updated_time,
                        }
                    ]
                },
            )
        )
    expected = max(observations(pages), key=lambda row: (row["received_at"], row["observation_id"]))
    assert expected["series_ticker"] != "OLDER-RECEIPT"
    for order in [pages, list(reversed(pages))]:
        index = ParentIndex()
        for manifest, payload in order:
            index.add_page(manifest, payload)
        assert index.events[event] == (
            expected["received_at"],
            expected["observation_id"],
            expected["series_ticker"],
        )


@pytest.mark.parametrize("spaced", [False, True], ids=["parentheses", "interior-space"])
def test_native_orphan_parent_receipt_resumes_without_reissuing_request(store, spaced):
    class PowerLoss(BaseException):
        pass

    event, market = (GDP_EVENT, GDP_MARKET) if spaced else (EVENT, WEATHER_MARKET)
    responses = _gdp_responses() if spaced else _responses()
    calls = []
    allocated = []
    orphan = []

    class Client(FakeClient):
        def __init__(self, store, budget, **options):
            super().__init__(store, budget, responses, **options)

        def get(self, endpoint, params=None, context=None):
            calls.append((endpoint, (context or {}).get("request_id")))
            receipt = super().get(endpoint, params, context)
            if endpoint == f"/events/{event}" and not orphan:
                orphan.append((receipt, context["request_id"]))
                raise PowerLoss("parent receipt finalized before page journal append")
            return receipt

    with pytest.raises(PowerLoss):
        capture.capture(
            store,
            selected=[market["ticker"]],
            client_factory=Client,
            _on_capture_id=allocated.append,
        )
    assert len(allocated) == len(orphan) == 1
    resumed = capture.capture(Store(store.root), resume=allocated[0], client_factory=Client)
    assert resumed["complete"]
    assert Counter(request_id for _, request_id in calls)[orphan[0][1]] == 1
    pages = list(capture.capture_pages(Store(store.root), resumed))
    recovered = [page for page, _ in pages if page.get("request_id") == orphan[0][1]]
    assert len(recovered) == 1
    assert orphan[0][0] == f"raw/{recovered[0]['page_id']}/manifest.json"
    assert any(
        row["entity_kind"] == "events" and row["event_ticker"] == event
        for row in observations(pages)
    )


def test_parenthesized_identity_survives_verified_full_v2_publication(store):
    responses = {
        **_responses(),
        "/series": (200, {"series": [{"ticker": "KXCITIESWEATHER"}]}),
        "/events": (200, {"events": [WEATHER_EVENT], "cursor": ""}),
        "/events/multivariate": (
            200,
            {
                "events": [{"event_ticker": "COMBO-EVENT", "series_ticker": "KXCITIESWEATHER"}],
                "cursor": "",
            },
        ),
        "/historical/markets": (200, {"markets": [], "cursor": ""}),
    }
    state = capture.capture(
        store,
        client_factory=lambda s, b, **options: FakeClient(s, b, responses, **options),
    )
    assert state["complete"]
    release = publish(store, state["capture_id"], catalogue_version=2)
    verified = verify_release(Path(release["output"]))
    assert verified["capture_id"] == state["capture_id"]
    assert not store.path("current.json").exists()
    with duckdb.connect() as connection:
        assert connection.execute(
            "select event_ticker, last_price_dollars from read_parquet(?) where market_ticker = ?",
            [str(Path(release["output"]) / "markets.parquet"), WEATHER_MARKET["ticker"]],
        ).fetchall() == [(EVENT, Decimal("0.123456789"))]
        assert connection.execute(
            "select event_ticker, market_ticker, leg_index, resolved from read_parquet(?)",
            [str(Path(release["output"]) / "combo_legs.parquet")],
        ).fetchall() == [(EVENT, WEATHER_MARKET["ticker"], 0, True)]


def test_spaced_identity_survives_verified_full_v2_publication(store):
    state = capture.capture(
        store,
        client_factory=lambda s, b, **options: FakeClient(
            s, b, _gdp_responses(full=True), **options
        ),
    )
    assert state["complete"]
    release = publish(store, state["capture_id"], catalogue_version=2)
    verified = verify_release(Path(release["output"]))
    assert verified["capture_id"] == state["capture_id"]
    with duckdb.connect() as connection:
        assert connection.execute(
            "select event_ticker, last_price_dollars from read_parquet(?) where market_ticker = ?",
            [str(Path(release["output"]) / "markets.parquet"), GDP_MARKET["ticker"]],
        ).fetchall() == [(GDP_EVENT, Decimal("0.123456789"))]
