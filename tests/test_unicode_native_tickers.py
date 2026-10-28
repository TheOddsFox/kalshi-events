"""Printable Unicode ticker identities survive acquisition without normalization."""

from __future__ import annotations

import json

import httpx
import pytest
from conftest import CUTOFF, MARKET, FakeClient

from kalshi_events import capture, metadata
from kalshi_events.common import (
    Budget,
    CatalogueError,
    Store,
    json_bytes,
    selections,
    ticker,
    ticker_from_path_segment,
    ticker_path_segment,
)
from kalshi_events.normalize import ParentIndex, observations
from kalshi_events.transport import PublicClient, read_page

NATIVE = "KXLALIGAGOAL-26SEP19OSARVC-RVCPDÍAZ4-1"
ACCENTED_EVENT = "LALIGA-26SEP19-RVCPDÍAZ"
ACCENTED_SERIES = "LALIGA-Í"
COMPOSED = "EVENT-Caf\u00e9"
DECOMPOSED = "EVENT-Cafe\u0301"


def test_printable_unicode_ticker_length_counts_code_points():
    assert ticker("Í") == "Í"
    longest = "Í" * 256
    assert len(longest) == 256
    assert ticker(longest) == longest
    with pytest.raises(CatalogueError, match="invalid native ticker"):
        ticker(longest + "Í")


def test_composed_and_decomposed_tickers_remain_distinct_in_selection_and_index():
    assert selections([COMPOSED, DECOMPOSED]) == [COMPOSED, DECOMPOSED]
    assert COMPOSED != DECOMPOSED
    assert ticker_path_segment(COMPOSED) != ticker_path_segment(DECOMPOSED)

    index = ParentIndex()
    manifest = {
        "entity_kind": "events",
        "status": 200,
        "endpoint": "/events",
        "page_id": "a" * 32,
        "received_at": "2026-01-01T00:00:00Z",
    }
    index.add_page(
        manifest,
        {
            "events": [
                {"event_ticker": COMPOSED, "series_ticker": "SERIES"},
                {"event_ticker": DECOMPOSED, "series_ticker": "SERIES"},
            ]
        },
    )

    assert set(index.events) == {COMPOSED, DECOMPOSED}
    assert index.events[COMPOSED][2] == index.events[DECOMPOSED][2] == "SERIES"


@pytest.mark.parametrize(
    "value",
    [
        "A\u0085B",  # C1 control
        "A\u00a0B",  # no-break space
        "A\u2003B",  # em space
        "A\u2028B",  # line separator
        "A\u2029B",  # paragraph separator
        "A\ud800B",  # lone surrogate
        "A\\B",
        " A",
        "../A",
        "A/../B",
    ],
)
def test_unicode_controls_whitespace_and_path_forms_remain_invalid(value):
    with pytest.raises(CatalogueError, match="invalid native ticker"):
        ticker(value)


@pytest.mark.parametrize("segment", ["%C3", "%FF", "%c3%8d", "Í"])
def test_unicode_path_decoder_rejects_invalid_or_noncanonical_spellings(segment):
    with pytest.raises(CatalogueError, match="invalid native ticker path segment"):
        ticker_from_path_segment(segment)


def test_unicode_and_literal_percent_path_segments_decode_once():
    unicode_segment = ticker_path_segment("Í")
    literal_percent_segment = ticker_path_segment("%C3%8D")

    assert unicode_segment == "%C3%8D"
    assert literal_percent_segment == "%25C3%258D"
    assert ticker_from_path_segment(unicode_segment) == "Í"
    assert ticker_from_path_segment(literal_percent_segment) == "%C3%8D"
    assert ticker_from_path_segment(literal_percent_segment) != ticker_from_path_segment(
        unicode_segment
    )
    assert ticker_from_path_segment("%252E%252E%252FA") == "%2E%2E%2FA"
    with pytest.raises(CatalogueError, match="invalid native ticker"):
        ticker_from_path_segment("%2E%2E%2FA")

    # Established ASCII spellings remain byte-for-byte stable.
    assert ticker_path_segment("GDP-232022 Q4") == "GDP-232022 Q4"
    assert ticker_path_segment("A%2F") == "A%252F"


@pytest.mark.parametrize("native", [NATIVE, "%C3%8D"])
def test_singular_request_preserves_unicode_and_literal_percent_identity(store, native):
    encoded = ticker_path_segment(native)
    if native == NATIVE:
        assert encoded == NATIVE.replace("Í", "%C3%8D")
    else:
        assert encoded == "%25C3%258D"
    endpoint = f"/events/{encoded}"
    payload = {"event": {"event_ticker": native, "series_ticker": "SERIES"}}
    requests = []

    def source(request):
        requests.append(request)
        return httpx.Response(200, stream=httpx.ByteStream(json_bytes(payload)))

    with PublicClient(store, Budget(store)) as client:
        client.client.close()
        client.client = httpx.Client(transport=httpx.MockTransport(source), trust_env=False)
        manifest, body = read_page(store, client.get(endpoint))

    assert len(requests) == 1
    assert requests[0].url.raw_path.endswith(endpoint.encode("ascii"))
    assert manifest["endpoint"] == endpoint
    assert body["event"]["event_ticker"] == native
    assert ticker_from_path_segment(encoded) == native


def _unicode_responses():
    market = {**MARKET, "ticker": NATIVE, "event_ticker": ACCENTED_EVENT}
    event = {"event_ticker": ACCENTED_EVENT, "series_ticker": ACCENTED_SERIES}
    series = {"ticker": ACCENTED_SERIES, "title": "Synthetic series"}
    return (
        market,
        event,
        series,
        {
            "/historical/cutoff": (200, CUTOFF),
            "/markets": (200, {"markets": [market], "cursor": ""}),
            f"/events/{ticker_path_segment(ACCENTED_EVENT)}": (200, {"event": event}),
            f"/series/{ticker_path_segment(ACCENTED_SERIES)}": (200, {"series": series}),
        },
    )


def test_selected_unicode_market_survives_batch_parent_index_and_metadata_export(store, tmp_path):
    market, event, series, responses = _unicode_responses()
    calls = []

    class Client(FakeClient):
        def __init__(self, root, budget, **options):
            super().__init__(root, budget, responses=responses, **options)

        def get(self, endpoint, params=None, context=None):
            calls.append((endpoint, dict(params or {})))
            return super().get(endpoint, params, context)

    state = capture.capture(store, selected=[NATIVE], client_factory=Client)
    assert state["complete"]
    assert state["selected"] == [NATIVE]
    assert state["coverage"][NATIVE]["status"] == "found"
    assert ("/markets", {"limit": 1000, "tickers": NATIVE}) in calls
    assert (f"/events/{ticker_path_segment(ACCENTED_EVENT)}", {"exclude_markets": "true"}) in calls

    pages = list(capture.capture_pages(Store(store.root), state))
    index = ParentIndex()
    for manifest, payload in pages:
        index.add_page(manifest, payload)
    assert index.markets[NATIVE][2] == ACCENTED_EVENT
    assert index.events[ACCENTED_EVENT][2] == ACCENTED_SERIES
    assert index.parent_lookups[("events", ACCENTED_EVENT)][0] == 200

    output = tmp_path / "unicode-metadata"
    metadata.export(Store(store.root), [NATIVE], output, capture_id=state["capture_id"])
    assert json.loads((output / "markets.json").read_text())[0]["market_ticker"] == NATIVE
    assert (
        json.loads((output / "events.json").read_text())[0]["event_ticker"] == event["event_ticker"]
    )
    assert json.loads((output / "series.json").read_text())[0]["series_ticker"] == series["ticker"]
    assert json.loads((output / "coverage.json").read_text()) == [
        {"market_ticker": NATIVE, "status": "found", "error": None}
    ]


def test_unicode_parent_orphan_receipt_resumes_without_reissuing_request(store):
    class PowerLoss(BaseException):
        pass

    _, _, _, responses = _unicode_responses()
    parent_endpoint = f"/events/{ticker_path_segment(ACCENTED_EVENT)}"
    calls = []
    allocated = []
    orphan = []

    class Client(FakeClient):
        def __init__(self, root, budget, **options):
            super().__init__(root, budget, responses=responses, **options)

        def get(self, endpoint, params=None, context=None):
            calls.append((endpoint, (context or {}).get("request_id")))
            receipt = super().get(endpoint, params, context)
            if endpoint == parent_endpoint and not orphan:
                orphan.append((receipt, context["request_id"]))
                raise PowerLoss("parent receipt finalized before page journal append")
            return receipt

    with pytest.raises(PowerLoss):
        capture.capture(
            store,
            selected=[NATIVE],
            client_factory=Client,
            _on_capture_id=allocated.append,
        )

    resumed = capture.capture(Store(store.root), resume=allocated[0], client_factory=Client)
    assert resumed["complete"]
    assert sum(request_id == orphan[0][1] for endpoint, request_id in calls) == 1
    pages = list(capture.capture_pages(Store(store.root), resumed))
    parent = next(page for page, _ in pages if page.get("request_id") == orphan[0][1])
    assert parent["endpoint"] == parent_endpoint
    assert any(row.get("event_ticker") == ACCENTED_EVENT for row in observations(pages))
