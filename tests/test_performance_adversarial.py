"""Protocol and selected-batch failure cases through the real offline HTTP client."""

from __future__ import annotations

import json
import threading
from collections import Counter

import httpx
import pytest
from conftest import CUTOFF, EVENTS, HISTORICAL, MARKET, SERIES

from kalshi_events import capture as acquisition
from kalshi_events import metadata
from kalshi_events.common import CatalogueError, digest, json_bytes
from kalshi_events.normalize import observations
from kalshi_events.transport import PublicClient, source_url


class PowerLoss(BaseException):
    pass


def public_factory(requests, responder=None):
    lock = threading.Lock()

    def factory(store, budget, **options):
        client = PublicClient(store, budget, **options)
        headers = dict(client.client.headers)
        client.client.close()

        def source(request):
            endpoint = request.url.path.removeprefix("/trade-api/v2")
            params = dict(request.url.params)
            with lock:
                requests.append((endpoint, params))
                response = responder(endpoint, params) if responder else None
            if response is None:
                payloads = {
                    "/historical/cutoff": CUTOFF,
                    "/markets": {"markets": [MARKET], "cursor": ""},
                    "/markets/MARKET": {"market": MARKET},
                    "/events/EVENT": {"event": EVENTS[0]},
                    "/events/HISTORY": {"event": EVENTS[1]},
                    "/series/SERIES": {"series": SERIES[0]},
                }
                response = (200, payloads[endpoint]) if endpoint in payloads else (404, {})
            status, payload = response
            return httpx.Response(status, stream=httpx.ByteStream(json_bytes(payload)))

        client.client = httpx.Client(
            transport=httpx.MockTransport(source), headers=headers, trust_env=False
        )
        return client

    return factory


def snapshot_path(store, capture_id):
    return store.path(f"captures/{capture_id}/state.json")


def rehash_snapshot(envelope):
    envelope["state_sha256"] = digest(
        json_bytes({"state": envelope["state"], "seen_cursors": envelope["seen_cursors"]})
    )
    envelope["snapshot_sha256"] = digest(
        json_bytes({key: value for key, value in envelope.items() if key != "snapshot_sha256"})
    )


@pytest.mark.parametrize("protocol", [None, True, False, 2, 4, 3.0, "3"])
def test_rehashed_snapshot_cannot_change_sealed_protocol(store, protocol):
    state = acquisition.new_capture(store, ["MARKET"])
    path = snapshot_path(store, state["capture_id"])
    envelope = json.loads(path.read_bytes())
    if protocol is None:
        envelope["state"].pop("acquisition_protocol")
    else:
        envelope["state"]["acquisition_protocol"] = protocol
    rehash_snapshot(envelope)
    path.write_bytes(json_bytes(envelope))

    with pytest.raises(
        CatalogueError,
        match="protocol|settings differ|checksum mismatch|selected market batch differs",
    ):
        acquisition.read_capture(store, state["capture_id"])


@pytest.mark.parametrize("protocol", [True, False, 2, 4, 3.0, "3"])
def test_rehashed_genesis_rejects_unsupported_protocol_without_snapshot(store, protocol):
    state = acquisition.new_capture(store, ["MARKET"])
    path = store.path(f"captures/{state['capture_id']}/pages.jsonl")
    record = json.loads(path.read_bytes())
    record["payload"]["state"]["acquisition_protocol"] = protocol
    record["payload"]["state_sha256"] = digest(
        json_bytes({"state": record["payload"]["state"], "seen_cursors": {}})
    )
    record["sha256"] = digest(
        json_bytes({key: value for key, value in record.items() if key != "sha256"})
    )
    path.write_bytes(json_bytes(record))
    snapshot_path(store, state["capture_id"]).unlink()

    with pytest.raises(CatalogueError, match="unsupported acquisition protocol|canonical JSON"):
        acquisition.read_capture(store, state["capture_id"])


@pytest.mark.parametrize("protocol", [None, True, False, 2, 4, 3.0, "3"])
def test_replay_binds_receipt_protocol_even_when_request_id_is_unchanged(
    store, advancing_clock, protocol
):
    state = acquisition.capture(store, selected=["MARKET"], client_factory=public_factory([]))
    relative = acquisition.page_receipts(store, state)[0]["relative"]
    path = store.path(relative)
    manifest = json.loads(path.read_bytes())
    if protocol is None:
        manifest.pop("acquisition_protocol")
    else:
        manifest["acquisition_protocol"] = protocol
    path.write_bytes(json_bytes(manifest))
    snapshot_path(store, state["capture_id"]).unlink()

    with pytest.raises(CatalogueError, match="protocol|traversal|identity"):
        acquisition.read_capture(store, state["capture_id"])


@pytest.mark.parametrize("protocol", [None, True, False, 2, 4, 3.0, "3"])
def test_orphan_recovery_rejects_mismatched_receipt_protocol_before_new_requests(
    store, advancing_clock, monkeypatch, protocol
):
    requests = []
    orphan = []
    stage = acquisition._stage

    def crash_before_journal(actual_store, state, index, relative, identities):
        orphan.append(relative)
        raise PowerLoss("raw receipt committed before journal attachment")

    monkeypatch.setattr(acquisition, "_stage", crash_before_journal)
    with pytest.raises(PowerLoss):
        acquisition.capture(store, selected=["MARKET"], client_factory=public_factory(requests))
    monkeypatch.setattr(acquisition, "_stage", stage)
    path = store.path(orphan[0])
    manifest = json.loads(path.read_bytes())
    if protocol is None:
        manifest.pop("acquisition_protocol")
    else:
        manifest["acquisition_protocol"] = protocol
    path.write_bytes(json_bytes(manifest))
    sent_before_resume = list(requests)

    with pytest.raises(CatalogueError, match="protocol|traversal|identity"):
        acquisition.capture(
            store,
            resume=manifest["capture_id"],
            client_factory=public_factory(requests),
        )
    assert requests == sent_before_resume


@pytest.mark.parametrize("protocol", ["new", "legacy-v1", "legacy-v2"])
def test_parent_request_parameters_follow_capture_protocol(store, advancing_clock, protocol):
    requests = []
    legacy = protocol != "new"
    if legacy:
        state = acquisition.new_capture(store, ["MARKET"])
        logical = dict(state)
        if protocol == "legacy-v1":
            logical.pop("journal_version")
        logical.pop("acquisition_protocol")
        logical.pop("rate_policy", None)
        logical.pop("parent_policy", None)
        logical.pop("cutoff_policy", None)
        logical.pop("acquisition_policy_sha256", None)
        logical.pop("compression", None)
        logical["tasks"] = [
            acquisition.task("/historical/cutoff", None, singular=True),
            acquisition.task("/markets/MARKET", "markets", singular=True, requested="MARKET"),
        ]
        logical["task_scopes"] = {}
        store.remove(f"captures/{state['capture_id']}/pages.jsonl")
        if protocol == "legacy-v1":
            store.write(f"captures/{state['capture_id']}/state.json", logical)
        else:
            acquisition._initialize_v2(store, logical)
        result = acquisition.capture(
            store, resume=state["capture_id"], client_factory=public_factory(requests)
        )
    else:
        result = acquisition.capture(
            store, selected=["MARKET"], client_factory=public_factory(requests)
        )
    assert result["complete"]
    parent_requests = [(path, params) for path, params in requests if path == "/events/EVENT"]
    assert parent_requests == [("/events/EVENT", {} if legacy else {"exclude_markets": "true"})]
    persisted = acquisition.read_capture(store, result["capture_id"])
    assert persisted["complete"]


@pytest.mark.parametrize("historical_found", [False, True])
def test_only_final_batch_generation_counts_for_omission_fallback(
    store, advancing_clock, historical_found
):
    requests = []
    batches = 0

    def source(endpoint, params):
        nonlocal batches
        if endpoint == "/markets":
            batches += 1
            if params.get("cursor"):
                return 400, {"message": "expired cursor"}
            if batches == 1:
                return 200, {"markets": [HISTORICAL], "cursor": "expired"}
            return 200, {"markets": [MARKET], "cursor": ""}
        if endpoint == "/historical/markets/HISTORICAL" and historical_found:
            return 200, {"market": HISTORICAL}
        return None

    state = acquisition.capture(
        store,
        selected=["HISTORICAL", "MARKET"],
        client_factory=public_factory(requests, source),
    )

    assert state["complete"]
    assert state["coverage"]["HISTORICAL"]["status"] == ("found" if historical_found else "absent")
    counts = Counter(path for path, _ in requests)
    assert counts["/markets"] == 3
    assert counts["/markets/HISTORICAL"] == 1
    assert counts["/historical/markets/HISTORICAL"] == 1
    assert counts["/markets/MARKET"] == 0
    batch = next(work for work in state["tasks"] if work["endpoint"] == "/markets")
    assert batch["restarts"] == 1
    assert not state["task_scopes"]
    assert acquisition.read_capture(store, state["capture_id"])["coverage"] == state["coverage"]


@pytest.mark.parametrize("missing_parent", ["event", "series"])
@pytest.mark.parametrize("journal_only", [False, True], ids=["snapshot", "journal-only"])
def test_final_batch_absence_survives_missing_parent_of_retained_observation(
    store, advancing_clock, tmp_path, missing_parent, journal_only
):
    requests = []
    generations = 0

    def source(endpoint, params):
        nonlocal generations
        if endpoint == "/markets":
            if params.get("cursor"):
                return 400, {"message": "expired cursor"}
            generations += 1
            if generations == 1:
                return 200, {"markets": [HISTORICAL], "cursor": "expired"}
            return 200, {"markets": [MARKET], "cursor": ""}
        if endpoint == "/events/HISTORY":
            if missing_parent == "event":
                return 404, {}
            return 200, {"event": {**EVENTS[1], "series_ticker": "MISSING-SERIES"}}
        return None

    selected = ["HISTORICAL", "MARKET"]
    state = acquisition.capture(
        store, selected=selected, client_factory=public_factory(requests, source)
    )
    assert state["complete"]
    absence = {
        "market_ticker": "HISTORICAL",
        "status": "absent",
        "error": "not returned by live or historical market lookup",
    }
    assert state["coverage"]["HISTORICAL"] == absence
    assert state["coverage"]["MARKET"]["status"] == "found"
    counts = Counter(path for path, _ in requests)
    assert counts["/markets"] == 3
    assert counts["/markets/HISTORICAL"] == 1
    assert counts["/historical/markets/HISTORICAL"] == 1
    assert counts["/events/HISTORY"] == 1
    if missing_parent == "series":
        assert counts["/series/MISSING-SERIES"] == 1

    capture_id = state["capture_id"]
    if journal_only:
        snapshot_path(store, capture_id).unlink()
    replayed = acquisition.read_capture(store, capture_id)
    assert replayed["complete"]
    assert replayed["coverage"] == state["coverage"]
    retained = list(observations(acquisition.capture_pages(store, replayed)))
    retained_market = [
        row
        for row in retained
        if row["entity_kind"] == "markets" and row["market_ticker"] == "HISTORICAL"
    ]
    assert len(retained_market) == 1
    assert json.loads(retained_market[0]["payload_json"]) == HISTORICAL

    output = tmp_path / "metadata"
    exported = metadata.export(store, selected, output, capture_id=capture_id)
    verified = metadata.verify(output)
    assert verified["source_release_id"] == exported["source_release_id"]
    coverage = json.loads((output / "coverage.json").read_bytes())
    assert coverage[0] == absence
    assert [(row["market_ticker"], row["status"]) for row in coverage] == [
        ("HISTORICAL", "absent"),
        ("MARKET", "found"),
    ]
    assert [row["market_ticker"] for row in json.loads((output / "markets.json").read_bytes())] == [
        "MARKET"
    ]
    sent_before_resume = list(requests)
    resumed = acquisition.capture(
        store, resume=capture_id, client_factory=public_factory(requests, source)
    )
    assert resumed["complete"] and resumed["coverage"] == state["coverage"]
    assert requests == sent_before_resume


@pytest.mark.parametrize("ticker_length", [8, 256])
def test_batch_partition_keeps_maximum_selection_inside_encoded_url_bound(
    store, advancing_clock, ticker_length
):
    selected = [f"M{index:03d}" + "X" * (ticker_length - 4) for index in range(100)]
    requests = []

    def source(endpoint, params):
        if endpoint == "/markets":
            return 200, {
                "markets": [{**MARKET, "ticker": value} for value in params["tickers"].split(",")],
                "cursor": "",
            }
        return None

    state = acquisition.capture(
        store, selected=selected, client_factory=public_factory(requests, source)
    )

    assert state["complete"] and set(state["coverage"]) == set(selected)
    batches = [params for endpoint, params in requests if endpoint == "/markets"]
    assert len(batches) == 1 if ticker_length == 8 else len(batches) > 1
    observed = []
    for params in batches:
        tickers = params["tickers"].split(",")
        assert 1 <= len(tickers) <= 100
        url = httpx.Request("GET", source_url() + "/markets", params=params).url
        assert len(str(url).encode("ascii")) <= 6144
        observed.extend(tickers)
    assert Counter(observed) == Counter(selected)
    assert not any(endpoint.startswith("/markets/") for endpoint, _ in requests)


def test_batch_response_with_unrequested_ticker_fails_closed(store, advancing_clock):
    def source(endpoint, params):
        if endpoint == "/markets":
            return 200, {"markets": [{**MARKET, "ticker": "UNREQUESTED"}], "cursor": ""}
        return None

    with pytest.raises(CatalogueError, match="requested|selected|batch|identity"):
        acquisition.capture(store, selected=["MARKET"], client_factory=public_factory([], source))
    capture_id = next(store.path("captures").iterdir()).name
    assert acquisition.read_capture(store, capture_id)["complete"] is False
    assert list(store.path("raw").glob("*/body.bin"))


def test_crash_after_terminal_batch_append_adds_each_fallback_once(
    store, advancing_clock, monkeypatch
):
    requests = []
    append = store.append
    crashed = False

    def crash_after_append(relative, body):
        nonlocal crashed
        append(relative, body)
        record = json.loads(body)
        if not crashed and record.get("op") == "page":
            manifest_path = store.path(record["payload"]["relative"])
            manifest = json.loads(manifest_path.read_bytes())
            if manifest["endpoint"] == "/markets":
                crashed = True
                raise PowerLoss("terminal batch page durable before state installation")

    monkeypatch.setattr(store, "append", crash_after_append)
    with pytest.raises(PowerLoss):
        acquisition.capture(
            store,
            selected=["HISTORICAL", "MARKET"],
            client_factory=public_factory(requests),
        )
    monkeypatch.setattr(store, "append", append)
    capture_id = next(store.path("captures").iterdir()).name
    state = acquisition.capture(store, resume=capture_id, client_factory=public_factory(requests))

    assert crashed and state["complete"]
    assert state["coverage"]["HISTORICAL"]["status"] == "absent"
    counts = Counter(path for path, _ in requests)
    assert counts["/markets"] == 1
    assert counts["/markets/HISTORICAL"] == 1
    assert counts["/historical/markets/HISTORICAL"] == 1
    tasks = Counter(work["endpoint"] for work in state["tasks"])
    assert tasks["/markets/HISTORICAL"] == tasks["/historical/markets/HISTORICAL"] == 1
    replayed = acquisition.read_capture(store, capture_id)
    assert replayed["coverage"] == state["coverage"]
