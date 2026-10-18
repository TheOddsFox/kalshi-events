from __future__ import annotations

import json

import httpx
import pytest
from conftest import COMBO, EVENTS, MARKET, FakeClient

from kalshi_events import capture as acquisition
from kalshi_events.common import CatalogueError
from kalshi_events.metadata import export
from kalshi_events.normalize import project_pages
from kalshi_events.transport import PublicClient


def _legacy_checkpoint(store, state):
    logical = dict(state)
    logical.pop("journal_version")
    logical.pop("acquisition_protocol", None)
    if logical.get("selected") is not None:
        logical["tasks"] = [
            acquisition.task("/historical/cutoff", None, singular=True),
            *[
                acquisition.task(
                    f"/markets/{value}", "markets", singular=True, requested=value
                )
                for value in logical["selected"]
            ],
        ]
    store.remove(f"captures/{state['capture_id']}/pages.jsonl")
    store.write(f"captures/{state['capture_id']}/state.json", logical)
    return logical


def test_combo_leg_with_wrong_native_event_remains_unresolved(store):
    combo = {
        **COMBO,
        "mve_selected_legs": [{**COMBO["mve_selected_legs"][0], "event_ticker": "HISTORY"}],
    }
    state = acquisition.capture(
        store,
        client_factory=lambda s, b: FakeClient(
            s, b, {"/markets": (200, {"markets": [MARKET, combo], "cursor": ""})}
        ),
    )
    projection = project_pages(acquisition.capture_pages(store, state))
    assert state["complete"]
    assert projection["combo_legs"][0]["event_ticker"] == "HISTORY"
    assert projection["combo_legs"][0]["market_ticker"] == "MARKET"
    assert projection["combo_legs"][0]["resolved"] is False


@pytest.mark.parametrize("status", ["initialized", "disputed", "amended"])
def test_metadata_preserves_native_lifecycle_and_settlement_strings(store, tmp_path, status):
    native = {
        **MARKET,
        "status": status,
        "result": "yes",
        "settlement_value_dollars": "0.000012345678901234",
        "volume_fp": "1000000000000.123456789012345678",
    }
    state = acquisition.capture(
        store,
        selected=["MARKET"],
        client_factory=lambda s, b: FakeClient(
            s, b, {"/markets/MARKET": (200, {"market": native})}
        ),
    )
    output = tmp_path / "metadata"
    export(store, ["MARKET"], output, capture_id=state["capture_id"])
    market = json.loads((output / "markets.json").read_text())[0]
    assert {
        key: market[key] for key in ("status", "result", "settlement_value_dollars", "volume_fp")
    } == {key: native[key] for key in ("status", "result", "settlement_value_dollars", "volume_fp")}


@pytest.mark.parametrize("failure", ["repeated", "invalidated"])
def test_cursor_failure_stays_incomplete_and_cannot_export(store, tmp_path, failure):
    def events(params):
        if failure == "invalidated" and params.get("cursor"):
            return 410, {"message": "cursor expired"}
        return 200, {"events": [EVENTS[0]], "cursor": "next"}

    with pytest.raises(CatalogueError, match="cursor"):
        acquisition.capture(
            store, client_factory=lambda s, b: FakeClient(s, b, {"/events": events})
        )
    capture_id = next(store.path("captures").iterdir()).name
    state = acquisition.read_capture(store, capture_id)
    assert not state["complete"]
    assert sum(1 for _ in acquisition.capture_pages(store, state)) >= 4
    with pytest.raises(CatalogueError, match="incomplete"):
        export(store, ["MARKET"], tmp_path / "metadata", capture_id=capture_id)
    assert not (tmp_path / "metadata").exists()


def test_direct_market_lookup_cannot_substitute_another_native_identity(store):
    with pytest.raises(CatalogueError, match="different native identity|unrequested ticker"):
        acquisition.capture(
            store,
            selected=["MARKET"],
            client_factory=lambda s, b: FakeClient(
                s, b, {"/markets/MARKET": (200, {"market": {**MARKET, "ticker": "OTHER"}})}
            ),
        )
    state = acquisition.read_capture(store, next(store.path("captures").iterdir()).name)
    assert state["complete"] is False
    assert "MARKET" not in state["coverage"]


@pytest.mark.parametrize("boundary", ["before_attachment", "before_checkpoint", "tampered_receipt"])
def test_raw_market_commit_before_pending_checkpoint_is_reused_on_resume(
    store, monkeypatch, advancing_clock, boundary
):
    initial = acquisition.new_capture(store, ["MARKET"])
    _legacy_checkpoint(store, initial)
    original = acquisition.save
    original_get = PublicClient.get
    requested = []

    def factory(actual_store, budget):
        client = PublicClient(actual_store, budget)
        client.client.close()

        def source(request):
            endpoint = request.url.path.removeprefix("/trade-api/v2")
            requested.append(endpoint)
            status, payload = FakeClient(actual_store, budget).response(
                endpoint, dict(request.url.params)
            )
            return httpx.Response(status, stream=httpx.ByteStream(json.dumps(payload).encode()))

        client.client = httpx.Client(transport=httpx.MockTransport(source), trust_env=False)
        return client

    def interrupted(actual_store, state):
        if boundary == "before_checkpoint" and any(
            work["pending"] and work["endpoint"] == "/markets/MARKET" for work in state["tasks"]
        ):
            raise RuntimeError("process death before pending checkpoint")
        original(actual_store, state)

    def interrupted_receipt(client, endpoint, params=None, context=None):
        relative = original_get(client, endpoint, params, context=context)
        if boundary != "before_checkpoint" and endpoint == "/markets/MARKET":
            raise RuntimeError("process death before pending checkpoint")
        return relative

    monkeypatch.setattr(acquisition, "save", interrupted)
    monkeypatch.setattr(PublicClient, "get", interrupted_receipt)
    with pytest.raises(RuntimeError, match="pending checkpoint"):
        acquisition.capture(store, resume=initial["capture_id"], client_factory=factory)
    capture_id = initial["capture_id"]
    state = acquisition.read_capture(store, capture_id)
    # The market receipt was committed to raw evidence but never checkpointed as pending.
    assert state["tasks"][1]["endpoint"] == "/markets/MARKET"
    assert state["tasks"][1]["pending"] is None
    assert len(list(store.path("raw").glob("*/manifest.json"))) == 2
    monkeypatch.setattr(acquisition, "save", original)
    monkeypatch.setattr(PublicClient, "get", original_get)
    if boundary == "tampered_receipt":
        receipt = next(
            path
            for path in store.path("raw").glob("*/manifest.json")
            if json.loads(path.read_text())["endpoint"] == "/markets/MARKET"
        )
        (receipt.parent / "body.bin").write_bytes(b"{}")
        with pytest.raises(CatalogueError, match="integrity"):
            acquisition.capture(store, resume=capture_id, client_factory=factory)
        assert requested.count("/markets/MARKET") == 1
        assert acquisition.read_capture(store, capture_id)["complete"] is False
        return
    resumed = acquisition.capture(store, resume=capture_id, client_factory=factory)
    assert resumed["complete"] and resumed["requests"] == 2
    assert requested.count("/markets/MARKET") == 1
    projected = project_pages(acquisition.capture_pages(store, resumed))
    assert [row["market_ticker"] for row in projected["markets"]] == ["MARKET"]
    assert len(list(store.path("raw").glob("*/manifest.json"))) == 4
