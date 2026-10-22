from __future__ import annotations

import hashlib
import json
import threading
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from conftest import COMBO, CUTOFF, EVENTS, HISTORICAL, MARKET, FakeClient

from kalshi_events import capture as acquisition
from kalshi_events import metadata, metrics, warehouse
from kalshi_events import publish as publishing
from kalshi_events.common import CatalogueError, json_bytes
from kalshi_events.normalize import project_pages


def body_paths(store, state):
    return {
        store.path(row["relative"]).with_name("body.bin")
        for row in acquisition.page_receipts(store, state)
    }


def assert_one_pass(measured, count):
    counters = measured.report()["counters"]
    assert counters.get("raw_body_reads", 0) == count
    assert counters.get("raw_body_json_decodes", 0) == count


def test_pipeline_fuses_evidence_passes_without_losing_cutoffs_or_fresh_verification(
    store, tmp_path, monkeypatch
):
    cutoffs = []
    guard = threading.Lock()

    def cutoff(_):
        with guard:
            value = {
                **CUTOFF,
                "market_settled_ts": f"2026-01-{len(cutoffs) + 1:02}T00:00:00Z",
            }
            cutoffs.append(value)
        return 200, value

    def historical(params):
        page = int(params.get("cursor", "0"))
        return 200, {
            "markets": [{**HISTORICAL, "ticker": f"HISTORICAL-{page}"}],
            "cursor": str(page + 1) if page < 2 else "",
        }

    with metrics.collect() as captured:
        state = acquisition.capture(
            store,
            client_factory=lambda s, b, **_options: FakeClient(
                s, b, {"/historical/cutoff": cutoff, "/historical/markets": historical}
            ),
        )
    assert state["complete"]
    paths = body_paths(store, state)
    assert_one_pass(captured, len(paths))

    opened = Counter()
    original = Path.open

    def audited_open(path, mode="r", *args, **kwargs):
        if path in paths and "r" in mode:
            opened[path] += 1
        return original(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", audited_open)
    with metrics.collect() as loaded:
        warehouse.load(store, state["capture_id"])
    assert_one_pass(loaded, len(paths))
    assert opened == Counter({path: 1 for path in paths})

    opened.clear()
    with metrics.collect() as built:
        warehouse.build(store, state["capture_id"])
    assert_one_pass(built, len(paths))
    assert opened == Counter({path: 1 for path in paths})

    opened.clear()
    with metrics.collect() as published:
        release = publishing.publish(store, state["capture_id"])
    counters = published.report()["counters"]
    assert counters.get("raw_body_json_decodes", 0) == len(cutoffs)
    assert all(opened[path] >= 1 for path in paths)
    coverage = pq.read_table(Path(release["output"]) / "coverage.parquet").to_pylist()
    retained = json.loads(coverage[0]["cutoff_json"])
    assert len(retained) == len(cutoffs) == 4
    assert sorted(json_bytes(row["cutoff"]) for row in retained) == sorted(
        json_bytes(value) for value in cutoffs
    )

    opened.clear()
    with metrics.collect() as exported:
        metadata.export(store, ["MARKET"], tmp_path / "metadata", capture_id=state["capture_id"])
    assert_one_pass(exported, len(paths))
    assert opened == Counter({path: 1 for path in paths})

    for _ in range(2):
        with metrics.collect() as completed:
            acquisition.complete_capture(store, state["capture_id"])
        assert_one_pass(completed, len(paths))

    pointer = store.path("current.json").read_bytes()
    market_receipt = next(
        row
        for row in acquisition.page_receipts(store, state)
        if json.loads(store.path(row["relative"]).read_text())["endpoint"] == "/markets"
    )
    body = store.path(market_receipt["relative"]).with_name("body.bin")
    body.write_bytes(body.read_bytes() + b" ")
    with pytest.raises(CatalogueError, match="integrity|evidence"):
        publishing.publish(store, state["capture_id"])
    assert store.path("current.json").read_bytes() == pointer
    with pytest.raises(CatalogueError, match="integrity|evidence"):
        acquisition.complete_capture(store, state["capture_id"])


@pytest.mark.parametrize("equal_times", [False, True])
def test_parent_hydration_uses_observation_winner_instead_of_receipt_iteration_order(
    store, tmp_path, equal_times
):
    requested = []
    first_page = "0" * 32
    second_page = "1" * 32
    assert (
        hashlib.sha256(json_bytes([first_page, "/events/0"])).hexdigest()
        > hashlib.sha256(json_bytes([second_page, "/events/0"])).hexdigest()
    )

    def events(params):
        second = bool(params.get("cursor"))
        return 200, {
            "events": [
                {
                    **EVENTS[0],
                    "series_ticker": "SERIES" if second else "WINNER",
                    "updated_time": "2099-01-01T00:00:00Z" if second else "2000-01-01T00:00:00Z",
                }
            ],
            "cursor": "second" if not second else "",
        }

    class TimedClient(FakeClient):
        def __init__(self, actual_store, budget, **options):
            super().__init__(
                actual_store,
                budget,
                {
                    "/events": events,
                    "/events/multivariate": (200, {"events": [], "cursor": ""}),
                    "/markets": (200, {"markets": [MARKET], "cursor": ""}),
                    "/historical/markets": (200, {"markets": [], "cursor": ""}),
                    "/series/WINNER": (200, {"series": {"ticker": "WINNER"}}),
                },
                **options,
            )

        def get(self, endpoint, params=None, context=None):
            requested.append(endpoint)
            relative = super().get(endpoint, params, context)
            if endpoint != "/events":
                return relative
            second = bool((params or {}).get("cursor"))
            receipt = json.loads(self.store.path(relative).read_text())
            receipt["received_at"] = (
                "2020-01-01T00:00:00Z" if second and not equal_times else "2030-01-01T00:00:00Z"
            )
            receipt["page_id"] = second_page if second else first_page
            previous_directory = self.store.path(relative).parent
            previous_relative = str(previous_directory.relative_to(self.store.root))
            destination_relative = f"raw/{receipt['page_id']}"
            destination = self.store.path(destination_relative)
            previous_directory.rename(destination)
            self.store.sync(previous_relative)
            self.store.sync(destination_relative)
            relative = f"{destination_relative}/manifest.json"
            self.store.write(relative, receipt)
            return relative

    state = acquisition.capture(store, client_factory=TimedClient)
    assert state["complete"]
    accounted_usage = store.usage()
    assert store.reconcile() == accounted_usage
    assert requested.count("/series/WINNER") == 1
    output = tmp_path / "metadata"
    metadata.export(store, ["MARKET"], output, capture_id=state["capture_id"])
    market = json.loads((output / "markets.json").read_text())[0]
    assert market["usable"] is True
    assert market["series_ticker"] == "WINNER"


def test_parent_hydration_does_not_let_newer_nested_market_override_direct_definition(
    store, tmp_path
):
    requested = []
    direct = {**MARKET, "event_ticker": "DIRECT", "rules_primary": "authoritative rules"}
    embedded = {**MARKET, "rules_primary": "embedded summary"}

    class ReferencedClient(FakeClient):
        def __init__(self, actual_store, budget, **options):
            super().__init__(
                actual_store,
                budget,
                {
                    "/events": (
                        200,
                        {"events": [{**EVENTS[0], "markets": [embedded]}], "cursor": ""},
                    ),
                    "/events/multivariate": (200, {"events": [], "cursor": ""}),
                    "/markets": (200, {"markets": [direct], "cursor": ""}),
                    "/historical/markets": (200, {"markets": [], "cursor": ""}),
                    "/events/DIRECT": (
                        200,
                        {"event": {"event_ticker": "DIRECT", "series_ticker": "SERIES"}},
                    ),
                },
                **options,
            )

        def get(self, endpoint, params=None, context=None):
            requested.append(endpoint)
            relative = super().get(endpoint, params, context)
            receipt = json.loads(self.store.path(relative).read_text())
            receipt["received_at"] = (
                "2099-01-01T00:00:00Z" if endpoint == "/events" else "2020-01-01T00:00:00Z"
            )
            self.store.write(relative, receipt)
            return relative

    state = acquisition.capture(store, client_factory=ReferencedClient)
    assert requested.count("/events/DIRECT") == 1
    output = tmp_path / "metadata"
    metadata.export(store, ["MARKET"], output, capture_id=state["capture_id"])
    market = json.loads((output / "markets.json").read_text())[0]
    assert market["event_ticker"] == "DIRECT"
    assert market["usable"] is True
    assert market["rules_primary"] == direct["rules_primary"]


def test_transient_parent_information_does_not_extend_capture_state_contract(store):
    state = acquisition.capture(store, client_factory=FakeClient)
    snapshot = json.loads(store.path(f"captures/{state['capture_id']}/state.json").read_text())
    persisted = snapshot["state"]
    assert set(persisted) == {
        "version",
        "journal_version",
        "acquisition_protocol",
        "rate_policy",
        "parent_policy",
        "cutoff_policy",
        "acquisition_policy_sha256",
        "capture_id",
        "selected",
        "started_at",
        "completed_at",
        "complete",
        "max_response_bytes",
        "max_decoded_response_bytes",
        "compression",
        "tasks",
        "task_scopes",
        "incremental",
        "cutoff",
        "coverage",
        "error",
    }
    assert persisted["acquisition_protocol"] == 5
    assert persisted["incremental"] is False
    assert persisted["task_scopes"] == {
        "1": "series",
        "2": "events_live",
        "3": "events_combo",
        "4": "markets_live",
        "5": "markets_history",
    }
    telemetry = {"requests", "downloaded_bytes", "accounting_complete", "budget_exhausted"}
    assert {key: value for key, value in state.items() if key not in telemetry} == persisted
    assert set(state) == set(persisted) | telemetry
    assert acquisition.complete_capture(store, state["capture_id"]) == persisted


@pytest.mark.parametrize("failure", ["nested_parent", "combo_side", "native_ticker"])
def test_compact_parent_collection_preserves_native_validation(store, failure):
    if failure == "nested_parent":
        endpoint = "/events"
        payload = {
            "events": [
                {**EVENTS[0], "markets": [{**MARKET, "event_ticker": "CONTRADICTS-PARENT"}]}
            ],
            "cursor": "",
        }
    else:
        endpoint = "/markets"
        native = (
            {**COMBO, "mve_selected_legs": [{**COMBO["mve_selected_legs"][0], "side": "maybe"}]}
            if failure == "combo_side"
            else {**MARKET, "ticker": "../bad"}
        )
        payload = {"markets": [native], "cursor": ""}
    with pytest.raises(CatalogueError):
        acquisition.capture(
            store,
            client_factory=lambda s, b, **_options: FakeClient(s, b, {endpoint: (200, payload)}),
        )
    capture_id = next(store.path("captures").iterdir()).name
    assert acquisition.read_capture(store, capture_id)["complete"] is False


def test_empty_event_series_parent_remains_empty_in_projection(store):
    state = acquisition.capture(
        store,
        client_factory=lambda s, b, **_options: FakeClient(
            s,
            b,
            {
                "/events": (
                    200,
                    {"events": [{**EVENTS[0], "series_ticker": ""}, EVENTS[1]], "cursor": ""},
                )
            },
        ),
    )
    assert state["complete"]
    projected = project_pages(acquisition.capture_pages(store, state))
    event = next(row for row in projected["events"] if row["event_ticker"] == "EVENT")
    market = next(row for row in projected["markets"] if row["market_ticker"] == "MARKET")
    assert event["series_ticker"] == ""
    assert market["series_ticker"] == ""
    assert market["usable"] is False
