"""Verified series lineage must preserve scope, reconciliation and recoverable evidence."""

from __future__ import annotations

import copy
import json
import shutil

import conftest
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import CUTOFF, FakeClient

from kalshi_events import capture, incremental, publish, refresh, warehouse
from kalshi_events.common import BudgetExceeded, CatalogueError, Store, digest, json_bytes

CONTRACT = "oddsfox.kalshi.series.v1"
SCOPE = {"kind": "series", "series_ticker": "KXCPI", "include_multivariate": False}
EVENT_ONE = {"event_ticker": "KXCPI-E1", "series_ticker": "KXCPI"}
EVENT_TWO = {"event_ticker": "KXCPI-E2", "series_ticker": "KXCPI"}
MARKET_ONE = {
    "ticker": "KXCPI-M1",
    "event_ticker": "KXCPI-E1",
    "status": "active",
    "last_price_dollars": "0.00123",
}
MARKET_TWO = {
    "ticker": "KXCPI-M2",
    "event_ticker": "KXCPI-E2",
    "status": "active",
    "last_price_dollars": "0.12345",
}
HISTORICAL = {
    "ticker": "KXCPI-H1",
    "event_ticker": "KXCPI-E1",
    "status": "finalized",
    "result": "no",
    "settlement_value_dollars": "0.0000",
}


def _factory(events, live, historical):
    responses = {
        "/historical/cutoff": (200, CUTOFF),
        "/series/KXCPI": (200, {"series": {"ticker": "KXCPI", "title": "Inflation"}}),
        "/events": (200, {"events": events, "cursor": ""}),
        "/markets": (200, {"markets": live, "cursor": ""}),
        "/historical/markets": (200, {"markets": historical, "cursor": ""}),
        **{f"/events/{row['event_ticker']}": (200, {"event": row}) for row in events},
    }
    return lambda store, budget: FakeClient(store, budget, responses=responses)


def _rows(publication, relation):
    return pq.read_table(f"{publication['output']}/{relation}.parquet").to_pylist()


def _set_pointer(store, publication):
    release_id = publication["source_release_id"]
    manifest = store.path(f"releases/{release_id}/manifest.json").read_bytes()
    with store.writer_guard():
        store.write(
            "current-series.json",
            {"source_release_id": release_id, "manifest_sha256": digest(manifest)},
        )


@pytest.fixture(scope="module")
def series_chain(tmp_path_factory):
    root = tmp_path_factory.mktemp("series-adversarial-chain") / "root"
    Store(root, series_ticker="KXCPI")
    moved = {
        **MARKET_ONE,
        "status": "finalized",
        "result": "yes",
        "settlement_value_dollars": "1.0000",
    }
    stages = [
        ([EVENT_ONE, EVENT_TWO], [MARKET_ONE, MARKET_TWO], [HISTORICAL]),
        ([EVENT_ONE], [], [HISTORICAL, moved]),
        ([EVENT_ONE, EVENT_TWO], [MARKET_TWO], [HISTORICAL, moved]),
    ]
    captures = []
    publications = []
    for number, (events, live, history) in enumerate(stages):
        timestamp = f"2026-10-{10 + number:02d}T12:00:00Z"
        store = Store(root)
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(conftest, "now", lambda value=timestamp: value)
            patch.setattr(capture, "now", lambda value=timestamp: value)
            state = capture.capture(
                store,
                incremental=number > 0,
                client_factory=_factory(events, live, history),
            )
        assert state["complete"] is True
        publication = publish.publish(store, state["capture_id"], catalogue_version=2)
        captures.append(state)
        publications.append(publication)
    return {"root": root, "captures": captures, "publications": publications}


@pytest.fixture
def cloned_chain(series_chain, tmp_path):
    root = tmp_path / "root"
    shutil.copytree(series_chain["root"], root)
    return Store(root)


def test_series_chain_reconciles_removal_reappearance_and_market_tier_handoff(series_chain):
    baseline, removed, reappeared = series_chain["publications"]
    assert {row["market_ticker"] for row in _rows(baseline, "markets")} == {
        "KXCPI-M1",
        "KXCPI-M2",
        "KXCPI-H1",
    }
    assert {row["event_ticker"] for row in _rows(removed, "events")} == {"KXCPI-E1"}
    removed_markets = _rows(removed, "markets")
    assert {row["market_ticker"] for row in removed_markets} == {"KXCPI-M1", "KXCPI-H1"}
    moved = next(row for row in removed_markets if row["market_ticker"] == "KXCPI-M1")
    assert moved["status"] == "finalized"
    assert moved["result"] == "yes"
    assert {row["market_ticker"] for row in _rows(reappeared, "markets")} == {
        "KXCPI-M1",
        "KXCPI-M2",
        "KXCPI-H1",
    }
    assert len(_rows(reappeared, "markets")) == 3
    original_history = {row["observation_id"] for row in _rows(baseline, "observation_history")}
    for publication in (removed, reappeared):
        history = _rows(publication, "observation_history")
        assert original_history <= {row["observation_id"] for row in history}
        assert len(history) == len({row["observation_id"] for row in history})
        assert any(row["entity_ticker"] == "KXCPI-M2" for row in history)


def test_series_chain_keeps_all_reachable_evidence_and_uses_only_series_pointer(series_chain):
    store = Store(series_chain["root"])
    current = publish.current(store, catalogue_version=2)
    assert current["source_release_id"] == series_chain["publications"][-1]["source_release_id"]
    assert current["contract"] == CONTRACT
    assert current["series_scope"] == SCOPE
    assert current["delta_depth"] == 2
    assert store.path("current-series.json").is_file()
    assert not store.path("current.json").exists()
    assert not store.path("current-v2.json").exists()
    for state, publication in zip(
        series_chain["captures"], series_chain["publications"], strict=True
    ):
        assert store.path(f"captures/{state['capture_id']}/state.json").is_file()
        assert store.path(f"builds-v2/{state['capture_id']}/warehouse.duckdb").is_file()
        assert (
            publish.verify_release(store.path(f"releases/{publication['source_release_id']}"))[
                "contract"
            ]
            == CONTRACT
        )
        assert list(capture.capture_pages(store, capture.read_capture(store, state["capture_id"])))


@pytest.mark.parametrize(
    "field", ["series_ticker", "include_multivariate", "coverage_scope", "contract"]
)
def test_standalone_series_verification_rejects_manifest_scope_tampering(
    cloned_chain, series_chain, field
):
    release_id = series_chain["publications"][-1]["source_release_id"]
    directory = cloned_chain.path(f"releases/{release_id}")
    path = directory / "manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest[field] = {
        "series_ticker": "KXFED",
        "include_multivariate": True,
        "coverage_scope": "catalogue",
        "contract": "oddsfox.kalshi.unknown.v1",
    }[field]
    path.write_bytes(json_bytes(manifest))

    with pytest.raises(CatalogueError):
        publish.verify_release(directory)


@pytest.mark.parametrize(
    ("contract", "rewrite_scope"),
    [
        ("oddsfox.kalshi.catalogue.v1", False),
        ("oddsfox.kalshi.catalogue.v1", True),
        ("oddsfox.kalshi.catalogue.v2", True),
    ],
)
def test_standalone_verification_rejects_series_release_downgrade(
    cloned_chain, series_chain, contract, rewrite_scope
):
    release_id = series_chain["publications"][-1]["source_release_id"]
    directory = cloned_chain.path(f"releases/{release_id}")
    path = directory / "manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["contract"] = contract
    if rewrite_scope:
        manifest["coverage_scope"] = "catalogue"
        manifest.pop("series_scope", None)
        manifest.pop("series_ticker", None)
        manifest.pop("include_multivariate", None)
    path.write_bytes(json_bytes(manifest))

    with pytest.raises(CatalogueError):
        publish.verify_release(directory)


def test_standalone_series_verification_checks_coverage_scope_after_checksums_are_updated(
    cloned_chain, series_chain
):
    release_id = series_chain["publications"][-1]["source_release_id"]
    directory = cloned_chain.path(f"releases/{release_id}")
    coverage_path = directory / "coverage.parquet"
    coverage = pq.read_table(coverage_path)
    index = coverage.schema.get_field_index("series_ticker")
    assert index >= 0
    coverage = coverage.set_column(index, coverage.schema.field(index), pa.array(["KXFED"]))
    pq.write_table(coverage, coverage_path)
    path = directory / "manifest.json"
    manifest = json.loads(path.read_bytes())
    inventory = publish.inventory(directory, publish.FILES, cloned_chain.limits.max_root_bytes)
    manifest["files"] = inventory
    manifest["source_release_id"] = digest(json_bytes(inventory).rstrip(b"\n"))
    path.write_bytes(json_bytes(manifest))

    with pytest.raises(CatalogueError):
        publish.verify_release(directory)


@pytest.mark.parametrize("field", ["series_scope", "release_contract", "root_id"])
def test_series_pin_rejects_scope_contract_or_root_substitution(cloned_chain, series_chain, field):
    state = copy.deepcopy(series_chain["captures"][-1])
    state["baseline"][field] = {
        "series_scope": {**SCOPE, "series_ticker": "KXFED"},
        "release_contract": "oddsfox.kalshi.catalogue.v2",
        "root_id": "f" * 32,
    }[field]
    with pytest.raises(CatalogueError):
        incremental.verify_pin(cloned_chain, state)


@pytest.mark.parametrize("mutation", ["root-id", "series"])
def test_series_current_rejects_wrong_root_identity_or_changed_series(cloned_chain, mutation):
    if mutation == "root-id":
        cloned_chain.path("root-id.json").write_bytes(
            json_bytes({"version": 1, "root_id": "f" * 32})
        )
    else:
        marker = json.loads(cloned_chain.path("root.json").read_bytes())
        marker["scope"]["series_ticker"] = "KXFED"
        cloned_chain.path("root.json").write_bytes(json_bytes(marker))
    reopened = Store(cloned_chain.root)
    pointer = reopened.path("current-series.json").read_bytes()

    with pytest.raises(CatalogueError):
        publish.current(reopened, catalogue_version=2)

    assert reopened.path("current-series.json").read_bytes() == pointer


def test_completed_series_resume_reuses_build_without_networking(
    cloned_chain, series_chain, monkeypatch
):
    capture_id = series_chain["captures"][-1]["capture_id"]
    before = set(cloned_chain.path("raw").glob("*/manifest.json"))

    def forbidden(*_args, **_kwargs):
        pytest.fail("completed resume must not acquire or land observations again")

    monkeypatch.setattr(capture, "PublicClient", forbidden)
    monkeypatch.setattr(warehouse, "_land", forbidden)
    with cloned_chain.writer_guard():
        result, code = refresh.run(cloned_chain, capture_id=capture_id)

    assert code == 0
    assert result["publication_state"] == "verified"
    assert result["source_release_id"] == series_chain["publications"][-1]["source_release_id"]
    assert set(cloned_chain.path("raw").glob("*/manifest.json")) == before


def test_series_publication_cleanup_budget_stop_is_unknown_and_resumable(
    cloned_chain, series_chain, monkeypatch
):
    capture_id = series_chain["captures"][-1]["capture_id"]
    _set_pointer(cloned_chain, series_chain["publications"][-2])
    before_pointer = cloned_chain.path("current-series.json").read_bytes()

    def exhausted(*_args, **_kwargs):
        raise BudgetExceeded("cleanup allowance exhausted")

    monkeypatch.setattr(incremental, "prune", exhausted)
    with cloned_chain.writer_guard():
        result, code = refresh.run(cloned_chain, capture_id=capture_id)

    assert code == 3
    assert result["capture_id"] == capture_id
    assert result["capture_complete"] is True
    assert result["complete"] is False
    assert result["publication_state"] == "unknown"
    assert cloned_chain.path("current-series.json").read_bytes() != before_pointer
    assert publish.current(cloned_chain, catalogue_version=2)["capture_id"] == capture_id


def test_series_predecessor_check_rejects_an_unrelated_current_publication(
    cloned_chain, series_chain
):
    _set_pointer(cloned_chain, series_chain["publications"][0])
    pointer = cloned_chain.path("current-series.json").read_bytes()

    with pytest.raises(CatalogueError):
        incremental.verify_predecessor(cloned_chain, series_chain["captures"][-1])

    assert cloned_chain.path("current-series.json").read_bytes() == pointer


def test_eight_series_refresh_limit_requires_explicit_rebase_without_new_capture(
    cloned_chain, series_chain, monkeypatch
):
    current = copy.deepcopy(series_chain["publications"][-1])
    current["delta_depth"] = 8
    monkeypatch.setattr(publish, "current", lambda *_args, **_kwargs: current)
    before = set(cloned_chain.path("captures").iterdir())

    with cloned_chain.writer_guard(), pytest.raises(CatalogueError, match="eight deltas"):
        capture.new_capture(cloned_chain, None, incremental=True)

    assert set(cloned_chain.path("captures").iterdir()) == before
