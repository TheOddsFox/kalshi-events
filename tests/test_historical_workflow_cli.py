"""Historical refresh stays scoped, offline, and explicit at workflow boundaries."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest
from conftest import COMBO, HISTORICAL, FakeClient

from kalshi_events import capture, cli, publish, warehouse
from kalshi_events.capture import page_receipts
from kalshi_events.common import Limits, Store


@pytest.fixture(autouse=True)
def _loopback_api(monkeypatch):
    monkeypatch.setenv("KALSHI_EVENTS_API_URL", "http://127.0.0.1:9/trade-api/v2")
    monkeypatch.setenv("KALSHI_EVENTS_ALLOW_LOOPBACK", "1")


def _client_factory(calls=None, responses=None):
    class RecordingClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            if calls is not None:
                calls.append(endpoint)
            return super().get(endpoint, params, context)

    return lambda store, budget, **options: RecordingClient(store, budget, responses, **options)


def _invoke_cli(capsys, root: Path, *arguments: str):
    status = cli.main(["--root", str(root), *arguments])
    output = capsys.readouterr().out
    return status, json.loads(output)


def _historical_combo():
    return {
        **COMBO,
        "ticker": "COMBO-HISTORY-UNRESOLVED",
        "event_ticker": "MISSING-EVENT",
        "mve_selected_legs": [
            {
                "event_ticker": "MISSING-EVENT",
                "market_ticker": "MISSING-MARKET",
                "side": "no",
                "yes_settlement_value_dollars": "0.000000000000000001",
            }
        ],
    }


def _incomplete_legacy_capture(root: Path, responses=None):
    cursor = 0

    def keep_multivariate_open(_params):
        nonlocal cursor
        value = cursor
        cursor += 1
        return 200, {"events": [], "cursor": f"next-{value}"}

    response_map = {"/events/multivariate": keep_multivariate_open, **(responses or {})}
    bounded_store = Store(root, Limits(max_requests=14))
    state = capture.capture(
        bounded_store,
        catalogue_profile="exchange",
        client_factory=_client_factory(responses=response_map),
    )
    assert state["complete"] is False
    return bounded_store, state


def test_full_refresh_defaults_to_historical_scope_and_retains_combo_payload(
    tmp_path, monkeypatch, capsys
):
    root = tmp_path / "operator"
    calls = []
    original_capture = capture.capture

    def offline_capture(store, **options):
        return original_capture(
            store,
            client_factory=_client_factory(
                calls,
                {
                    "/historical/markets": (
                        200,
                        {"markets": [HISTORICAL, _historical_combo()], "cursor": ""},
                    )
                },
            ),
            **options,
        )

    monkeypatch.setattr(capture, "capture", offline_capture)

    status, result = _invoke_cli(capsys, root, "refresh", "--full")

    assert status == 0
    assert result["complete"] is True
    assert result["coverage_scope"] == "historical"
    assert result["scope_complete"] is True
    assert result["source_capture_complete"] is True
    assert {"/historical/cutoff", "/series", "/events", "/historical/markets"} <= set(calls)
    assert not ({"/markets", "/events/multivariate"} & set(calls))

    store = Store(root)
    manifest = publish.current(store, catalogue_version=3)
    assert manifest["coverage_scope"] == "historical"
    database = store.path(
        f"{warehouse.build_directory(manifest['capture_id'], 3)}/warehouse.duckdb"
    )
    with duckdb.connect(str(database), read_only=True) as connection:
        events = {
            row[0]
            for row in connection.execute("select event_ticker from main.current_events").fetchall()
        }
        combo = connection.execute(
            """select market_ticker, event_ticker, usable
               from main.current_markets where market_ticker='COMBO-HISTORY-UNRESOLVED'"""
        ).fetchone()
        leg = connection.execute(
            """select event_ticker, market_ticker, resolved
               from main.combo_legs where combo_market_ticker='COMBO-HISTORY-UNRESOLVED'"""
        ).fetchone()
    assert {"EVENT", "HISTORY"} <= events
    assert "COMBO" not in events
    assert combo == ("COMBO-HISTORY-UNRESOLVED", "MISSING-EVENT", False)
    assert leg == ("MISSING-EVENT", "MISSING-MARKET", False)


def test_saved_incomplete_legacy_capture_refreshes_offline_and_preserves_other_catalogues(
    tmp_path, monkeypatch, capsys
):
    root = tmp_path / "operator"
    store = Store(root)
    baseline = capture.capture(
        store,
        catalogue_profile="exchange",
        client_factory=_client_factory(),
    )
    warehouse.build(store, baseline["capture_id"])
    publish.publish(store, baseline["capture_id"], catalogue_version=2)
    legacy_pointer = store.path("current-v2.json").read_bytes()

    store, source = _incomplete_legacy_capture(
        root,
        {
            "/historical/markets": (
                200,
                {"markets": [HISTORICAL, _historical_combo()], "cursor": ""},
            )
        },
    )
    assert source["complete"] is False
    capture_id = source["capture_id"]
    capture_dir = store.path(f"captures/{capture_id}")
    state_before = (capture_dir / "state.json").read_bytes()
    journal_before = (capture_dir / "pages.jsonl").read_bytes()
    evidence_before = {}
    for receipt in page_receipts(store, source):
        manifest_path = store.path(receipt["relative"])
        body_path = manifest_path.parent / "body.bin"
        evidence_before[manifest_path] = manifest_path.read_bytes()
        evidence_before[body_path] = body_path.read_bytes()

    def no_capture(*_args, **_kwargs):
        pytest.fail("saved historical refresh must not start or resume capture")

    monkeypatch.setattr(capture, "capture", no_capture)
    status, result = _invoke_cli(
        capsys,
        root,
        "refresh",
        "--historical",
        "--capture-id",
        capture_id,
    )

    assert status == 0
    assert result["complete"] is True
    assert result["coverage_scope"] == "historical"
    assert result["scope_complete"] is True
    assert result["capture_complete"] is False
    assert result["source_capture_complete"] is False
    assert result["requests"] == 0
    assert result["downloaded_bytes"] == 0
    assert result["accounting_complete"] is True
    assert (capture_dir / "state.json").read_bytes() == state_before
    assert (capture_dir / "pages.jsonl").read_bytes() == journal_before
    assert {path: path.read_bytes() for path in evidence_before} == evidence_before
    assert store.path("current-v2.json").read_bytes() == legacy_pointer
    assert publish.current(store, catalogue_version=3)["capture_id"] == capture_id
    assert publish.current(store, catalogue_version=2)["capture_id"] == baseline["capture_id"]

    default_status, default_current = _invoke_cli(capsys, root, "current")
    legacy_status, legacy_current = _invoke_cli(capsys, root, "current", "--catalogue-version", "2")
    assert default_status == legacy_status == 0
    assert default_current["capture_id"] == capture_id
    assert default_current["contract"] == "oddsfox.kalshi.historical.v1"
    assert legacy_current["capture_id"] == baseline["capture_id"]


def test_saved_historical_refresh_rejects_a_selected_scope_that_did_not_finish(
    tmp_path, monkeypatch, capsys
):
    root = tmp_path / "operator"
    bounded_store = Store(root, Limits(max_requests=1))
    source = capture.capture(
        bounded_store,
        catalogue_profile="exchange",
        client_factory=_client_factory(),
    )
    assert source["complete"] is False

    def no_capture(*_args, **_kwargs):
        pytest.fail("incomplete saved evidence must be rejected before capture resumes")

    monkeypatch.setattr(capture, "capture", no_capture)
    status, result = _invoke_cli(
        capsys,
        root,
        "refresh",
        "--historical",
        "--capture-id",
        source["capture_id"],
    )

    assert status == 1
    assert result["complete"] is False
    assert "incomplete" in result["error"].lower()
    assert not (root / "current-historical.json").exists()


def test_saved_historical_cli_does_not_initialize_a_missing_root(tmp_path, capsys):
    root = tmp_path / "missing-operator"

    status, result = _invoke_cli(
        capsys,
        root,
        "refresh",
        "--historical",
        "--capture-id",
        "a" * 32,
    )

    assert status == 1
    assert result["complete"] is False
    assert "existing operator root" in result["error"]
    assert not root.exists()
