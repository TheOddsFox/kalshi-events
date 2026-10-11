from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from kalshi_events.capture import capture
from kalshi_events.common import CatalogueError, Store
from kalshi_events.publish import current, publish, verify_release
from kalshi_events.recovery import backup, rebuild, restore
from kalshi_events.warehouse import build


def test_dlt_dbt_publish_replay_exact_decimals_and_backup(store, factory, tmp_path):
    state = capture(store, client_factory=factory)
    build(store, state["capture_id"])
    release = publish(store)
    assert current(store)["source_release_id"] == release["source_release_id"]
    verify_release(Path(release["output"]))
    with duckdb.connect(str(store.path("warehouse.duckdb")), read_only=True) as connection:
        result = connection.execute(
            "select last_price_dollars, volume_fp from main.current_markets where market_ticker='MARKET'"
        ).fetchone()
        assert result == (Decimal("0.00123"), Decimal("12.3456789"))
        count = connection.execute("select count(*) from main.observation_history").fetchone()[0]
        assert count == 7
        assert (
            connection.execute("select count(*) from main.combo_legs where resolved").fetchone()[0]
            == 1
        )
    build(store, state["capture_id"])
    assert publish(store)["source_release_id"] == release["source_release_id"]
    with duckdb.connect(str(store.path("warehouse.duckdb")), read_only=True) as connection:
        assert (
            connection.execute("select count(*) from main.observation_history").fetchone()[0]
            == count
        )
    backup_path = tmp_path / "backup"
    backup(store, backup_path)
    restored_root = tmp_path / "restored"
    restore(backup_path, restored_root, store.limits)
    restored = Store(restored_root)
    assert current(restored)["source_release_id"] == release["source_release_id"]
    rebuild(restored)
    assert publish(restored)["source_release_id"] == release["source_release_id"]


def test_changed_build_evidence_cannot_promote_pointer(store, factory):
    state = capture(store, client_factory=factory)
    build(store, state["capture_id"])
    release = publish(store)
    manifest = store.path(state["pages"][0])
    original = manifest.read_bytes()
    manifest.write_bytes(original + b" ")
    with pytest.raises(CatalogueError, match="evidence"):
        publish(store)
    assert current(store)["source_release_id"] == release["source_release_id"]


def test_failed_build_preserves_prior_publication(store, factory, monkeypatch):
    state = capture(store, client_factory=factory)
    build(store, state["capture_id"])
    release = publish(store)
    from kalshi_events import warehouse

    def failed(*args, **kwargs):
        raise CatalogueError("synthetic dbt failure")

    monkeypatch.setattr(warehouse, "dbt", failed)
    with pytest.raises(CatalogueError, match="synthetic"):
        warehouse.build(store, state["capture_id"])
    assert current(store)["source_release_id"] == release["source_release_id"]


def test_interrupted_load_receipt_cannot_hide_prior_raw_evidence(store, factory):
    from kalshi_events.warehouse import load

    first = capture(store, client_factory=factory)
    load(store, first["capture_id"])
    store.path(f"loaded/{first['capture_id']}.json").unlink()
    second = capture(store, client_factory=factory)
    receipt = build(store, second["capture_id"])
    assert any(first["capture_id"] in path for path in receipt["evidence"])
    first_body = store.path(str(Path(first["pages"][0]).parent / "body.bin"))
    first_body.unlink()
    with pytest.raises(CatalogueError, match="missing|integrity"):
        publish(store)


def test_combo_leg_with_missing_authoritative_event_remains_unresolved_in_warehouse(store):
    from conftest import COMBO, MARKET, FakeClient

    from kalshi_events.capture import capture_pages
    from kalshi_events.normalize import project_pages

    native = {**MARKET, "event_ticker": "UNKNOWN"}
    combo = {
        **COMBO,
        "mve_selected_legs": [
            {"event_ticker": "UNKNOWN", "market_ticker": "MARKET", "side": "yes"}
        ],
    }
    state = capture(
        store,
        client_factory=lambda s, b: FakeClient(
            s,
            b,
            {
                "/markets": (200, {"markets": [native, combo], "cursor": ""}),
            },
        ),
    )
    assert project_pages(capture_pages(store, state))["combo_legs"][0]["resolved"] is False
    build(store, state["capture_id"])
    release = publish(store)
    with duckdb.connect(str(store.path("warehouse.duckdb")), read_only=True) as connection:
        assert connection.execute(
            "select market_ticker,event_ticker,resolved from main.combo_legs"
        ).fetchall() == [("MARKET", "UNKNOWN", False)]
        assert connection.execute(
            "select market_ticker,usable from main.current_markets where market_ticker='MARKET'"
        ).fetchall() == [("MARKET", False)]
    with duckdb.connect() as connection:
        assert connection.execute(
            "select resolved from read_parquet(?)",
            [str(Path(release["output"]) / "combo_legs.parquet")],
        ).fetchall() == [(False,)]
