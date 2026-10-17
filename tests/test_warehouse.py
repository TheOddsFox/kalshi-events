from __future__ import annotations

import json
import os
import subprocess
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest
from conftest import COMBO, MARKET, FakeClient

from kalshi_events import publish as publishing
from kalshi_events import recovery, warehouse
from kalshi_events.capture import (
    capture,
    capture_pages,
    latest_catalogue_capture,
    page_receipts,
)
from kalshi_events.cli import main
from kalshi_events.common import BudgetExceeded, CatalogueError, Limits, Store
from kalshi_events.normalize import project_pages
from kalshi_events.publish import current, publish, verify_release
from kalshi_events.recovery import rebuild
from kalshi_events.warehouse import build, certified, load


def warehouse_path(store, capture_id):
    return store.path(f"builds/{capture_id}/warehouse.duckdb")


def test_dlt_dbt_publish_replay_exact_decimals_and_rebuild(store, factory):
    state = capture(store, client_factory=factory)
    build(store, state["capture_id"])
    release = publish(store)
    assert current(store)["source_release_id"] == release["source_release_id"]
    verify_release(Path(release["output"]))
    with duckdb.connect(
        str(warehouse_path(store, state["capture_id"])), read_only=True
    ) as connection:
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
    assert rebuild(store)["capture_id"] == state["capture_id"]
    assert publish(store)["source_release_id"] == release["source_release_id"]


def test_backup_and_restore_are_removed(tmp_path, capsys):
    assert not hasattr(recovery, "backup")
    assert not hasattr(recovery, "restore")
    assert not hasattr(recovery, "verify_backup")
    for argv in (
        ["backup", "--output", str(tmp_path / "backup")],
        ["restore", "--backup", str(tmp_path / "backup")],
    ):
        with pytest.raises(SystemExit):
            main(["--root", str(tmp_path / "root"), *argv])
    assert not (tmp_path / "root").exists()


def test_changed_build_evidence_cannot_promote_pointer(store, factory):
    state = capture(store, client_factory=factory)
    build(store, state["capture_id"])
    release = publish(store)
    manifest = store.path(page_receipts(store, state)[0]["relative"])
    manifest.write_bytes(manifest.read_bytes() + b" ")
    with pytest.raises(CatalogueError, match="evidence"):
        publish(store)
    assert current(store)["source_release_id"] == release["source_release_id"]


def test_failed_build_preserves_prior_publication(store, factory, monkeypatch):
    state = capture(store, client_factory=factory)
    build(store, state["capture_id"])
    release = publish(store)

    def failed(*args, **kwargs):
        raise CatalogueError("synthetic dbt failure")

    monkeypatch.setattr(warehouse, "dbt", failed)
    with pytest.raises(CatalogueError, match="synthetic"):
        warehouse.build(store, state["capture_id"])
    assert current(store)["source_release_id"] == release["source_release_id"]
    assert not list(store.path("tmp").glob("build-*"))


def test_latest_only_build_ignores_targeted_capture(store, factory, monkeypatch, capsys):
    catalogue = capture(store, client_factory=factory)
    targeted = capture(store, selected=["MARKET"], client_factory=factory)
    assert latest_catalogue_capture(store) == catalogue["capture_id"]
    with pytest.raises(CatalogueError, match="targeted"):
        build(store, targeted["capture_id"])
    assert not store.path(f"builds/{targeted['capture_id']}").exists()
    build(store, catalogue["capture_id"])
    with pytest.raises(CatalogueError, match="targeted"):
        publish(store, targeted["capture_id"])
    release = publish(store)
    assert release["capture_id"] == catalogue["capture_id"]
    assert release["pruned_captures"] == []
    assert current(store)["capture_id"] == catalogue["capture_id"]
    # Targeted captures are never a fallback for the default catalogue build.
    assert main(["--root", str(store.root), "build"]) == 0
    assert json.loads(capsys.readouterr().out)["capture_id"] == catalogue["capture_id"]


def test_catalogue_build_lands_only_its_own_observations(store, factory):
    catalogue = capture(store, client_factory=factory)
    targeted = capture(store, selected=["MARKET"], client_factory=factory)
    assert targeted["complete"]
    build(store, catalogue["capture_id"])
    with duckdb.connect(
        str(warehouse_path(store, catalogue["capture_id"])), read_only=True
    ) as connection:
        owners = connection.execute(
            "select capture_id, count(*) from main.observation_history group by capture_id"
        ).fetchall()
    assert owners == [(catalogue["capture_id"], 7)]


def test_prune_after_verify_keeps_current_capture(store, factory):
    first = capture(store, client_factory=factory)
    build(store, first["capture_id"])
    publish(store)
    second = capture(store, client_factory=factory)
    build(store, second["capture_id"])
    release = publish(store)
    assert release["pruned_captures"] == [first["capture_id"]]
    assert not store.path(f"captures/{first['capture_id']}").exists()
    assert sorted(path.name for path in store.path("builds").iterdir()) == [second["capture_id"]]
    assert sorted(path.name for path in store.path("releases").iterdir()) == [
        release["source_release_id"]
    ]
    assert current(store)["capture_id"] == second["capture_id"]
    raw_owners = {
        json.loads(path.read_text())["capture_id"]
        for path in store.path("raw").glob("*/manifest.json")
    }
    assert raw_owners == {second["capture_id"]}


def test_prune_deletes_superseded_raw_bodies_not_just_receipts(store, factory):
    first = capture(store, client_factory=factory)
    build(store, first["capture_id"])
    publish(store)
    superseded = {path.name for path in store.path("raw").iterdir()}
    second = capture(store, client_factory=factory)
    build(store, second["capture_id"])
    publish(store)
    remaining = {path.name for path in store.path("raw").iterdir()}
    assert superseded and not superseded & remaining
    assert remaining == {path.parent.name for path in store.path("raw").glob("*/manifest.json")}
    assert all((store.path("raw") / name / "body.bin").is_file() for name in remaining)


def test_verification_failure_blocks_pruning(store, factory, monkeypatch):
    first = capture(store, client_factory=factory)
    build(store, first["capture_id"])
    publish(store)
    second = capture(store, client_factory=factory)
    build(store, second["capture_id"])

    def unverified(_store):
        raise CatalogueError("synthetic verification failure")

    monkeypatch.setattr(publishing, "current", unverified)
    with pytest.raises(CatalogueError, match="synthetic"):
        publish(store, second["capture_id"])
    assert store.path(f"captures/{first['capture_id']}").exists()
    assert store.path(f"builds/{first['capture_id']}").exists()


def test_missing_bodies_of_superseded_captures_do_not_fail_publication(store, factory):
    first = capture(store, client_factory=factory)
    build(store, first["capture_id"])
    publish(store)
    for receipt in page_receipts(store, first):
        store.path(receipt["relative"]).parent.joinpath("body.bin").unlink()
    second = capture(store, client_factory=factory)
    build(store, second["capture_id"])
    release = publish(store)
    assert release["pruned_captures"] == [first["capture_id"]]
    assert current(store)["source_release_id"] == release["source_release_id"]


def test_missing_body_of_current_capture_fails_build_and_publish(store, factory):
    state = capture(store, client_factory=factory)
    build(store, state["capture_id"])
    release = publish(store)
    body = store.path(page_receipts(store, state)[0]["relative"]).parent / "body.bin"
    body.unlink()
    with pytest.raises(CatalogueError, match="missing|integrity"):
        build(store, state["capture_id"])
    with pytest.raises(CatalogueError, match="missing|integrity|evidence"):
        publish(store, state["capture_id"])
    assert current(store)["source_release_id"] == release["source_release_id"]


def test_certification_rejects_missing_body_of_current_capture(store, factory):
    state = capture(store, client_factory=factory)
    build(store, state["capture_id"])
    assert certified(store, state["capture_id"])["capture_id"] == state["capture_id"]
    body = store.path(page_receipts(store, state)[0]["relative"]).parent / "body.bin"
    body.unlink()
    # Directly exercises certification; build and publish reject earlier, in capture completion.
    with pytest.raises(CatalogueError, match="missing"):
        certified(store, state["capture_id"])


def test_rebuild_rebuilds_only_the_current_capture(store, factory):
    published = capture(store, client_factory=factory)
    build(store, published["capture_id"])
    release = publish(store)
    capture(store, client_factory=factory)
    assert rebuild(store)["capture_id"] == published["capture_id"]
    assert sorted(path.name for path in store.path("builds").iterdir()) == [published["capture_id"]]
    assert current(store)["source_release_id"] == release["source_release_id"]


def test_load_stages_and_discards_landing(store, factory):
    state = capture(store, client_factory=factory)
    receipt = load(store, state["capture_id"])
    assert receipt["observation_count"] == 7
    assert not store.path("loaded").exists()
    assert not list(store.path("tmp").glob("load-*"))


def test_deadline_is_enforced_while_dlt_loads(store, factory):
    state = capture(store, client_factory=factory)
    calls = 0
    original = store.check_deadline

    def expiring():
        nonlocal calls
        calls += 1
        if calls == 3:
            raise BudgetExceeded("operation deadline exhausted")
        original()

    store.check_deadline = expiring
    with pytest.raises(BudgetExceeded, match="deadline"):
        load(store, state["capture_id"])
    assert calls == 3


def test_expired_deadline_blocks_publication_without_pointer_change(store, factory):
    state = capture(store, client_factory=factory)
    build(store, state["capture_id"])
    release = publish(store)
    expired = Store(store.root, Limits(max_duration_seconds=1))
    expired.started -= 60
    with pytest.raises(BudgetExceeded, match="deadline"):
        publish(expired, state["capture_id"])
    assert current(store)["source_release_id"] == release["source_release_id"]


def test_dbt_profile_sets_bounded_parallelism_and_memory(store, monkeypatch):
    seen = {}

    def fake_run(command, **kwargs):
        profiles = Path(command[command.index("--profiles-dir") + 1]) / "profiles.yml"
        seen.update(json.loads(profiles.read_text()))
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(warehouse.subprocess, "run", fake_run)
    warehouse.dbt(store, ["parse"])
    output = seen["kalshi_events"]["outputs"]["local"]
    assert output["threads"] == 4
    assert output["settings"]["memory_limit"] == "4GB"


def test_combo_leg_with_missing_authoritative_event_remains_unresolved_in_warehouse(store):
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
    with duckdb.connect(
        str(warehouse_path(store, state["capture_id"])), read_only=True
    ) as connection:
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


def test_dbt_operation_deadline_is_a_budget_stop(store, monkeypatch, capsys):
    def timed_out(command, **kwargs):
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr(warehouse.subprocess, "run", timed_out)
    with pytest.raises(BudgetExceeded, match="deadline"):
        warehouse.dbt(store, ["parse"])
    assert main(["--root", str(store.root), "dbt", "--", "parse"]) == 3
    assert json.loads(capsys.readouterr().out)["complete"] is False


def test_failed_publication_rename_restores_previous_build(store, factory, monkeypatch):
    state = capture(store, client_factory=factory)
    build(store, state["capture_id"])
    release = publish(store)
    before = certified(store, state["capture_id"])
    destination = str(warehouse_path(store, state["capture_id"]).parent)
    real_rename = os.rename

    def failing_rename(source, target, *args, **kwargs):
        if str(target) == destination and Path(source).name.startswith("build-"):
            raise OSError("synthetic staging rename failure")
        return real_rename(source, target, *args, **kwargs)

    monkeypatch.setattr(warehouse.os, "rename", failing_rename)
    with pytest.raises(OSError, match="synthetic"):
        build(store, state["capture_id"])
    assert certified(store, state["capture_id"]) == before
    assert current(store)["source_release_id"] == release["source_release_id"]
    assert not list(store.path("tmp").glob("replaced-*"))
    assert not list(store.path("tmp").glob("build-*"))


def test_evidence_walk_checks_deadline_on_every_page(store, factory, monkeypatch):
    state = capture(store, client_factory=factory)
    expired = Store(store.root, Limits(max_duration_seconds=60))
    file_digest = warehouse.file_digest
    body_reads = []

    def expire_after_first_body(path, *args, **kwargs):
        result = file_digest(path, *args, **kwargs)
        if Path(path).name == "body.bin":
            body_reads.append(path)
            expired.started -= 120
        return result

    monkeypatch.setattr(warehouse, "file_digest", expire_after_first_body)
    with pytest.raises(BudgetExceeded, match="deadline"):
        warehouse.evidence_inventory(expired, state["capture_id"])
    assert len(body_reads) == 1


def test_status_and_current_report_the_published_release(store, factory, capsys):
    state = capture(store, client_factory=factory)
    build(store, state["capture_id"])
    release = publish(store)
    assert main(["--root", str(store.root), "status"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["current"]["source_release_id"] == release["source_release_id"]
    assert [row["capture_id"] for row in report["captures"]] == [state["capture_id"]]
    assert report["accounting"]["accounting_complete"] is True
    assert main(["--root", str(store.root), "current"]) == 0
    assert json.loads(capsys.readouterr().out)["source_release_id"] == release["source_release_id"]
