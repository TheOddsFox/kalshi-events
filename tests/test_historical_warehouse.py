from __future__ import annotations

import json
import os

import conftest
import duckdb
import pytest
from conftest import COMBO, HISTORICAL, FakeClient

from kalshi_events import capture as acquisition
from kalshi_events import normalize, warehouse
from kalshi_events.capture import page_receipts
from kalshi_events.common import BudgetExceeded, CatalogueError, Limits, Store
from kalshi_events.historical import HISTORICAL_CONTRACT, historical_selection


def _factory(responses=None, calls=None):
    class Client(FakeClient):
        def get(self, endpoint, params=None, context=None):
            if calls is not None:
                calls.append(endpoint)
            return super().get(endpoint, params, context)

    return lambda store, budget, **options: Client(store, budget, responses, **options)


def _duckdb_settings(connection):
    return connection.execute(
        """select current_setting('memory_limit'), current_setting('threads'),
                  current_setting('temp_directory'), current_setting('max_temp_directory_size')"""
    ).fetchone()


def _incomplete_legacy_capture(store, responses=None):
    cursor = 0

    def incomplete_multivariate(_params):
        nonlocal cursor
        value = cursor
        cursor += 1
        return 200, {"events": [], "cursor": f"next-{value}"}

    bounded_store = Store(store.root, Limits(max_requests=14))
    responses = {"/events/multivariate": incomplete_multivariate, **(responses or {})}
    state = acquisition.capture(
        bounded_store,
        catalogue_profile="exchange",
        client_factory=_factory(responses),
    )
    assert state["complete"] is False
    selection = historical_selection(state, allow_legacy_incomplete=True)
    assert all(state["tasks"][item["task_index"]]["done"] for item in selection.values())
    return state


def test_historical_build_uses_selected_evidence_when_global_capture_is_incomplete(
    store,
):
    state = _incomplete_legacy_capture(store)
    capture_id = state["capture_id"]
    selection = historical_selection(state, allow_legacy_incomplete=True)
    selected_indices = {item["task_index"] for item in selection.values()} | {0}
    rows = page_receipts(store, state)
    excluded = next(row for row in rows if row["index"] not in selected_indices)
    journal = [
        json.loads(line)
        for line in store.path(f"captures/{capture_id}/pages.jsonl").read_text().splitlines()
    ]
    excluded_seq = next(
        record["seq"]
        for record in journal
        if record.get("op") == "page" and record["payload"]["relative"] == excluded["relative"]
    )
    snapshot = json.loads(store.path(f"captures/{capture_id}/state.json").read_text())
    assert excluded_seq <= snapshot["_journal"]["seq"]
    store.path(excluded["relative"]).parent.joinpath("body.bin").unlink()

    with pytest.raises(CatalogueError, match="incomplete"):
        warehouse.build(store, capture_id)

    receipt = warehouse.build_historical(store, capture_id)
    assert receipt["catalogue_version"] == 3
    assert receipt["scope"] == "historical"
    assert receipt["source_capture_complete"] is False
    assert receipt["historical_contract"] == HISTORICAL_CONTRACT
    assert receipt["evidence"][f"captures/{capture_id}/pages.jsonl"]
    assert excluded["relative"] not in receipt["evidence"]
    assert warehouse.certified(store, capture_id, catalogue_version=3) == receipt

    historical_row = next(
        row for row in rows if row["index"] == selection["markets_history"]["task_index"]
    )
    body = store.path(historical_row["relative"]).parent / "body.bin"
    body.write_bytes(body.read_bytes() + b" ")
    with pytest.raises(CatalogueError, match="integrity|hash|evidence|changed"):
        warehouse.certified(store, capture_id, catalogue_version=3)


def test_historical_disk_index_keeps_membership_and_eligibility_without_winners(store):
    listed_page = {
        "entity_kind": "events",
        "endpoint": "/events",
        "singular": False,
        "status": 200,
        "page_id": "1" * 32,
        "request_id": "2" * 64,
        "received_at": "2026-10-28T12:00:00Z",
        "task_scope": "events_live",
        "task_generation": 2,
    }
    listed_payload = {
        "events": [{"event_ticker": "HISTORY-EVENT", "series_ticker": "HISTORY-SERIES"}],
        "cursor": "",
    }
    missing_parent_page = {
        "entity_kind": "events",
        "endpoint": "/events/MISSING-EVENT",
        "singular": True,
        "status": 404,
        "page_id": "3" * 32,
        "request_id": "4" * 64,
        "received_at": "2026-10-28T12:01:00Z",
    }
    state = {
        "tasks": [acquisition.task("/events", "events")],
        "task_scopes": {"0": "events_live"},
    }
    state["tasks"][0].update(done=True, restarts=2)
    index = normalize.DiskParentIndex(store, collect_winners=False)
    try:
        index.add_page(listed_page, listed_payload)
        index.add_page(missing_parent_page, {"message": "missing"})

        assert list(index.stream_membership(state)) == [("events_live", "HISTORY-EVENT")]
        assert list(index.stream_eligibility(state)) == [("events", "MISSING-EVENT", 404, "3" * 32)]
        assert index.db.execute("select count(*) from winners").fetchone() == (0,)
        with pytest.raises(CatalogueError, match="membership-only"):
            list(index.stream_winners("events"))
        with pytest.raises(CatalogueError, match="membership-only"):
            list(index.missing_parent_batches(state))
    finally:
        index.close()

    full_index = normalize.DiskParentIndex(store)
    try:
        full_index.add_page(listed_page, listed_payload)
        winners = list(full_index.stream_winners("events"))
        assert [row[0] for row in winners] == ["HISTORY-EVENT"]
        assert winners[0][3] == "HISTORY-SERIES"
    finally:
        full_index.close()


def test_historical_build_keeps_all_generations_combos_and_tie_winner(store, monkeypatch):
    timestamp = "2026-10-28T12:00:00Z"
    monkeypatch.setattr(acquisition, "now", lambda: timestamp)
    monkeypatch.setattr(conftest, "now", lambda: timestamp)
    old_market = {**HISTORICAL, "title": "older generation"}
    new_market = {**HISTORICAL, "title": "final generation"}
    resolved_combo = {
        **COMBO,
        "ticker": "COMBO-HISTORY-RESOLVED",
        "event_ticker": "HISTORY",
        "mve_selected_legs": [
            {
                "event_ticker": "HISTORY",
                "market_ticker": "HISTORICAL",
                "side": "yes",
                "yes_settlement_value_dollars": "0.00001",
            }
        ],
    }
    unresolved_combo = {
        **COMBO,
        "ticker": "COMBO-HISTORY-UNRESOLVED",
        "event_ticker": "MISSING-EVENT",
        "mve_selected_legs": [
            {
                "event_ticker": "MISSING-EVENT",
                "market_ticker": "MISSING-MARKET",
                "side": "no",
                "yes_settlement_value_dollars": "0.00002",
            }
        ],
    }
    first_page = True

    def historical_markets(params):
        nonlocal first_page
        if params.get("cursor") == "expired":
            return 400, {"message": "invalid cursor"}
        if first_page:
            first_page = False
            return 200, {"markets": [old_market], "cursor": "expired"}
        return 200, {
            "markets": [new_market, resolved_combo, unresolved_combo],
            "cursor": "",
        }

    calls = []
    state = acquisition.capture(
        store,
        client_factory=_factory({"/historical/markets": historical_markets}, calls),
    )
    capture_id = state["capture_id"]
    assert state["complete"]
    assert state["tasks"][3]["restarts"] == 1
    assert not any(endpoint in {"/markets", "/events/multivariate"} for endpoint in calls)

    legacy_build = store.path(f"builds/{capture_id}")
    legacy_build.mkdir(parents=True)
    (legacy_build / "keep").write_text("v1")
    v2_build = store.path(f"builds-v2/{capture_id}")
    v2_build.mkdir(parents=True)
    (v2_build / "keep").write_text("v2")

    winner_modes = []
    disk_index = warehouse.DiskParentIndex

    class TrackingDiskParentIndex(disk_index):
        def __init__(self, store, **options):
            winner_modes.append(options.get("collect_winners", True))
            super().__init__(store, **options)

    monkeypatch.setattr(warehouse, "DiskParentIndex", TrackingDiskParentIndex)
    original_connect = warehouse.duckdb.connect
    historical_settings = []

    def record_projection(*args, **kwargs):
        connection = original_connect(*args, **kwargs)
        database = args[0] if args else kwargs.get("database")
        config = kwargs.get("config") or {}
        if "build-historical-" in str(database) and config.get("memory_limit"):
            historical_settings.append((dict(config), _duckdb_settings(connection)))
        return connection

    monkeypatch.setattr(warehouse.duckdb, "connect", record_projection)
    receipt = warehouse.build_historical(store, capture_id)
    assert winner_modes == [False]
    assert len(historical_settings) == 2
    for historical_config, actual_settings in historical_settings:
        assert historical_config["memory_limit"] == warehouse.DBT_MEMORY_LIMIT
        assert historical_config["threads"] == str(warehouse.DBT_THREADS)
        assert historical_config["temp_directory"].endswith("/duckdb-spill")
        expected_connection = original_connect(config=historical_config)
        assert actual_settings == _duckdb_settings(expected_connection)
        expected_connection.close()
    assert (legacy_build / "keep").read_text() == "v1"
    assert (v2_build / "keep").read_text() == "v2"
    assert warehouse.build_directory(capture_id, 3) == f"builds-historical/{capture_id}"

    with duckdb.connect(
        str(store.path(f"builds-historical/{capture_id}/warehouse.duckdb")), read_only=True
    ) as connection:
        history_rows = connection.execute(
            """select received_at, observation_id, payload_json
               from main.observation_history
               where entity_kind='markets' and entity_ticker='HISTORICAL'"""
        ).fetchall()
        current = connection.execute(
            """select observation_id, json_extract_string(payload_json, '$.title')
               from main.current_markets where market_ticker='HISTORICAL'"""
        ).fetchone()
        combo_status = connection.execute(
            """select combo_market_ticker, resolved from main.combo_legs
               order by combo_market_ticker"""
        ).fetchall()
        coverage = connection.execute("select * from main.catalogue_coverage").fetchall()

    assert len(history_rows) == 2
    assert {row[0] for row in history_rows} == {timestamp}
    winner = max(history_rows, key=lambda row: row[1])
    assert current == (winner[1], json.loads(winner[2])["title"])
    assert combo_status == [
        ("COMBO-HISTORY-RESOLVED", True),
        ("COMBO-HISTORY-UNRESOLVED", False),
    ]
    assert coverage == [
        (
            HISTORICAL_CONTRACT,
            "historical",
            ["events_combo", "markets_live", "markets_combo"],
            capture_id,
            True,
        )
    ]
    assert warehouse.certified(store, capture_id, catalogue_version=3) == receipt


def test_historical_build_certifies_an_empty_selected_catalogue(store):
    empty = {
        "/series": (200, {"series": [], "cursor": ""}),
        "/events": (200, {"events": [], "cursor": ""}),
        "/historical/markets": (200, {"markets": [], "cursor": ""}),
    }
    state = acquisition.capture(store, client_factory=_factory(empty))
    receipt = warehouse.build_historical(store, state["capture_id"])
    with duckdb.connect(
        str(store.path(f"builds-historical/{state['capture_id']}/warehouse.duckdb")),
        read_only=True,
    ) as connection:
        assert connection.execute("select count(*) from main.observation_history").fetchone() == (
            0,
        )
        assert connection.execute("select count(*) from main.catalogue_coverage").fetchone() == (1,)
    assert warehouse.certified(store, state["capture_id"], catalogue_version=3) == receipt


def _cleanup_capture(store):
    return acquisition.capture(store, client_factory=_factory({}))


def _historical_stage(store, capture_id, suffix):
    relative = f"tmp/build-historical-{suffix}"
    path = store.path(relative)
    (path / "landing").mkdir(parents=True)
    (path / "landing" / "receipt.bin").write_bytes(b"stage evidence")
    return relative, path


def test_abandoned_historical_stage_cleanup_prunes_only_owned_direct_stages(store):
    state = _cleanup_capture(store)
    capture_id = state["capture_id"]
    relative, stage = _historical_stage(store, capture_id, "a" * 32)
    second_relative, second_stage = _historical_stage(store, capture_id, "b" * 32)
    untouched = {
        "tmp/build-historical-not-a-uuid": b"unknown name",
        "tmp/build-v2-" + "c" * 32: b"v2 stage",
        "tmp/replaced-" + "9" * 32 + "-v3/keep": b"replacement backup",
        "tmp/parent-index-test/index.sqlite": b"parent index",
        "tmp/recovery-index/cache/index.sqlite": b"recovery cache",
        f"builds-historical/{capture_id}/warehouse.duckdb": b"published build",
    }
    for name, value in untouched.items():
        path = store.path(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)
    store.sync("tmp")
    store.sync(f"builds-historical/{capture_id}")
    before = store.usage()["temporary_bytes"]

    with store.lock():
        warehouse.recover_and_cleanup_abandoned_historical_stages(store, capture_id)
        assert store.check_storage()["temporary_bytes"] < before

    assert not stage.exists()
    assert not second_stage.exists()
    assert store.usage()["temporary_bytes"] == before - len(b"stage evidence") * 2
    for name, value in untouched.items():
        assert store.path(name).read_bytes() == value
    assert relative.startswith("tmp/build-historical-")
    assert second_relative.startswith("tmp/build-historical-")


def test_abandoned_historical_stage_cleanup_requires_lock_root_and_capture(store, tmp_path):
    state = _cleanup_capture(store)
    capture_id = state["capture_id"]
    _relative, stage = _historical_stage(store, capture_id, "d" * 32)
    with pytest.raises(CatalogueError, match="canonical writer lock"):
        warehouse.recover_and_cleanup_abandoned_historical_stages(store, capture_id)
    assert stage.exists()

    with store.lock(), pytest.raises(CatalogueError):
        warehouse.recover_and_cleanup_abandoned_historical_stages(store, "0" * 32)
    assert stage.exists()

    scoped = Store(tmp_path / "scoped-root", series_ticker="SCOPED")
    with scoped.lock(), pytest.raises(CatalogueError, match="unscoped"):
        warehouse.recover_and_cleanup_abandoned_historical_stages(scoped, capture_id)


def test_abandoned_historical_cleanup_recovers_before_pruning(store, monkeypatch):
    state = _cleanup_capture(store)
    capture_id = state["capture_id"]
    _relative, stage = _historical_stage(store, capture_id, "e" * 32)
    original_recover = warehouse._recover_build
    calls = []

    def record_recover(target_store, target_capture, version):
        calls.append((target_capture, version))
        assert stage.exists()
        return original_recover(target_store, target_capture, version)

    monkeypatch.setattr(warehouse, "_recover_build", record_recover)
    with store.lock():
        warehouse.recover_and_cleanup_abandoned_historical_stages(store, capture_id)
    assert calls == [(capture_id, 3)]
    assert not stage.exists()


def test_abandoned_historical_cleanup_preserves_stages_when_backup_recovery_fails(
    store, monkeypatch
):
    state = _cleanup_capture(store)
    capture_id = state["capture_id"]
    _relative, stage = _historical_stage(store, capture_id, "f" * 32)
    backup = store.path(f"tmp/replaced-{capture_id}-v3")
    backup.mkdir(parents=True)
    (backup / "keep").write_bytes(b"backup")

    def fail_recover(*_args):
        raise CatalogueError("synthetic recovery failure")

    monkeypatch.setattr(warehouse, "_recover_build", fail_recover)
    with store.lock(), pytest.raises(CatalogueError, match="synthetic"):
        warehouse.recover_and_cleanup_abandoned_historical_stages(store, capture_id)
    assert (stage / "landing" / "receipt.bin").read_bytes() == b"stage evidence"
    assert (backup / "keep").read_bytes() == b"backup"


@pytest.mark.parametrize("unsafe", ["symlink", "hardlink", "special"])
def test_abandoned_historical_cleanup_prevalidates_all_stages(store, tmp_path, unsafe):
    state = _cleanup_capture(store)
    capture_id = state["capture_id"]
    _safe_relative, safe_stage = _historical_stage(store, capture_id, "1" * 32)
    _unsafe_relative, unsafe_stage = _historical_stage(store, capture_id, "2" * 32)
    marker = tmp_path / "outside.bin"
    marker.write_bytes(b"outside")
    if unsafe == "symlink":
        (unsafe_stage / "landing" / "unsafe-link").symlink_to(marker)
    elif unsafe == "hardlink":
        os.link(marker, unsafe_stage / "landing" / "hard-link")
    else:
        os.mkfifo(unsafe_stage / "landing" / "pipe")

    with store.lock(), pytest.raises(CatalogueError, match="unsafe|hard-linked"):
        warehouse.recover_and_cleanup_abandoned_historical_stages(store, capture_id)
    assert safe_stage.exists()
    assert unsafe_stage.exists()
    assert marker.read_bytes() == b"outside"


@pytest.mark.parametrize("reference", ["pointer", "build_receipt", "release_manifest"])
def test_abandoned_historical_cleanup_rejects_known_stage_references(store, reference):
    state = _cleanup_capture(store)
    capture_id = state["capture_id"]
    relative, stage = _historical_stage(store, capture_id, "3" * 32)
    if reference == "pointer":
        target = store.path("current-historical.json")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps({"staging_path": relative}), encoding="utf-8")
    elif reference == "release_manifest":
        release_id = "a" * 64
        pointer = store.path("current-historical.json")
        pointer.parent.mkdir(parents=True, exist_ok=True)
        pointer.write_text(json.dumps({"source_release_id": release_id}), encoding="utf-8")
        target = store.path(f"releases/{release_id}/manifest.json")
    else:
        target = store.path(f"builds-historical/{capture_id}/build.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    if reference != "pointer":
        target.write_text(json.dumps({"staging_path": relative}), encoding="utf-8")

    with store.lock(), pytest.raises(CatalogueError, match="referenced"):
        warehouse.recover_and_cleanup_abandoned_historical_stages(store, capture_id)
    assert (stage / "landing" / "receipt.bin").read_bytes() == b"stage evidence"


def test_abandoned_historical_cleanup_uses_receipt_size_contract(store):
    state = _cleanup_capture(store)
    capture_id = state["capture_id"]
    _relative, stage = _historical_stage(store, capture_id, "6" * 32)
    receipt = store.path(f"builds-historical/{capture_id}/build.json")
    receipt.parent.mkdir(parents=True, exist_ok=True)
    receipt.write_text(json.dumps({"payload": "x" * (1024**2 + 16)}), encoding="utf-8")

    with store.lock():
        warehouse.recover_and_cleanup_abandoned_historical_stages(store, capture_id)
    assert not stage.exists()


def test_abandoned_historical_cleanup_rejects_receipts_over_canonical_limit(store, monkeypatch):
    state = _cleanup_capture(store)
    capture_id = state["capture_id"]
    _relative, stage = _historical_stage(store, capture_id, "7" * 32)
    receipt = store.path(f"builds-historical/{capture_id}/build.json")
    receipt.parent.mkdir(parents=True, exist_ok=True)
    receipt.write_bytes(b'{"payload":"' + b"x" * 1024 + b'"}')
    monkeypatch.setattr(warehouse, "MAX_BUNDLE_BYTES", 1024)

    with store.lock(), pytest.raises(CatalogueError, match="oversized"):
        warehouse.recover_and_cleanup_abandoned_historical_stages(store, capture_id)
    assert stage.exists()


def test_abandoned_historical_cleanup_rechecks_stage_inode_before_remove(store, monkeypatch):
    state = _cleanup_capture(store)
    capture_id = state["capture_id"]
    _relative, stage = _historical_stage(store, capture_id, "4" * 32)
    moved = stage.with_name("retained-after-race")
    validate = warehouse._validate_historical_stage

    def replace_after_validation(path, root_device):
        identity = validate(path, root_device)
        path.rename(moved)
        path.mkdir()
        (path / "replacement").write_bytes(b"preserve")
        return identity

    monkeypatch.setattr(warehouse, "_validate_historical_stage", replace_after_validation)
    with store.lock(), pytest.raises(CatalogueError, match="changed during cleanup"):
        warehouse.recover_and_cleanup_abandoned_historical_stages(store, capture_id)
    assert stage.is_dir()
    assert (stage / "replacement").read_bytes() == b"preserve"
    assert moved.is_dir()
    store.sync("tmp")


def test_abandoned_historical_cleanup_releases_actual_temporary_budget(store):
    state = _cleanup_capture(store)
    capture_id = state["capture_id"]
    relative, stage = _historical_stage(store, capture_id, "5" * 32)
    payload = stage / "large.bin"
    payload.write_bytes(b"x" * 4096)
    store.sync(relative)
    usage = store.usage()["temporary_bytes"]
    store.limits = Limits(max_temp_bytes=usage - payload.stat().st_size + 1)

    with store.lock():
        with pytest.raises(BudgetExceeded, match="temporary storage"):
            store.check_storage()
        warehouse.recover_and_cleanup_abandoned_historical_stages(store, capture_id)
        assert store.check_storage()["temporary_bytes"] < store.limits.max_temp_bytes
    assert not stage.exists()
