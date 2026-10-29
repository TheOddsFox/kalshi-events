"""Adversarial refresh contracts against real durable capture artifacts."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from conftest import FakeClient

from kalshi_events import capture, cli, incremental, publish, refresh, warehouse
from kalshi_events.common import BudgetExceeded, Store


def test_genesis_survives_first_snapshot_failure_and_exact_id_can_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = Store(tmp_path / "root")
    original_save = capture._save_v2

    def fail_snapshot(*_args, **_kwargs):
        raise OSError("first snapshot synchronization failed")

    monkeypatch.setattr(capture, "_save_v2", fail_snapshot)
    with store.lock():
        result, code = refresh.run(store, full=True)
    assert code == 1
    assert result["capture_id"] is not None
    identity = result["capture_id"]
    assert result["resumable"] is None
    assert result["capture_complete"] is None
    assert result["requests"] is None
    assert result["accounting_complete"] is False
    assert store.path(f"captures/{identity}/pages.jsonl").is_file()
    assert not store.path(f"captures/{identity}/state.json").exists()

    monkeypatch.setattr(capture, "_save_v2", original_save)
    recovered = capture.read_capture(store, identity)
    assert recovered["capture_id"] == identity
    assert recovered["complete"] is False
    monkeypatch.setattr(capture, "PublicClient", FakeClient)
    monkeypatch.setattr(
        publish,
        "publish",
        lambda _store, capture_id, catalogue_version: {
            "output": str(store.path("releases/test")),
            "source_release_id": "c" * 64,
        },
    )
    with store.lock():
        resumed, resumed_code = refresh.run(store, capture_id=identity)
    assert resumed_code == 0
    assert resumed["capture_id"] == identity
    assert resumed["resumable"] is True
    assert resumed["capture_complete"] is True
    assert {p.name for p in store.path("captures").iterdir()} == {identity}


def test_budget_failure_during_final_reconcile_preserves_exit_three_and_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Post-capture budgets are workflow stops even when publication already succeeded."""
    identity = "a" * 32
    original_reconcile = Store.reconcile
    reconciles = 0

    def reconcile(store):
        nonlocal reconciles
        reconciles += 1
        if reconciles > 1:
            raise BudgetExceeded("final reconciliation deadline exhausted")
        return original_reconcile(store)

    def finished(_store, **_kwargs):
        return {
            **refresh.result_template(),
            "complete": True,
            "capture_id": identity,
            "capture_complete": True,
            "resumable": True,
            "stage": "complete",
            "publication_state": "verified",
            "requests": 0,
            "downloaded_bytes": 0,
            "accounting_complete": True,
            "source_release_id": "b" * 64,
        }, 0

    monkeypatch.setattr(Store, "reconcile", reconcile)
    monkeypatch.setattr(refresh, "run", finished)
    code = cli.main(["--root", str(tmp_path / "root"), "refresh", "--capture-id", identity])
    streams = capsys.readouterr()
    result = json.loads(streams.out)
    assert streams.err == ""
    assert code == 3
    assert result["complete"] is False
    assert result["capture_id"] == identity
    assert result["capture_complete"] is True
    assert "deadline" in result["error"]


@pytest.mark.timeout(90)
def test_completed_refresh_matches_manual_v2_publication_from_frozen_capture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Compare actual public bytes while allowing internal dlt database IDs to differ."""
    seed = Store(tmp_path / "seed")
    state = capture.capture(seed, client_factory=FakeClient, catalogue_profile="exchange")
    assert state["complete"]
    with seed.lock():
        incremental.root_id(seed, create=True)
    for name in ("coordinated", "manual"):
        shutil.copytree(seed.root, tmp_path / name)
    coordinated = Store(tmp_path / "coordinated")
    manual = Store(tmp_path / "manual")

    def no_network(*_args, **_kwargs):
        pytest.fail("completed-capture resume must not create a source client")

    monkeypatch.setattr(capture, "PublicClient", no_network)
    original_land = warehouse._land
    landed_roots = []

    def land(store, *args, **kwargs):
        landed_roots.append(store.root)
        return original_land(store, *args, **kwargs)

    monkeypatch.setattr(warehouse, "_land", land)
    with coordinated.lock():
        result, code = refresh.run(coordinated, capture_id=state["capture_id"])
    assert code == 0, result
    assert result["requests"] == 0
    assert result["downloaded_bytes"] == 0
    assert result["capture_complete"] is True
    assert result["publication_state"] == "verified"
    manual_result = publish.publish(manual, state["capture_id"], catalogue_version=2)
    assert result["source_release_id"] == manual_result["source_release_id"]
    for name in publish.FILES:
        assert (Path(result["output"]) / name).read_bytes() == (
            Path(manual_result["output"]) / name
        ).read_bytes(), name
    assert landed_roots == [coordinated.root, manual.root]
    assert not coordinated.path("builds").exists()
    assert not manual.path("builds").exists()
    assert not coordinated.path("current.json").exists()
    assert not manual.path("current.json").exists()
    publish.verify_release(Path(result["output"]))


@pytest.mark.timeout(90)
def test_cli_exchange_resume_delta_and_explicit_rebase_use_only_v2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from conftest import COMBO, MARKET

    class ReconciliationClient(FakeClient):
        def response(self, endpoint, params):
            if endpoint == "/markets" and params.get("mve_filter") == "only":
                return 200, {"markets": [COMBO], "cursor": ""}
            if endpoint == "/markets" and params.get("mve_filter") == "exclude":
                return 200, {"markets": [MARKET], "cursor": ""}
            return super().response(endpoint, params)

    monkeypatch.setattr(capture, "PublicClient", ReconciliationClient)
    root = tmp_path / "root"

    def invoke(*arguments):
        code = cli.main(["--root", str(root), "refresh", *arguments])
        streams = capsys.readouterr()
        result = json.loads(streams.out)
        assert code == 0, result
        assert result["publication_state"] == "verified"
        assert result["capture_complete"] is True
        assert result["complete"] is True
        assert result["resumable"] is True
        assert not (root / "current.json").exists()
        assert not (root / "builds").exists()
        release = publish.verify_release(Path(result["output"]))
        assert release["contract"] == publish.CONTRACT_V2
        assert release["capture_id"] == result["capture_id"]
        return result, release

    baseline_capture = capture.capture(Store(root), catalogue_profile="exchange")
    baseline, baseline_release = invoke("--capture-id", baseline_capture["capture_id"])
    assert baseline_release["delta_depth"] == 0
    delta, delta_release = invoke()
    assert delta_release["delta_depth"] == 1
    assert delta_release["baseline_capture_id"] == baseline["capture_id"]
    assert capture.latest_catalogue_capture(Store(root)) == baseline["capture_id"]
    rebase_capture = capture.capture(Store(root), catalogue_profile="exchange")
    rebased, rebase_release = invoke("--capture-id", rebase_capture["capture_id"])
    assert rebase_release["delta_depth"] == 0
    assert rebase_release["baseline_capture_id"] is None
    assert rebased["capture_id"] not in {baseline["capture_id"], delta["capture_id"]}
    assert capture.latest_catalogue_capture(Store(root)) == rebased["capture_id"]
