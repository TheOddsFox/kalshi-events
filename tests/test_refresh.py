from __future__ import annotations

import json
from pathlib import Path

import pytest

from kalshi_events import capture, cli, publish, refresh
from kalshi_events.common import BudgetExceeded, CatalogueError, Store


@pytest.fixture
def store(tmp_path: Path) -> Store:
    return Store(tmp_path / "root")


def _successful_capture(
    monkeypatch: pytest.MonkeyPatch, *, incremental: bool = False
) -> list[dict]:
    identity = "b" * 32
    calls: list[dict] = []

    def run(_store, **kwargs):
        calls.append(kwargs)
        if kwargs.get("_on_capture_id"):
            kwargs["_on_capture_id"](identity)
        return {
            "capture_id": identity,
            "complete": True,
            "incremental": incremental,
            "baseline": {"source_release_id": "a" * 64} if incremental else None,
            "requests": 3,
            "downloaded_bytes": 123,
            "accounting_complete": True,
        }

    monkeypatch.setattr(capture, "capture", run)
    monkeypatch.setattr(
        capture,
        "read_capture",
        lambda *_args, **_kwargs: {"capture_id": identity, "complete": True},
    )
    monkeypatch.setattr(
        publish,
        "publish",
        lambda *_args, **_kwargs: {
            "source_release_id": "c" * 64,
            "output": "/root/release",
        },
    )
    return calls


def test_incremental_refresh_uses_v2_baseline_and_only_publishes_v2(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    store.path("current-v2.json").touch()
    calls = _successful_capture(monkeypatch, incremental=True)
    result, code = refresh.run(store)
    assert code == 0
    assert result["complete"] is True
    assert result["publication_state"] == "verified"
    assert result["source_release_id"] == "c" * 64
    assert calls[0]["incremental"] is True


def test_v1_fallback_and_missing_baseline(store: Store, monkeypatch: pytest.MonkeyPatch) -> None:
    store.path("current.json").touch()
    calls = _successful_capture(monkeypatch, incremental=True)
    result, code = refresh.run(store)
    assert code == 0
    assert result["source_release_id"] == "c" * 64
    assert calls[0]["incremental"] is True

    missing_store = Store(store.root.parent / "missing")
    result, code = refresh.run(missing_store)
    assert code == 1
    assert result["stage"] == "preflight"
    assert result["publication_state"] == "not_attempted"


def test_full_rebase_skips_baseline_and_never_changes_v1_pointer(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    v1_pointer = store.path("current.json")
    v1_pointer.write_text("keep")
    _successful_capture(monkeypatch, incremental=False)
    result, code = refresh.run(store, full=True)
    assert code == 0
    assert v1_pointer.read_text() == "keep"


def test_eight_delta_limit_stops_before_capture(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    store.path("current-v2.json").touch()

    def depth_failure(*_args, **_kwargs):
        raise CatalogueError("eight deltas already depend on this baseline; publish a full rebase")

    monkeypatch.setattr(capture, "capture", depth_failure)
    result, code = refresh.run(store)
    assert code == 1
    assert "run refresh --full" in result["error"]


def test_resume_rejects_targeted_and_conflicting_compression_before_capture(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        capture,
        "read_capture",
        lambda *_a, **_k: {
            "capture_id": "d" * 32,
            "selected": ["MKT"],
            "compression": "gzip",
        },
    )
    monkeypatch.setattr(capture, "capture", lambda *_a, **_k: pytest.fail("network started"))
    result, code = refresh.run(store, capture_id="d" * 32, compression="identity")
    assert code == 1
    assert result["capture_id"] == "d" * 32
    assert result["error"] == "targeted captures cannot be resumed by refresh"


def test_resume_rejects_conflicting_compression_before_capture(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        capture,
        "read_capture",
        lambda *_a, **_k: {
            "capture_id": "d" * 32,
            "selected": None,
            "compression": "gzip",
        },
    )
    monkeypatch.setattr(capture, "capture", lambda *_a, **_k: pytest.fail("network started"))
    result, code = refresh.run(store, capture_id="d" * 32, compression="identity")
    assert code == 1
    assert result["resumable"] is True
    assert result["error"] == "resume compression differs from capture"


def test_conflicting_full_and_capture_id_is_structured_failure(store: Store) -> None:
    result, code = refresh.run(store, full=True, capture_id="d" * 32)
    assert code == 1
    assert result["capture_id"] is None
    assert result["stage"] == "preflight"


def test_cli_conflicting_modes_emits_json_without_creating_root(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "root"
    code = cli.main(["--root", str(root), "refresh", "--full", "--capture-id", "d" * 32])
    captured = capsys.readouterr()
    assert code == 1
    assert json.loads(captured.out)["error"] == "--full and --capture-id cannot be used together"
    assert captured.err == ""
    assert not root.exists()


def test_capture_budget_retains_id_and_reports_unavailable_accounting(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    identity = "e" * 32

    def stop(_store, **kwargs):
        kwargs["_on_capture_id"](identity)
        raise BudgetExceeded("time budget")

    monkeypatch.setattr(capture, "capture", stop)
    result, code = refresh.run(store, full=True)
    assert code == 3
    assert result["capture_id"] == identity
    assert result["stage"] == "capture"
    assert result["requests"] is None
    assert result["accounting_complete"] is False
    assert result["resumable"] is None


def test_budget_stop_with_uncertain_journal_does_not_confirm_resumability(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_append = capture._append_v2_record

    def fail_stop_record(_store, state, op, payload):
        if op == "control" and payload.get("kind") == "stop":
            state.journal_uncertain = True
            raise OSError("journal append failed")
        return original_append(_store, state, op, payload)

    monkeypatch.setattr(capture, "_append_v2_record", fail_stop_record)
    monkeypatch.setattr(
        capture,
        "_run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(BudgetExceeded("time limit")),
    )
    result, code = refresh.run(store, full=True)
    assert code == 3
    assert result["capture_id"] is not None
    assert result["capture_complete"] is False
    assert result["resumable"] is None


def test_publication_failure_is_unknown_and_capture_can_be_resumed(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    _successful_capture(monkeypatch, incremental=False)

    def fail(*_args, **_kwargs):
        raise OSError("pruning failed after promotion")

    monkeypatch.setattr(publish, "publish", fail)
    result, code = refresh.run(store, full=True)
    assert code == 1
    assert result["capture_complete"] is True
    assert result["resumable"] is True
    assert result["publication_state"] == "unknown"
    assert result["stage"] == "publication"


def test_cli_store_and_metrics_failures_emit_one_refresh_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def fail_store(*_args, **_kwargs):
        raise OSError("root cannot be opened")

    monkeypatch.setattr(cli, "Store", fail_store)
    identity = "9" * 32
    code = cli.main(["--root", str(tmp_path / "root"), "refresh", "--capture-id", identity])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 1
    assert captured.err == ""
    assert payload["stage"] == "preflight"
    assert payload["publication_state"] == "not_attempted"
    assert payload["capture_id"] == identity


def test_metrics_write_failure_is_in_refresh_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "root"
    report = tmp_path / "metrics.json"
    monkeypatch.setattr(
        refresh,
        "run",
        lambda *_a, **_k: (
            {
                **refresh.result_template(),
                "complete": True,
                "stage": "complete",
                "publication_state": "verified",
            },
            0,
        ),
    )
    monkeypatch.setattr(
        cli.metrics,
        "write_report",
        lambda *_a, **_k: (_ for _ in ()).throw(OSError("metrics disk full")),
    )
    code = cli.main(["--root", str(root), "--metrics-file", str(report), "refresh", "--full"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 1
    assert captured.err == ""
    assert payload["complete"] is False
    assert payload["publication_state"] == "verified"
    assert "metrics report failed" in payload["error"]


def test_initialization_failure_preserves_allocated_id_and_unknown_resumability(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    identity = "f" * 32
    monkeypatch.setattr(
        capture, "read_capture", lambda *_a, **_k: (_ for _ in ()).throw(CatalogueError("missing"))
    )

    def fail(_store, **kwargs):
        kwargs["_on_capture_id"](identity)
        raise CatalogueError("genesis write failed")

    monkeypatch.setattr(capture, "capture", fail)
    result, code = refresh.run(store, full=True)
    assert code == 1
    assert result["capture_id"] == identity
    assert result["resumable"] is None


def test_capture_id_hook_runs_before_genesis_persistence(
    store: Store, monkeypatch: pytest.MonkeyPatch
) -> None:
    notified: list[str] = []
    observed: list[str] = []

    def initialize(_store, state):
        observed.append(state["capture_id"])
        return state

    monkeypatch.setattr(capture, "_initialize_v2", initialize)
    state = capture.new_capture(store, None, _on_capture_id=notified.append)
    assert notified == observed == [state["capture_id"]]
