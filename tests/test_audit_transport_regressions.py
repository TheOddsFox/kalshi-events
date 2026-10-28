from __future__ import annotations

import json
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

from kalshi_events import cli, common, transport
from kalshi_events.common import Budget, BudgetExceeded, CatalogueError, Limits, Store
from kalshi_events.transport import PublicClient


@pytest.mark.parametrize("failure", ["walk", "stat", "missing", "file"])
def test_reconcile_fails_closed_without_discarding_known_usage(
    store: Store, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    store.write("tracked.json", {"known": True})
    known_usage = store.usage()
    untracked = store.path("untracked.bin")
    untracked.write_bytes(b"new evidence")

    if failure == "walk":

        def denied_walk(path, *, followlinks=False, onerror=None):
            assert onerror is not None
            onerror(PermissionError("injected unreadable directory"))
            yield

        monkeypatch.setattr(common.os, "walk", denied_walk)
        expected = "scan"
    elif failure == "stat":
        original_stat = Path.stat

        def denied_stat(path, *args, **kwargs):
            if path == untracked:
                raise PermissionError("injected stat failure")
            return original_stat(path, *args, **kwargs)

        monkeypatch.setattr(Path, "stat", denied_stat)
        expected = "stat"
    elif failure == "missing":
        shutil.rmtree(store.root)
        expected = "disappeared"
    else:
        shutil.rmtree(store.root)
        store.root.write_text("not a directory")
        expected = "not a directory"

    with pytest.raises(CatalogueError, match=expected):
        store.reconcile()
    assert store.usage() == known_usage

    if failure not in {"missing", "file"}:
        monkeypatch.undo()
        assert store.reconcile()["retained_bytes"] == known_usage["retained_bytes"] + len(
            b"new evidence"
        )


class _FakeClock:
    def __init__(self) -> None:
        self.value = 0.0
        self.lock = threading.Lock()

    def monotonic(self) -> float:
        with self.lock:
            return self.value

    def sleep(self, seconds: float) -> None:
        with self.lock:
            self.value += seconds


def test_http_start_spacing_is_applied_after_delayed_spool_setup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(transport.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(transport.time, "sleep", clock.sleep)
    store = Store(tmp_path / "root", Limits(max_rate=5, max_requests=4))
    starts: list[float] = []
    first_spooling = threading.Event()
    release_first = threading.Event()
    second_started = threading.Event()
    original_spool = store.spool

    def delayed_spool(relative: str):
        if threading.current_thread().name.endswith("_0"):
            first_spooling.set()
            if not release_first.wait(timeout=2):
                raise RuntimeError("first spool was not released")
        return original_spool(relative)

    store.spool = delayed_spool
    with PublicClient(store, Budget(store)) as client:
        client.client.close()

        def handler(_request):
            starts.append(clock.monotonic())
            second_started.set()
            return httpx.Response(200, stream=httpx.ByteStream(b"{}"))

        client.client = httpx.Client(transport=httpx.MockTransport(handler), trust_env=False)
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="rate") as pool:
            first = pool.submit(client.get, "/series")
            assert first_spooling.wait(timeout=2)
            second = pool.submit(client.get, "/series")
            try:
                assert second_started.wait(timeout=2)
            finally:
                release_first.set()
            first.result(timeout=2)
            second.result(timeout=2)

    assert len(starts) == 2
    assert abs(starts[1] - starts[0]) >= 0.2 - 1e-9


def test_rate_wait_that_exceeds_deadline_keeps_durable_attempt_accounting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(transport.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(transport.time, "sleep", clock.sleep)
    store = Store(
        tmp_path / "root",
        Limits(max_rate=1, max_requests=4, max_duration_seconds=0.5),
    )
    store.started = 0.0
    starts = []
    with PublicClient(store, Budget(store)) as client:
        client.client.close()
        client.client = httpx.Client(
            transport=httpx.MockTransport(
                lambda _request: (
                    starts.append(clock.monotonic()),
                    httpx.Response(200, stream=httpx.ByteStream(b"{}")),
                )[1]
            ),
            trust_env=False,
        )
        client.get("/series")
        with pytest.raises(BudgetExceeded, match="request spacing"):
            client.get("/series")

    receipts = [json.loads(path.read_text()) for path in store.path("raw").glob("*/manifest.json")]
    assert len(receipts) == 2
    assert len(starts) == 1
    failed = next(receipt for receipt in receipts if receipt["status"] is None)
    assert all(receipt["accounting_complete"] for receipt in receipts)
    assert failed["error"] == "budget exhausted"
    assert failed["downloaded_bytes"] == 0


def test_cli_deadline_during_final_reconciliation_retains_stopped_capture_accounting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    clock = [0.0]
    monkeypatch.setattr(common.time, "monotonic", lambda: clock[0])
    original_walk = common.os.walk
    walk_calls = 0

    def delayed_final_walk(path, *, followlinks=False, onerror=None):
        nonlocal walk_calls
        walk_calls += 1
        if walk_calls == 2:
            clock[0] = 2.0
        yield from original_walk(path, followlinks=followlinks, onerror=onerror)

    monkeypatch.setattr(common.os, "walk", delayed_final_walk)
    monkeypatch.setattr(
        cli.acquisition,
        "capture",
        lambda _store, **_kwargs: {
            "capture_id": "c" * 32,
            "complete": False,
            "error": "capture budget exhausted",
            "requests": 2,
            "downloaded_bytes": 17,
            "accounting_complete": False,
        },
    )

    code = cli.main(
        [
            "--root",
            str(tmp_path / "root"),
            "capture",
            "--max-duration-seconds",
            "1",
        ]
    )
    output = capsys.readouterr()
    result = json.loads(output.out)

    assert walk_calls == 2
    assert output.err == ""
    assert code == 3
    assert result["complete"] is False
    assert result["capture_id"] == "c" * 32
    assert result["requests"] == 2
    assert result["downloaded_bytes"] == 17
    assert result["accounting_complete"] is False
    assert result["error"] == "capture budget exhausted"


@pytest.mark.parametrize("expire_final_scan", [False, True])
def test_cli_final_reconciliation_checks_deadline_and_preserves_usage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    expire_final_scan: bool,
) -> None:
    clock = [0.0]
    monkeypatch.setattr(common.time, "monotonic", lambda: clock[0])
    original_walk = common.os.walk
    walk_calls = 0
    stores = []

    def delayed_final_walk(path, *, followlinks=False, onerror=None):
        nonlocal walk_calls
        walk_calls += 1
        if expire_final_scan and walk_calls == 2:
            clock[0] = 2.0
        yield from original_walk(path, followlinks=followlinks, onerror=onerror)

    monkeypatch.setattr(common.os, "walk", delayed_final_walk)

    def finished(store, **_kwargs):
        stores.append(store)
        return {
            "capture_id": "a" * 32,
            "complete": True,
            "error": None,
            "requests": 1,
            "downloaded_bytes": 0,
            "accounting_complete": True,
        }

    monkeypatch.setattr(cli.acquisition, "capture", finished)
    code = cli.main(
        [
            "--root",
            str(tmp_path / "root"),
            "capture",
            "--max-duration-seconds",
            "1",
        ]
    )
    output = capsys.readouterr()
    result = json.loads(output.out)

    assert walk_calls == 2
    assert output.err == ""
    if expire_final_scan:
        assert code == 3
        assert result["complete"] is False
        assert result["error"] == "operation deadline exhausted"
        assert stores[0].usage()["retained_bytes"] > 0
    else:
        assert code == 0
        assert result["complete"] is True


def test_cli_final_scan_error_does_not_mask_operation_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    original_walk = common.os.walk
    walk_calls = 0
    walk_error_seen = False

    def failing_final_walk(path, *, followlinks=False, onerror=None):
        nonlocal walk_calls, walk_error_seen
        walk_calls += 1
        if walk_calls == 2:
            assert onerror is not None
            walk_error_seen = True
            onerror(PermissionError("injected final scan failure"))
        yield from original_walk(path, followlinks=followlinks, onerror=onerror)

    monkeypatch.setattr(common.os, "walk", failing_final_walk)

    def fail_capture(_store, **_kwargs):
        raise CatalogueError("original capture failure")

    monkeypatch.setattr(cli.acquisition, "capture", fail_capture)
    code = cli.main(["--root", str(tmp_path / "root"), "capture"])
    output = capsys.readouterr()

    assert walk_calls == 2
    assert walk_error_seen
    assert code == 1
    assert output.out == ""
    assert "original capture failure" in output.err
    assert "cannot scan" not in output.err
