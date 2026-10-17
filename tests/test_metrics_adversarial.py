from __future__ import annotations

import json
import threading

import pytest
from conftest import FakeClient

from kalshi_events import cli, metrics
from kalshi_events.capture import capture
from kalshi_events.common import BudgetExceeded, CatalogueError


def test_capture_propagates_active_measurement_into_every_worker(store):
    caller = threading.get_ident()
    workers = set()
    lock = threading.Lock()

    class MeasuredClient(FakeClient):
        def response(self, endpoint, params):
            with lock:
                workers.add(threading.get_ident())
            with metrics.phase("worker_request"):
                metrics.add("worker_requests")
            return super().response(endpoint, params)

    with metrics.collect() as measured:
        state = capture(store, client_factory=MeasuredClient)

    report = measured.report()
    assert state["complete"]
    assert workers and caller not in workers
    assert report["counters"]["worker_requests"] == state["requests"]
    assert report["phases_seconds"]["worker_request"] >= 0


def test_inactive_hooks_and_nested_failed_collection_do_not_leak():
    metrics.add("outside_before")
    metrics.peak("outside_before", 9)
    with metrics.phase("outside_before"):
        pass
    with metrics.collect() as outer:
        metrics.add("outer")
        with (
            pytest.raises(RuntimeError, match="synthetic failure"),
            metrics.collect() as inner,
            metrics.phase("failed_phase"),
        ):
            metrics.add("inner")
            raise RuntimeError("synthetic failure")
        metrics.add("outer")
    metrics.add("outside_after")
    metrics.peak("outside_after", 10)
    with metrics.phase("outside_after"):
        pass
    with metrics.collect() as fresh:
        pass

    assert outer.report()["counters"] == {"outer": 2}
    assert outer.report()["phases_seconds"] == {}
    assert inner.report()["counters"] == {"inner": 1}
    assert inner.report()["phases_seconds"]["failed_phase"] >= 0
    assert fresh.report()["counters"] == {}
    assert fresh.report()["phases_seconds"] == {}
    assert fresh.report()["store_accounted_high_water_bytes"] == {}


@pytest.mark.parametrize("error,code", [(CatalogueError, 1), (BudgetExceeded, 3)])
def test_cli_preserves_error_protocol_and_writes_failed_phase_report(
    tmp_path, capsys, monkeypatch, error, code
):
    def failing_operation(args):
        with metrics.phase("failed_operation"):
            metrics.add("attempts")
            raise error("synthetic operation failure")

    monkeypatch.setattr(cli, "execute", failing_operation)
    root = tmp_path / "root"
    assert cli.main(["--root", str(root), "status"]) == code
    unmeasured = capsys.readouterr()
    report = tmp_path / "metrics.json"
    assert cli.main(["--root", str(root), "--metrics-file", str(report), "status"]) == code
    measured = capsys.readouterr()

    assert measured.out == unmeasured.out
    assert measured.err == unmeasured.err
    data = json.loads(report.read_text())
    assert data["counters"] == {"attempts": 1}
    assert data["phases_seconds"]["failed_operation"] >= 0


def test_existing_report_rejected_before_capture_or_root_creation(tmp_path, monkeypatch, capsys):
    report = tmp_path / "metrics.json"
    report.write_text("keep this report\n")
    root = tmp_path / "root"
    calls = []

    class RecordingClient(FakeClient):
        def get(self, *args, **kwargs):
            calls.append(args)
            return super().get(*args, **kwargs)

    monkeypatch.setattr("kalshi_events.capture.PublicClient", RecordingClient)
    assert cli.main(["--root", str(root), "--metrics-file", str(report), "capture"]) == 1
    assert capsys.readouterr().out == ""
    assert report.read_text() == "keep this report\n"
    assert calls == []
    assert not root.exists()


def test_report_inside_new_root_rejected_before_any_files_are_created(tmp_path, capsys):
    root = tmp_path / "root"
    report = root / "nested" / "metrics.json"
    assert cli.main(["--root", str(root), "--metrics-file", str(report), "status"]) == 1
    assert "outside the operator root" in capsys.readouterr().err
    assert not root.exists()


@pytest.mark.parametrize("link_kind", ["file", "parent"])
def test_write_report_rejects_symlink_without_touching_target(tmp_path, link_kind):
    target = tmp_path / "target"
    target.mkdir()
    existing = target / "metrics.json"
    existing.write_text("keep\n")
    link = tmp_path / "linked"
    if link_kind == "file":
        link.symlink_to(existing)
        report = link
    else:
        link.symlink_to(target, target_is_directory=True)
        report = link / "new-report.json"

    with metrics.collect() as measured:
        metrics.add("sentinel")
    with pytest.raises(OSError, match="symlink"):
        metrics.write_report(report, measured)

    assert existing.read_text() == "keep\n"
    assert not (target / "new-report.json").exists()


def test_report_sibling_with_shared_root_prefix_is_allowed(tmp_path, capsys):
    root = tmp_path / "root"
    report = tmp_path / "root-reports" / "metrics.json"
    assert cli.main(["--root", str(root), "--metrics-file", str(report), "status"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["root"] == str(root)
    assert "metrics" not in result
    assert json.loads(report.read_text())["version"] == 1
    assert not list(root.rglob("*metrics*"))
