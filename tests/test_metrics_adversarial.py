from __future__ import annotations

import json
import shutil
import threading

import pytest
from conftest import FakeClient

from kalshi_events import cli, metadata, metrics, publish, warehouse
from kalshi_events.capture import capture
from kalshi_events.common import BudgetExceeded, CatalogueError, Store


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


@pytest.fixture(scope="module")
def certified_metrics_artifacts(tmp_path_factory):
    directory = tmp_path_factory.mktemp("metrics-artifacts")
    store = Store(directory / "root")
    state = capture(store, client_factory=FakeClient, catalogue_profile="exchange")
    warehouse.build(store, state["capture_id"])
    release = publish.publish(store, state["capture_id"])
    bundle = directory / "metadata"
    metadata.export(store, ["MARKET"], bundle, capture_id=state["capture_id"])
    return bundle, store.path(f"releases/{release['source_release_id']}")


def _artifact_bytes(directory):
    return {
        str(path.relative_to(directory)): path.read_bytes()
        for path in directory.rglob("*")
        if path.is_file()
    }


@pytest.mark.parametrize("kind", ["metadata", "release"])
@pytest.mark.parametrize("report_location", ["equal", "child", "nested"])
def test_metrics_cannot_change_a_standalone_verified_artifact(
    certified_metrics_artifacts, tmp_path, capsys, kind, report_location
):
    source = certified_metrics_artifacts[kind == "release"]
    artifact = tmp_path / "artifact"
    shutil.copytree(source, artifact)
    verify = metadata.verify if kind == "metadata" else publish.verify_release
    expected = verify(artifact)
    before = _artifact_bytes(artifact)
    report = {
        "equal": artifact,
        "child": artifact / "metrics.json",
        "nested": artifact / "reports" / "metrics.json",
    }[report_location]
    arguments = (
        ["metadata", "verify", "--output", str(artifact)]
        if kind == "metadata"
        else ["verify", "--release", str(artifact)]
    )
    root = tmp_path / "unrelated-root"

    assert cli.main(["--root", str(root), "--metrics-file", str(report), *arguments]) == 1
    assert capsys.readouterr().out == ""
    assert _artifact_bytes(artifact) == before
    assert verify(artifact) == expected
    assert not root.exists()


@pytest.mark.parametrize("action", ["refresh", "export"])
@pytest.mark.parametrize("report_location", ["equal", "child", "nested", "ancestor"])
def test_metadata_metrics_overlap_rejected_before_acquisition_or_output_creation(
    tmp_path, monkeypatch, capsys, action, report_location
):
    output = tmp_path / "artifact" / "metadata"
    report = {
        "equal": output,
        "child": output / "metrics.json",
        "nested": output / "reports" / "metrics.json",
        "ancestor": output.parent,
    }[report_location]
    acquisitions = []

    def unexpected_capture(*args, **kwargs):
        acquisitions.append((args, kwargs))
        raise AssertionError("metrics overlap must be rejected before acquisition")

    monkeypatch.setattr(metadata, "capture", unexpected_capture)
    root = tmp_path / "unrelated-root"
    arguments = [
        "--root",
        str(root),
        "--metrics-file",
        str(report),
        "metadata",
        action,
        "--market-ticker",
        "MARKET",
        "--output",
        str(output),
    ]

    assert cli.main(arguments) == 1
    assert capsys.readouterr().out == ""
    assert acquisitions == []
    assert not output.parent.exists()
    assert not root.exists()


@pytest.mark.parametrize("kind", ["metadata", "release"])
def test_standalone_verification_allows_external_sibling_metrics(
    certified_metrics_artifacts, tmp_path, capsys, kind
):
    source = certified_metrics_artifacts[kind == "release"]
    artifact = tmp_path / "artifact"
    shutil.copytree(source, artifact)
    before = _artifact_bytes(artifact)
    report = tmp_path / "artifact-reports" / "metrics.json"
    arguments = (
        ["metadata", "verify", "--output", str(artifact)]
        if kind == "metadata"
        else ["verify", "--release", str(artifact)]
    )

    assert cli.main(["--metrics-file", str(report), *arguments]) == 0
    assert json.loads(capsys.readouterr().out)["source_release_id"]
    assert json.loads(report.read_bytes())["version"] == 1
    assert _artifact_bytes(artifact) == before


@pytest.mark.parametrize("action", ["refresh", "export"])
def test_metadata_output_allows_external_sibling_metrics(tmp_path, monkeypatch, capsys, action):
    store = Store(tmp_path / "root")
    monkeypatch.setattr("kalshi_events.capture.PublicClient", FakeClient)
    if action == "export":
        capture(store, selected=["MARKET"], client_factory=FakeClient)
    output = tmp_path / "artifact"
    report = tmp_path / "artifact-reports" / "metrics.json"
    arguments = [
        "--root",
        str(store.root),
        "--metrics-file",
        str(report),
        "metadata",
        action,
        "--market-ticker",
        "MARKET",
        "--output",
        str(output),
    ]

    assert cli.main(arguments) == 0
    result = json.loads(capsys.readouterr().out)
    assert metadata.verify(output)["source_release_id"] == result["source_release_id"]
    assert json.loads(report.read_bytes())["version"] == 1
    assert not list(output.rglob("*metrics*"))
