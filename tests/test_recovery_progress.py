from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

import httpx
import pytest
from conftest import FakeClient

from kalshi_events import capture, cli, metrics
from kalshi_events.common import BudgetExceeded, Limits, Store
from kalshi_events.recovery_index import RecoveryParentIndex
from kalshi_events.transport import PublicClient


class FakeClock:
    def __init__(self, value: float = 100.0) -> None:
        self.value = value

    def monotonic(self) -> float:
        return self.value


def test_progress_flushes_phase_changes_and_throttles_same_phase(tmp_path, monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(metrics.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(uuid, "uuid4", lambda: pytest.fail("metrics must not consume capture IDs"))
    links = []
    replacements = []
    real_link = metrics.os.link
    real_replace = metrics.os.replace

    def link(source, destination, **kwargs):
        links.append((Path(source), Path(destination)))
        return real_link(source, destination, **kwargs)

    def replace(source, destination):
        source, destination = Path(source), Path(destination)
        assert source.parent == destination.parent
        assert source.is_file() and destination.is_file()
        json.loads(destination.read_text())
        replacements.append((source, destination))
        real_replace(source, destination)
        json.loads(destination.read_text())

    monkeypatch.setattr(metrics.os, "link", link)
    monkeypatch.setattr(metrics.os, "replace", replace)
    path = tmp_path / "metrics.json.progress.json"

    with metrics.collect(progress_path=path) as measured:
        clock.value = 101.0
        measured.progress("recovery_validation", journal_records=1)
        clock.value = 115.9
        measured.progress(journal_records=2)
        assert json.loads(path.read_text())["counters"]["journal_records"] == 1
        clock.value = 116.0
        measured.progress(journal_records=3)

    final = json.loads(path.read_text())
    assert final["phase"] == "complete"
    assert final["elapsed_seconds"] == 16.0
    assert final["counters"]["journal_records"] == 3
    assert len(links) == 1
    assert len(replacements) == 3
    assert list(tmp_path.glob(".metrics-progress-*.tmp")) == []


@pytest.mark.parametrize("kind", ["existing", "symlink"])
def test_progress_refuses_preexisting_or_symlink_companions(tmp_path, kind):
    target = tmp_path / "target.json"
    target.write_text("keep\n")
    path = tmp_path / "metrics.json.progress.json"
    if kind == "existing":
        path.write_text("keep\n")
        expected = "already exists"
    else:
        path.symlink_to(target)
        expected = "symlink"

    with (
        pytest.raises((FileExistsError, OSError), match=expected),
        metrics.collect(progress_path=path),
    ):
        pytest.fail("unsafe progress path must fail before work starts")

    assert target.read_text() == "keep\n"
    assert list(tmp_path.glob(".metrics-progress-*.tmp")) == []


def test_progress_io_error_does_not_replace_operation_exception(tmp_path, monkeypatch):
    path = tmp_path / "metrics.json.progress.json"
    original = metrics.Metrics._write_progress
    calls = 0

    def fail_on_finish(self, now):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise OSError("synthetic progress fsync failure")
        return original(self, now)

    monkeypatch.setattr(metrics.Metrics, "_write_progress", fail_on_finish)
    with (
        pytest.raises(RuntimeError, match="original operation failure"),
        metrics.collect(progress_path=path) as measured,
    ):
        measured.progress("recovery_validation")
        raise RuntimeError("original operation failure")

    assert calls == 3
    assert json.loads(path.read_text())["phase"] == "recovery_validation"


def test_progress_is_noop_when_collection_has_no_companion(tmp_path):
    with metrics.collect() as measured:
        measured.progress("unknown_phase", unknown_counter=object())
        metrics.progress("unknown_phase", unknown_counter=object())

    assert list(tmp_path.iterdir()) == []


def test_offline_build_progress_distinguishes_phases_and_aggregate_counts(tmp_path):
    path = tmp_path / "metrics.json.progress.json"
    with metrics.collect(progress_path=path):
        metrics.progress(
            "landing", receipts_verified=3, observations_emitted=4096, batches_yielded=1
        )
        report = json.loads(path.read_text())
        assert report["phase"] == "landing"
        assert report["counters"]["receipts_verified"] == 3
        assert report["counters"]["observations_emitted"] == 4096
        assert report["counters"]["batches_yielded"] == 1
        for phase in ("current_projection", "dbt", "export", "verification", "activation"):
            with metrics.phase(phase):
                assert json.loads(path.read_text())["phase"] == phase
                with metrics.phase("hashing"):
                    assert json.loads(path.read_text())["phase"] == phase


@pytest.mark.parametrize(
    "counter",
    [{"receipts_verified": -1}, {"observations_emitted": True}, {"batches_yielded": 2**63}],
)
def test_offline_build_progress_rejects_invalid_counts(tmp_path, counter):
    with (
        metrics.collect(progress_path=tmp_path / "metrics.json.progress.json"),
        pytest.raises(ValueError, match="invalid recovery progress counter"),
    ):
        metrics.progress("landing", **counter)


def test_first_data_request_progress_uses_non_cutoff_http_attempt_time(tmp_path, monkeypatch):
    clock = {"now": 0.0}
    monkeypatch.setattr("kalshi_events.transport.time.monotonic", lambda: clock["now"])
    monkeypatch.setattr(
        "kalshi_events.transport.time.sleep",
        lambda delay: clock.__setitem__("now", clock["now"] + delay),
    )
    store = Store(tmp_path / "root", Limits(max_duration_seconds=120))
    store.started = 0.0
    http_attempts = []
    delayed_data_response = False

    def handler(request):
        nonlocal delayed_data_response
        endpoint = request.url.path.removeprefix("/trade-api/v2")
        http_attempts.append((endpoint, clock["now"]))
        if endpoint != capture.CUTOFF_ENDPOINT and not delayed_data_response:
            delayed_data_response = True
            clock["now"] += 3.0
        payloads = {
            capture.CUTOFF_ENDPOINT: {
                "market_settled_ts": "2026-01-01T00:00:00Z",
                "trades_created_ts": "2026-02-01T00:00:00Z",
            },
            "/series": {"series": []},
            "/events": {"events": [], "cursor": ""},
            "/events/multivariate": {"events": [], "cursor": ""},
            "/markets": {"markets": [], "cursor": ""},
            "/historical/markets": {"markets": [], "cursor": ""},
        }
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            stream=httpx.ByteStream(json.dumps(payloads[endpoint]).encode()),
        )

    class CooldownClient(PublicClient):
        def __init__(self, actual_store, budget, **options):
            super().__init__(actual_store, budget, **options)
            self.limiter.defer(10.0)
            self.client.close()
            self.client = httpx.Client(
                transport=httpx.MockTransport(handler),
                trust_env=False,
            )

    original_consume = capture._consume

    def delayed_consume(actual_store, state, index, *args):
        result = original_consume(actual_store, state, index, *args)
        if state["tasks"][index]["endpoint"] != capture.CUTOFF_ENDPOINT:
            clock["now"] += 2.0
        return result

    monkeypatch.setattr(capture, "MAX_CONCURRENT_REQUESTS", 1)
    monkeypatch.setattr(capture, "_consume", delayed_consume)
    progress_path = tmp_path / "metrics.json.progress.json"
    with metrics.collect(progress_path=progress_path):
        result = capture.capture(store, client_factory=CooldownClient)

    progress = json.loads(progress_path.read_text())
    counters = progress["counters"]
    assert result["complete"] is True
    assert http_attempts[0][0] == capture.CUTOFF_ENDPOINT
    assert http_attempts[0][1] == pytest.approx(10.0)
    assert http_attempts[1][0] != capture.CUTOFF_ENDPOINT
    assert counters["first_data_request_elapsed_seconds"] == pytest.approx(10.2)
    assert counters["first_data_page_elapsed_seconds"] == pytest.approx(13.2)


def test_first_committed_page_time_survives_cache_corruption_fallback(tmp_path, monkeypatch):
    clock = {"now": 0.0}
    monkeypatch.setattr("kalshi_events.capture.time.monotonic", lambda: clock["now"])
    store = Store(tmp_path / "root", Limits(max_duration_seconds=300))
    store.started = 0.0
    with store.writer_guard():
        store.write("root-id.json", {"version": 1, "root_id": "c" * 32})

    class SteppingClient(FakeClient):
        def get(self, endpoint, params=None, context=None):
            relative = super().get(endpoint, params, context)
            if endpoint != capture.CUTOFF_ENDPOINT:
                clock["now"] += 10.0
            return relative

    append_times = []
    original_append_record = capture._append_v2_record

    def record_page_append(actual_store, state, op, payload):
        result = original_append_record(actual_store, state, op, payload)
        if op == "page" and payload["index"] != 0:
            append_times.append(clock["now"])
        return result

    injected = []
    original_merge = RecoveryParentIndex.merge_at_anchor

    def corrupt_after_append(index, page, relative, anchor):
        if not injected and (page.series or page.events or page.markets):
            injected.append(clock["now"])
            raise sqlite3.DatabaseError("database disk image is malformed")
        return original_merge(index, page, relative, anchor)

    monkeypatch.setattr(capture, "MAX_CONCURRENT_REQUESTS", 1)
    monkeypatch.setattr(capture, "_append_v2_record", record_page_append)
    monkeypatch.setattr(RecoveryParentIndex, "merge_at_anchor", corrupt_after_append)
    progress_path = tmp_path / "metrics.json.progress.json"

    with metrics.collect(progress_path=progress_path):
        result = capture.capture(store, client_factory=SteppingClient)

    counters = json.loads(progress_path.read_text())["counters"]
    assert result["complete"] is True
    assert len(injected) == 1
    assert len(append_times) > 1
    assert injected[0] == pytest.approx(append_times[0])
    assert append_times[1] > append_times[0]
    assert counters["first_data_page_elapsed_seconds"] == pytest.approx(append_times[0])


def test_cli_metrics_file_writes_companion_without_changing_result_or_report(tmp_path, capsys):
    root = tmp_path / "root"
    Store(root)
    assert cli.main(["--root", str(root), "status"]) == 0
    original_result = capsys.readouterr().out
    report_path = tmp_path / "metrics.json"

    assert cli.main(["--root", str(root), "--metrics-file", str(report_path), "status"]) == 0

    assert capsys.readouterr().out == original_result
    report = json.loads(report_path.read_text())
    progress = json.loads(Path(f"{report_path}.progress.json").read_text())
    assert report.keys() == metrics.Metrics().report().keys()
    assert progress["phase"] == "complete"
    assert progress["version"] == 1


@pytest.mark.parametrize("outcome", ["incomplete", "budget"])
def test_cli_incomplete_result_finishes_progress_as_stopped(tmp_path, capsys, monkeypatch, outcome):
    if outcome == "incomplete":
        monkeypatch.setattr(cli, "execute", lambda _args: {"complete": False})
    else:

        def exhausted(_args):
            raise BudgetExceeded("synthetic allowance exhaustion")

        monkeypatch.setattr(cli, "execute", exhausted)
    report_path = tmp_path / "metrics.json"

    assert (
        cli.main(
            [
                "--root",
                str(tmp_path / "root"),
                "--metrics-file",
                str(report_path),
                "status",
            ]
        )
        == 3
    )

    result = json.loads(capsys.readouterr().out)
    progress = json.loads(Path(f"{report_path}.progress.json").read_text())
    assert result["complete"] is False
    assert progress["phase"] == "stopped"


@pytest.mark.parametrize("collision", ["report", "progress", "nested_progress"])
@pytest.mark.parametrize("action", ["export", "refresh"])
def test_cli_rejects_metrics_artifact_overlap_before_execution(
    tmp_path, capsys, monkeypatch, action, collision
):
    root = tmp_path / "root"
    report_path = tmp_path / "metrics.json"
    progress_path = report_path.with_name(f"{report_path.name}.progress.json")
    output_path = {
        "report": report_path,
        "progress": progress_path,
        "nested_progress": progress_path / "bundle",
    }[collision]
    monkeypatch.setattr(
        cli,
        "execute",
        lambda _args: pytest.fail("overlapping metrics paths must fail before execution"),
    )

    result = cli.main(
        [
            "--root",
            str(root),
            "--metrics-file",
            str(report_path),
            "metadata",
            action,
            "--market-ticker",
            "MKT",
            "--output",
            str(output_path),
        ]
    )

    assert result == 1
    assert "must not overlap" in capsys.readouterr().err
    assert not report_path.exists()
    assert not Path(f"{report_path}.progress.json").exists()


@pytest.mark.parametrize("command", ["status", "refresh"])
@pytest.mark.parametrize("root_layout", ["progress_equal", "progress_ancestor"])
def test_cli_rejects_progress_overlap_with_operator_root_before_reserving(
    tmp_path, capsys, monkeypatch, command, root_layout
):
    report_path = tmp_path / "metrics.json"
    progress_path = report_path.with_name(f"{report_path.name}.progress.json")
    root = progress_path if root_layout == "progress_equal" else progress_path / "operator"
    monkeypatch.setattr(
        cli,
        "execute",
        lambda _args: pytest.fail("metrics/root overlap must fail before execution"),
    )
    monkeypatch.setattr(
        cli.refresh,
        "run",
        lambda *_args, **_kwargs: pytest.fail("metrics/root overlap must fail before refresh"),
    )

    assert cli.main(["--root", str(root), "--metrics-file", str(report_path), command]) == 1

    captured = capsys.readouterr()
    assert (
        "outside the operator root" in captured.err or "outside the operator root" in captured.out
    )
    assert not report_path.exists()
    assert not progress_path.exists()
    assert not root.exists()


def test_refresh_incomplete_marks_stopped_before_collection_finishes(tmp_path, capsys, monkeypatch):
    args = cli.parser().parse_args(
        [
            "--root",
            str(tmp_path / "root"),
            "--metrics-file",
            str(tmp_path / "refresh-metrics.json"),
            "refresh",
        ]
    )
    written_phases = []
    original_write = metrics.Metrics._write_progress

    def incomplete_refresh(*_args, **_kwargs):
        result = cli.refresh.result_template()
        result.update(complete=False, error="synthetic budget stop")
        return result, 3

    def record_phase(self, now):
        written_phases.append(self._progress_phase)
        return original_write(self, now)

    monkeypatch.setattr(cli.refresh, "run", incomplete_refresh)
    monkeypatch.setattr(metrics.Metrics, "_write_progress", record_phase)

    assert cli._refresh_main(args) == 3

    progress = json.loads(Path(f"{args.metrics_file}.progress.json").read_text())
    assert "complete" not in written_phases
    assert progress["phase"] == "stopped"
    assert json.loads(capsys.readouterr().out)["complete"] is False
