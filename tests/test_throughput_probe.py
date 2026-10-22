from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from kalshi_events import capture, cli
from kalshi_events.common import Store

SCRIPT = Path(__file__).parents[1] / "scripts" / "acquisition_probe.py"
SPEC = importlib.util.spec_from_file_location("acquisition_probe_test", SCRIPT)
assert SPEC and SPEC.loader
probe = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = probe
SPEC.loader.exec_module(probe)


def _worker_report(
    *, attempts: int = 100, downloaded: int = 1000, http_429: bool = False
) -> dict[str, Any]:
    return {
        "version": 1,
        "command": "capture",
        "returncode": 0,
        "capture": {"capture_id": "a" * 32, "complete": True, "error": None},
        "accounting": {
            "attempts": attempts,
            "downloaded_or_reserved_bytes": downloaded,
            "accounting_complete": True,
            "retained_bytes": downloaded,
            "temporary_bytes": 0,
        },
        "receipts": {
            "data_pages": 101,
            "control_pages": 1,
            "control_responses": 1,
            "non_data_responses": 0,
            "attempts": attempts,
            "retry_attempts": 0,
            "encoded_bytes": downloaded,
            "http_429": http_429,
            "response_statuses": {"200": attempts},
            "observed_encodings": {"identity": attempts},
        },
        "metrics": {
            "counters": {"requests": attempts},
            "phases_seconds": {"capture": 10.0, "request": 8.0},
            "store_accounted_high_water_bytes": {"temporary": 4},
            "endpoint_families": {},
            "elapsed_seconds": 10.5,
            "peak_http_concurrency": 1,
            "peak_rss_bytes": {"self": 1234, "children": None},
        },
        "timings": {"acquisition_seconds": 10.0, "accounting_seconds": 0.1, "worker_seconds": 10.6},
        "error": None,
    }


class FakeWorker:
    reports: list[dict[str, Any]] = []
    commands: list[list[str]] = []
    behavior = "success"
    killed = False

    def __init__(
        self, command: list[str], *, stdout: Any, stderr: Any, env: dict[str, str], shell: bool
    ):
        assert shell is False
        self.commands.append(command)
        self.returncode = 0
        self.killed = False
        self.report = self.reports.pop(0) if self.reports else _worker_report()
        root = Path(command[command.index("--root") + 1])
        summary_path = root.parent.parent / "reports" / "summary.json"
        active = json.loads(summary_path.read_text(encoding="utf-8"))["active_scenario"]
        assert active["root"] == str(root)
        result_path = Path(command[command.index("--result") + 1])
        result_path.write_text(json.dumps(self.report), encoding="utf-8")

    def wait(self, timeout: float | None = None) -> int:
        if type(self).behavior == "timeout" and not self.killed:
            raise subprocess.TimeoutExpired("probe-worker", timeout)
        return self.returncode

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9


def setup_function() -> None:
    FakeWorker.reports = []
    FakeWorker.commands = []
    FakeWorker.behavior = "success"


def test_throughput_rates_sort_and_reject_invalid_values() -> None:
    assert probe.parse_throughput_rates(None) == [10.0]
    assert probe.parse_throughput_rates(["20", "5", "10"]) == [5.0, 10.0, 20.0]
    for values in ([], ["1", "1"], ["nan"], ["inf"], ["0"], ["20.1"], ["1", "2", "3", "4"]):
        with pytest.raises(ValueError):
            probe.parse_throughput_rates(values)


def test_partition_selection_is_explicit_bounded_and_distinct() -> None:
    assert probe.validate_throughput_selection(None, None) == []
    selection = probe.validate_throughput_selection(["KXALPHA", "KXBETA"], ["KXCOLLECTION"])
    assert selection == [
        {"kind": "series", "ticker": "KXALPHA"},
        {"kind": "series", "ticker": "KXBETA"},
        {"kind": "collection", "ticker": "KXCOLLECTION"},
    ]
    for series, collections in (
        (["KXONE"], None),
        (["KXONE", "KXONE"], None),
        (None, ["KXONE", "KXONE"]),
    ):
        with pytest.raises(ValueError):
            probe.validate_throughput_selection(series, collections)


def test_global_matrix_uses_active_python_and_subtracts_each_sample(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    report = probe.run_throughput(tmp_path / "throughput", [5.0, 10.0])
    assert report["complete"] is True
    assert report["version"] == 2
    assert [(sample["rate"], sample["compression"]) for sample in report["samples"]] == [
        (5.0, "identity"),
        (5.0, "gzip"),
        (10.0, "identity"),
        (10.0, "gzip"),
    ]
    assert [sample["throughput_conclusion"] for sample in report["samples"]] == [
        "short_window_only"
    ] * 4
    assert report["samples"][1]["encoding_fallback"] is True
    commands = FakeWorker.commands
    assert all(command[0] == sys.executable for command in commands)
    assert all(command[command.index("--max-requests") + 1] == "250" for command in commands)
    assert all(
        command[command.index("--max-response-bytes") + 1] == str(32 * probe.MIB)
        for command in commands
    )
    assert all(
        command[command.index("--max-decoded-response-bytes") + 1] == str(64 * probe.MIB)
        for command in commands
    )
    assert report["accounting"]["attempts"] == 400
    assert (tmp_path / "throughput" / "reports" / "summary.json").is_file()


def test_http_429_stops_remaining_scenarios_and_preserves_partial_report(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    FakeWorker.reports = [_worker_report(http_429=True)]
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    report = probe.run_throughput(tmp_path / "throttled", [10.0, 15.0])
    assert len(FakeWorker.commands) == 1
    assert report["complete"] is False
    assert "429" in report["stop_reason"]
    saved = json.loads((tmp_path / "throttled" / "reports" / "summary.json").read_text())
    assert len(saved["samples"]) == 1


def test_aggregate_attempt_allowance_is_subtracted_before_each_child(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    monkeypatch.setitem(probe.TOTAL, "attempts", 150)
    monkeypatch.setitem(probe.GLOBAL, "attempts", 100)
    report = probe.run_throughput(tmp_path / "bounded", [10.0])
    assert len(FakeWorker.commands) == 2
    assert FakeWorker.commands[0][FakeWorker.commands[0].index("--max-requests") + 1] == "100"
    assert FakeWorker.commands[1][FakeWorker.commands[1].index("--max-requests") + 1] == "50"
    assert report["stop_reason"] == "worker accounting exceeded its assigned allowance"
    assert report["accounting"]["attempts"] is None


def test_temporary_high_water_includes_prior_roots_retained_temporary_bytes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    first = _worker_report()
    first["accounting"]["temporary_bytes"] = 10
    first["metrics"]["store_accounted_high_water_bytes"]["temporary"] = 15
    second = _worker_report()
    second["accounting"]["temporary_bytes"] = 5
    second["metrics"]["store_accounted_high_water_bytes"]["temporary"] = 20
    FakeWorker.reports = [first, second]
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    report = probe.run_throughput(tmp_path / "temp-high-water", [10.0])
    assert report["complete"] is True
    assert report["samples"][1]["aggregate_temporary_high_water_bytes"] == 30
    assert report["accounting"]["temporary_high_water_bytes"] == 30


def test_missing_acquisition_duration_stays_unknown_without_zero_throughput(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    worker = _worker_report()
    worker["timings"]["acquisition_seconds"] = None
    FakeWorker.reports = [worker]
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    report = probe.run_throughput(tmp_path / "unknown-duration", [10.0])
    sample = report["samples"][0]
    assert sample["acquisition_seconds"] is None
    assert sample["throughput_conclusion"] == "unknown"
    assert sample["attempts_per_second"] is None
    assert sample["validated_data_pages_per_second"] is None


def test_untrusted_worker_accounting_stops_and_marks_totals_unknown(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    invalid = _worker_report()
    invalid["accounting"]["attempts"] = True
    FakeWorker.reports = [invalid]
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    report = probe.run_throughput(tmp_path / "unknown", [10.0, 15.0])
    assert len(FakeWorker.commands) == 1
    assert report["samples"][0]["accounting"] == "unknown"
    assert report["accounting"]["attempts"] is None


@pytest.mark.parametrize(
    ("command_returncode", "error", "expected_reason"),
    [
        (1, "source failed", "unexpected capture command failure"),
        (
            3,
            "operator root storage exhausted",
            "command stopped for a non-request/download budget condition",
        ),
    ],
)
def test_worker_command_failure_is_not_hidden_by_successful_supervisor(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    command_returncode: int,
    error: str,
    expected_reason: str,
) -> None:
    failed = _worker_report()
    failed["returncode"] = command_returncode
    failed["capture"] = {"capture_id": "a" * 32, "complete": False, "error": error}
    FakeWorker.reports = [failed]
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    report = probe.run_throughput(tmp_path / f"command-{command_returncode}", [10.0])
    assert report["samples"][0]["supervisor_returncode"] == 0
    assert report["samples"][0]["command_returncode"] == command_returncode
    assert report["stop_reason"] == expected_reason


def test_trustworthy_request_budget_stop_allows_next_fresh_scenario(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    partial = _worker_report()
    partial["returncode"] = 3
    partial["capture"] = {
        "capture_id": "a" * 32,
        "complete": False,
        "error": "request allowance exhausted",
    }
    FakeWorker.reports = [partial, _worker_report()]
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    report = probe.run_throughput(tmp_path / "budget-partial", [10.0])
    assert report["complete"] is True
    assert [sample["status"] for sample in report["samples"]] == ["partial", "completed"]


def test_partition_mode_writes_initial_summary_without_starting_global_capture(
    tmp_path: Path,
) -> None:
    output = tmp_path / "pilot-pending"
    report = probe.run_throughput(
        output,
        [10.0],
        partitions=[{"kind": "series", "ticker": "KXONE"}, {"kind": "series", "ticker": "KXTWO"}],
    )
    assert report["complete"] is False
    assert report["samples"] == []
    assert "not available" in report["stop_reason"]
    assert json.loads((output / "reports" / "summary.json").read_text())["version"] == 2


def test_supervised_worker_accounts_and_writes_bounded_result_before_exit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    root = tmp_path / "root"
    Store(root)
    metrics_path = tmp_path / "metrics.json"
    result_path = tmp_path / "worker.json"

    def fake_capture(arguments: list[str]) -> int:
        metrics = Path(arguments[arguments.index("--metrics-file") + 1])
        metrics.write_text(
            json.dumps(
                {
                    "counters": {"requests": 0},
                    "phases_seconds": {},
                    "store_accounted_high_water_bytes": {"temporary": 0},
                    "endpoint_families": {},
                    "elapsed_seconds": 0.1,
                    "peak_http_concurrency": 0,
                    "peak_rss_bytes": {"self": 1000, "children": None},
                }
            ),
            encoding="utf-8",
        )
        print(json.dumps({"capture_id": None, "complete": False, "error": "test stop"}))
        return 3

    monkeypatch.setattr(cli, "main", fake_capture)
    arguments = [
        "--result",
        str(result_path),
        "--",
        "--root",
        str(root),
        "--metrics-file",
        str(metrics_path),
        "capture",
        "--max-rate",
        "10",
        "--max-requests",
        "10",
        "--max-download-bytes",
        "4096",
        "--max-response-bytes",
        "1024",
        "--max-decoded-response-bytes",
        "2048",
        "--max-duration-seconds",
        "5",
        "--max-root-bytes",
        "8192",
        "--max-temp-bytes",
        "4096",
        "--compression",
        "identity",
    ]
    assert probe.worker_main(arguments) == 0
    result = json.loads(result_path.read_text(encoding="utf-8"))
    assert probe._valid_worker_report(result)
    assert result["returncode"] == 3
    assert result["accounting"]["accounting_complete"] is True
    assert result["timings"]["accounting_seconds"] >= 0
    assert result["timings"]["acquisition_seconds"] is None


def test_only_committed_http_200_data_pages_count_for_throughput(
    monkeypatch: pytest.MonkeyPatch, store: Any
) -> None:
    statuses = [200, 404, 400, 410, 200]
    endpoints = [
        "/events",
        "/events/PARENT",
        "/events",
        "/historical/markets",
        "/historical/cutoff",
    ]
    rows = []
    for index, (status, endpoint) in enumerate(zip(statuses, endpoints, strict=True)):
        relative = f"raw/page-{index}/manifest.json"
        store.write(
            relative,
            {
                "version": 1,
                "endpoint": endpoint,
                "status": status,
                "size": 0,
                "downloaded_bytes": 0,
                "accounting_complete": True,
                "sha256": "0" * 64,
            },
        )
        rows.append({"relative": relative, "index": index})
    monkeypatch.setattr(capture, "read_capture", lambda *_args: {"capture_id": "fixture"})
    monkeypatch.setattr(capture, "page_receipts", lambda *_args: rows)
    report = probe._inspect_capture_receipts(store, capture, {"capture_id": "fixture"})
    assert report["data_pages"] == 1
    assert report["control_pages"] == 4
    assert report["control_responses"] == 1
    assert report["non_data_responses"] == 3


def test_worker_timeout_kills_child_and_stops_matrix(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    class TimeoutWorker(FakeWorker):
        def wait(self, timeout: float | None = None) -> int:
            if not self.killed:
                raise subprocess.TimeoutExpired("probe-worker", timeout)
            return self.returncode

        def kill(self) -> None:
            self.killed = True
            type(self).killed = True
            self.returncode = -9

    monkeypatch.setattr(probe.subprocess, "Popen", TimeoutWorker)
    report = probe.run_throughput(tmp_path / "timeout", [10.0, 15.0])
    assert len(TimeoutWorker.commands) == 1
    assert report["complete"] is False
    assert report["accounting"]["attempts"] is None


def test_nonzero_supervisor_exit_ignores_seemingly_valid_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    class ExitAfterReport(FakeWorker):
        def __init__(self, *args: Any, **kwargs: Any):
            super().__init__(*args, **kwargs)
            self.returncode = 1

    monkeypatch.setattr(probe.subprocess, "Popen", ExitAfterReport)
    report = probe.run_throughput(tmp_path / "worker-exit", [10.0])
    assert len(ExitAfterReport.commands) == 1
    assert report["complete"] is False
    assert report["accounting"]["attempts"] is None
    assert "exit cleanly" in report["stop_reason"]
