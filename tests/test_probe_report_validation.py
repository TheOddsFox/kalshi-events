"""Malformed child reports must stop the supervisor rather than escape validation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from test_throughput_probe import FakeWorker, MixedWorker, _worker_report, probe

from kalshi_events.common import CatalogueError

PARTITIONS = [
    {"kind": "series", "ticker": "KXSERIES"},
    {"kind": "collection", "ticker": "KXCOLL"},
]
HUGE = 10**400


@pytest.fixture(autouse=True)
def offline_workers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KALSHI_EVENTS_API_URL", "http://127.0.0.1:9/trade-api/v2")
    monkeypatch.setenv("KALSHI_EVENTS_ALLOW_LOOPBACK", "1")
    FakeWorker.reports = []
    FakeWorker.commands = []
    FakeWorker.behavior = "success"


def _pilot_report() -> dict[str, Any]:
    report = _worker_report(attempts=2)
    report["command"] = "partition"
    report.pop("capture")
    report["result"] = {
        "workers": 8,
        "completed": True,
        "budget_stop": None,
        "pages": 2,
        "observations": 0,
        "overlap_unique_identities": 0,
        "partitions": [
            {
                **partition,
                "pages": 1,
                "observations": 0,
                "unique_identities": 0,
                "identity_digests": [],
                "inventory_sha256": "0" * 64,
                "inventory_rows": 0,
            }
            for partition in PARTITIONS
        ],
    }
    report["receipts"] = {
        "response_statuses": {"200": 2},
        "observed_encodings": {"gzip": 2},
        "retry_attempts": 0,
        "encoded_bytes": 1000,
        "http_429": False,
    }
    report["metrics"]["counters"] = {"diagnostic_verified_pages": 2}
    return report


def _huge_attempts(report: dict[str, Any]) -> None:
    report["accounting"]["attempts"] = HUGE
    report["receipts"]["response_statuses"] = {"200": HUGE}
    if "attempts" in report["receipts"]:
        report["receipts"]["attempts"] = HUGE


def test_valid_report_controls_are_accepted() -> None:
    assert probe._valid_worker_report(_worker_report())
    assert probe._valid_pilot_report(
        _pilot_report(), "partitions", partitions=PARTITIONS, workers=8
    )


@pytest.mark.parametrize("kind", ["global", "pilot"])
def test_impossible_attempt_counts_are_rejected_before_rate_calculation(kind: str) -> None:
    report = _worker_report() if kind == "global" else _pilot_report()
    _huge_attempts(report)
    valid = (
        probe._valid_worker_report(report)
        if kind == "global"
        else probe._valid_pilot_report(report, "partitions", partitions=PARTITIONS, workers=8)
    )
    assert valid is False


@pytest.mark.parametrize("kind", ["global", "pilot"])
@pytest.mark.parametrize(
    "section, field",
    [
        ("timings", "acquisition_seconds"),
        ("timings", "accounting_seconds"),
        ("timings", "worker_seconds"),
        ("metrics", "phases_seconds"),
    ],
)
def test_integer_measurements_too_large_for_finite_durations_are_rejected(
    kind: str, section: str, field: str
) -> None:
    report = _worker_report() if kind == "global" else _pilot_report()
    report[section][field] = {"request": HUGE} if field == "phases_seconds" else HUGE
    valid = (
        probe._valid_worker_report(report)
        if kind == "global"
        else probe._valid_pilot_report(report, "partitions", partitions=PARTITIONS, workers=8)
    )
    assert valid is False


@pytest.mark.parametrize("value", [{}, [], {"reason": "request allowance exhausted"}])
@pytest.mark.parametrize("field", ["budget_stop", "partition_kind"])
def test_container_values_cannot_escape_pilot_enum_validation(field: str, value: Any) -> None:
    report = _pilot_report()
    if field == "budget_stop":
        report["result"][field] = value
    else:
        report["result"]["partitions"][0]["kind"] = value
    assert (
        probe._valid_pilot_report(report, "partitions", partitions=PARTITIONS, workers=8) is False
    )


@pytest.mark.parametrize("case", ["huge_attempts", "huge_duration", "container_error"])
def test_malformed_global_report_saves_unknown_accounting_and_stops(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, case: str
) -> None:
    report = _worker_report()
    if case == "huge_attempts":
        _huge_attempts(report)
    elif case == "huge_duration":
        report["timings"]["acquisition_seconds"] = HUGE
    else:
        report["returncode"] = 3
        report["capture"].update(complete=False, error={})
    FakeWorker.reports = [report]
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    output = tmp_path / "invalid-global"
    result = probe.run_throughput(output, [10.0, 20.0])
    assert len(FakeWorker.commands) == 1
    assert result["complete"] is False
    assert result["stop_reason"]
    assert all(value is None for value in result["accounting"].values())
    assert "active_scenario" not in result
    saved = json.loads((output / "reports" / "summary.json").read_text())
    assert saved == result


@pytest.mark.parametrize("value", [{}, [], True, 1])
def test_global_capture_error_must_be_text_or_null(value: Any) -> None:
    report = _worker_report()
    report["returncode"] = 3
    report["capture"].update(complete=False, error=value)
    assert probe._valid_worker_report(report) is False


def test_subnormal_duration_cannot_create_infinite_rates_or_break_report_writing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    report = _worker_report()
    report["timings"]["acquisition_seconds"] = 5e-324
    FakeWorker.reports = [report]
    monkeypatch.setattr(probe.subprocess, "Popen", FakeWorker)
    output = tmp_path / "subnormal-duration"
    result = probe.run_throughput(output, [10.0])
    assert result["samples"][0].get("attempts_per_second") is None
    assert result["samples"][0].get("data_pages_per_second") is None
    assert "active_scenario" not in result
    assert json.loads((output / "reports" / "summary.json").read_text()) == result


@pytest.mark.parametrize("loader", ["_load_report", "_load_pilot_report"])
def test_report_loaders_reject_actual_symlinks(tmp_path: Path, loader: str) -> None:
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    link = tmp_path / "report.json"
    try:
        link.symlink_to(target)
    except OSError as exc:
        pytest.fail(f"POSIX symlink creation failed: {exc}")
    assert getattr(probe, loader)(link) is None


@pytest.mark.parametrize("loader", ["_load_report", "_load_pilot_report"])
def test_report_loaders_treat_unsafe_path_errors_as_untrusted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, loader: str
) -> None:
    report_path = tmp_path / "report.json"
    report_path.write_text("{}", encoding="utf-8")

    def reject_unsafe_path(path: Path) -> Path:
        assert path == report_path
        raise CatalogueError("symlink in report path")

    monkeypatch.setattr(probe, "safe_path", reject_unsafe_path)
    assert getattr(probe, loader)(report_path) is None


def test_pilot_report_unsafe_path_saves_unknown_accounting_and_stops(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    MixedWorker.commands = []
    MixedWorker.reports = []
    MixedWorker.pilot_configs = []
    real_safe_path = probe.safe_path

    def reject_pilot_report(path: Path) -> Path:
        path = Path(path)
        if path.name.startswith("partition-") and path.name.endswith(".worker.json"):
            raise CatalogueError("symlink in pilot report path")
        return real_safe_path(path)

    monkeypatch.setattr(probe, "safe_path", reject_pilot_report)
    monkeypatch.setattr(probe.subprocess, "Popen", MixedWorker)
    output = tmp_path / "untrusted-pilot-path"
    result = probe.run_throughput(
        output,
        [10.0, 20.0],
        partitions=[{"kind": "series", "ticker": ticker} for ticker in ("KXFIRST", "KXSECOND")],
    )
    assert len(MixedWorker.commands) == 3
    assert result["complete"] is False
    assert result["stop_reason"]
    assert all(value is None for value in result["accounting"].values())
    assert "active_scenario" not in result
    assert result["samples"][-1]["accounting"] == "unknown"
    assert json.loads((output / "reports" / "summary.json").read_text()) == result
