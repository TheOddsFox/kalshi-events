from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from kalshi_events.cli import main
from kalshi_events.common import Store
from kalshi_events.metrics import collect, phase


def test_measurements_are_opt_in_and_cli_json_is_unchanged(tmp_path, capsys):
    root = tmp_path / "root"
    Store(root)
    assert main(["--root", str(root), "status"]) == 0
    without_report = capsys.readouterr().out
    report = tmp_path / "metrics.json"
    assert main(["--root", str(root), "--metrics-file", str(report), "status"]) == 0
    assert capsys.readouterr().out == without_report
    data = json.loads(report.read_text())
    assert data["version"] == 1
    assert "self" in data["peak_rss_bytes"] and "children" in data["peak_rss_bytes"]
    assert "file_reads" in data["counters"]


def test_metrics_report_cannot_overwrite_or_live_inside_root(tmp_path, capsys):
    root = tmp_path / "root"
    Store(root)
    existing = tmp_path / "metrics.json"
    existing.write_text("keep")
    assert main(["--root", str(root), "--metrics-file", str(existing), "status"]) == 1
    assert existing.read_text() == "keep"
    capsys.readouterr()
    inside = root / "metrics.json"
    assert main(["--root", str(root), "--metrics-file", str(inside), "status"]) == 1
    assert not inside.exists()


def test_metrics_phase_and_counters_are_thread_safe():
    with collect() as measured, phase("fixture"):
        measured.add("observations", 1)
    report = measured.report()
    assert report["phases_seconds"]["fixture"] >= 0
    assert report["counters"]["observations"] == 1


@pytest.mark.timeout(60)
def test_offline_benchmark_smoke(tmp_path):
    repository = Path(__file__).resolve().parents[1]
    report_dir = tmp_path / "reports"
    subprocess.run(
        [
            sys.executable,
            str(repository / "scripts" / "benchmark-pipeline.py"),
            "--markets",
            "12",
            "--runs",
            "1",
            "--warmups",
            "0",
            "--report-dir",
            str(report_dir),
            "--compare-root",
            str(repository),
        ],
        check=True,
        cwd=repository,
    )
    summary = json.loads((report_dir / "benchmark-summary.json").read_text())
    assert summary["fixture"]["markets"] == 12
    assert summary["results"]["source"][0]["observations"] == 15
    assert summary["paired_comparisons"][0]["artifacts_match"]
