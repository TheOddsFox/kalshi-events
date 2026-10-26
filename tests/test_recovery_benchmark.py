from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from recovery_fixtures import build_recovery_root

from kalshi_events import capture
from kalshi_events.common import Store

SCRIPT = Path(__file__).parents[1] / "scripts/benchmark-recovery.py"


def _benchmark_module():
    spec = importlib.util.spec_from_file_location("benchmark_recovery", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_benchmark_help_is_read_only_and_describes_external_outputs(tmp_path):
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )

    assert result.returncode == 0
    assert "--reference-source-root" in result.stdout
    assert "--fixture-template" in result.stdout
    assert "--cache-mode" in result.stdout
    assert "--complete" in result.stdout
    assert list(tmp_path.iterdir()) == []


def test_primary_fixture_supports_multiple_valid_pages_for_smoke_runs(tmp_path):
    root = tmp_path / "root"
    capture_id = build_recovery_root(root, "primary", repeats=0, num_pages=3)
    state = capture.read_capture(Store(root), capture_id)
    receipts = capture.page_receipts(Store(root), state)
    manifests = [json.loads((root / row["relative"]).read_text()) for row in receipts]

    assert len(receipts) == 3
    assert state["tasks"][2]["cursor"] == "fixture-next-3"
    assert all(row["accounting_complete"] is True and row["status"] == 200 for row in manifests)
    assert (root / ".recovery-fixture.json").is_file()


def test_fixture_template_must_have_the_synthetic_identity_marker(tmp_path):
    benchmark = _benchmark_module()
    fixture = tmp_path / "fixture"
    capture_id = build_recovery_root(fixture, "primary", repeats=0, num_pages=1)

    assert benchmark._validate_fixture_root(fixture) == capture_id

    unmarked = tmp_path / "unmarked"
    (unmarked / "captures" / capture_id).mkdir(parents=True)
    (unmarked / "root-id.json").write_text(
        '{"version": 1, "root_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}'
    )
    with pytest.raises(ValueError, match="marked synthetic"):
        benchmark._validate_fixture_root(unmarked)


def test_external_output_path_rejects_a_symlink_ancestor(tmp_path):
    benchmark = _benchmark_module()
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real, target_is_directory=True)

    with pytest.raises(ValueError, match="symlink"):
        benchmark._external(link / "report", [])


def test_cache_mode_uses_recovery_metrics_instead_of_filename_guesses(tmp_path):
    benchmark = _benchmark_module()
    (tmp_path / "root-id.json").write_text('{"version": 1, "root_id": "b"}')

    assert benchmark._cache_mode(tmp_path, {"counters": {"recovery_cache_enabled": 1}}) == (
        "cold-supported"
    )
    assert (
        benchmark._cache_mode(
            tmp_path,
            {"counters": {"recovery_cache_enabled": 1, "recovery_cache_reused": 2}},
        )
        == "warm"
    )


def test_report_updates_are_atomic_and_leave_the_last_complete_value(tmp_path):
    benchmark = _benchmark_module()
    report = tmp_path / "report.json"

    benchmark._write_report(report, {"status": "running", "runs": []})
    benchmark._write_report(report, {"status": "complete", "runs": [1]})

    assert json.loads(report.read_text()) == {"status": "complete", "runs": [1]}
    assert not (tmp_path / ".report.json.tmp").exists()


def test_provenance_includes_runtime_files_fixture_sources_and_environment():
    benchmark = _benchmark_module()
    repo = SCRIPT.parents[1]

    provenance = benchmark._source_provenance(repo, repo)

    assert len(provenance["source_inventory_sha256"]) == 64
    assert "src/kalshi_events/capture.py" in provenance["source_files_sha256"]
    assert "benchmark-recovery.py" in provenance["source_files_sha256"]
    assert "recovery_fixtures.py" in provenance["source_files_sha256"]
    assert provenance["python_executable"] == sys.executable


def test_fixture_marker_size_is_bounded_before_json_parsing(tmp_path):
    benchmark = _benchmark_module()
    fixture = tmp_path / "fixture"
    build_recovery_root(fixture, "primary", repeats=0)
    (fixture / ".recovery-fixture.json").write_bytes(b" " * (benchmark.MAX_MARKER_BYTES + 1))

    with pytest.raises(ValueError, match="identity file exceeds"):
        benchmark._validate_fixture_root(fixture)


def test_fixture_journal_hash_is_streamed_with_a_strict_size_limit(tmp_path, monkeypatch):
    benchmark = _benchmark_module()
    fixture = tmp_path / "fixture"
    capture_id = build_recovery_root(fixture, "primary", repeats=0)
    journal = fixture / "captures" / capture_id / "pages.jsonl"
    monkeypatch.setattr(benchmark, "MAX_JOURNAL_BYTES", 32)
    with journal.open("ab") as stream:
        stream.truncate(33)

    with pytest.raises(ValueError, match="journal exceeds the 32-byte limit"):
        benchmark._validate_fixture_root(fixture)


def test_fixture_identity_rejects_fifo_without_opening_it(tmp_path):
    if not hasattr(os, "mkfifo"):
        pytest.skip("FIFO creation is unavailable")
    benchmark = _benchmark_module()
    fixture = tmp_path / "fixture"
    build_recovery_root(fixture, "primary", repeats=0)
    identity = fixture / "root-id.json"
    identity.unlink()
    os.mkfifo(identity)

    with pytest.raises(ValueError, match="regular files and directories"):
        benchmark._validate_fixture_root(fixture)


def test_report_directory_inside_fixture_is_rejected_without_modifying_it(tmp_path):
    benchmark = _benchmark_module()
    repo = SCRIPT.parents[1]
    reference = tmp_path / "reference"
    (reference / "src/kalshi_events").mkdir(parents=True)
    (reference / "src/kalshi_events/__init__.py").write_text("")
    fixture = tmp_path / "fixture"
    build_recovery_root(fixture, "primary", repeats=0)

    def inventory(root):
        return {
            path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in root.rglob("*")
            if path.is_file()
        }

    before = inventory(fixture)
    args = benchmark._parser().parse_args(
        [
            "--reference-source-root",
            str(reference),
            "--source-root",
            str(repo),
            "--fixture-template",
            str(fixture),
            "--report-dir",
            str(fixture / "benchmark-report"),
            "--warmups",
            "0",
            "--runs",
            "1",
        ]
    )

    with pytest.raises(ValueError, match="outside source checkouts|disjoint"):
        benchmark._benchmark(args)
    assert not (fixture / "benchmark-report").exists()
    assert inventory(fixture) == before


def test_child_environment_omits_operator_secrets(tmp_path, monkeypatch):
    benchmark = _benchmark_module()
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "fixture-secret")
    monkeypatch.setenv("KALSHI_API_KEY", "fixture-secret")

    environment = benchmark._child_env(tmp_path, tmp_path)

    assert "AWS_SECRET_ACCESS_KEY" not in environment
    assert "KALSHI_API_KEY" not in environment
    assert environment["PYTHONPATH"] == os.pathsep.join(
        (str(tmp_path / "src"), str(tmp_path / "tests"))
    )
    assert environment["PYTHONNOUSERSITE"] == "1"
