from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path


def test_benchmark_records_platform_requirements_and_actual_source_bytes(tmp_path, monkeypatch):
    script = Path(__file__).resolve().parents[1] / "scripts/benchmark-pipeline.py"
    spec = importlib.util.spec_from_file_location("windows_benchmark", script)
    benchmark = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(benchmark)
    source = tmp_path / "src/kalshi_events"
    source.mkdir(parents=True)
    (source / "_windows.py").write_bytes(b"platform compatibility\n")
    (source / "cache.pyc").write_bytes(b"excluded")
    monkeypatch.setattr(benchmark.platform, "system", lambda: "Windows")
    result = benchmark._environment(tmp_path, None)
    assert result["durability_requirement"] == "elevated-local-NTFS-volume-flush"
    assert result["child_peak_rss_available"] is False
    assert result["runtime_source_sha256"] == {
        "src/kalshi_events/_windows.py": hashlib.sha256(b"platform compatibility\n").hexdigest()
    }
    (source / "_windows.py").write_bytes(b"changed source\n")
    assert (
        benchmark._environment(tmp_path, None)["runtime_source_sha256"]
        != result["runtime_source_sha256"]
    )
