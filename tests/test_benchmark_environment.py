from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path


def test_benchmark_records_posix_requirements_and_actual_source_bytes(tmp_path):
    script = Path(__file__).resolve().parents[1] / "scripts/benchmark-pipeline.py"
    spec = importlib.util.spec_from_file_location("benchmark_pipeline", script)
    benchmark = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(benchmark)
    source = tmp_path / "src/kalshi_events"
    source.mkdir(parents=True)
    (source / "cache.py").write_bytes(b"runtime source\n")
    (source / "cache.pyc").write_bytes(b"excluded")
    result = benchmark._environment(tmp_path, None)
    assert result["durability_requirement"] == "POSIX-file-and-directory-fsync"
    assert result["child_peak_rss_available"] is True
    assert result["runtime_source_sha256"] == {
        "src/kalshi_events/cache.py": hashlib.sha256(b"runtime source\n").hexdigest()
    }
    (source / "cache.py").write_bytes(b"changed source\n")
    assert (
        benchmark._environment(tmp_path, None)["runtime_source_sha256"]
        != result["runtime_source_sha256"]
    )
