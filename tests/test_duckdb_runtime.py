from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def production():
    path = Path(__file__).parents[1] / "scripts" / "production_run.py"
    spec = importlib.util.spec_from_file_location("production_run", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _runtime_versions(production, *, pyarrow: str = "26.0.0") -> dict[str, str | None]:
    return {**production.SUPPORTED_RUNTIME_VERSIONS, "pyarrow": pyarrow}


def test_supported_runtime_accepts_tested_locked_versions(production):
    production._validate_runtime_versions(_runtime_versions(production))


@pytest.mark.parametrize(
    ("package", "version"),
    [
        ("duckdb", "1.5.6"),
        ("dlt", "1.30.0"),
        ("dbt-core", "1.12.4"),
        ("dbt-duckdb", "1.11.0"),
        ("httpx", "0.28.0"),
        ("httpcore", "1.0.8"),
        ("duckdb", None),
        ("pyarrow", "17.0.0"),
        ("pyarrow", "16.1.0"),
    ],
)
def test_runtime_version_drift_is_rejected(production, package, version):
    versions = _runtime_versions(production)
    versions[package] = version
    with pytest.raises(production.RunnerError, match=package):
        production._validate_runtime_versions(versions)


def test_unsupported_runtime_stops_before_directory_preflight(production, monkeypatch, tmp_path):
    versions = _runtime_versions(production)
    versions["dbt-duckdb"] = "1.11.0"
    monkeypatch.setattr(production, "_package_versions", lambda: versions)
    monkeypatch.setattr(
        production,
        "prepare_directory",
        lambda *_args: pytest.fail("unsupported runtime reached directory preflight"),
    )

    with pytest.raises(production.RunnerError, match="dbt-duckdb"):
        production.run_production(tmp_path, full=True)
