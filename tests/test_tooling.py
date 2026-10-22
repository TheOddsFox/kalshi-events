from __future__ import annotations

import importlib.util
from pathlib import Path


def _installed_script():
    path = Path(__file__).resolve().parents[1] / "scripts" / "check-installed.py"
    spec = importlib.util.spec_from_file_location("check_installed", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_installed_venv_executables_use_posix_bin_directory(tmp_path):
    executable = _installed_script().venv_executable
    assert executable(tmp_path, "python") == tmp_path / "bin" / "python"
    assert executable(tmp_path, "kalshi-events") == tmp_path / "bin" / "kalshi-events"


def test_dbt_executable_is_resolved_beside_python(tmp_path):
    from kalshi_events.warehouse import _dbt_executable, _receipt_body_path

    python = tmp_path / "bin/python"
    assert _dbt_executable(str(python)) == python.parent / "dbt"
    assert _receipt_body_path("captures/id/raw/page/manifest.json") == (
        "captures/id/raw/page/body.bin"
    )
