from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path


def _installed_script():
    path = Path(__file__).resolve().parents[1] / "scripts" / "check-installed.py"
    spec = importlib.util.spec_from_file_location("check_installed", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _verify_script():
    path = Path(__file__).resolve().parents[1] / "scripts" / "verify.py"
    spec = importlib.util.spec_from_file_location("verify_script", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_installed_venv_executable_paths_cover_windows_and_posix(tmp_path):
    executable = _installed_script().venv_executable
    assert executable(tmp_path, "python", windows=True) == tmp_path / "Scripts" / "python.exe"
    assert executable(tmp_path, "kalshi-events", windows=True) == (
        tmp_path / "Scripts" / "kalshi-events.exe"
    )
    assert executable(tmp_path, "python", windows=False) == tmp_path / "bin" / "python"


def test_dbt_executable_is_resolved_beside_python_for_each_platform(tmp_path):
    from kalshi_events.warehouse import _dbt_executable, _receipt_body_path

    python = tmp_path / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    assert _dbt_executable(str(python), windows=True) == python.parent / "dbt.exe"
    assert _dbt_executable(str(python), windows=False) == python.parent / "dbt"
    assert _receipt_body_path("captures/id/raw/page/manifest.json") == (
        "captures/id/raw/page/body.bin"
    )


def test_fast_verification_commands_keep_offline_api_overrides():
    commands = _verify_script().verification_commands(fast=True, uv="uv")
    argv = [command for command, _ in commands]
    assert argv == [
        ["git", "diff", "--check"],
        ["git", "diff", "--cached", "--check"],
        ["uv", "run", "--locked", "ruff", "check", "."],
        ["uv", "run", "--locked", "ruff", "format", "--check", "."],
        ["uv", "run", "--locked", "pytest", "-q"],
    ]
    assert commands[-1][1]["KALSHI_EVENTS_API_URL"] == "http://127.0.0.1:9/trade-api/v2"
    assert commands[-1][1]["KALSHI_EVENTS_ALLOW_LOOPBACK"] == "1"


def test_full_verification_retains_sync_parse_and_build():
    commands = _verify_script().verification_commands(fast=False, uv="uv")
    argv = [command for command, _ in commands]
    assert ["uv", "lock", "--check"] in argv
    assert ["uv", "sync", "--locked"] in argv
    assert ["uv", "run", "--locked", "kalshi-events", "dbt", "--", "parse"] in argv
    assert argv[-1] == ["uv", "build"]
    assert commands[-1][1] == {"UV_LINK_MODE": "copy"}


def test_verification_uses_child_environment_overrides_without_mutating_parent(
    monkeypatch, tmp_path
):
    verify = _verify_script()

    calls = []
    monkeypatch.setattr(verify.shutil, "which", lambda _: "uv")
    monkeypatch.setattr(
        verify.subprocess,
        "run",
        lambda *args, **kwargs: calls.append((args[0], kwargs)),
    )
    monkeypatch.setattr(sys, "argv", ["verify.py", "--fast"])
    before = os.environ.get("KALSHI_EVENTS_API_URL")

    verify.main()

    assert os.environ.get("KALSHI_EVENTS_API_URL") == before
    pytest_environment = next(kwargs["env"] for argv, kwargs in calls if "pytest" in argv)
    assert pytest_environment["KALSHI_EVENTS_API_URL"] == "http://127.0.0.1:9/trade-api/v2"
    assert pytest_environment["KALSHI_EVENTS_ALLOW_LOOPBACK"] == "1"
