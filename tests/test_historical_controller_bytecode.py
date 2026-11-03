"""Verify private historical controller bytecode policy reaches real children."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import sys
import textwrap
import uuid
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
GRANTS_SPEC = importlib.util.spec_from_file_location(
    "controller_bytecode_historical_grants", SCRIPTS / "historical_grants.py"
)
assert GRANTS_SPEC and GRANTS_SPEC.loader
grants = importlib.util.module_from_spec(GRANTS_SPEC)
GRANTS_SPEC.loader.exec_module(grants)
PRODUCTION_SPEC = importlib.util.spec_from_file_location(
    "controller_bytecode_production_run", SCRIPTS / "production_run.py"
)
assert PRODUCTION_SPEC and PRODUCTION_SPEC.loader
production = importlib.util.module_from_spec(PRODUCTION_SPEC)
PRODUCTION_SPEC.loader.exec_module(production)


def _new_control_root() -> Path:
    runtime = ROOT / "data" / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    for offset in range(3650):
        day = date.today() - timedelta(days=offset)
        path = runtime / f"kalshi-historical-completion-{day:%Y%m%d}"
        try:
            path.mkdir(mode=0o700)
        except FileExistsError:
            continue
        path.chmod(0o700)
        return path
    raise AssertionError("no unused managed historical control directory name")


def _pyc_files(root: Path) -> set[Path]:
    return set(root.rglob("*.pyc"))


def _write_sampler_probe(hook_root: Path) -> Path:
    module = hook_root / "controller_bytecode_probe.py"
    module.write_text('MARKER = "controller-memory-sampler-probe"\n')
    observed_path = hook_root / "startup-observed.json"
    (hook_root / "sitecustomize.py").write_text(
        textwrap.dedent(
            f"""\
            import json, os, sys
            from pathlib import Path
            import controller_bytecode_probe

            Path({str(observed_path)!r}).write_text(json.dumps({{
                "bytecode_env": os.environ.get("PYTHONDONTWRITEBYTECODE"),
                "dont_write_bytecode": sys.dont_write_bytecode,
                "module_marker": controller_bytecode_probe.MARKER,
                "orig_argv": sys.orig_argv,
            }}, sort_keys=True) + "\\n")
            """
        )
    )
    return observed_path


def test_real_private_controller_protects_unflagged_memory_sampler_imports(monkeypatch, capsys):
    run_root = ROOT / "data" / "testing" / "historical-controller-bytecode" / uuid.uuid4().hex
    run_root.mkdir(parents=True, mode=0o700)
    production_root = run_root / "production"
    production_root.mkdir(mode=0o700)
    (production_root / ".production-run.lock").touch(mode=0o600)
    hook_root = run_root / "sampler-hook"
    hook_root.mkdir(mode=0o700)
    observed_path = _write_sampler_probe(hook_root)
    hook_bytecode_before = _pyc_files(hook_root)
    source_bytecode_before = _pyc_files(SCRIPTS) | _pyc_files(ROOT / "src")
    control_root = _new_control_root()
    observed: dict[str, object] = {}
    issue_calls = 0

    class StopBeforeGrant(Exception):
        pass

    def prelaunch_authority(*args, **kwargs):
        observed["authority_calls"] = int(observed.get("authority_calls", 0)) + 1
        observed["authority_args"] = args
        observed["authority_kwargs"] = kwargs
        observed["controller_env"] = os.environ.get("PYTHONDONTWRITEBYTECODE")
        sample = production._sample_memory(None, timeout=5)
        observed["sample"] = sample
        observed["startup"] = json.loads(observed_path.read_text())
        raise StopBeforeGrant("stop after sampler probe, before grant issuance")

    def forbidden_issue_launch(*_args, **_kwargs):
        nonlocal issue_calls
        issue_calls += 1
        raise AssertionError("test must stop before issuing a grant")

    monkeypatch.delenv("PYTHONDONTWRITEBYTECODE", raising=False)
    assert "PYTHONDONTWRITEBYTECODE" not in os.environ
    monkeypatch.setenv("PYTHONPATH", str(hook_root))
    monkeypatch.setitem(sys.modules, "historical_grants", grants)
    monkeypatch.setattr(production, "_historical_authority", prelaunch_authority)
    monkeypatch.setattr(grants, "issue_launch", forbidden_issue_launch)
    try:
        result = production._historical_control_main(production_root, control_root, "a" * 32)

        assert result == 1
        assert observed["authority_calls"] == 1
        assert observed["authority_kwargs"]["prelaunch"] is True
        assert observed["authority_kwargs"]["cheap_only"] is True
        assert observed["controller_env"] == "1"
        assert issue_calls == 0

        sample = observed["sample"]
        assert sample["system_memory_pressure"] in {"normal", "warn", "critical"}
        assert sample["child_process_group_rss_bytes"] is None
        assert sample["child_process_group_physical_footprint_bytes"] is None

        startup = observed["startup"]
        assert startup["bytecode_env"] == "1"
        assert startup["dont_write_bytecode"] is True
        assert startup["module_marker"] == "controller-memory-sampler-probe"
        assert "-B" not in startup["orig_argv"]
        assert _pyc_files(hook_root) == hook_bytecode_before
        assert (_pyc_files(SCRIPTS) | _pyc_files(ROOT / "src")) == source_bytecode_before
        assert "stop after sampler probe" in capsys.readouterr().out

        with production._run_lock(production_root):
            pass
    finally:
        shutil.rmtree(control_root, ignore_errors=True)
        shutil.rmtree(run_root, ignore_errors=True)
