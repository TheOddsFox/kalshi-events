from __future__ import annotations

import importlib.util
import json
import sys
import time
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts" / "production_run.py"
CAPTURE_ID = "a" * 32
ROOT_ID = "b" * 32


@pytest.fixture
def production(monkeypatch):
    spec = importlib.util.spec_from_file_location("expanded_policy_test_production", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


def _session_report(production, tmp_path, monkeypatch, **changes):
    directory = tmp_path / "production"
    report_path = directory / "runs" / ("c" * 32) / production.REPORT_NAME
    report_path.parent.mkdir(parents=True)
    report = {
        "capture_id": CAPTURE_ID,
        "root_id": ROOT_ID,
        "mode": "historical",
        "phase": "stopped",
        "active_process_group": None,
        "network_activity_this_invocation": {
            "requests": 0,
            "downloaded_bytes": 0,
            "accounting_complete": True,
        },
        "accounting_complete": True,
        "production_state": "not_attempted",
        "resource_profile": "default",
        "limits": dict(production.RUN_LIMITS),
        **changes,
    }
    report_path.write_text(json.dumps(report))
    monkeypatch.setattr(production, "_owner_root_id", lambda _directory: ROOT_ID)
    monkeypatch.setattr(production, "_boot_stamp", lambda: "current-boot")
    monkeypatch.setattr(production, "_memory_preflight_sample_state", lambda *_args: None)
    return directory, report_path, report


@pytest.mark.parametrize(
    ("change", "value"),
    [
        ("phase", "preflight"),
        ("active_process_group", 12345),
        (
            "network_activity_this_invocation",
            {"requests": 1, "downloaded_bytes": 64, "accounting_complete": True},
        ),
        ("production_state", "verified"),
        ("mode", "activate-historical"),
    ],
)
def test_expanded_profile_transition_rejects_ineligible_stopped_capture(
    production, tmp_path, monkeypatch, change, value
):
    directory, report_path, _report = _session_report(
        production, tmp_path, monkeypatch, **{change: value}
    )
    original_bytes = report_path.read_bytes()

    with pytest.raises(production.RunnerError, match="conflicting historical resource profile"):
        production._load_resume(
            directory,
            CAPTURE_ID,
            historical=True,
            resource_profile="expanded",
        )
    assert report_path.read_bytes() == original_bytes


def test_expanded_profile_transition_accepts_zero_network_stopped_capture(
    production, tmp_path, monkeypatch
):
    now_epoch = time.time()
    now_monotonic = time.monotonic()
    directory, _report_path, _report = _session_report(
        production,
        tmp_path,
        monkeypatch,
        started_epoch=now_epoch - 60,
        started_monotonic=now_monotonic - 60,
        time_budget_started_epoch=now_epoch - 60,
        time_budget_started_monotonic=now_monotonic - 60,
        deadline_epoch=now_epoch + 3600,
        deadline_monotonic=now_monotonic + 3600,
        last_observed_epoch=now_epoch - 1,
        boot_stamp="current-boot",
        system={"filesystem_device": tmp_path.stat().st_dev},
        phase_seconds={},
    )

    _path, report, _paths = production._load_resume(
        directory,
        CAPTURE_ID,
        historical=True,
        resource_profile="expanded",
    )

    assert report["resource_profile"] == "expanded"
    assert [entry["profile"] for entry in report["resource_profile_history"]] == [
        "default",
        "expanded",
    ]


def test_expanded_profile_transition_requires_the_selected_exact_capture(
    production, tmp_path, monkeypatch
):
    directory, report_path, _report = _session_report(production, tmp_path, monkeypatch)
    original_bytes = report_path.read_bytes()

    with pytest.raises(
        production.RunnerError, match="one durable session report for this capture ID"
    ):
        production._load_resume(
            directory,
            "d" * 32,
            historical=True,
            resource_profile="expanded",
        )
    assert report_path.read_bytes() == original_bytes
