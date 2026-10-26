"""Interrupted attempts consume conservative budgets without becoming exact evidence."""

from __future__ import annotations

import copy
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture(scope="module")
def production_recovery():
    path = Path(__file__).parents[1] / "scripts" / "production_run.py"
    spec = importlib.util.spec_from_file_location("production_recovery_adversarial", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _snapshot():
    reservation = {
        "capture_id": "c" * 32,
        "acquisition_protocol": 5,
        "version": 1,
        "request_id": "a" * 64,
        "reserved_bytes": 384 * 1024**2,
        "status": None,
        "accounting_complete": False,
    }
    return {
        "accounting_complete": False,
        "attempts": 3,
        "downloaded_or_reserved_bytes": reservation["reserved_bytes"] + 64 * 1024 + 123,
        "unfinished_reservations": [reservation],
        "root_id": "d" * 32,
        "storage": {},
    }


def test_interrupted_attempt_charges_full_reservation_without_rewriting_exact_accounting(
    production_recovery,
):
    snapshot = _snapshot()
    before = copy.deepcopy(snapshot)
    bound = production_recovery._reserved_upper_bound(snapshot, "c" * 32)

    assert snapshot == before
    assert bound["accounting_complete"] is False
    assert bound["accounting_basis"] == "reserved-upper-bound"
    assert bound["accounting_bound_verified"] is True
    assert bound["attempts"] == 3
    assert bound.get("downloaded_bytes") is None
    assert bound["downloaded_bytes_upper_bound"] == 384 * 1024**2 + 64 * 1024 + 123
    remaining = production_recovery.remaining_allowances(bound, 100, now=160)
    assert remaining["max-requests"] == production_recovery.RUN_LIMITS["attempts"] - 3
    assert remaining["max-download-bytes"] == (
        production_recovery.RUN_LIMITS["downloaded_bytes"] - bound["downloaded_bytes_upper_bound"]
    )


@pytest.mark.parametrize("reserved", [None, True, False, 0, -1, 1.5, "123", 384 * 1024**2 + 1])
def test_invalid_unfinished_reservation_never_authorizes_networking(production_recovery, reserved):
    snapshot = _snapshot()
    snapshot["unfinished_reservations"][0]["reserved_bytes"] = reserved
    with pytest.raises((ValueError, RuntimeError)):
        production_recovery._reserved_upper_bound(snapshot, "c" * 32)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("capture_id", "e" * 32),
        ("acquisition_protocol", True),
        ("acquisition_protocol", 4),
        ("version", True),
        ("version", 9),
        ("request_id", "bad"),
        ("status", 200),
        ("accounting_complete", True),
    ],
)
def test_contradictory_or_foreign_unfinished_receipt_fails_closed(
    production_recovery, field, value
):
    snapshot = _snapshot()
    snapshot["unfinished_reservations"][0][field] = value
    with pytest.raises((ValueError, RuntimeError)):
        production_recovery._reserved_upper_bound(snapshot, "c" * 32)


def test_duplicate_unfinished_request_identity_is_not_independent_accounting(production_recovery):
    snapshot = _snapshot()
    snapshot["unfinished_reservations"].append(
        copy.deepcopy(snapshot["unfinished_reservations"][0])
    )
    snapshot["downloaded_or_reserved_bytes"] *= 2
    with pytest.raises((ValueError, RuntimeError)):
        production_recovery._reserved_upper_bound(snapshot, "c" * 32)


def test_bound_cannot_be_smaller_than_its_own_unfinished_reservation(production_recovery):
    snapshot = _snapshot()
    snapshot["downloaded_or_reserved_bytes"] = 1
    with pytest.raises((ValueError, RuntimeError)):
        production_recovery._reserved_upper_bound(snapshot, "c" * 32)


@pytest.mark.parametrize("attempts", [0, True, -1, 1.1])
def test_invalid_attempt_count_cannot_back_a_recovery_bound(production_recovery, attempts):
    snapshot = _snapshot()
    snapshot["attempts"] = attempts
    with pytest.raises((ValueError, RuntimeError)):
        production_recovery._reserved_upper_bound(snapshot, "c" * 32)


@pytest.mark.parametrize("field", ["accounting_basis", "accounting_bound_verified"])
def test_an_unverified_numeric_bound_is_not_a_resume_permission(production_recovery, field):
    bound = production_recovery._reserved_upper_bound(_snapshot(), "c" * 32)
    del bound[field]
    with pytest.raises((ValueError, RuntimeError)):
        production_recovery.remaining_allowances(bound, 100, now=100)


def test_reconciliation_does_not_restart_the_original_wall_clock(production_recovery):
    bound = production_recovery._reserved_upper_bound(_snapshot(), "c" * 32)
    start = 1_000
    expired = start + production_recovery.RUN_LIMITS["workflow_seconds"]
    with pytest.raises((ValueError, RuntimeError)):
        production_recovery.remaining_allowances(
            bound,
            100,
            now=101,
            started_epoch=start,
            wall_now=expired,
        )


@pytest.mark.parametrize("exhausted", ["attempts", "downloaded_bytes_upper_bound"])
def test_reconciled_but_exhausted_budget_still_prevents_acquisition(production_recovery, exhausted):
    bound = production_recovery._reserved_upper_bound(_snapshot(), "c" * 32)
    bound[exhausted] = production_recovery.RUN_LIMITS[
        "attempts" if exhausted == "attempts" else "downloaded_bytes"
    ]
    with pytest.raises((ValueError, RuntimeError)):
        production_recovery.remaining_allowances(bound, 100, now=100)


@pytest.mark.parametrize("page_size", [4096, 16384])
def test_memory_guard_counts_logical_compressor_pages_not_only_resident_pages(
    production_recovery, monkeypatch, page_size
):
    text = (
        f"Mach Virtual Memory Statistics: (page size of {page_size} bytes)\n"
        "Pages free: 1.\n"
        "Pages stored in compressor: 2000000.\n"
        "Pages occupied by compressor: 1.\n"
    )
    monkeypatch.setattr(
        production_recovery.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(stdout=text),
    )
    assert production_recovery._system_compressed_bytes() == 2_000_000 * page_size


@pytest.mark.parametrize(
    "text",
    [
        "",
        "Mach Virtual Memory Statistics: (page size of 16384 bytes)\n",
        "Pages stored in compressor: 2000000.\n",
        "Mach Virtual Memory Statistics: (page size of 16384 bytes)\nPages stored in compressor: -1.\n",
        "Mach Virtual Memory Statistics: (page size of 0 bytes)\nPages stored in compressor: 2000000.\n",
    ],
)
def test_unavailable_or_invalid_compressor_measurements_do_not_report_safe_zero(
    production_recovery, monkeypatch, text
):
    monkeypatch.setattr(
        production_recovery.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(stdout=text),
    )
    with pytest.raises((ValueError, RuntimeError)):
        production_recovery._system_compressed_bytes()
