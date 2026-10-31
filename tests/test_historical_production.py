"""Historical production work stays offline and reports its narrower coverage."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import shutil
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from conftest import FakeClient

from kalshi_events import capture, publish, transport, warehouse
from kalshi_events.common import CatalogueError, Store, json_bytes

RUNNER = Path(__file__).parents[1] / "scripts" / "production_run.py"
ACTIVATION = Path(__file__).parents[1] / "scripts" / "production_activation.py"
CAPTURE_ID = "a" * 32
RELEASE_ID = "b" * 64
RESERVED_BYTES = 384 * 1024**2 + 64 * 1024


@pytest.fixture(autouse=True)
def loopback_api(monkeypatch):
    monkeypatch.setenv("KALSHI_EVENTS_API_URL", "http://127.0.0.1:9/trade-api/v2")
    monkeypatch.setenv("KALSHI_EVENTS_ALLOW_LOOPBACK", "1")


@pytest.fixture(scope="module")
def production():
    spec = importlib.util.spec_from_file_location("historical_production_run", RUNNER)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def production_activation():
    spec = importlib.util.spec_from_file_location("historical_production_activation", ACTIVATION)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def historical_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("historical-source") / "operator"
    store = Store(root)
    state = capture.capture(store, client_factory=FakeClient)
    assert state["complete"] is True
    manifest = publish.publish(store, state["capture_id"], catalogue_version=3)
    return root, manifest


@pytest.fixture
def historical_candidate(historical_root, tmp_path):
    source, manifest = historical_root
    directory = tmp_path / "production"
    root = directory / "operator"
    directory.mkdir()
    shutil.copytree(source, root)
    return (
        directory,
        root,
        root / warehouse.build_directory(manifest["capture_id"], 3) / "warehouse.duckdb",
        manifest,
    )


def _snapshot():
    reservations = [
        {
            "capture_id": CAPTURE_ID,
            "request_id": f"{index:064x}",
            "reserved_bytes": 1024,
            "status": None,
            "accounting_complete": False,
            "acquisition_protocol": 5,
            "version": 1,
        }
        for index in range(4)
    ]
    return {
        "attempts": 12,
        "downloaded_or_reserved_bytes": 4 * RESERVED_BYTES,
        "accounting_complete": False,
        "unfinished_reservations": reservations,
        "storage": {
            "retained_bytes": 1,
            "temporary_bytes": 0,
            "database_bytes": 1,
        },
        "root_id": "c" * 32,
    }


def _historical_report(directory: Path, operator_root: Path, production) -> tuple[Path, dict]:
    run_id = "d" * 32
    run_root = directory / "runs" / run_id
    (run_root / "logs").mkdir(parents=True)
    now_epoch = time.time()
    now_mono = time.monotonic()
    snapshot = _snapshot()
    store = Store(operator_root)
    with store.lock():
        from kalshi_events.incremental import root_id

        snapshot["root_id"] = root_id(store, create=True)
    report = {
        "version": 1,
        "session_id": run_id,
        "mode": "full",
        "started_epoch": now_epoch - 10,
        "deadline_epoch": now_epoch + production.RUN_LIMITS["session_seconds"] - 10,
        "started_monotonic": now_mono - 10,
        "deadline_monotonic": now_mono + production.RUN_LIMITS["session_seconds"] - 10,
        "time_budget_started_epoch": now_epoch - 10,
        "time_budget_started_monotonic": now_mono - 10,
        "last_observed_epoch": now_epoch,
        "boot_stamp": "same boot",
        "system": {"filesystem_device": directory.stat().st_dev},
        "capture_id": CAPTURE_ID,
        "root_id": snapshot["root_id"],
        "capture_complete": False,
        "publication_state": "unknown",
        "production_state": "not_attempted",
        "accounting_complete": False,
        "requests": snapshot["attempts"],
        "downloaded_bytes": None,
        "downloaded_bytes_upper_bound": snapshot["downloaded_or_reserved_bytes"],
        "accounting_basis": "reserved-upper-bound",
        "accounting_bound_verified": True,
        "unfinished_reservation_ids": sorted(
            item["request_id"] for item in snapshot["unfinished_reservations"]
        ),
        "phase": "stopped",
        "active_process_group": None,
        "phase_seconds": {},
        "limits": dict(production.RUN_LIMITS),
    }
    owner = directory / production.OWNER_MARKER
    production._atomic_report(owner, {"version": 1, "root_id": report["root_id"]})
    report_path = run_root / production.REPORT_NAME
    production._atomic_report(report_path, report)
    return report_path, report


def _renewable_session(
    directory: Path, production, *, epoch_remaining=-3600, monotonic_remaining=-3600
):
    directory.mkdir()
    (directory / "runs").mkdir()
    operator = directory / "operator"
    report_path, report = _historical_report(directory, operator, production)
    _write_capture_genesis(operator)
    report["system"]["filesystem_volume_uuid"] = "ACAD66E7-42DA-426B-B20D-FB87A453396B"
    report["offline_historical_window"] = {
        "version": 1,
        "capture_id": CAPTURE_ID,
        "root_id": report["root_id"],
        "revision": "f" * 40,
        "started_at": production._utc(),
        "started_epoch": time.time() + epoch_remaining - 3600,
        "started_monotonic": time.monotonic() + monotonic_remaining - 3600,
        "deadline_epoch": time.time() + epoch_remaining,
        "deadline_monotonic": time.monotonic() + monotonic_remaining,
    }
    report["phase"] = "stopped"
    production._atomic_report(report_path, report)
    return report_path, report, operator


def _write_capture_genesis(operator: Path):
    capture_root = operator / "captures" / CAPTURE_ID
    capture_root.mkdir(parents=True)
    capture_state = {"version": 1, "capture_id": CAPTURE_ID}
    (capture_root / "state.json").write_bytes(json_bytes(capture_state))
    genesis = {
        "version": 2,
        "capture_id": CAPTURE_ID,
        "seq": 1,
        "prev": "0" * 64,
        "op": "start",
        "payload": {
            "state": capture_state,
            "seen_cursors": {},
            "state_sha256": "a" * 64,
        },
    }
    genesis["sha256"] = hashlib.sha256(json_bytes(genesis)).hexdigest()
    (capture_root / "pages.jsonl").write_bytes(json_bytes(genesis) + b"\n")


def test_historical_flag_selects_offline_build_or_version_three_activation(production):
    assert production.validate_mode(False, CAPTURE_ID, False) == "resume"
    assert production.validate_mode(False, CAPTURE_ID, False, historical=True) == "historical"
    assert production.validate_mode(False, None, True, historical=True) == "activate-historical"
    assert (
        production.validate_mode(
            False,
            CAPTURE_ID,
            False,
            historical=True,
            offline_duration_seconds=production.MAX_OFFLINE_HISTORICAL_SECONDS,
        )
        == "historical"
    )
    assert (
        production.validate_mode(
            False,
            CAPTURE_ID,
            False,
            historical=True,
            offline_duration_seconds=3600,
            supersede_offline_window=True,
        )
        == "historical"
    )
    with pytest.raises(production.RunnerError):
        production.validate_mode(True, None, False, historical=True)
    with pytest.raises(production.RunnerError, match="--historical --capture-id"):
        production.validate_mode(False, CAPTURE_ID, False, offline_duration_seconds=1)
    with pytest.raises(production.RunnerError, match="--renew-offline-window"):
        production.validate_mode(
            False,
            CAPTURE_ID,
            False,
            historical=True,
            renew_offline_window=True,
        )
    with pytest.raises(production.RunnerError, match="--supersede-offline-window"):
        production.validate_mode(
            False,
            CAPTURE_ID,
            False,
            historical=True,
            supersede_offline_window=True,
        )
    with pytest.raises(production.RunnerError, match="--renew-offline-window"):
        production.validate_mode(
            False,
            None,
            True,
            historical=True,
            renew_offline_window=True,
        )
    for duration in (0, -1, float("inf"), float("nan"), 86_400.01, 86_401):
        with pytest.raises(production.RunnerError):
            production.validate_mode(
                False,
                CAPTURE_ID,
                False,
                historical=True,
                offline_duration_seconds=duration,
            )


@pytest.mark.parametrize(
    ("version", "duration", "valid"),
    [
        (1, 14_400, True),
        (1, 14_400.01, False),
        (1, 14_401, False),
        (2, 86_400, True),
        (2, 86_400.01, False),
        (2, 86_401, False),
        (0, 60, False),
        (3, 60, False),
        (True, 60, False),
    ],
)
def test_offline_historical_window_versions_keep_their_own_duration_caps(
    production, monkeypatch, version, duration, valid
):
    now_epoch = time.time()
    now_monotonic = time.monotonic()
    report = {
        "root_id": "c" * 32,
        "offline_historical_window": {
            "version": version,
            "capture_id": CAPTURE_ID,
            "root_id": "c" * 32,
            "revision": "f" * 40,
            "started_at": production._utc(),
            "started_epoch": now_epoch - 10,
            "started_monotonic": now_monotonic - 10,
            "deadline_epoch": now_epoch - 10 + duration,
            "deadline_monotonic": now_monotonic - 10 + duration,
        },
    }
    monkeypatch.setattr(production, "_revision", lambda: "f" * 40)

    if valid:
        assert (
            production._validate_offline_historical_window(report, CAPTURE_ID)
            == report["offline_historical_window"]
        )
    else:
        with pytest.raises(production.RunnerError):
            production._validate_offline_historical_window(report, CAPTURE_ID)


def test_configure_offline_historical_window_accepts_exact_limit_with_fractional_clocks(
    production, monkeypatch
):
    started = 101_254.75427489051
    deadline = started + production.MAX_OFFLINE_HISTORICAL_SECONDS
    assert deadline - started > production.MAX_OFFLINE_HISTORICAL_SECONDS
    monkeypatch.setattr(production.time, "time", lambda: started)
    monkeypatch.setattr(production.time, "monotonic", lambda: started)
    monkeypatch.setattr(production, "_revision", lambda: "f" * 40)

    window = production._configure_offline_historical_window(
        {"root_id": "c" * 32},
        CAPTURE_ID,
        production.MAX_OFFLINE_HISTORICAL_SECONDS,
    )

    assert window["version"] == 2
    assert window["deadline_epoch"] == deadline
    assert window["deadline_monotonic"] == deadline


def test_ordinary_reuse_of_v1_window_ignores_larger_requested_duration(
    production, tmp_path, monkeypatch
):
    directory = tmp_path / "production"
    report_path, report, _operator = _renewable_session(
        directory, production, epoch_remaining=3600, monotonic_remaining=3600
    )
    legacy_window = dict(report["offline_historical_window"])
    monkeypatch.setattr(production, "_revision", lambda: legacy_window["revision"])
    monkeypatch.setattr(production, "_boot_stamp", lambda: report["boot_stamp"])

    loaded_path, loaded, _paths = production._load_resume(
        directory,
        CAPTURE_ID,
        historical=True,
        offline_duration_seconds=86_400,
    )
    reused = production._configure_offline_historical_window(loaded, CAPTURE_ID, 86_400)

    assert loaded_path == report_path
    assert reused == legacy_window
    assert reused["version"] == 1
    assert "offline_historical_window_history" not in loaded


@pytest.mark.parametrize(("legacy_duration", "valid"), [(14_400, True), (14_401, False)])
def test_mixed_window_history_applies_each_version_duration_cap(
    production, monkeypatch, legacy_duration, valid
):
    now_epoch = time.time()
    now_monotonic = time.monotonic()
    current = {
        "version": 2,
        "capture_id": CAPTURE_ID,
        "root_id": "c" * 32,
        "revision": "e" * 40,
        "started_at": production._utc(),
        "started_epoch": now_epoch - 10,
        "started_monotonic": now_monotonic - 10,
        "deadline_epoch": now_epoch - 10 + 86_400,
        "deadline_monotonic": now_monotonic - 10 + 86_400,
    }
    legacy_end_epoch = current["started_epoch"] - 1
    legacy_end_monotonic = current["started_monotonic"] - 1
    legacy = {
        "version": 1,
        "capture_id": CAPTURE_ID,
        "root_id": "c" * 32,
        "revision": "f" * 40,
        "started_at": production._utc(),
        "started_epoch": legacy_end_epoch - legacy_duration,
        "started_monotonic": legacy_end_monotonic - legacy_duration,
        "deadline_epoch": legacy_end_epoch,
        "deadline_monotonic": legacy_end_monotonic,
    }
    report = {
        "root_id": "c" * 32,
        "offline_historical_window": current,
        "offline_historical_window_history": [legacy],
    }
    monkeypatch.setattr(production, "_revision", lambda: "e" * 40)

    if valid:
        assert production._validate_offline_historical_window(report, CAPTURE_ID) == current
    else:
        with pytest.raises(production.RunnerError, match="duration is invalid"):
            production._validate_offline_historical_window(report, CAPTURE_ID)


def test_historical_resume_uses_existing_report_and_does_not_extend_its_window(
    production, tmp_path, monkeypatch
):
    directory = tmp_path / "production"
    directory.mkdir()
    (directory / "runs").mkdir()
    operator = directory / "operator"
    store = Store(operator)
    with store.lock():
        from kalshi_events.incremental import root_id

        root_id(store, create=True)
    report_path, initial = _historical_report(directory, operator, production)
    initial["system"]["filesystem_device"] = directory.stat().st_dev
    production._atomic_report(report_path, initial)
    monkeypatch.setattr(production, "_boot_stamp", lambda: "same boot")
    deadlines = (
        initial["time_budget_started_epoch"],
        initial["deadline_epoch"],
        initial["time_budget_started_monotonic"],
        initial["deadline_monotonic"],
    )

    loaded_path, loaded, _paths = production._load_resume(directory, CAPTURE_ID, historical=True)

    assert loaded_path == report_path
    assert loaded["accounting_complete"] is False
    assert loaded["accounting_basis"] == "reserved-upper-bound"
    assert loaded["requests"] == initial["requests"]
    assert loaded["downloaded_bytes_upper_bound"] == initial["downloaded_bytes_upper_bound"]
    assert deadlines == (
        loaded["time_budget_started_epoch"],
        loaded["deadline_epoch"],
        loaded["time_budget_started_monotonic"],
        loaded["deadline_monotonic"],
    )
    with pytest.raises(production.RunnerError):
        production._load_resume(directory, CAPTURE_ID)
    expired = dict(loaded)
    expired["deadline_epoch"] = time.time() - 1
    expired["deadline_monotonic"] = time.monotonic() - 1
    production._atomic_report(report_path, expired)
    with pytest.raises(production.RunnerError, match="48-hour session expired"):
        production._load_resume(directory, CAPTURE_ID, historical=True)


def test_explicit_offline_window_renewal_accepts_expired_old_revision_without_mutating_it(
    production, tmp_path, monkeypatch
):
    directory = tmp_path / "production"
    report_path, report, _operator = _renewable_session(directory, production)
    monkeypatch.setattr(production, "_revision", lambda: "e" * 40)
    monkeypatch.setattr(production, "_boot_stamp", lambda: report["boot_stamp"])
    monkeypatch.setattr(
        production,
        "_volume_uuid",
        lambda _path: "ACAD66E7-42DA-426B-B20D-FB87A453396B",
    )
    old_window = dict(report["offline_historical_window"])

    loaded_path, loaded, _paths = production._load_resume(
        directory,
        CAPTURE_ID,
        historical=True,
        offline_duration_seconds=600,
        renew_offline_window=True,
    )

    assert loaded_path == report_path
    assert loaded["offline_historical_window"] == old_window
    assert "offline_historical_window_history" not in loaded
    with pytest.raises(production.RunnerError, match="identity does not match"):
        production._load_resume(
            directory,
            CAPTURE_ID,
            historical=True,
            offline_duration_seconds=600,
        )


def test_explicit_supersede_archives_the_unchanged_window_and_preserves_budgets(
    production, tmp_path, monkeypatch
):
    directory = tmp_path / "production"
    report_path, report, _operator = _renewable_session(
        directory, production, epoch_remaining=3600, monotonic_remaining=3600
    )
    old_window = dict(report["offline_historical_window"])
    old_accounting = {
        key: report.get(key)
        for key in ("requests", "downloaded_bytes", "downloaded_bytes_upper_bound")
    }
    old_deadlines = {
        key: report[key]
        for key in (
            "started_epoch",
            "deadline_epoch",
            "started_monotonic",
            "deadline_monotonic",
            "time_budget_started_epoch",
            "time_budget_started_monotonic",
        )
    }
    monkeypatch.setattr(production, "_revision", lambda: "e" * 40)
    monkeypatch.setattr(production, "_boot_stamp", lambda: report["boot_stamp"])
    monkeypatch.setattr(
        production,
        "_volume_uuid",
        lambda _path: "ACAD66E7-42DA-426B-B20D-FB87A453396B",
    )

    with pytest.raises(production.RunnerError, match="both clocks to expire"):
        production._load_resume(
            directory,
            CAPTURE_ID,
            historical=True,
            offline_duration_seconds=3600,
            renew_offline_window=True,
        )

    loaded_path, loaded, _paths = production._load_resume(
        directory,
        CAPTURE_ID,
        historical=True,
        offline_duration_seconds=3600,
        supersede_offline_window=True,
    )
    production._configure_offline_historical_window(
        loaded,
        CAPTURE_ID,
        3600,
        renew=True,
        supersede=True,
    )
    production._atomic_report(loaded_path, loaded)

    assert loaded["offline_historical_window_history"] == [old_window]
    assert loaded["offline_historical_window"] != old_window
    assert loaded["offline_historical_window"]["revision"] == "e" * 40
    assert loaded["offline_historical_window"]["started_epoch"] < old_window["deadline_epoch"]
    assert (
        loaded["offline_historical_window"]["started_monotonic"] < old_window["deadline_monotonic"]
    )
    assert loaded["offline_historical_window_supersessions"] == [
        {
            "history_index": 0,
            "window_started_epoch": old_window["started_epoch"],
            "window_started_monotonic": old_window["started_monotonic"],
            "window_deadline_epoch": old_window["deadline_epoch"],
            "window_deadline_monotonic": old_window["deadline_monotonic"],
            "successor_started_epoch": loaded["offline_historical_window"]["started_epoch"],
            "successor_started_monotonic": loaded["offline_historical_window"]["started_monotonic"],
        }
    ]
    assert {key: loaded[key] for key in old_accounting} == old_accounting
    assert {key: loaded[key] for key in old_deadlines} == old_deadlines

    _path, reloaded, _paths = production._load_resume(directory, CAPTURE_ID, historical=True)
    assert reloaded["offline_historical_window_history"] == [old_window]
    reloaded["offline_historical_window_supersessions"][0]["successor_started_epoch"] += 1
    with pytest.raises(production.RunnerError, match="supersession history is invalid"):
        production._validate_offline_historical_window(reloaded, CAPTURE_ID)


@pytest.mark.parametrize(
    ("history", "supersessions"),
    [({}, []), ([{}], []), ([], {}), ([], [{}])],
)
def test_ordinary_first_offline_window_does_not_ignore_malformed_history(
    production, tmp_path, monkeypatch, history, supersessions
):
    directory = tmp_path / "production"
    directory.mkdir()
    (directory / "runs").mkdir()
    operator = directory / "operator"
    report_path, report = _historical_report(directory, operator, production)
    report["offline_historical_window_history"] = history
    report["offline_historical_window_supersessions"] = supersessions
    production._atomic_report(report_path, report)
    monkeypatch.setattr(production, "_boot_stamp", lambda: report["boot_stamp"])

    with pytest.raises(production.RunnerError, match="history has no current window"):
        production._load_resume(
            directory,
            CAPTURE_ID,
            historical=True,
            offline_duration_seconds=600,
        )
    saved = production._strict_json(report_path)
    assert "offline_historical_window" not in saved
    assert saved["offline_historical_window_history"] == history
    assert saved["offline_historical_window_supersessions"] == supersessions


@pytest.mark.parametrize(
    ("epoch_remaining", "monotonic_remaining"),
    [(-1, 60), (60, -1)],
)
def test_explicit_offline_window_renewal_requires_both_clocks_to_expire(
    production, tmp_path, monkeypatch, epoch_remaining, monotonic_remaining
):
    directory = tmp_path / "production"
    report_path, report, _operator = _renewable_session(
        directory,
        production,
        epoch_remaining=epoch_remaining,
        monotonic_remaining=monotonic_remaining,
    )
    monkeypatch.setattr(production, "_revision", lambda: "e" * 40)
    monkeypatch.setattr(production, "_boot_stamp", lambda: report["boot_stamp"])
    monkeypatch.setattr(
        production,
        "_volume_uuid",
        lambda _path: "ACAD66E7-42DA-426B-B20D-FB87A453396B",
    )

    with pytest.raises(production.RunnerError, match="both clocks to expire"):
        production._load_resume(
            directory,
            CAPTURE_ID,
            historical=True,
            offline_duration_seconds=600,
            renew_offline_window=True,
        )
    assert (
        production._strict_json(report_path)["offline_historical_window"]
        == report["offline_historical_window"]
    )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("phase", "stopped session"),
        ("active", "unreconciled child process group"),
        ("verified", "production activation already succeeded"),
        ("boot", "Mac restarted"),
        ("device", "filesystem changed"),
        ("volume", "volume UUID differs"),
        ("root-id", "root identity does not match"),
        ("owner", "ownership marker"),
        ("scoped-root", "current unscoped operator root"),
        ("capture-genesis", "capture genesis identity"),
        ("capture-path", "capture genesis is missing"),
    ],
)
def test_explicit_offline_window_renewal_fails_closed_on_changed_session_identity(
    production, tmp_path, monkeypatch, mutation, message
):
    directory = tmp_path / "production"
    report_path, report, operator = _renewable_session(directory, production)
    monkeypatch.setattr(production, "_revision", lambda: "e" * 40)
    monkeypatch.setattr(production, "_boot_stamp", lambda: report["boot_stamp"])
    monkeypatch.setattr(
        production,
        "_volume_uuid",
        lambda _path: "ACAD66E7-42DA-426B-B20D-FB87A453396B",
    )
    if mutation == "phase":
        report["phase"] = "refresh"
        production._atomic_report(report_path, report)
    elif mutation == "active":
        report["active_process_group"] = 12345
        production._atomic_report(report_path, report)
    elif mutation == "verified":
        report["production_state"] = "verified"
        production._atomic_report(report_path, report)
    elif mutation == "boot":
        monkeypatch.setattr(production, "_boot_stamp", lambda: "different boot")
    elif mutation == "device":
        report["system"]["filesystem_device"] = -1
        production._atomic_report(report_path, report)
    elif mutation == "volume":
        monkeypatch.setattr(production, "_volume_uuid", lambda _path: "0" * 36)
    elif mutation == "root-id":
        identity = production._strict_json(operator / "root-id.json")
        identity["root_id"] = "9" * 32
        (operator / "root-id.json").write_text(json.dumps(identity))
    elif mutation == "owner":
        production._atomic_report(
            directory / production.OWNER_MARKER,
            {"version": 1, "root_id": "9" * 32},
        )
    elif mutation == "scoped-root":
        (operator / "root.json").write_text(
            json.dumps(
                {
                    "owner": "oddsfox.kalshi.events.v1",
                    "storage_version": 5,
                    "scope": {
                        "kind": "series",
                        "series_ticker": "KXCPI",
                        "include_multivariate": False,
                    },
                }
            )
        )
    elif mutation == "capture-genesis":
        journal = operator / "captures" / CAPTURE_ID / "pages.jsonl"
        genesis = json.loads(journal.read_text())
        genesis["payload"]["state"]["capture_id"] = "9" * 32
        journal.write_bytes(json_bytes(genesis) + b"\n")
    elif mutation == "capture-path":
        (operator / "captures" / CAPTURE_ID / "pages.jsonl").unlink()

    with pytest.raises(production.RunnerError, match=message):
        production._load_resume(
            directory,
            CAPTURE_ID,
            historical=True,
            offline_duration_seconds=600,
            renew_offline_window=True,
        )


@pytest.mark.parametrize("link_type", ["symlink", "hardlink"])
def test_offline_renewal_does_not_open_unsafe_operator_lock_for_writing(
    production, tmp_path, monkeypatch, link_type
):
    directory = tmp_path / "production"
    _report_path, report, operator = _renewable_session(directory, production)
    monkeypatch.setattr(production, "_revision", lambda: "e" * 40)
    monkeypatch.setattr(production, "_boot_stamp", lambda: report["boot_stamp"])
    monkeypatch.setattr(
        production,
        "_volume_uuid",
        lambda _path: "ACAD66E7-42DA-426B-B20D-FB87A453396B",
    )
    lock_path = operator / "writer.lock"
    lock_bytes = lock_path.read_bytes()
    lock_path.unlink()
    target = tmp_path / "external-writer-lock"
    target.write_bytes(lock_bytes)
    if link_type == "symlink":
        lock_path.symlink_to(target)
    else:
        os.link(target, lock_path)

    with pytest.raises(production.RunnerError, match="writer lock"):
        production._load_resume(
            directory,
            CAPTURE_ID,
            historical=True,
            offline_duration_seconds=600,
            renew_offline_window=True,
        )
    assert target.read_bytes() == lock_bytes


def test_offline_renewal_preflight_does_not_read_large_capture_snapshot(
    production, tmp_path, monkeypatch
):
    directory = tmp_path / "production"
    report_path, report, operator = _renewable_session(directory, production)
    monkeypatch.setattr(production, "_revision", lambda: "e" * 40)
    monkeypatch.setattr(production, "_boot_stamp", lambda: report["boot_stamp"])
    monkeypatch.setattr(
        production,
        "_volume_uuid",
        lambda _path: "ACAD66E7-42DA-426B-B20D-FB87A453396B",
    )
    state_path = operator / "captures" / CAPTURE_ID / "state.json"
    state_path.write_bytes(b" " * (1024**2 + 1) + b"{}")
    original_open = Path.open

    def reject_snapshot_open(path, *args, **kwargs):
        if path == state_path:
            pytest.fail("offline renewal preflight read the large capture snapshot")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", reject_snapshot_open)

    loaded_path, loaded, _paths = production._load_resume(
        directory,
        CAPTURE_ID,
        historical=True,
        offline_duration_seconds=600,
        renew_offline_window=True,
    )

    assert loaded_path == report_path
    assert loaded["offline_historical_window"] == report["offline_historical_window"]


def test_expired_unstarted_offline_grant_can_be_explicitly_renewed_again(
    production, tmp_path, monkeypatch
):
    directory = tmp_path / "production"
    report_path, report, _operator = _renewable_session(directory, production)
    original_window = dict(report["offline_historical_window"])
    original_deadlines = {
        key: report[key]
        for key in (
            "started_epoch",
            "deadline_epoch",
            "started_monotonic",
            "deadline_monotonic",
            "time_budget_started_epoch",
            "time_budget_started_monotonic",
        )
    }
    original_accounting = {
        key: report.get(key)
        for key in (
            "requests",
            "downloaded_bytes",
            "downloaded_bytes_upper_bound",
            "accounting_complete",
            "accounting_basis",
            "accounting_bound_verified",
            "unfinished_reservation_ids",
        )
    }
    original_limits = json.loads(json.dumps(report["limits"]))
    epoch_now = time.time()
    monotonic_now = time.monotonic()
    clock = {"epoch": epoch_now, "monotonic": monotonic_now}
    monkeypatch.setattr(production.time, "time", lambda: clock["epoch"])
    monkeypatch.setattr(production.time, "monotonic", lambda: clock["monotonic"])
    monkeypatch.setattr(production, "_revision", lambda: "e" * 40)
    monkeypatch.setattr(production, "_boot_stamp", lambda: report["boot_stamp"])
    monkeypatch.setattr(
        production,
        "_volume_uuid",
        lambda _path: "ACAD66E7-42DA-426B-B20D-FB87A453396B",
    )

    first_path, first, _paths = production._load_resume(
        directory,
        CAPTURE_ID,
        historical=True,
        offline_duration_seconds=600,
        renew_offline_window=True,
    )
    production._configure_offline_historical_window(first, CAPTURE_ID, 600, renew=True)
    production._atomic_report(first_path, first)
    first_grant = dict(first["offline_historical_window"])
    persisted = production._strict_json(report_path)
    assert persisted["phase"] == "stopped"
    assert persisted.get("active_process_group") is None

    clock["epoch"] = first_grant["deadline_epoch"] + 1
    clock["monotonic"] = first_grant["deadline_monotonic"] + 1
    second_path, second, _paths = production._load_resume(
        directory,
        CAPTURE_ID,
        historical=True,
        offline_duration_seconds=600,
        renew_offline_window=True,
    )
    production._configure_offline_historical_window(second, CAPTURE_ID, 600, renew=True)
    production._atomic_report(second_path, second)

    assert second_path == report_path
    assert second["offline_historical_window_history"] == [original_window, first_grant]
    assert original_window["version"] == 1
    assert first_grant["version"] == 2
    assert second["offline_historical_window"]["version"] == 2
    assert second["offline_historical_window"]["revision"] == "e" * 40
    assert second["phase"] == "stopped"
    assert {key: second[key] for key in original_deadlines} == original_deadlines
    assert {key: second.get(key) for key in original_accounting} == original_accounting
    assert second["limits"] == original_limits


@pytest.mark.parametrize(
    "tamper", ["not-a-list", "malformed-entry", "wrong-capture", "overlap", "boolean-version"]
)
def test_offline_renewal_rejects_malformed_or_overlapping_window_history(
    production, tmp_path, monkeypatch, tamper
):
    directory = tmp_path / "production"
    report_path, report, _operator = _renewable_session(directory, production)
    monkeypatch.setattr(production, "_revision", lambda: "e" * 40)
    monkeypatch.setattr(production, "_boot_stamp", lambda: report["boot_stamp"])
    monkeypatch.setattr(
        production,
        "_volume_uuid",
        lambda _path: "ACAD66E7-42DA-426B-B20D-FB87A453396B",
    )
    if tamper == "not-a-list":
        report["offline_historical_window_history"] = {}
    elif tamper == "malformed-entry":
        report["offline_historical_window_history"] = [{}]
    elif tamper == "wrong-capture":
        prior = dict(report["offline_historical_window"])
        prior["capture_id"] = "b" * 32
        report["offline_historical_window_history"] = [prior]
    elif tamper == "overlap":
        report["offline_historical_window_history"] = [dict(report["offline_historical_window"])]
    elif tamper == "boolean-version":
        report["offline_historical_window"]["version"] = True
    production._atomic_report(report_path, report)

    with pytest.raises(production.RunnerError):
        production._load_resume(
            directory,
            CAPTURE_ID,
            historical=True,
            offline_duration_seconds=600,
            renew_offline_window=True,
        )


@pytest.mark.parametrize("offline_window", [False, True, "renew", "supersede", "renew-atomic-fail"])
def test_offline_historical_work_accepts_scoped_completion_without_faking_capture_completion(
    production, tmp_path, monkeypatch, offline_window
):
    renewal = offline_window in {"renew", "supersede", "renew-atomic-fail"}
    supersede = offline_window == "supersede"
    atomic_failure = offline_window == "renew-atomic-fail"
    has_offline_window = bool(offline_window)
    directory = tmp_path / "production"
    directory.mkdir()
    (directory / "runs").mkdir()
    operator = directory / "operator"
    store = Store(operator)
    with store.lock():
        from kalshi_events.incremental import root_id

        root_id(store, create=True)
    report_path, report = _historical_report(directory, operator, production)
    report["system"]["filesystem_device"] = directory.stat().st_dev
    original_deadlines = {
        key: report[key]
        for key in (
            "started_epoch",
            "deadline_epoch",
            "started_monotonic",
            "deadline_monotonic",
            "time_budget_started_epoch",
            "time_budget_started_monotonic",
        )
    }
    if has_offline_window:
        past_epoch = time.time() - 50 * 3600
        past_monotonic = time.monotonic() - 50 * 3600
        report["started_epoch"] = past_epoch
        report["time_budget_started_epoch"] = past_epoch
        report["deadline_epoch"] = past_epoch + production.RUN_LIMITS["session_seconds"]
        report["started_monotonic"] = past_monotonic
        report["time_budget_started_monotonic"] = past_monotonic
        report["deadline_monotonic"] = past_monotonic + production.RUN_LIMITS["session_seconds"]
        original_deadlines.update({key: report[key] for key in original_deadlines})
    previous_window = None
    if renewal:
        _write_capture_genesis(operator)
        now_epoch = time.time()
        now_monotonic = time.monotonic()
        previous_window = {
            "version": 1,
            "capture_id": CAPTURE_ID,
            "root_id": report["root_id"],
            "revision": "f" * 40,
            "started_at": production._utc(),
            "started_epoch": now_epoch - 7200,
            "started_monotonic": now_monotonic - 7200,
            "deadline_epoch": now_epoch + 3600 if supersede else now_epoch - 3600,
            "deadline_monotonic": (now_monotonic + 3600 if supersede else now_monotonic - 3600),
        }
        report["offline_historical_window"] = previous_window
        report["system"]["filesystem_volume_uuid"] = "ACAD66E7-42DA-426B-B20D-FB87A453396B"
        report["phase"] = "stopped"
        report["session_extension_history"] = [{"capture_id": CAPTURE_ID}]
    original_extension_history = json.loads(json.dumps(report.get("session_extension_history")))
    original_acquisition = {
        key: report.get(key)
        for key in (
            "requests",
            "downloaded_bytes",
            "downloaded_bytes_upper_bound",
            "accounting_complete",
            "accounting_basis",
            "accounting_bound_verified",
            "unfinished_reservation_ids",
        )
    }
    original_limits = json.loads(json.dumps(report["limits"]))
    production._atomic_report(report_path, report)
    snapshot = _snapshot()
    snapshot["root_id"] = report["root_id"]

    class Awake:
        pid = 424242

        def __init__(self, *_args, **_kwargs):
            pass

        def poll(self):
            return None

    monkeypatch.setattr(
        production, "_package_versions", lambda: production.SUPPORTED_RUNTIME_VERSIONS
    )
    monkeypatch.setattr(production, "_revision", lambda: "e" * 40)
    monkeypatch.setattr(
        production, "_system_info", lambda path: {"filesystem_device": path.stat().st_dev}
    )
    monkeypatch.setattr(production, "_boot_stamp", lambda: "same boot")
    monkeypatch.setattr(
        production,
        "_volume_uuid",
        lambda _path: "ACAD66E7-42DA-426B-B20D-FB87A453396B",
    )
    monkeypatch.setattr(production, "_validate_runtime_versions", lambda _versions: None)
    monkeypatch.setattr(production.subprocess, "Popen", Awake)
    monkeypatch.setattr(production, "_kill_awake_guard", lambda _awake: None)
    monkeypatch.setattr(
        production.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(
            free=1000 * 1024**3, total=1200 * 1024**3, used=200 * 1024**3
        ),
    )
    grant_saved = False
    original_atomic_report = production._atomic_report

    def atomic_report(path, value):
        nonlocal grant_saved
        if (
            renewal
            and value.get("offline_historical_window_history")
            and value.get("phase") == "stopped"
        ):
            if atomic_failure:
                raise OSError("synthetic grant persistence failure")
            grant_saved = True
        original_atomic_report(path, value)

    monkeypatch.setattr(production, "_atomic_report", atomic_report)

    guard_calls = 0

    def resource_guard(*_args, **kwargs):
        nonlocal guard_calls
        guard_calls += 1
        if renewal and guard_calls == 1:
            assert grant_saved is True
            assert kwargs["report"]["phase"] == "stopped"
        return snapshot

    monkeypatch.setattr(production, "_resource_guard", resource_guard)
    monkeypatch.setattr(
        transport,
        "source_url",
        lambda: pytest.fail("offline historical mode inspected the API origin"),
    )
    events = []

    def run_child(command, *, paths, label, deadline, report, **_kwargs):
        events.append((label, command))
        if has_offline_window:
            active = production._ACTIVE_OFFLINE_HISTORICAL_WINDOW
            assert active is not None
            assert deadline <= active["deadline_monotonic"]
            wall_deadline = production._child_wall_deadline_epoch(
                report, deadline, workflow_deadline=label == "refresh"
            )
            assert wall_deadline <= active["deadline_epoch"]
        stdout = paths["logs"] / f"{label}.stdout.json"
        stderr = paths["logs"] / f"{label}.stderr.log"
        stderr.touch()
        if label == "refresh":
            assert command[-4] == "historical"
            assert command[-3] == CAPTURE_ID
            if has_offline_window:
                limits = json.loads(command[-2])
                assert 0 < limits["max-duration-seconds"] <= 3600
            value = {
                "complete": True,
                "capture_id": CAPTURE_ID,
                "capture_complete": False,
                "source_capture_complete": False,
                "scope_complete": True,
                "coverage_scope": "historical",
                "publication_state": "verified",
                "accounting_complete": False,
                "requests": None,
                "downloaded_bytes": None,
                "resumable": None,
                "output": str(tmp_path / "historical-release"),
                "source_release_id": RELEASE_ID,
                "error": None,
            }
            code = 0
        else:
            assert label == "activation"
            assert command[-3] == f"historical:{RELEASE_ID}"
            value = {
                "state": "verified",
                "capture_id": CAPTURE_ID,
                "coverage_scope": "historical",
                "source_capture_complete": False,
                "scope_complete": True,
                "catalogue_coverage": {"scope": "historical"},
            }
            code = 0
        stdout.write_text(json.dumps(value))
        return code, None, True, stdout, stderr

    monkeypatch.setattr(production, "_run_external_child", run_child)

    def verify_release(*args, **kwargs):
        if has_offline_window:
            deadline = kwargs.get("deadline", args[-1])
            assert deadline <= production._ACTIVE_OFFLINE_HISTORICAL_WINDOW["deadline_monotonic"]
        return {
            "source_release_id": RELEASE_ID,
            "coverage_scope": "historical",
            "source_capture_complete": False,
        }

    monkeypatch.setattr(production, "_release_verify", verify_release)

    if atomic_failure:
        with pytest.raises(OSError, match="synthetic grant persistence failure"):
            production.run_production(
                directory,
                capture_id=CAPTURE_ID,
                historical=True,
                offline_duration_seconds=3600,
                renew_offline_window=True,
            )
        saved = production._strict_json(report_path)
        assert saved["offline_historical_window"] == previous_window
        assert "offline_historical_window_history" not in saved
        assert events == []
        return

    result, code = production.run_production(
        directory,
        capture_id=CAPTURE_ID,
        historical=True,
        offline_duration_seconds=3600 if offline_window else None,
        renew_offline_window=renewal and not supersede,
        supersede_offline_window=supersede,
    )

    assert code == 0, result.get("error")
    assert [label for label, _command in events] == ["refresh", "activation"]
    assert result["capture_complete"] is False
    assert result["source_capture_complete"] is False
    assert result["scope_complete"] is True
    assert result["coverage_scope"] == "historical"
    assert result["accounting_complete"] is False
    assert result["requests"] == snapshot["attempts"]
    assert result["downloaded_bytes"] is None
    assert result["downloaded_bytes_upper_bound"] == snapshot["downloaded_or_reserved_bytes"]
    assert result["network_activity_this_invocation"] == {
        "requests": 0,
        "downloaded_bytes": 0,
        "accounting_complete": True,
    }
    assert {key: result[key] for key in original_deadlines} == original_deadlines
    assert result.get("session_extension_history") == original_extension_history
    assert {key: result.get(key) for key in original_acquisition} == original_acquisition
    assert result["limits"] == original_limits
    if has_offline_window:
        window = result["offline_historical_window"]
        assert window["capture_id"] == CAPTURE_ID
        assert window["root_id"] == result["root_id"]
        assert window["version"] == 2
        assert window["revision"] == "e" * 40
        assert 3599 < window["deadline_epoch"] - window["started_epoch"] <= 3600
        assert [label for label, _command in events] == ["refresh", "activation"]

        if renewal:
            assert result["offline_historical_window_history"] == [previous_window]
            assert previous_window["version"] == 1
            if supersede:
                assert window["started_epoch"] < previous_window["deadline_epoch"]
                assert window["started_monotonic"] < previous_window["deadline_monotonic"]
                assert len(result["offline_historical_window_supersessions"]) == 1
            else:
                assert window["started_epoch"] >= previous_window["deadline_epoch"]
                assert window["started_monotonic"] >= previous_window["deadline_monotonic"]
            assert result.get("session_extension_history") == original_extension_history
            return

        # Repeating the flag reuses the established deadline, even if it asks
        # for a longer duration. It cannot grant a second offline window.
        saved = production._strict_json(report_path)
        saved["production_state"] = "not_attempted"
        saved["phase"] = "stopped"
        production._atomic_report(report_path, saved)
        loaded_path, loaded, _paths = production._load_resume(
            directory,
            CAPTURE_ID,
            historical=True,
            offline_duration_seconds=7200,
        )
        reused = production._configure_offline_historical_window(loaded, CAPTURE_ID, 7200)
        assert loaded_path == report_path
        assert reused["deadline_epoch"] == window["deadline_epoch"]
        assert reused["deadline_monotonic"] == window["deadline_monotonic"]

        saved["offline_historical_window"]["started_epoch"] -= 7200
        saved["offline_historical_window"]["deadline_epoch"] -= 7200
        saved["offline_historical_window"]["started_monotonic"] -= 7200
        saved["offline_historical_window"]["deadline_monotonic"] -= 7200
        production._atomic_report(report_path, saved)
        with pytest.raises(production.RunnerError, match="offline historical window expired"):
            production._load_resume(
                directory,
                CAPTURE_ID,
                historical=True,
                offline_duration_seconds=3600,
            )


def test_historical_refresh_worker_does_not_read_api_or_call_capture(
    production, tmp_path, monkeypatch, capsys
):
    operator = tmp_path / "operator"
    store = Store(operator)
    with store.lock():
        from kalshi_events.incremental import root_id

        logical_root = root_id(store, create=True)
    production._atomic_report(
        tmp_path / production.OWNER_MARKER,
        {"version": 1, "root_id": logical_root},
    )
    report = tmp_path / "run.json"
    report.write_text(json.dumps({"capture_id": CAPTURE_ID}))
    metrics = tmp_path / "metrics.json"
    notification = tmp_path / "capture-notification.json"
    limits = {
        "max-rate": 5,
        "max-requests": 100,
        "max-download-bytes": 1024**3,
        "max-response-bytes": 384 * 1024**2,
        "max-decoded-response-bytes": 768 * 1024**2,
        "max-duration-seconds": 3600,
        "max-root-bytes": 350 * 1024**3,
        "max-temp-bytes": 64 * 1024**3,
        "max-database-bytes": 64 * 1024**3,
        "compression": "gzip",
        "rate-policy": "decrease-on-429",
    }
    called = {}

    def historical_run(_store, **kwargs):
        called.update(kwargs)
        return (
            {
                "complete": True,
                "capture_id": CAPTURE_ID,
                "capture_complete": False,
                "source_capture_complete": False,
                "scope_complete": True,
                "coverage_scope": "historical",
                "publication_state": "verified",
                "accounting_complete": False,
            },
            0,
        )

    from kalshi_events import refresh

    monkeypatch.setattr(refresh, "run", historical_run)
    monkeypatch.setattr(transport, "source_url", lambda: pytest.fail("API origin read"))
    monkeypatch.setattr(capture, "capture", lambda *_args, **_kwargs: pytest.fail("capture called"))

    code = production._refresh_worker_main(
        operator,
        report,
        metrics,
        notification,
        "historical",
        CAPTURE_ID,
        json.dumps(limits),
    )

    assert code == 0
    assert called["historical"] is True
    assert called["capture_id"] == CAPTURE_ID
    assert capsys.readouterr().out
    assert not notification.exists()


def test_historical_activation_reads_coverage_and_uses_build_three(
    production_activation, historical_candidate
):
    directory, root, warehouse_path, manifest = historical_candidate

    result = production_activation.activate_current(
        root,
        directory,
        expected_release_id=manifest["source_release_id"],
        catalogue_version=3,
    )

    assert result["coverage_scope"] == "historical"
    assert result["source_capture_complete"] is True
    assert result["scope_complete"] is True
    assert result["catalogue_coverage"]["scope"] == "historical"
    assert result["catalogue_coverage"]["source_capture_id"] == manifest["capture_id"]
    assert (directory / "production.duckdb").stat().st_ino == warehouse_path.stat().st_ino


def test_historical_activation_rejects_v2_selector_wal_and_changed_build_hash(
    production_activation, historical_candidate
):
    directory, root, warehouse_path, _manifest = historical_candidate
    with pytest.raises((CatalogueError, OSError)):
        production_activation.activate_current(root, directory, catalogue_version=2)
    assert not (directory / "production.duckdb").exists()

    wal = warehouse_path.with_name(warehouse_path.name + ".wal")
    wal.write_bytes(b"uncheckpointed")
    with pytest.raises(CatalogueError):
        production_activation.activate_current(root, directory, catalogue_version=3)
    wal.unlink()

    with warehouse_path.open("ab") as stream:
        stream.write(b"changed")
    with pytest.raises(CatalogueError):
        production_activation.activate_current(root, directory, catalogue_version=3)
    assert not (directory / "production.duckdb").exists()


def test_historical_release_cannot_be_a_v2_incremental_baseline(historical_candidate):
    _directory, root, _warehouse_path, _manifest = historical_candidate
    store = Store(root)
    with store.lock():
        from kalshi_events.incremental import root_id, verify_pin

        identity = root_id(store)
        with pytest.raises(CatalogueError, match="catalogue version 1 or 2"):
            verify_pin(
                store,
                {
                    "baseline": {
                        "catalogue_version": 3,
                        "root_id": identity,
                    }
                },
            )
