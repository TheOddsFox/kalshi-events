"""Private, write-once controls for bounded offline historical work."""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import stat
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

GIB = 1024**3
MAX_GRANT_SECONDS = 24 * 60 * 60
MIN_RESERVE_SECONDS = 2 * 60 * 60
FRESH_APPROVAL_SECONDS = 5 * 60
_HEX_32 = re.compile(r"[0-9a-f]{32}\Z")
_HEX_64 = re.compile(r"[0-9a-f]{64}\Z")
_STAGE = re.compile(r"build-historical-[0-9a-f]{32}\Z")

PHASES = (
    "source",
    "readiness",
    "adoption",
    "models",
    "certified-build",
    "export",
    "verified-publication",
    "independent-verification",
    "activation",
)
PHASE_OUTPUTS = {
    "source": {
        "startup_maintenance_sha256",
        "source_snapshot_sha256",
        "capture_state_sha256",
        "receipt_evidence_sha256",
        "source_sha256",
        "source_wal_sha256",
        "receipt_count",
        "population_report_sha256",
        "receipt_index_sha256",
        "exact_observations",
        "observations_by_endpoint",
        "scope_sha256",
        "code_sha256",
    },
    "readiness": {
        "readiness_sha256",
        "verification_sha256",
        "capture_state_sha256",
        "receipt_evidence_sha256",
        "source_sha256",
        "source_wal_sha256",
        "candidate_sha256",
        "observation_count",
        "membership_count",
        "eligibility_count",
        "scope_sha256",
        "code_sha256",
    },
    "adoption": {
        "adoption_sha256",
        "verification_receipt_sha256",
        "candidate_sha256",
        "source_retired",
        "retirement_receipt_sha256",
    },
    "models": {
        "build_receipt_sha256",
        "warehouse_sha256",
        "dbt_artifacts_sha256",
        "source_retirement_sha256",
    },
    "certified-build": {"build_sha256", "build_manifest_sha256", "certified_receipt_sha256"},
    "export": {"export_policy_sha256", "parquet_inventory_sha256", "release_metadata_sha256"},
    "verified-publication": {
        "release_id",
        "release_manifest_sha256",
        "publication_pointer_sha256",
    },
    "independent-verification": {
        "verification_receipt_sha256",
        "release_id",
        "release_manifest_sha256",
    },
    "activation": {
        "activation_audit_sha256",
        "production_database_sha256",
        "release_id",
        "activation_readback_sha256",
        "relation_rows",
    },
}
_BINDING_FIELDS = (
    "capture_id",
    "session_id",
    "root_id",
    "stage_name",
    "failed_attempts_sha256",
    "prior_session_sha256",
    "state_sha256",
    "source_sha256",
    "runtime_sha256",
)
_HASH_OUTPUT_FIELDS = {
    key
    for fields in PHASE_OUTPUTS.values()
    for key in fields
    if key.endswith("_sha256") or key == "release_id"
}


class ControlError(RuntimeError):
    """A private grant, phase seal, or control artifact is invalid."""


def canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode(
            "utf-8"
        )
    except (TypeError, ValueError) as exc:
        raise ControlError("control artifact is not strict JSON") from exc


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_bytes(canonical_bytes(value))


def _digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _HEX_64.fullmatch(value):
        raise ControlError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _identity(value: Any, *, allow_null_pins: bool = False) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != set(_BINDING_FIELDS):
        raise ControlError("historical control bindings are incomplete")
    for key in ("capture_id", "session_id", "root_id"):
        if not isinstance(value.get(key), str) or not _HEX_32.fullmatch(value[key]):
            raise ControlError(f"historical control {key} is invalid")
    if not isinstance(value.get("stage_name"), str) or not _STAGE.fullmatch(value["stage_name"]):
        raise ControlError("historical control stage name is invalid")
    for key in _BINDING_FIELDS[4:]:
        item = value.get(key)
        if allow_null_pins and item is None:
            continue
        _digest(item, key)
    return value


def _validate_authorized_census_binding(value: Any) -> dict[str, Any]:
    fields = {
        "population_report_path",
        "population_report_sha256",
        "receipt_index_path",
        "receipt_index_sha256",
        "capture_id",
        "root_id",
        "capture_state_sha256",
        "candidate_pages",
        "observations_by_endpoint",
        "nested_event_markets",
        "exact_observations",
    }
    if not isinstance(value, dict) or set(value) != fields:
        raise ControlError("authorized historical census binding is incomplete")
    for key in ("population_report_sha256", "receipt_index_sha256", "capture_state_sha256"):
        _digest(value.get(key), key)
    for key in ("population_report_path", "receipt_index_path"):
        item = value.get(key)
        if not isinstance(item, str) or not Path(item).is_absolute():
            raise ControlError("authorized census artifact path must be absolute")
    for key in ("capture_id", "root_id"):
        item = value.get(key)
        if not isinstance(item, str) or not _HEX_32.fullmatch(item):
            raise ControlError(f"authorized census {key} is invalid")
    population = value.get("observations_by_endpoint")
    expected_endpoints = {"/series", "/events", "/historical/markets"}
    if (
        not isinstance(population, dict)
        or set(population) != expected_endpoints
        or any(type(count) is not int or count < 0 for count in population.values())
        or type(value.get("candidate_pages")) is not int
        or value["candidate_pages"] <= 0
        or type(value.get("nested_event_markets")) is not int
        or value["nested_event_markets"] < 0
        or type(value.get("exact_observations")) is not int
        or value["exact_observations"] != sum(population.values())
    ):
        raise ControlError("authorized historical census totals are invalid")
    return value


def validate_inert_control(control: Any, expected_identity: dict[str, Any]) -> dict[str, Any]:
    """Validate the identity-specific inert template without treating it as authorization."""
    required = {
        "version",
        "state",
        "identity",
        "prior_attempts",
        "fresh_pins",
        "phase_seals",
        "authorized_census",
    }
    if not isinstance(control, dict) or set(control) != required or control.get("version") != 1:
        raise ControlError("private historical control template has an invalid schema")
    if control.get("state") != "inert":
        raise ControlError("private historical controls require the separate frozen-control path")
    identity = _identity(control.get("identity"), allow_null_pins=True)
    expected = _identity(expected_identity, allow_null_pins=True)
    if identity != expected:
        raise ControlError("private historical controls do not match this session")
    attempts = control.get("prior_attempts")
    attempt_fields = {
        "attempt_outcome_sha256",
        "failed_launch_sha256",
        "audit_after_sha256",
        "failed_session_sha256",
        "bundle_sha256",
    }
    if not isinstance(attempts, dict) or set(attempts) != attempt_fields:
        raise ControlError("saved failed-attempt bindings are incomplete")
    for key, value in attempts.items():
        _digest(value, key)
    if (
        attempts["bundle_sha256"] != identity["failed_attempts_sha256"]
        or attempts["failed_session_sha256"] != identity["prior_session_sha256"]
    ):
        raise ControlError("saved failed-attempt bundle differs from its identity binding")
    pins = control.get("fresh_pins")
    if not isinstance(pins, dict) or set(pins) != {"source", "state", "runtime", "approval"}:
        raise ControlError("fresh historical state pins are invalid")
    if any(pins[key] is not None for key in pins):
        raise ControlError(
            "inert controls must not contain fresh source, state, runtime, or approval pins"
        )
    if control.get("phase_seals") != []:
        raise ControlError("inert controls must not contain execution phase seals")
    _validate_authorized_census_binding(control.get("authorized_census"))
    return identity


def validate_frozen_control(
    freeze: Any,
    *,
    control_sha256: str,
    expected_identity: dict[str, Any],
) -> dict[str, Any]:
    required = {
        "version",
        "control_sha256",
        "identity",
        "state_sha256",
        "source_sha256",
        "runtime_sha256",
        "source_snapshot_sha256",
        "receipt_evidence_sha256",
        "scope_sha256",
        "code_sha256",
        "runtime_manifest_sha256",
        "operational_source_sha256",
        "root_volume_uuid",
        "root_device",
        "frozen_at_epoch",
    }
    if not isinstance(freeze, dict) or set(freeze) != required or freeze.get("version") != 1:
        raise ControlError("frozen historical control artifact is missing or invalid")
    if freeze.get("control_sha256") != control_sha256:
        raise ControlError("frozen control artifact does not bind the inert control template")
    identity = freeze.get("identity")
    if not isinstance(identity, dict) or set(identity) != set(_BINDING_FIELDS):
        raise ControlError("frozen historical control identity is incomplete")
    expected = _identity(expected_identity, allow_null_pins=True)
    bound = _identity(identity)
    for key in _BINDING_FIELDS[:6]:
        if bound[key] != expected[key]:
            raise ControlError("frozen historical control identity changed")
    if bound["state_sha256"] != freeze.get("state_sha256"):
        raise ControlError("frozen state digest differs from its identity binding")
    if bound["source_sha256"] != freeze.get("source_sha256"):
        raise ControlError("frozen source digest differs from its identity binding")
    if bound["runtime_sha256"] != freeze.get("runtime_sha256"):
        raise ControlError("frozen runtime digest differs from its identity binding")
    for key in (
        "state_sha256",
        "source_sha256",
        "runtime_sha256",
        "source_snapshot_sha256",
        "receipt_evidence_sha256",
        "scope_sha256",
        "code_sha256",
        "runtime_manifest_sha256",
        "operational_source_sha256",
    ):
        _digest(freeze.get(key), key)
    runtime_sha256 = sha256_json(
        {
            "runtime_manifest_sha256": freeze["runtime_manifest_sha256"],
            "operational_source_sha256": freeze["operational_source_sha256"],
        }
    )
    if freeze["runtime_sha256"] != runtime_sha256:
        raise ControlError("frozen runtime digest does not bind the runtime and source manifests")
    volume_uuid = freeze.get("root_volume_uuid")
    if (
        not isinstance(volume_uuid, str)
        or re.fullmatch(
            r"[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}", volume_uuid
        )
        is None
    ):
        raise ControlError("frozen production volume UUID is invalid")
    if type(freeze.get("root_device")) is not int or freeze["root_device"] <= 0:
        raise ControlError("frozen production filesystem device is invalid")
    created = freeze.get("frozen_at_epoch")
    if type(created) not in {int, float} or not math.isfinite(created) or created <= 0:
        raise ControlError("frozen control timestamp is invalid")
    return bound


def next_phase(current: str | None) -> str:
    if current is None:
        return PHASES[0]
    try:
        index = PHASES.index(current)
    except ValueError as exc:
        raise ControlError("historical phase is unknown") from exc
    if index + 1 >= len(PHASES):
        raise ControlError("historical phase chain is already complete")
    return PHASES[index + 1]


def _validate_outputs(phase: str, outputs: Any) -> None:
    required = PHASE_OUTPUTS.get(phase)
    if required is None or not isinstance(outputs, dict) or set(outputs) != required:
        raise ControlError(f"{phase} phase outputs are incomplete or unexpected")
    for key, value in outputs.items():
        if key == "source_wal_sha256" and value is None:
            continue
        if key in {"retirement_receipt_sha256", "source_retirement_sha256"} and value is None:
            continue
        if key == "observations_by_endpoint":
            if (
                not isinstance(value, dict)
                or set(value) != {"/series", "/events", "/historical/markets"}
                or any(type(count) is not int or count < 0 for count in value.values())
                or sum(value.values()) != outputs.get("exact_observations")
            ):
                raise ControlError("authorized source endpoint census is invalid")
        elif key == "relation_rows":
            if (
                not isinstance(value, dict)
                or set(value)
                != {
                    "series",
                    "events",
                    "markets",
                    "relationships",
                    "combo_legs",
                    "observation_history",
                }
                or any(type(count) is not int or count < 0 for count in value.values())
            ):
                raise ControlError("activation relation row counts are incomplete or invalid")
        elif key in _HASH_OUTPUT_FIELDS:
            _digest(value, key)
        elif key == "source_retired":
            if type(value) is not bool:
                raise ControlError("source_retired must be a boolean")
        elif type(value) is not int or value < 0:
            raise ControlError(f"{key} must be a nonnegative integer")


def validate_phase_seal(
    seal: Any,
    *,
    expected_phase: str,
    expected_bindings: dict[str, Any],
    previous_sha256: str | None,
    prior_activation_audits: set[str] | None = None,
    grant_launch: dict[str, Any] | None = None,
) -> str:
    required = {
        "version",
        "phase",
        "bindings",
        "previous_sha256",
        "outputs",
        "completed_at_epoch",
        "grant_id",
    }
    if not isinstance(seal, dict) or set(seal) != required or seal.get("version") != 1:
        raise ControlError("historical phase seal has an invalid schema")
    if seal.get("phase") != expected_phase or expected_phase not in PHASE_OUTPUTS:
        raise ControlError("historical phase seal is out of order")
    if seal.get("bindings") != _identity(expected_bindings):
        raise ControlError("historical phase seal bindings changed")
    if seal.get("previous_sha256") != previous_sha256:
        raise ControlError("historical phase seal chain is broken")
    if previous_sha256 is not None:
        _digest(previous_sha256, "previous_sha256")
    if not isinstance(seal.get("grant_id"), str) or not _HEX_32.fullmatch(seal["grant_id"]):
        raise ControlError("historical phase seal grant identity is invalid")
    completed = seal.get("completed_at_epoch")
    if type(completed) not in {int, float} or not math.isfinite(completed) or completed <= 0:
        raise ControlError("historical phase seal completion time is invalid")
    if grant_launch is not None:
        validate_phase_completion_time(expected_phase, float(completed), grant_launch)
    _validate_outputs(expected_phase, seal.get("outputs"))
    if expected_phase == "activation":
        audit = seal["outputs"]["activation_audit_sha256"]
        if audit in (prior_activation_audits or set()):
            raise ControlError("activation audit artifact was already used")
    return sha256_json(seal)


def validate_phase_completion_time(
    phase: str, completed_at_epoch: float, launch: dict[str, Any]
) -> None:
    started = _finite_nonnegative(launch.get("started_epoch"), "launch epoch")
    deadline = _finite_nonnegative(launch.get("deadline_epoch"), "launch deadline")
    workflow_deadline = _finite_nonnegative(
        launch.get("workflow_deadline_epoch"), "workflow deadline"
    )
    final_phases = {"independent-verification", "activation"}
    cutoff = deadline if phase in final_phases else workflow_deadline
    if completed_at_epoch < started or completed_at_epoch > cutoff:
        raise ControlError("historical phase seal completed outside its approved launch window")


def validate_live_phase_deadline(
    phase: str,
    launch: dict[str, Any],
    *,
    now_epoch: float | None = None,
    now_monotonic: float | None = None,
) -> None:
    if phase not in PHASES:
        raise ControlError("historical phase seal phase is invalid")
    now_epoch = time.time() if now_epoch is None else now_epoch
    now_monotonic = time.monotonic() if now_monotonic is None else now_monotonic
    started_epoch = _finite_nonnegative(launch.get("started_epoch"), "launch epoch")
    started_monotonic = _finite_nonnegative(
        launch.get("started_monotonic"), "launch monotonic time"
    )
    final_phase = phase in {"independent-verification", "activation"}
    epoch_cutoff = _finite_nonnegative(
        launch.get("deadline_epoch" if final_phase else "workflow_deadline_epoch"),
        "phase epoch deadline",
    )
    monotonic_cutoff = _finite_nonnegative(
        launch.get("deadline_monotonic" if final_phase else "workflow_deadline_monotonic"),
        "phase monotonic deadline",
    )
    if (
        now_epoch < started_epoch
        or now_monotonic < started_monotonic
        or now_epoch > epoch_cutoff
        or now_monotonic > monotonic_cutoff
    ):
        raise ControlError("historical phase seal live grant deadline expired")


def validate_adoption_retirement(approval: Any, source_retired: Any) -> None:
    if (
        not isinstance(approval, dict)
        or type(source_retired) is not bool
        or type(approval.get("retire_source")) is not bool
        or source_retired is not approval["retire_source"]
    ):
        raise ControlError("historical adoption retirement differs from its linked grant")


def validate_phase_chain(
    seals: Any,
    *,
    expected_bindings: dict[str, Any],
    prior_activation_audits: set[str] | None = None,
    grant_launches: dict[str, dict[str, Any]] | None = None,
) -> tuple[str | None, str | None]:
    if not isinstance(seals, list) or len(seals) > len(PHASES):
        raise ControlError("historical phase seal inventory is invalid")
    previous = None
    used_audits = set(prior_activation_audits or ())
    current = None
    for index, seal in enumerate(seals):
        phase = PHASES[index]
        digest = validate_phase_seal(
            seal,
            expected_phase=phase,
            expected_bindings=expected_bindings,
            previous_sha256=previous,
            prior_activation_audits=used_audits,
            grant_launch=(grant_launches or {}).get(seal.get("grant_id"))
            if isinstance(seal, dict)
            else None,
        )
        if phase == "activation":
            used_audits.add(seal["outputs"]["activation_audit_sha256"])
        current, previous = phase, digest
    return current, previous


def _phase_grant_launch(
    root: Path, grant_id: str, bindings: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    approval, latch, launch = read_grant_bundle(root, grant_id)
    started_epoch = _finite_nonnegative(launch.get("started_epoch"), "launch epoch")
    started_monotonic = _finite_nonnegative(
        launch.get("started_monotonic"), "launch monotonic time"
    )
    validated = validate_grant_bundle(
        approval,
        latch,
        launch,
        expected_bindings=bindings,
        now_epoch=started_epoch + 0.01,
        now_monotonic=started_monotonic + 0.01,
        boot_stamp=launch.get("boot_stamp"),
    )
    session_binding = read_session_binding(root, grant_id)
    validate_session_binding(
        session_binding,
        approval=approval,
        expected_identity=bindings,
    )
    if sha256_json(session_binding) != launch.get("session_binding_sha256"):
        raise ControlError("historical phase grant session binding changed")
    return approval, launch | validated


def expected_resource_admission(
    *, dbt_memory_limit: str = "4GB", dbt_threads: int = 4
) -> dict[str, Any]:
    if dbt_memory_limit != "4GB" or dbt_threads != 4:
        raise ControlError("historical DBT memory and thread limits changed")
    return {
        "profile": "expanded",
        "attempts": 850_000,
        "downloaded_bytes": 256 * GIB,
        "response_bytes": 384 * 1024**2,
        "decoded_response_bytes": 768 * 1024**2,
        "root_bytes": 350 * GIB,
        "aggregate_bytes": 400 * GIB,
        "minimum_free_bytes": 150 * GIB,
        "database_bytes": 128 * GIB,
        "temporary_bytes": 192 * GIB,
        "storage_database_admission_bytes": 96 * GIB,
        "storage_temporary_admission_bytes": 144 * GIB,
        "storage_database_stop_bytes": 112 * GIB,
        "storage_temporary_stop_bytes": 168 * GIB,
        "storage_database_stop_inclusive": True,
        "storage_temporary_stop_inclusive": True,
        "storage_sample_seconds": 1,
        "storage_inventory_entries": 65_536,
        "storage_release_directories": 4096,
        "peak_rss_bytes": 24 * GIB,
        "peak_rss_seconds": 60,
        "memory_pressure_warn_seconds": 60,
        "memory_pressure_max_gap_seconds": 22.5,
        "sample_seconds": 15,
        "terminate_seconds": 5,
        "rate": 5,
        "compression": "gzip",
        "rate_policy": "decrease-on-429",
        "dbt_memory_limit": dbt_memory_limit,
        "dbt_threads": dbt_threads,
    }


def _finite_nonnegative(value: Any, label: str) -> float:
    if type(value) not in {int, float} or not math.isfinite(value) or value < 0:
        raise ControlError(f"{label} must be finite and nonnegative")
    return float(value)


def validate_grant_bundle(
    approval: Any,
    latch: Any,
    launch: Any,
    *,
    expected_bindings: dict[str, Any],
    now_epoch: float,
    now_monotonic: float,
    boot_stamp: str,
    dbt_memory_limit: str = "4GB",
    dbt_threads: int = 4,
) -> dict[str, float | str]:
    approval_fields = {
        "version",
        "grant_id",
        "phase",
        "phases",
        "bindings",
        "approved_at_epoch",
        "duration_seconds",
        "forecast_seconds",
        "measured_final_verification_seconds",
        "measurements",
        "resource_admission",
        "freeze",
        "retire_source",
        "retirement_policy",
        "activation_audit_sha256",
    }
    latch_fields = {"version", "grant_id", "approval_sha256", "nonce", "created_at_epoch"}
    launch_fields = {
        "version",
        "grant_id",
        "approval_sha256",
        "latch_sha256",
        "launch_id",
        "session_binding_sha256",
        "boot_stamp",
        "started_epoch",
        "started_monotonic",
        "duration_seconds",
        "deadline_epoch",
        "deadline_monotonic",
        "reserve_seconds",
        "workflow_deadline_epoch",
        "workflow_deadline_monotonic",
    }
    if (
        not isinstance(approval, dict)
        or set(approval) != approval_fields
        or approval.get("version") != 1
    ):
        raise ControlError("historical grant approval has an invalid schema")
    if not isinstance(latch, dict) or set(latch) != latch_fields or latch.get("version") != 1:
        raise ControlError("historical grant latch has an invalid schema")
    if not isinstance(launch, dict) or set(launch) != launch_fields or launch.get("version") != 1:
        raise ControlError("historical grant launch has an invalid schema")
    grant_id = approval.get("grant_id")
    if not isinstance(grant_id, str) or not _HEX_32.fullmatch(grant_id):
        raise ControlError("historical grant ID is invalid")
    phase = approval.get("phase")
    if phase not in PHASES:
        raise ControlError("historical grant phase is invalid")
    phases = approval.get("phases")
    if (
        not isinstance(phases, list)
        or not phases
        or len(phases) > len(PHASES)
        or any(item not in PHASES for item in phases)
        or phase != phases[0]
        or phases != list(PHASES[PHASES.index(phase) : PHASES.index(phase) + len(phases)])
    ):
        raise ControlError("historical grant phase range must be a contiguous ordered sequence")
    if approval.get("bindings") != _identity(expected_bindings):
        raise ControlError("historical grant is bound to a different capture or failed attempt")
    if latch.get("grant_id") != grant_id or launch.get("grant_id") != grant_id:
        raise ControlError("historical grant approval, latch, and launch do not match")
    approval_hash = sha256_json(approval)
    latch_hash = sha256_json(latch)
    if (
        latch.get("approval_sha256") != approval_hash
        or launch.get("approval_sha256") != approval_hash
    ):
        raise ControlError("historical grant approval digest changed")
    if launch.get("latch_sha256") != latch_hash:
        raise ControlError("historical grant latch digest changed")
    if not isinstance(latch.get("nonce"), str) or not _HEX_32.fullmatch(latch["nonce"]):
        raise ControlError("historical grant latch nonce is invalid")
    if not isinstance(boot_stamp, str) or not boot_stamp or launch.get("boot_stamp") != boot_stamp:
        raise ControlError("historical grant launch belongs to another boot")
    _digest(launch.get("session_binding_sha256"), "session binding")
    now_epoch = _finite_nonnegative(now_epoch, "current epoch")
    now_monotonic = _finite_nonnegative(now_monotonic, "current monotonic time")
    approved = _finite_nonnegative(approval.get("approved_at_epoch"), "approval time")
    latched = _finite_nonnegative(latch.get("created_at_epoch"), "latch time")
    duration = _finite_nonnegative(approval.get("duration_seconds"), "grant duration")
    forecast = _finite_nonnegative(approval.get("forecast_seconds"), "phase forecast")
    final_verification = _finite_nonnegative(
        approval.get("measured_final_verification_seconds"), "final-verification estimate"
    )
    if duration <= 0 or duration > MAX_GRANT_SECONDS or forecast <= 0:
        raise ControlError("historical grant must have a finite positive duration and forecast")
    if approved > now_epoch or latched < approved or latched > now_epoch:
        raise ControlError("historical approval or latch time is invalid")
    freeze = approval.get("freeze")
    if freeze != {"engineering": True, "runtime": True, "controls": True}:
        raise ControlError("readiness approval requires frozen engineering, runtime, and controls")
    retire_source = approval.get("retire_source")
    retirement_policy = approval.get("retirement_policy")
    if (
        type(retire_source) is not bool
        or (retire_source and retirement_policy != "coexistence-capacity-relief-approved-v1")
        or (not retire_source and retirement_policy is not None)
    ):
        raise ControlError("source retirement lacks its explicit approved policy")
    if retire_source and not {"adoption", "models"}.intersection(phases):
        raise ControlError("source retirement is permitted only by adoption or models authority")
    activation_audit = approval.get("activation_audit_sha256")
    if "activation" in phases:
        _digest(activation_audit, "activation audit")
    elif activation_audit is not None:
        raise ControlError("activation audit binding is only valid for activation grants")
    admission = expected_resource_admission(
        dbt_memory_limit=dbt_memory_limit, dbt_threads=dbt_threads
    )
    if approval.get("resource_admission") != admission:
        raise ControlError("historical grant resource admission changed")
    measurements = approval.get("measurements")
    measurement_fields = {
        "report_sha256",
        "session_report_sha256",
        "measured_at_epoch",
        "forecast_seconds",
        "final_verification_seconds",
        "projected_database_bytes",
        "projected_temporary_bytes",
        "projected_root_bytes",
        "projected_aggregate_bytes",
        "projected_free_bytes",
        "inventory_entries",
        "projected_peak_rss_bytes",
    }
    if not isinstance(measurements, dict) or set(measurements) != measurement_fields:
        raise ControlError("measured forecast and resource inventory are incomplete")
    _digest(measurements.get("report_sha256"), "measurement report")
    _digest(measurements.get("session_report_sha256"), "session report")
    measured_at = _finite_nonnegative(measurements.get("measured_at_epoch"), "measurement time")
    if measured_at <= 0 or measured_at > now_epoch:
        raise ControlError("measured resource report timestamp is invalid")
    measured_forecast = _finite_nonnegative(
        measurements.get("forecast_seconds"), "measured forecast"
    )
    measured_verification = _finite_nonnegative(
        measurements.get("final_verification_seconds"), "measured final verification"
    )
    if (
        measured_forecast <= 0
        or measured_forecast != forecast
        or measured_verification != final_verification
    ):
        raise ControlError("approval forecast differs from the measured report")
    projected = {
        key: measurements.get(key)
        for key in (
            "projected_database_bytes",
            "projected_temporary_bytes",
            "projected_root_bytes",
            "projected_aggregate_bytes",
            "projected_free_bytes",
            "inventory_entries",
            "projected_peak_rss_bytes",
        )
    }
    if any(type(value) is not int or value < 0 for value in projected.values()):
        raise ControlError("measured resource forecast contains an invalid count")
    # The 192 GiB ceiling is a hard cleanup ceiling. Positive work is always
    # admitted only through the ordinary 144 GiB temporary-storage projection.
    # A source maintenance phase may begin with a larger recorded baseline, but
    # that is handled by the runner's narrowly scoped maintenance check.
    temporary_admission = admission["storage_temporary_admission_bytes"]
    if (
        projected["projected_database_bytes"] > admission["storage_database_admission_bytes"]
        or projected["projected_temporary_bytes"] > temporary_admission
        or projected["projected_root_bytes"] > admission["root_bytes"]
        or projected["projected_aggregate_bytes"] > admission["aggregate_bytes"]
        or projected["projected_free_bytes"] < admission["minimum_free_bytes"]
        or projected["inventory_entries"] > admission["storage_inventory_entries"]
        or projected["projected_peak_rss_bytes"] > admission["peak_rss_bytes"]
    ):
        raise ControlError("measured resource forecast exceeds fixed admission ceilings")
    reserve = max(MIN_RESERVE_SECONDS, final_verification)
    if not math.isfinite(reserve) or duration <= reserve or forecast * 1.3 > duration - reserve:
        raise ControlError("forecast plus 30% and final-verification reserve exceed the grant")
    launch_id = launch.get("launch_id")
    if not isinstance(launch_id, str) or not _HEX_32.fullmatch(launch_id):
        raise ControlError("historical launch identity is invalid")
    started_epoch = _finite_nonnegative(launch.get("started_epoch"), "launch epoch")
    started_mono = _finite_nonnegative(launch.get("started_monotonic"), "launch monotonic time")
    deadline_epoch = _finite_nonnegative(launch.get("deadline_epoch"), "launch deadline")
    deadline_mono = _finite_nonnegative(
        launch.get("deadline_monotonic"), "launch monotonic deadline"
    )
    workflow_epoch = _finite_nonnegative(launch.get("workflow_deadline_epoch"), "workflow deadline")
    workflow_mono = _finite_nonnegative(
        launch.get("workflow_deadline_monotonic"), "workflow monotonic deadline"
    )
    if (
        started_epoch < approved
        or started_epoch - approved > FRESH_APPROVAL_SECONDS
        or started_epoch - latched > FRESH_APPROVAL_SECONDS
        or latched - approved > FRESH_APPROVAL_SECONDS
        or started_epoch > now_epoch
        or started_mono > now_monotonic
        or now_epoch >= deadline_epoch
        or now_monotonic >= deadline_mono
        or launch.get("duration_seconds") != duration
        or abs(deadline_epoch - started_epoch - duration) > 0.05
        or abs(deadline_mono - started_mono - duration) > 0.05
        or launch.get("reserve_seconds") != reserve
        or abs(workflow_epoch - deadline_epoch + reserve) > 0.05
        or abs(workflow_mono - deadline_mono + reserve) > 0.05
        or workflow_epoch <= started_epoch
        or workflow_mono <= started_mono
    ):
        raise ControlError("historical launch deadline or termination reserve is invalid")
    return {
        "phase": phase,
        "phases": phases,
        "grant_id": grant_id,
        "started_epoch": started_epoch,
        "started_monotonic": started_mono,
        "deadline_epoch": deadline_epoch,
        "deadline_monotonic": deadline_mono,
        "workflow_deadline_epoch": workflow_epoch,
        "workflow_deadline_monotonic": workflow_mono,
        "reserve_seconds": reserve,
        "retire_source": retire_source,
    }


def create_launch_record(
    approval: dict[str, Any],
    latch: dict[str, Any],
    *,
    now_epoch: float,
    now_monotonic: float,
    boot_stamp: str,
    launch_id: str,
    session_binding_sha256: str,
) -> dict[str, Any]:
    duration = _finite_nonnegative(approval.get("duration_seconds"), "grant duration")
    final_verification = _finite_nonnegative(
        approval.get("measured_final_verification_seconds"), "final-verification estimate"
    )
    reserve = max(MIN_RESERVE_SECONDS, final_verification)
    return {
        "version": 1,
        "grant_id": approval.get("grant_id"),
        "approval_sha256": sha256_json(approval),
        "latch_sha256": sha256_json(latch),
        "launch_id": launch_id,
        "session_binding_sha256": _digest(session_binding_sha256, "session binding"),
        "boot_stamp": boot_stamp,
        "started_epoch": now_epoch,
        "started_monotonic": now_monotonic,
        "duration_seconds": duration,
        "deadline_epoch": now_epoch + duration,
        "deadline_monotonic": now_monotonic + duration,
        "reserve_seconds": reserve,
        "workflow_deadline_epoch": now_epoch + duration - reserve,
        "workflow_deadline_monotonic": now_monotonic + duration - reserve,
    }


def _private_regular_file(path: Path, *, max_bytes: int = 1024 * 1024) -> bytes:
    try:
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > max_bytes:
            raise ControlError("private control artifact is unsafe or oversized")
        if info.st_mode & 0o077 or info.st_uid != os.geteuid():
            raise ControlError("private control artifact must be owner-only")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
    except ControlError:
        raise
    except OSError as exc:
        raise ControlError("private control artifact is missing or unsafe") from exc
    try:
        current = os.fstat(descriptor)
        if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino):
            raise ControlError("private control artifact changed while opening")
        chunks = []
        size = 0
        while body := os.read(descriptor, min(65536, max_bytes + 1 - size)):
            size += len(body)
            if size > max_bytes:
                raise ControlError("private control artifact is oversized")
            chunks.append(body)
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(
            _private_regular_file(path).decode("utf-8"), parse_constant=_reject_constant
        )
    except ControlError:
        raise
    except (UnicodeDecodeError, ValueError) as exc:
        raise ControlError("private control artifact is malformed JSON") from exc
    if not isinstance(value, dict):
        raise ControlError("private control artifact must be a JSON object")
    return value


def _reject_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def _ensure_private_directory(path: Path) -> None:
    missing: list[Path] = []
    cursor = path
    while True:
        try:
            info = cursor.lstat()
        except FileNotFoundError:
            missing.append(cursor)
            cursor = cursor.parent
            continue
        if not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.geteuid():
            raise ControlError("private control directory must be owner-only")
        break
    for directory in reversed(missing):
        with suppress(FileExistsError):
            os.mkdir(directory, 0o700)
        info = directory.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.geteuid():
            raise ControlError("private control directory must be owner-only")


def write_once_json(path: Path, value: Any) -> str:
    _ensure_private_directory(path.parent)
    try:
        parent = path.parent.lstat()
        if (
            not stat.S_ISDIR(parent.st_mode)
            or parent.st_mode & 0o077
            or parent.st_uid != os.geteuid()
        ):
            raise ControlError("private control directory must be owner-only")
    except OSError as exc:
        raise ControlError("private control directory is missing or unsafe") from exc
    body = canonical_bytes(value) + b"\n"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError as exc:
        raise ControlError("immutable control artifact already exists") from exc
    except OSError as exc:
        raise ControlError("cannot create immutable control artifact") from exc
    try:
        view = memoryview(body)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise ControlError("short write of immutable control artifact")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return sha256_bytes(body)


@contextmanager
def control_lock(root: Path) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    _control_root(root)
    lock_path = root / ".controls.lock"
    descriptor = os.open(
        lock_path,
        os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    locked = False
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_mode & 0o077:
            raise ControlError("private controls lock is unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ControlError("another private historical control operation is active") from exc
        locked = True
        current = os.stat(lock_path, follow_symlinks=False)
        if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino):
            raise ControlError("private controls lock changed during acquisition")
        yield
    finally:
        if locked:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _control_root(root: Path) -> None:
    try:
        info = root.lstat()
    except OSError as exc:
        raise ControlError("private historical control root is missing") from exc
    if not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.geteuid():
        raise ControlError("private historical control root must be owner-only")


def _private_directory(path: Path, label: str) -> None:
    try:
        info = path.lstat()
    except OSError as exc:
        raise ControlError(f"{label} is missing or unsafe") from exc
    if not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.geteuid():
        raise ControlError(f"{label} must be owner-only")


def read_control_template(
    root: Path, *, expected_identity: dict[str, Any]
) -> tuple[dict[str, Any], str]:
    _control_root(root)
    body = _private_regular_file(root / "control.json")
    try:
        control = json.loads(body.decode("utf-8"), parse_constant=_reject_constant)
    except (UnicodeDecodeError, ValueError) as exc:
        raise ControlError("private historical control template is malformed") from exc
    if not isinstance(control, dict):
        raise ControlError("private historical control template must be an object")
    validate_inert_control(control, expected_identity)
    return control, sha256_bytes(body)


def read_frozen_control(
    root: Path,
    *,
    expected_identity: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    control, control_hash = read_control_template(root, expected_identity=expected_identity)
    freeze_path = root / "freeze.json"
    try:
        body = _private_regular_file(freeze_path)
    except ControlError as exc:
        raise ControlError(
            "private controls are inert; no frozen approval snapshot exists"
        ) from exc
    try:
        freeze = json.loads(body.decode("utf-8"), parse_constant=_reject_constant)
    except (UnicodeDecodeError, ValueError) as exc:
        raise ControlError("frozen historical control artifact is malformed") from exc
    identity = validate_frozen_control(
        freeze,
        control_sha256=control_hash,
        expected_identity=control["identity"],
    )
    return {"control": control, "freeze": freeze, "identity": identity}, sha256_bytes(body)


def _grant_paths(root: Path, grant_id: str) -> dict[str, Path]:
    if not isinstance(grant_id, str) or not _HEX_32.fullmatch(grant_id):
        raise ControlError("historical grant ID is invalid")
    grant_root = root / "grants" / grant_id
    return {
        "approval": grant_root / "approval.json",
        "latch": grant_root / "latch.json",
        "launch": grant_root / "launch.json",
        "session_binding": grant_root / "session-binding.json",
    }


def read_grant_bundle(
    root: Path, grant_id: str
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    paths = _grant_paths(root, grant_id)
    _private_directory(root / "grants", "historical grants directory")
    _private_directory(paths["approval"].parent, "historical grant directory")
    return read_json(paths["approval"]), read_json(paths["latch"]), read_json(paths["launch"])


def read_control_bundle(root: Path, grant_id: str, *, prelaunch: bool = False) -> dict[str, Any]:
    """Take one lock-consistent read of the template, freeze, grant, session binding, and seals."""
    with control_lock(root):
        control, control_sha256 = read_control_template(
            root,
            expected_identity=read_json(root / "control.json").get("identity"),
        )
        frozen, freeze_sha256 = read_frozen_control(root, expected_identity=control["identity"])
        paths = _grant_paths(root, grant_id)
        _private_directory(root / "grants", "historical grants directory")
        _private_directory(paths["approval"].parent, "historical grant directory")
        approval = read_json(paths["approval"])
        latch = read_json(paths["latch"])
        launch = None if prelaunch else read_json(paths["launch"])
        session_binding = None if prelaunch else read_json(paths["session_binding"])
        current_phase, previous = read_phase_chain(root / "phases", frozen["identity"])
        return {
            "control": control,
            "control_sha256": control_sha256,
            "freeze": frozen["freeze"],
            "freeze_sha256": freeze_sha256,
            "identity": frozen["identity"],
            "approval": approval,
            "latch": latch,
            "launch": launch,
            "session_binding": session_binding,
            "current_phase": current_phase,
            "previous_sha256": previous,
        }


def validate_session_binding(
    binding: Any,
    *,
    approval: dict[str, Any],
    expected_identity: dict[str, Any],
) -> dict[str, Any]:
    fields = {
        "version",
        "grant_id",
        "approval_sha256",
        "capture_id",
        "session_id",
        "root_id",
        "stage_name",
        "report_sha256",
        "report_path",
        "boot_stamp",
        "root_volume_uuid",
        "root_device",
    }
    if not isinstance(binding, dict) or set(binding) != fields or binding.get("version") != 1:
        raise ControlError("historical grant session binding is invalid")
    measurements = approval.get("measurements")
    if (
        binding.get("grant_id") != approval.get("grant_id")
        or binding.get("approval_sha256") != sha256_json(approval)
        or any(
            binding.get(key) != expected_identity.get(key)
            for key in ("capture_id", "session_id", "root_id", "stage_name")
        )
    ):
        raise ControlError("historical grant session binding does not match its approval")
    _digest(binding.get("report_sha256"), "session report")
    if (
        not isinstance(measurements, dict)
        or measurements.get("session_report_sha256") != binding["report_sha256"]
    ):
        raise ControlError("historical grant approval does not bind its stopped session report")
    report_path = binding.get("report_path")
    if not isinstance(report_path, str) or not Path(report_path).is_absolute():
        raise ControlError("historical grant session report path is invalid")
    if not isinstance(binding.get("boot_stamp"), str) or not binding["boot_stamp"]:
        raise ControlError("historical grant session boot identity is missing")
    if (
        not isinstance(binding.get("root_volume_uuid"), str)
        or re.fullmatch(
            r"[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}",
            binding["root_volume_uuid"],
        )
        is None
    ):
        raise ControlError("historical grant session volume UUID is invalid")
    if type(binding.get("root_device")) is not int or binding["root_device"] <= 0:
        raise ControlError("historical grant session filesystem device is invalid")
    return binding


def write_session_binding(root: Path, grant_id: str, binding: dict[str, Any]) -> str:
    paths = _grant_paths(root, grant_id)
    _private_directory(root / "grants", "historical grants directory")
    _private_directory(paths["approval"].parent, "historical grant directory")
    return write_once_json(paths["session_binding"], binding)


def read_session_binding(root: Path, grant_id: str) -> dict[str, Any]:
    paths = _grant_paths(root, grant_id)
    _private_directory(root / "grants", "historical grants directory")
    _private_directory(paths["approval"].parent, "historical grant directory")
    return read_json(paths["session_binding"])


def claim_activation_audit(
    root: Path,
    *,
    audit_sha256: str,
    grant_id: str,
    launch_id: str,
    identity: dict[str, Any],
    prior_audits: set[str],
) -> str:
    """Reserve an exact audit artifact once for this validated grant and launch."""
    _digest(audit_sha256, "activation audit")
    if audit_sha256 in prior_audits:
        raise ControlError("activation audit artifact was already used by a prior attempt")
    if (
        not isinstance(grant_id, str)
        or not _HEX_32.fullmatch(grant_id)
        or not isinstance(launch_id, str)
        or not _HEX_32.fullmatch(launch_id)
    ):
        raise ControlError("activation audit reservation identity is invalid")
    expected_identity = _identity(identity)
    with control_lock(root):
        path = root / "activation-audits" / f"{grant_id}.json"
        return write_once_json(
            path,
            {
                "version": 1,
                "audit_sha256": audit_sha256,
                "identity": expected_identity,
                "grant_id": grant_id,
                "launch_id": launch_id,
            },
        )


def issue_launch(
    root: Path,
    *,
    expected_identity: dict[str, Any],
    requested_phase: str,
    grant_id: str,
    now_epoch: float,
    now_monotonic: float,
    boot_stamp: str,
    session_binding: dict[str, Any],
    dbt_memory_limit: str = "4GB",
    dbt_threads: int = 4,
) -> tuple[dict[str, Any], str]:
    """Consume one fresh approval/latch pair and create its immutable launch record."""
    with control_lock(root):
        _control_root(root)
        # The frozen identity can contain the new source/state/runtime pins,
        # while the immutable inert template may still have null pin slots.
        # Validate the template against its own sealed schema, then require its
        # stable session bindings and the full frozen snapshot to match the
        # caller's independently checked identity.
        template_identity = _identity(
            read_json(root / "control.json").get("identity"), allow_null_pins=True
        )
        expected = _identity(expected_identity)
        if any(template_identity[key] != expected[key] for key in _BINDING_FIELDS[:6]) or any(
            template_identity[key] is not None and template_identity[key] != expected[key]
            for key in _BINDING_FIELDS[6:]
        ):
            raise ControlError("frozen launch identity differs from the inert control template")
        read_control_template(root, expected_identity=template_identity)
        frozen, _freeze_hash = read_frozen_control(root, expected_identity=template_identity)
        if frozen["identity"] != expected:
            raise ControlError("frozen launch identity differs from current authority")
        phases_root = root / "phases"
        phases = read_phase_chain(phases_root, frozen["identity"])
        if requested_phase != next_phase(phases[0]):
            raise ControlError("requested historical phase is not the next sealed phase")
        paths = _grant_paths(root, grant_id)
        _private_directory(root / "grants", "historical grants directory")
        _private_directory(paths["approval"].parent, "historical grant directory")
        approval = read_json(paths["approval"])
        latch = read_json(paths["latch"])
        validate_session_binding(
            session_binding,
            approval=approval,
            expected_identity=frozen["identity"],
        )
        launch = create_launch_record(
            approval,
            latch,
            now_epoch=now_epoch,
            now_monotonic=now_monotonic,
            boot_stamp=boot_stamp,
            launch_id=uuid.uuid4().hex,
            session_binding_sha256=sha256_json(session_binding),
        )
        validated = validate_grant_bundle(
            approval,
            latch,
            launch,
            expected_bindings=frozen["identity"],
            now_epoch=now_epoch,
            now_monotonic=now_monotonic,
            boot_stamp=boot_stamp,
            dbt_memory_limit=dbt_memory_limit,
            dbt_threads=dbt_threads,
        )
        if validated["phase"] != requested_phase:
            raise ControlError("historical grant phase range does not start at the next phase")
        write_once_json(paths["session_binding"], session_binding)
        launch_hash = write_once_json(paths["launch"], launch)
        return launch, launch_hash


def read_phase_chain(phases_root: Path, bindings: dict[str, Any]) -> tuple[str | None, str | None]:
    if not phases_root.exists() and not phases_root.is_symlink():
        return None, None
    _private_directory(phases_root, "historical phase seal directory")
    allowed = {f"{phase}.json" for phase in PHASES}
    entries = []
    try:
        with os.scandir(phases_root) as inventory:
            for entry in inventory:
                if len(entries) >= len(PHASES) or entry.name not in allowed:
                    raise ControlError(
                        "historical phase seal inventory contains unexpected entries"
                    )
                entries.append(entry.name)
    except ControlError:
        raise
    except OSError as exc:
        raise ControlError("historical phase seal inventory is unavailable") from exc
    present = {name.removesuffix(".json") for name in entries}
    if present != set(PHASES[: len(present)]):
        raise ControlError("historical phase seal chain has a gap")
    seals = [read_json(phases_root / f"{phase}.json") for phase in PHASES if phase in present]
    grant_launches: dict[str, dict[str, Any]] = {}
    for seal in seals:
        grant_id = seal.get("grant_id") if isinstance(seal, dict) else None
        if not isinstance(grant_id, str) or not _HEX_32.fullmatch(grant_id):
            raise ControlError("historical phase seal grant identity is invalid")
        if grant_id not in grant_launches:
            approval, linked_launch = _phase_grant_launch(phases_root.parent, grant_id, bindings)
            if seal["phase"] not in approval["phases"]:
                raise ControlError("historical phase seal is outside its linked grant range")
            grant_launches[grant_id] = linked_launch
    return validate_phase_chain(seals, expected_bindings=bindings, grant_launches=grant_launches)


def write_phase_seal(
    root: Path,
    seal: dict[str, Any],
    *,
    bindings: dict[str, Any],
    grant_launch: dict[str, Any] | None = None,
) -> str:
    phase = seal.get("phase") if isinstance(seal, dict) else None
    if phase not in PHASES:
        raise ControlError("historical phase seal phase is invalid")
    with control_lock(root):
        phases_root = root / "phases"
        current, previous = read_phase_chain(phases_root, bindings)
        if phase != next_phase(current):
            raise ControlError("historical phase seal is not the next phase")
        grant_id = seal.get("grant_id") if isinstance(seal, dict) else None
        if not isinstance(grant_id, str) or not _HEX_32.fullmatch(grant_id):
            raise ControlError("historical phase seal grant identity is invalid")
        approval, linked_launch = _phase_grant_launch(root, grant_id, bindings)
        if phase not in approval["phases"]:
            raise ControlError("historical phase seal is outside its linked grant range")
        validate_phase_seal(
            seal,
            expected_phase=phase,
            expected_bindings=bindings,
            previous_sha256=previous,
            prior_activation_audits=set(),
            grant_launch=linked_launch,
        )
        validate_live_phase_deadline(phase, linked_launch)
        return write_once_json(phases_root / f"{phase}.json", seal)
