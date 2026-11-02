"""Verified, external production.duckdb hard-link activation."""

from __future__ import annotations

import contextlib
import hashlib
import json
import math
import os
import re
import shutil
import stat
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

_ACTIVATION_TEMP_NAME = re.compile(r"\.production\.duckdb\.[0-9a-f]{32}\.tmp\Z")
_ACTIVATION_AUDIT_NAME = re.compile(r"activation-audit-([0-9a-f]{64})\.json\Z")
_ACTIVATION_AUDIT_FIELDS = {
    "version",
    "session_id",
    "capture_id",
    "root_id",
    "capture_state_sha256",
    "receipt_index_sha256",
    "population_report_sha256",
    "observations_by_endpoint",
    "exact_observations",
    "sample_strategy",
}
_MAX_ACTIVATION_AUDIT_BYTES = 64 * 1024
_MAX_ACTIVATION_SESSION_REPORT_BYTES = 16 * 1024**2
_ENDPOINT_SEGMENT = re.compile(r"(?:[A-Za-z0-9._()~ -]|%[0-9A-F]{2})+\Z")
_MAX_PRODUCTION_DIRECTORY_ENTRIES = 50_000


def _canonical_endpoint(value: Any) -> bool:
    if not isinstance(value, str) or not value.startswith("/") or value.endswith("/"):
        return False
    parts = value[1:].split("/")
    return bool(parts) and all(
        part not in {"", ".", ".."}
        and not part.endswith(" ")
        and _ENDPOINT_SEGMENT.fullmatch(part) is not None
        for part in parts
    )


def _check_activation_audit_owner_mode(
    info: os.stat_result, *, path: Path, immutable: bool = False
) -> None:
    mode = stat.S_IMODE(info.st_mode)
    if info.st_uid != os.geteuid() or mode & 0o022 or immutable and mode & 0o222:
        raise RuntimeError(f"activation audit path ownership or mode is unsafe: {path.name}")


def _activation_audit_payload(
    directory: Path,
    binding: dict[str, Any],
    *,
    expected_session_id: str | None = None,
) -> dict[str, Any]:
    """Read the session-local immutable artifact bound by its report and digest."""
    from kalshi_events.common import regular_bytes

    if not isinstance(binding, dict):
        raise RuntimeError("activation audit binding must be an object")
    binding_fields = {"relative_path", "sha256"}
    if frozenset(binding) not in {
        frozenset(binding_fields),
        frozenset(_ACTIVATION_AUDIT_FIELDS | binding_fields),
    }:
        raise RuntimeError("activation audit binding has an invalid schema")
    relative_path = binding.get("relative_path")
    checksum = binding.get("sha256")
    session_id = expected_session_id or binding.get("session_id")
    if (
        not isinstance(session_id, str)
        or not re.fullmatch(r"[0-9a-f]{32}", session_id)
        or not isinstance(relative_path, str)
        or not isinstance(checksum, str)
        or not re.fullmatch(r"[0-9a-f]{64}", checksum)
    ):
        raise RuntimeError("activation audit binding identity is invalid")
    filename = _ACTIVATION_AUDIT_NAME.fullmatch(relative_path)
    if filename is None or filename.group(1) != checksum:
        raise RuntimeError("activation audit path does not bind its exact digest")

    runs = directory / "runs"
    session_directory = runs / session_id
    path = session_directory / relative_path
    for candidate in (directory, runs, session_directory):
        info = candidate.lstat()
        if (
            not stat.S_ISDIR(info.st_mode)
            or candidate.is_symlink()
            or info.st_dev != directory.stat().st_dev
        ):
            raise RuntimeError("activation audit is outside a safe session directory")
        _check_activation_audit_owner_mode(info, path=candidate)
    info = path.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or path.is_symlink()
        or info.st_nlink != 1
        or info.st_dev != directory.stat().st_dev
        or info.st_size > _MAX_ACTIVATION_AUDIT_BYTES
    ):
        raise RuntimeError("activation audit file is unsafe or exceeds its bound")
    _check_activation_audit_owner_mode(info, path=path, immutable=True)
    raw = regular_bytes(path, _MAX_ACTIVATION_AUDIT_BYTES)
    if hashlib.sha256(raw).hexdigest() != checksum:
        raise RuntimeError("activation audit file digest differs from its session binding")

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate JSON field")
            value[key] = item
        return value

    def reject_constant(value: str) -> None:
        raise ValueError(f"invalid JSON constant: {value}")

    try:
        artifact = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except (UnicodeError, ValueError) as exc:
        raise RuntimeError("activation audit JSON is invalid") from exc
    if not isinstance(artifact, dict) or set(artifact) != _ACTIVATION_AUDIT_FIELDS:
        raise RuntimeError("activation audit artifact has an invalid schema")

    def hex_value(value: Any, length: int) -> bool:
        return isinstance(value, str) and re.fullmatch(rf"[0-9a-f]{{{length}}}", value) is not None

    observations = artifact.get("observations_by_endpoint")
    exact = artifact.get("exact_observations")
    if (
        type(artifact.get("version")) is not int
        or artifact["version"] != 1
        or not hex_value(artifact.get("session_id"), 32)
        or artifact["session_id"] != session_id
        or not hex_value(artifact.get("capture_id"), 32)
        or not hex_value(artifact.get("root_id"), 32)
        or not hex_value(artifact.get("capture_state_sha256"), 64)
        or not hex_value(artifact.get("receipt_index_sha256"), 64)
        or not hex_value(artifact.get("population_report_sha256"), 64)
        or not isinstance(observations, dict)
        or not observations
        or any(
            not _canonical_endpoint(endpoint)
            or type(count) is not int
            or count < 0
            or count > 2**63 - 1
            for endpoint, count in observations.items()
        )
        or type(exact) is not int
        or exact < 0
        or exact > 2**63 - 1
        or sum(observations.values()) != exact
        or artifact.get("sample_strategy") != "verified-parquet-keyed-v1"
    ):
        raise RuntimeError("activation audit artifact values are invalid")

    if set(binding) == _ACTIVATION_AUDIT_FIELDS | binding_fields:
        expected = {key: binding[key] for key in _ACTIVATION_AUDIT_FIELDS}
        if expected != artifact:
            raise RuntimeError("activation audit artifact changed after session binding")

    report_path = session_directory / "session.json"
    report_info = report_path.lstat()
    if (
        not stat.S_ISREG(report_info.st_mode)
        or report_path.is_symlink()
        or report_info.st_nlink != 1
        or report_info.st_dev != directory.stat().st_dev
        or report_info.st_size > _MAX_ACTIVATION_SESSION_REPORT_BYTES
    ):
        raise RuntimeError("activation session report is unsafe or exceeds its bound")
    _check_activation_audit_owner_mode(report_info, path=report_path)
    report_raw = regular_bytes(report_path, _MAX_ACTIVATION_SESSION_REPORT_BYTES)
    try:
        report = json.loads(
            report_raw.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except (UnicodeError, ValueError) as exc:
        raise RuntimeError("activation session report JSON is invalid") from exc
    if (
        not isinstance(report, dict)
        or report.get("session_id") != session_id
        or report.get("capture_id") != artifact["capture_id"]
        or report.get("root_id") != artifact["root_id"]
        or not isinstance(report.get("activation_audit"), dict)
        or set(report["activation_audit"]) != binding_fields
        or report["activation_audit"].get("relative_path") != relative_path
        or report["activation_audit"].get("sha256") != checksum
    ):
        raise RuntimeError("activation session report no longer binds this audit artifact")
    return {**artifact, "relative_path": relative_path, "sha256": checksum}


def _validate_audit_current_capture(
    audit: dict[str, Any],
    store: Any,
    state: dict[str, Any],
    current: dict[str, Any],
    capture_id: str,
) -> None:
    from kalshi_events.common import decode, regular_bytes

    root_record = decode(regular_bytes(store.path("root-id.json"), 1024**2))
    root_id = root_record.get("root_id") if isinstance(root_record, dict) else None
    state_sha256 = getattr(state, "verified_snapshot_sha256", None)
    state_capture_id = state.get("capture_id")
    state_root_id = state.get("root_id")
    if (
        not isinstance(root_id, str)
        or re.fullmatch(r"[0-9a-f]{32}", root_id) is None
        or audit.get("root_id") != root_id
        or current.get("root_id") != root_id
        or state_root_id is not None
        and state_root_id != root_id
        or state_capture_id != capture_id
        or current.get("capture_id") != capture_id
        or audit.get("capture_id") != capture_id
        or not isinstance(state_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", state_sha256) is None
        or audit.get("capture_state_sha256") != state_sha256
    ):
        raise RuntimeError("activation audit does not bind the current capture state and root")


def _activation_temporary_names(directory: Path) -> list[Path]:
    names = []
    with os.scandir(directory) as entries:
        for index, entry in enumerate(entries, start=1):
            if index > _MAX_PRODUCTION_DIRECTORY_ENTRIES:
                raise RuntimeError("production directory exceeded its activation recovery bound")
            if _ACTIVATION_TEMP_NAME.fullmatch(entry.name):
                names.append(Path(entry.path))
    return names


def _remove_stale_activation_temporaries(
    store: Any,
    production_directory: Path,
    current: dict[str, Any],
    source: Path,
    checksum: str,
    *,
    catalogue_version: int,
    max_database_bytes: int,
) -> int:
    """Remove only orphan links to the still-current certified source warehouse."""
    from kalshi_events import publish
    from kalshi_events.common import file_digest, fsync_directory

    if not store.writer_locked_by_current_thread:
        raise RuntimeError("activation temporary recovery requires the canonical writer lock")
    names = _activation_temporary_names(production_directory)
    if not names:
        return 0

    source_info = source.lstat()
    if (
        not stat.S_ISREG(source_info.st_mode)
        or source.is_symlink()
        or source_info.st_size > max_database_bytes
        or file_digest(source, max_database_bytes)[0] != checksum
    ):
        raise RuntimeError("certified warehouse changed during activation temporary recovery")
    if publish.current(store, catalogue_version=catalogue_version) != current:
        raise RuntimeError("current publication changed during activation temporary recovery")

    temporaries: list[tuple[Path, tuple[int, int]]] = []
    for temporary in names:
        info = temporary.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink < 2
            or (info.st_dev, info.st_ino) != (source_info.st_dev, source_info.st_ino)
        ):
            raise RuntimeError(
                "stale production activation temporary is not a certified source link"
            )
        if temporary.with_name(temporary.name + ".wal").exists():
            raise RuntimeError("stale production activation temporary has an uncheckpointed WAL")
        temporaries.append((temporary, (info.st_dev, info.st_ino)))

    for temporary, identity in temporaries:
        info = temporary.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink < 2
            or (info.st_dev, info.st_ino) != identity
            or (info.st_dev, info.st_ino) != (source_info.st_dev, source_info.st_ino)
        ):
            raise RuntimeError("stale production activation temporary changed during recovery")
        temporary.unlink()
    if temporaries:
        fsync_directory(production_directory)
    return len(temporaries)


def recover_activation_temporaries(
    operator_root: Path,
    production_directory: Path,
    *,
    expected_release_id: str,
    catalogue_version: int = 2,
    expected_capture_id: str | None = None,
    max_database_bytes: int = 64 * 1024**3,
    max_temp_bytes: int = 64 * 1024**3,
    max_duration_seconds: float | None = None,
) -> int:
    """Recover only validated activation hard links for the current release."""
    from kalshi_events import publish, warehouse
    from kalshi_events.common import (
        CatalogueError,
        Limits,
        Store,
    )
    from kalshi_events.paths import PathPolicyError, persistent_path, project_root

    if max_duration_seconds is not None and (
        isinstance(max_duration_seconds, bool)
        or not isinstance(max_duration_seconds, (int, float))
        or not math.isfinite(max_duration_seconds)
        or max_duration_seconds <= 0
    ):
        raise RuntimeError("activation duration limit is invalid")
    try:
        checkout = project_root(code_root=Path(__file__).resolve().parents[1])
        directory = persistent_path(
            production_directory, checkout=checkout, label="production directory"
        )
        root = persistent_path(operator_root, checkout=checkout, label="production operator root")
    except PathPolicyError as exc:
        raise CatalogueError(str(exc)) from exc
    if not directory.is_dir() or root != directory / "operator":
        raise RuntimeError("activation paths do not match the production layout")
    if isinstance(catalogue_version, bool) or catalogue_version not in {2, 3}:
        raise RuntimeError("production activation supports catalogue versions 2 and 3")
    if not isinstance(expected_release_id, str) or not re.fullmatch(
        r"[0-9a-f]{64}", expected_release_id
    ):
        raise RuntimeError("expected activation release identity is invalid")
    if expected_capture_id is not None and (
        catalogue_version != 3
        or not isinstance(expected_capture_id, str)
        or not re.fullmatch(r"[0-9a-f]{32}", expected_capture_id)
    ):
        raise RuntimeError("expected historical capture identity is invalid")
    alias = directory / "production.duckdb"
    if alias.is_symlink():
        raise RuntimeError("production database path cannot be a symlink")
    if alias.exists() and not stat.S_ISREG(alias.lstat().st_mode):
        raise RuntimeError("existing production database is not a regular file")
    if not _activation_temporary_names(directory):
        return 0

    duration = 48 * 3600 if max_duration_seconds is None else max_duration_seconds
    limits = Limits(
        max_rate=5,
        max_requests=850_000,
        max_download_bytes=256 * 1024**3,
        max_response_bytes=384 * 1024**2,
        max_decoded_response_bytes=768 * 1024**2,
        max_duration_seconds=duration,
        max_root_bytes=350 * 1024**3,
        max_temp_bytes=max_temp_bytes,
        max_database_bytes=max_database_bytes,
    )
    store = Store(root, limits)
    if store.scope is not None:
        raise RuntimeError("scoped series roots cannot be production catalogue roots")
    with store.lock(), warehouse.operation_context(store) as verification:
        current = publish.current(store, catalogue_version=catalogue_version)
        if current.get("source_release_id") != expected_release_id:
            raise RuntimeError("current publication changed before activation temporary recovery")
        capture_id = current.get("capture_id")
        if expected_capture_id is not None and capture_id != expected_capture_id:
            raise RuntimeError("current publication belongs to a different capture")
        standalone = publish.verify_release(Path(current["output"]), limits.max_root_bytes)
        published_manifest = {key: value for key, value in current.items() if key != "output"}
        if standalone != published_manifest:
            raise RuntimeError("standalone verification does not match the current publication")
        receipt = warehouse.certified(
            store, capture_id, catalogue_version=catalogue_version, _context=verification
        )
        source = store.path(
            f"{warehouse.build_directory(capture_id, catalogue_version)}/warehouse.duckdb"
        )
        checksum = receipt.get("warehouse_sha256")
        if not isinstance(checksum, str):
            raise RuntimeError("certified warehouse has no valid activation checksum")
        if source.with_name(source.name + ".wal").exists():
            raise RuntimeError("certified warehouse has an uncheckpointed WAL")
        return _remove_stale_activation_temporaries(
            store,
            directory,
            current,
            source,
            checksum,
            catalogue_version=catalogue_version,
            max_database_bytes=limits.max_database_bytes,
        )


def activate_current(
    operator_root: Path,
    production_directory: Path,
    *,
    expected_release_id: str | None = None,
    expected_capture_id: str | None = None,
    standalone_verified: dict[str, Any] | None = None,
    catalogue_version: int = 2,
    max_database_bytes: int = 64 * 1024**3,
    max_temp_bytes: int = 64 * 1024**3,
    max_duration_seconds: float | None = None,
    expected_observations: int | None = None,
    activation_audit: dict[str, Any] | None = None,
    link_fn: Any = None,
    replace_fn: Any = None,
    sync_fn: Any = None,
) -> dict[str, Any]:
    """Certify the selected warehouse, then atomically expose its hard link."""
    import pyarrow.parquet as pq

    from kalshi_events import capture, incremental, publish, warehouse
    from kalshi_events.common import (
        CatalogueError,
        Limits,
        Store,
        digest,
        file_digest,
        fsync_directory,
        regular_bytes,
    )
    from kalshi_events.paths import PathPolicyError, persistent_path, project_root

    link_fn = os.link if link_fn is None else link_fn
    replace_fn = os.replace if replace_fn is None else replace_fn
    sync_fn = fsync_directory if sync_fn is None else sync_fn
    try:
        checkout = project_root(code_root=Path(__file__).resolve().parents[1])
        directory = persistent_path(
            production_directory, checkout=checkout, label="production directory"
        )
        root = persistent_path(operator_root, checkout=checkout, label="production operator root")
    except PathPolicyError as exc:
        raise CatalogueError(str(exc)) from exc
    if not directory.is_dir() or root != directory / "operator":
        raise RuntimeError("activation paths do not match the production layout")
    if isinstance(catalogue_version, bool) or catalogue_version not in {2, 3}:
        raise RuntimeError("production activation supports catalogue versions 2 and 3")
    if max_duration_seconds is not None and (
        isinstance(max_duration_seconds, bool)
        or not isinstance(max_duration_seconds, (int, float))
        or not math.isfinite(max_duration_seconds)
        or max_duration_seconds <= 0
    ):
        raise RuntimeError("activation duration limit is invalid")
    if expected_observations is not None and (
        isinstance(expected_observations, bool)
        or not isinstance(expected_observations, int)
        or expected_observations < 0
    ):
        raise RuntimeError("expected historical observation count is invalid")
    verified_audit = None
    if activation_audit is not None:
        if catalogue_version != 3:
            raise RuntimeError("activation audit only supports historical catalogue releases")
        verified_audit = _activation_audit_payload(directory, activation_audit)
        if expected_capture_id != verified_audit["capture_id"]:
            raise RuntimeError("activation audit capture differs from the expected capture")
    alias = directory / "production.duckdb"
    if alias.is_symlink():
        raise RuntimeError("production database path cannot be a symlink")
    limits = Limits(
        max_rate=5,
        max_requests=850_000,
        max_download_bytes=256 * 1024**3,
        max_response_bytes=384 * 1024**2,
        max_decoded_response_bytes=768 * 1024**2,
        max_duration_seconds=48 * 3600 if max_duration_seconds is None else max_duration_seconds,
        max_root_bytes=350 * 1024**3,
        max_temp_bytes=max_temp_bytes,
        max_database_bytes=max_database_bytes,
    )
    store = Store(root, limits)
    if store.scope is not None:
        raise RuntimeError("scoped series roots cannot be production catalogue roots")
    with store.lock():
        incremental.finish_pending_retirement(store)
        with warehouse.operation_context(store) as verification:
            current = publish.current(store, catalogue_version=catalogue_version)
            release_id = current.get("source_release_id")
            capture_id = current.get("capture_id")
            if expected_release_id is not None and release_id != expected_release_id:
                raise RuntimeError("current publication changed after standalone verification")
            if expected_capture_id is not None:
                if (
                    catalogue_version != 3
                    or not isinstance(expected_capture_id, str)
                    or len(expected_capture_id) != 32
                    or any(character not in "0123456789abcdef" for character in expected_capture_id)
                ):
                    raise RuntimeError("expected historical capture identity is invalid")
                if capture_id != expected_capture_id:
                    raise RuntimeError("current publication belongs to a different capture")
            manifest_body = regular_bytes(Path(current["output"]) / "manifest.json", 1024**2)
            manifest_sha256 = digest(manifest_body)
            if standalone_verified is None:
                standalone_verified = publish.verify_release(
                    Path(current["output"]), limits.max_root_bytes
                )
            published_manifest = {key: value for key, value in current.items() if key != "output"}
            if standalone_verified != published_manifest:
                raise RuntimeError("standalone verification does not match the current publication")
            receipt = warehouse.certified(
                store, capture_id, catalogue_version=catalogue_version, _context=verification
            )
            checksum = receipt.get("warehouse_sha256")
            manifest_checksum = current.get("warehouse_sha256")
            if manifest_checksum == checksum:
                verification_mode = "warehouse_hash"
            else:
                pin = {
                    "catalogue_version": catalogue_version,
                    "source_release_id": release_id,
                    "manifest_sha256": manifest_sha256,
                    "capture_id": capture_id,
                    "root_id": current.get("root_id"),
                    "delta_depth": current.get("delta_depth"),
                    "series_scope": current.get("series_scope"),
                }
                verification_mode = "semantic"
                with incremental._baseline_data(
                    store, capture_id, catalogue_version, pin, _context=verification
                ):
                    pass
            state = capture.read_capture(store, capture_id)
            if verified_audit is not None:
                _validate_audit_current_capture(verified_audit, store, state, current, capture_id)
            source = store.path(
                f"{warehouse.build_directory(capture_id, catalogue_version)}/warehouse.duckdb"
            )
            source_info = source.lstat()
            if not stat.S_ISREG(source_info.st_mode) or source.is_symlink():
                raise RuntimeError("certified warehouse is not a regular file")
            if source_info.st_size > limits.max_database_bytes:
                raise RuntimeError("certified warehouse exceeds the selected database limit")
            source_checksum, _ = file_digest(source, limits.max_database_bytes)
            if source_checksum != checksum:
                raise RuntimeError("certified warehouse changed before production linking")
            recovered_temporaries = _remove_stale_activation_temporaries(
                store,
                directory,
                current,
                source,
                checksum,
                catalogue_version=catalogue_version,
                max_database_bytes=limits.max_database_bytes,
            )
            if alias.is_symlink():
                raise RuntimeError("production database path cannot be a symlink")
            if alias.exists() and not stat.S_ISREG(alias.lstat().st_mode):
                raise RuntimeError("existing production database is not a regular file")
            if (
                source.with_name(source.name + ".wal").exists()
                or alias.with_name(alias.name + ".wal").exists()
            ):
                raise RuntimeError("an uncheckpointed production DuckDB WAL exists")
            counts = {
                name: pq.ParquetFile(Path(current["output"]) / f"{name}.parquet").metadata.num_rows
                for name in warehouse.RELATIONS
            }
            if (
                expected_observations is not None
                and counts["observation_history"] != expected_observations
            ):
                raise RuntimeError(
                    "verified historical Parquet count differs from the expected observation census"
                )
            if source_info.st_dev != directory.stat().st_dev:
                raise RuntimeError(
                    "production database and warehouse must be on the same filesystem"
                )
            temporary = directory / f".production.duckdb.{uuid.uuid4().hex}.tmp"
            spill_relative = f"tmp/activation-readback-{uuid.uuid4().hex}"
            spill_directory = store.path(spill_relative)
            usage = store.usage()
            spill_budget = min(
                limits.max_temp_bytes - usage["temporary_bytes"],
                limits.max_root_bytes - usage["retained_bytes"],
            )
            if spill_budget < 1:
                raise RuntimeError(
                    "no bounded temporary storage is available for activation readback"
                )
            store.reserve_storage(spill_budget)
            spill_reserved = True
            try:
                spill_directory.mkdir(parents=True)
                store.sync(spill_relative)
                link_fn(source, temporary, follow_symlinks=False)
                linked_info = temporary.lstat()
                if (
                    not stat.S_ISREG(linked_info.st_mode)
                    or (linked_info.st_dev, linked_info.st_ino)
                    != (source_info.st_dev, source_info.st_ino)
                    or temporary.with_name(temporary.name + ".wal").exists()
                ):
                    raise RuntimeError(
                        "temporary production link differs from the certified warehouse"
                    )
                relation_map = dict(warehouse.RELATIONS)
                audit_enabled = verified_audit is not None
                query = """
import duckdb, json, sys
from pathlib import Path
database, spill, spill_bytes, version, relation_map_json, release_directory, audit_enabled = sys.argv[1:]
relation_map = json.loads(relation_map_json)
audit_enabled = audit_enabled == "1"
config = {
    "memory_limit": "4GB",
    "threads": "4",
    "temp_directory": spill,
    "max_temp_directory_size": spill_bytes + "B",
}
connection = duckdb.connect(database, read_only=True, config=config)
try:
    result = {"relation_rows": {}, "samples": {}}
    for name, table in relation_map.items():
        result["relation_rows"][name] = connection.execute(
            f"select count(*) from main.{table}"
        ).fetchone()[0]
    sample_specs = {
        "series": (
            ["series_ticker"],
            "series_ticker, observation_id, capture_id, received_at, source_endpoint, "
            "sha256(cast(payload_json as varchar)), substr(cast(payload_json as varchar), 1, 256)",
        ),
        "events": (
            ["event_ticker"],
            "event_ticker, series_ticker, observation_id, capture_id, received_at, "
            "source_endpoint, sha256(cast(payload_json as varchar)), "
            "substr(cast(payload_json as varchar), 1, 256)",
        ),
        "markets": (
            ["market_ticker"],
            "market_ticker, event_ticker, series_ticker, usable, observation_id, "
            "capture_id, received_at, source_endpoint, cast(last_price_dollars as varchar), "
            "cast(volume_fp as varchar), cast(open_interest_fp as varchar), "
            "sha256(cast(payload_json as varchar)), "
            "substr(cast(payload_json as varchar), 1, 256)",
        ),
        "observation_history": (
            ["observation_id"],
            "observation_id, capture_id, entity_kind, entity_ticker, received_at, "
            "source_endpoint, source_kind, sha256(cast(payload_json as varchar)), "
            "substr(cast(payload_json as varchar), 1, 256)",
        ),
        "combo_legs": (
            ["combo_market_ticker", "leg_index"],
            "combo_market_ticker, leg_index, event_ticker, market_ticker, side, "
            "cast(yes_settlement_value_dollars as varchar), "
            "sha256(cast(payload_json as varchar))",
        ),
        "relationships": (
            ["parent_kind", "parent_ticker", "child_kind", "child_ticker"],
            "parent_kind, parent_ticker, child_kind, child_ticker, resolved",
        ),
    }
    if not audit_enabled:
        legacy_samples = {
            "series": "select series_ticker, observation_id, capture_id, received_at, source_endpoint, sha256(cast(payload_json as varchar)), substr(cast(payload_json as varchar), 1, 256) from main.current_series limit 1",
            "events": "select event_ticker, series_ticker, observation_id, capture_id, received_at, source_endpoint, sha256(cast(payload_json as varchar)), substr(cast(payload_json as varchar), 1, 256) from main.current_events limit 1",
            "markets": "select market_ticker, event_ticker, series_ticker, usable, observation_id, capture_id, received_at, source_endpoint, cast(last_price_dollars as varchar), cast(volume_fp as varchar), cast(open_interest_fp as varchar), sha256(cast(payload_json as varchar)), substr(cast(payload_json as varchar), 1, 256) from main.current_markets limit 1",
            "observation_history": "select observation_id, capture_id, entity_kind, entity_ticker, received_at, source_endpoint, source_kind, sha256(cast(payload_json as varchar)), substr(cast(payload_json as varchar), 1, 256) from main.observation_history limit 1",
            "combo_legs": "select combo_market_ticker, leg_index, event_ticker, market_ticker, side, cast(yes_settlement_value_dollars as varchar), sha256(cast(payload_json as varchar)) from main.combo_legs limit 1",
            "relationships": "select parent_kind, parent_ticker, child_kind, child_ticker, resolved from main.relationships limit 1",
        }
        for name, sql in legacy_samples.items():
            result["samples"][name] = connection.execute(sql).fetchone()
    else:
        endpoint_rows = connection.execute(
            "select source_endpoint, count(*) from main.observation_history "
            "group by source_endpoint order by source_endpoint"
        ).fetchall()
        result["observations_by_endpoint"] = endpoint_rows
        result["sample_audit"] = {}
    for name, (keys, projection) in (sample_specs.items() if audit_enabled else ()):
        parquet_path = str(Path(release_directory) / (name + ".parquet"))
        key_names = ", ".join(keys)
        order_keys = ", ".join(f"{key} asc nulls last" for key in keys)
        decimal_filters = {
            "markets": [
                "last_price_dollars is not null and volume_fp is not null "
                "and open_interest_fp is not null",
                "last_price_dollars is not null or volume_fp is not null "
                "or open_interest_fp is not null",
            ],
            "combo_legs": ["yes_settlement_value_dollars is not null"],
        }
        key_row = None
        where_terms = decimal_filters.get(name, [])
        for condition in [*where_terms, None]:
            where = "" if condition is None else f" where {condition}"
            key_row = connection.execute(
                f"select {key_names} from read_parquet(?){where} "
                f"order by {order_keys} limit 1",
                [parquet_path],
            ).fetchone()
            if key_row is not None:
                break
        if key_row is None:
            result["samples"][name] = None
            sample_audit = {"matched": True, "key": None}
            if name == "markets":
                sample_audit.update({"decimal_fields_sampled": [], "decimal_sample_absent": True})
            elif name == "combo_legs":
                sample_audit.update({"decimal_fields_sampled": [], "decimal_sample_absent": True})
            result["sample_audit"][name] = sample_audit
            continue
        key_values = list(key_row)
        predicates = " and ".join(f"{key} is not distinct from ?" for key in keys)
        result["samples"][name] = connection.execute(
            f"select {projection} from main.{relation_map[name]} where {predicates}",
            key_values,
        ).fetchone()
        if audit_enabled:
            expected = connection.execute(
                f"select {projection} from read_parquet(?) where {predicates}",
                [parquet_path, *key_values],
            ).fetchone()
            actual = result["samples"][name]
            result["sample_audit"][name] = {
                "matched": expected == actual,
                "key": key_values,
            }
            if name == "markets":
                decimal_fields = [
                    field
                    for field, index in zip(
                        ["last_price_dollars", "volume_fp", "open_interest_fp"],
                        (8, 9, 10),
                        strict=True,
                    )
                    if expected[index] is not None
                ]
                result["sample_audit"][name].update(
                    {
                        "decimal_fields_sampled": decimal_fields,
                        "decimal_sample_absent": not decimal_fields,
                    }
                )
            elif name == "combo_legs":
                decimal_fields = (
                    ["yes_settlement_value_dollars"] if expected[5] is not None else []
                )
                result["sample_audit"][name].update(
                    {
                        "decimal_fields_sampled": decimal_fields,
                        "decimal_sample_absent": not decimal_fields,
                    }
                )
    if version == "3":
        row = connection.execute(
            "select historical_contract, scope, omitted_scopes, source_capture_id, source_capture_complete from main.catalogue_coverage"
        ).fetchone()
        result["catalogue_coverage"] = dict(zip(
            ["historical_contract", "scope", "omitted_scopes", "source_capture_id", "source_capture_complete"],
            row,
        ))
    print(json.dumps(result, default=str))
finally:
    connection.close()
"""
                readback = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        query,
                        str(temporary),
                        str(spill_directory),
                        str(spill_budget),
                        str(catalogue_version),
                        json.dumps(relation_map),
                        str(Path(current["output"])),
                        "1" if audit_enabled else "0",
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=min(600.0, store.remaining),
                )
                readback_data = json.loads(readback.stdout)
                readback_rows = readback_data.get("relation_rows")
                if (
                    not isinstance(readback_rows, dict)
                    or set(readback_rows) != set(counts)
                    or any(
                        isinstance(readback_rows.get(name), bool)
                        or not isinstance(readback_rows.get(name), int)
                        or readback_rows[name] != expected_rows
                        for name, expected_rows in counts.items()
                    )
                ):
                    raise RuntimeError(
                        "production DuckDB row counts differ from verified Parquet footers"
                    )
                if (
                    expected_observations is not None
                    and readback_rows.get("observation_history") != expected_observations
                ):
                    raise RuntimeError(
                        "production history count differs from the expected observation census"
                    )
                samples = readback_data.get("samples")
                if not isinstance(samples, dict) or any(
                    expected_rows > 0 and samples.get(name) is None
                    for name, table in relation_map.items()
                    if (expected_rows := counts[name]) > 0
                ):
                    raise RuntimeError(
                        "production DuckDB public readback missed a representative relation row"
                    )
                activation_audit_result = None
                if verified_audit is not None:
                    raw_endpoint_rows = readback_data.get("observations_by_endpoint")
                    if not isinstance(raw_endpoint_rows, list):
                        raise RuntimeError("production endpoint census is missing")
                    endpoint_counts: dict[str, int] = {}
                    for row in raw_endpoint_rows:
                        if (
                            not isinstance(row, list)
                            or len(row) != 2
                            or not isinstance(row[0], str)
                            or not _canonical_endpoint(row[0])
                            or type(row[1]) is not int
                            or row[1] < 0
                            or row[0] in endpoint_counts
                        ):
                            raise RuntimeError("production endpoint census is malformed")
                        endpoint_counts[row[0]] = row[1]
                    expected_endpoint_counts = verified_audit["observations_by_endpoint"]
                    if (
                        set(endpoint_counts) - set(expected_endpoint_counts)
                        or any(
                            endpoint_counts.get(endpoint, 0) != expected_count
                            for endpoint, expected_count in expected_endpoint_counts.items()
                        )
                        or sum(endpoint_counts.values()) != verified_audit["exact_observations"]
                        or verified_audit["exact_observations"] != counts["observation_history"]
                    ):
                        raise RuntimeError(
                            "production endpoint census differs from the verified population audit"
                        )
                    sample_audit = readback_data.get("sample_audit")
                    if not isinstance(sample_audit, dict) or set(sample_audit) != set(relation_map):
                        raise RuntimeError(
                            "production keyed sample differs from its verified Parquet row"
                        )
                    decimal_columns = {
                        "markets": [
                            "last_price_dollars",
                            "volume_fp",
                            "open_interest_fp",
                        ],
                        "combo_legs": ["yes_settlement_value_dollars"],
                    }
                    decimal_samples = {}
                    for name in relation_map:
                        sample = sample_audit[name]
                        expected_fields = decimal_columns.get(name)
                        expected_keys = {"matched", "key"}
                        if expected_fields is not None:
                            expected_keys |= {
                                "decimal_fields_sampled",
                                "decimal_sample_absent",
                            }
                        key = sample.get("key") if isinstance(sample, dict) else None
                        if (
                            not isinstance(sample, dict)
                            or set(sample) != expected_keys
                            or sample.get("matched") is not True
                            or (counts[name] == 0 and key is not None)
                            or (counts[name] > 0 and not isinstance(key, list))
                        ):
                            raise RuntimeError(
                                "production keyed sample differs from its verified Parquet row"
                            )
                        if expected_fields is not None:
                            sampled_fields = sample.get("decimal_fields_sampled")
                            decimal_absent = sample.get("decimal_sample_absent")
                            if (
                                not isinstance(sampled_fields, list)
                                or any(field not in expected_fields for field in sampled_fields)
                                or len(sampled_fields) != len(set(sampled_fields))
                                or sampled_fields
                                != [field for field in expected_fields if field in sampled_fields]
                                or type(decimal_absent) is not bool
                                or decimal_absent != (not sampled_fields)
                            ):
                                raise RuntimeError(
                                    "production decimal sample evidence is malformed"
                                )
                            decimal_samples[name] = {
                                "decimal_fields_sampled": sampled_fields,
                                "decimal_sample_absent": decimal_absent,
                            }
                    activation_audit_result = {
                        "state": "verified",
                        "sha256": verified_audit["sha256"],
                        "capture_state_sha256": verified_audit["capture_state_sha256"],
                        "receipt_index_sha256": verified_audit["receipt_index_sha256"],
                        "population_report_sha256": verified_audit["population_report_sha256"],
                        "sample_strategy": verified_audit["sample_strategy"],
                        "sample_matches": {name: True for name in sorted(relation_map)},
                        "decimal_samples": decimal_samples,
                        "observations_by_endpoint": {
                            endpoint: expected_endpoint_counts[endpoint]
                            for endpoint in sorted(expected_endpoint_counts)
                        },
                        "exact_observations": verified_audit["exact_observations"],
                    }
                readback_coverage = readback_data.get("catalogue_coverage")
                if catalogue_version == 3:
                    expected_omitted = json.loads(current["omitted_scopes_json"])
                    if readback_coverage != {
                        "historical_contract": current.get("historical_contract"),
                        "scope": "historical",
                        "omitted_scopes": expected_omitted,
                        "source_capture_id": capture_id,
                        "source_capture_complete": current.get("source_capture_complete"),
                    }:
                        raise RuntimeError(
                            "production catalogue coverage readback differs from release"
                        )

                rechecked = publish.current(store, catalogue_version=catalogue_version)
                rechecked_manifest_sha256 = digest(
                    regular_bytes(Path(rechecked["output"]) / "manifest.json", 1024**2)
                )
                if rechecked != current or rechecked_manifest_sha256 != manifest_sha256:
                    raise RuntimeError("current publication changed during production readback")
                source_info_after = source.lstat()
                if (
                    not stat.S_ISREG(source_info_after.st_mode)
                    or (source_info_after.st_dev, source_info_after.st_ino)
                    != (source_info.st_dev, source_info.st_ino)
                    or source_info_after.st_size != source_info.st_size
                    or file_digest(source, limits.max_database_bytes)[0] != checksum
                ):
                    raise RuntimeError("certified warehouse changed during production readback")
                if (
                    source.with_name(source.name + ".wal").exists()
                    or alias.with_name(alias.name + ".wal").exists()
                    or temporary.with_name(temporary.name + ".wal").exists()
                ):
                    raise RuntimeError("an uncheckpointed production DuckDB WAL exists")
                linked_after = temporary.lstat()
                if not stat.S_ISREG(linked_after.st_mode) or (
                    linked_after.st_dev,
                    linked_after.st_ino,
                ) != (source_info_after.st_dev, source_info_after.st_ino):
                    raise RuntimeError("temporary production link changed during readback")
                if alias.is_symlink() or (
                    alias.exists() and not stat.S_ISREG(alias.lstat().st_mode)
                ):
                    raise RuntimeError("production database path changed during readback")

                spill_reserved = False
                try:
                    shutil.rmtree(spill_directory, ignore_errors=True)
                    store.sync(spill_relative)
                finally:
                    store.release_storage(spill_budget)

                pruned = incremental.prune(
                    store, state, release_id, catalogue_version, _context=verification
                )
                rechecked = publish.current(store, catalogue_version=catalogue_version)
                rechecked_manifest_sha256 = digest(
                    regular_bytes(Path(rechecked["output"]) / "manifest.json", 1024**2)
                )
                if rechecked != current or rechecked_manifest_sha256 != manifest_sha256:
                    raise RuntimeError("current publication changed during retention cleanup")
                receipt = warehouse.certified(
                    store, capture_id, catalogue_version=catalogue_version, _context=verification
                )
                if receipt.get("warehouse_sha256") != checksum:
                    raise RuntimeError("certified warehouse changed during retention cleanup")
                source_info_after = source.lstat()
                if (
                    not stat.S_ISREG(source_info_after.st_mode)
                    or source.is_symlink()
                    or (source_info_after.st_dev, source_info_after.st_ino)
                    != (source_info.st_dev, source_info.st_ino)
                    or source_info_after.st_size != source_info.st_size
                    or file_digest(source, limits.max_database_bytes)[0] != checksum
                ):
                    raise RuntimeError("certified warehouse changed before production promotion")
                if (
                    source.with_name(source.name + ".wal").exists()
                    or alias.with_name(alias.name + ".wal").exists()
                    or temporary.with_name(temporary.name + ".wal").exists()
                ):
                    raise RuntimeError("an uncheckpointed production DuckDB WAL exists")
                linked_after = temporary.lstat()
                if not stat.S_ISREG(linked_after.st_mode) or (
                    linked_after.st_dev,
                    linked_after.st_ino,
                ) != (source_info_after.st_dev, source_info_after.st_ino):
                    raise RuntimeError("temporary production link changed during retention cleanup")
                if alias.is_symlink() or (
                    alias.exists() and not stat.S_ISREG(alias.lstat().st_mode)
                ):
                    raise RuntimeError("production database path changed during retention cleanup")
                if verified_audit is not None:
                    latest_audit = _activation_audit_payload(directory, verified_audit)
                    latest_state = capture.read_capture(store, capture_id)
                    _validate_audit_current_capture(
                        latest_audit, store, latest_state, rechecked, capture_id
                    )
                replace_fn(temporary, alias)
                sync_fn(directory)
            finally:
                try:
                    with contextlib.suppress(FileNotFoundError):
                        temporary.unlink()
                finally:
                    if spill_reserved:
                        try:
                            shutil.rmtree(spill_directory, ignore_errors=True)
                            store.sync(spill_relative)
                        finally:
                            store.release_storage(spill_budget)
            alias_info = alias.stat()
            if (alias_info.st_dev, alias_info.st_ino) != (source_info.st_dev, source_info.st_ino):
                raise RuntimeError("production hard-link inode differs from certified warehouse")
            alias_checksum, _ = file_digest(alias, limits.max_database_bytes)
            if alias_checksum != checksum:
                raise RuntimeError("production hard-link checksum differs from certified warehouse")
            return {
                "state": "verified",
                "path": str(alias),
                "source_release_id": release_id,
                "capture_id": capture_id,
                "warehouse_sha256": checksum,
                "manifest_warehouse_sha256": manifest_checksum,
                "verification_mode": verification_mode,
                "database_bytes": alias_info.st_size,
                "pruned_artifacts": pruned,
                "production_readback": {
                    "current_markets": readback_rows["markets"],
                    "relation_rows": readback_rows,
                    "samples": samples,
                    "read_only": True,
                    **(
                        {"catalogue_coverage": readback_coverage}
                        if readback_coverage is not None
                        else {}
                    ),
                },
                "relation_rows": counts,
                "coverage_scope": current.get("coverage_scope"),
                "source_capture_complete": state.get("complete") is True,
                "scope_complete": True,
                **(
                    {"catalogue_coverage": readback_coverage}
                    if readback_coverage is not None
                    else {}
                ),
                "observation_interval": {
                    "started_at": state.get("started_at"),
                    "completed_at": state.get("completed_at"),
                },
                "atomic_snapshot": False,
                "recovered_activation_temporaries": recovered_temporaries,
                **(
                    {"activation_audit": activation_audit_result}
                    if activation_audit_result is not None
                    else {}
                ),
            }
