#!/usr/bin/env python3
"""Compare historical landing paths on bounded, verified saved receipts."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import time
from collections import Counter, defaultdict
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import Any

sys.dont_write_bytecode = True

MAX_CAPTURE_BYTES = 512 * 1024**2
MAX_CAPTURE_ENTRIES = 200_000
MAX_SAMPLE_PAGES = 256
MAX_SAMPLE_BYTES = 1024**3
MAX_SAMPLE_ROWS = 1_000_000
MAX_ARTIFACT_BYTES = 16 * 1024**3
MAX_CHILD_SECONDS = 180
MAX_TOTAL_SECONDS = 3600
UNKNOWN_POPULATION_MULTIPLIER = 2.0
RAW_ROOT_BASELINE_BYTES = 35_000_000_000
SAFE_CATEGORY_BYTES = 48 * 1024**3
CATEGORY_LIMIT_BYTES = 64 * 1024**3
ROOT_AGGREGATE_LIMIT_BYTES = 400_000_000_000
MANIFEST_LIMIT_BYTES = 1024**2
STATE_LIMIT_BYTES = 64 * 1024**2
DEFAULT_CAPTURE_ID = "19caddd934f74d13ad3d585b915601c9"


class ReadOnlyStore:
    """Small read-only adapter: Store() would reconcile the unrelated 270 GiB root."""

    def __init__(self, root: Path):
        from kalshi_events.common import safe_path

        self.root = safe_path(root)
        self.scope = None
        self.started = time.monotonic()
        self.limits = SimpleNamespace(
            max_root_bytes=MAX_CAPTURE_BYTES,
            max_response_bytes=384 * 1024**2,
            max_decoded_response_bytes=768 * 1024**2,
        )

    def path(self, relative: str) -> Path:
        from kalshi_events.common import CatalogueError, safe_path

        candidate = PurePosixPath(relative)
        if candidate.is_absolute() or any(part in {"", ".", ".."} for part in candidate.parts):
            raise CatalogueError("unsafe read-only sample path")
        path = safe_path(self.root.joinpath(*candidate.parts))
        if not path.is_relative_to(self.root):
            raise CatalogueError("sample path escaped the operator root")
        return path

    def check_deadline(self) -> None:
        from kalshi_events.common import BudgetExceeded

        if time.monotonic() - self.started > MAX_CHILD_SECONDS:
            raise BudgetExceeded("saved-receipt sampling exceeded its 180-second bound")


def _checkout() -> Path:
    return Path(__file__).resolve().parents[1]


def _source_path(root: Path, relative: str) -> Path:
    from kalshi_events.common import CatalogueError, safe_path

    candidate = PurePosixPath(relative)
    if candidate.is_absolute() or any(part in {"", ".", ".."} for part in candidate.parts):
        raise CatalogueError("unsafe sample path")
    path = safe_path(root.joinpath(*candidate.parts))
    if not path.is_relative_to(root):
        raise CatalogueError("sample path escaped the operator root")
    return path


def _read_json(path: Path, limit: int) -> Any:
    from kalshi_events.common import CatalogueError

    if path.is_symlink():
        raise CatalogueError(f"sample metadata cannot be a symlink: {path}")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode) or details.st_size > limit:
            raise CatalogueError(f"sample metadata is nonregular or exceeds {limit} bytes")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            raw = stream.read(limit + 1)
        if len(raw) > limit:
            raise CatalogueError("sample metadata grew beyond its byte bound")
        return json.loads(raw.decode("utf-8"))
    finally:
        os.close(descriptor)


def _capture_id(root: Path, requested: str | None) -> str:
    from kalshi_events.common import CatalogueError

    if requested is not None:
        if len(requested) != 32 or any(char not in "0123456789abcdef" for char in requested):
            raise CatalogueError("capture ID must be 32 lowercase hexadecimal characters")
        return requested
    captures = _source_path(root, "captures")
    candidates = []
    with os.scandir(captures) as children:
        for number, child in enumerate(children, start=1):
            if number > 1024:
                raise CatalogueError("operator root has too many captures to select safely")
            if child.is_symlink():
                raise CatalogueError("capture directories cannot be symlinks")
            if child.is_dir(follow_symlinks=False) and len(child.name) == 32:
                candidates.append(child.name)
    if requested is None and len(candidates) == 1:
        return candidates[0]
    raise CatalogueError("pass --capture-id when the operator root has multiple captures")


def _page_metadata(source: ReadOnlyStore, row: dict[str, Any], order: int) -> dict[str, Any]:
    from kalshi_events.common import CatalogueError

    relative = row.get("relative")
    if not isinstance(relative, str) or not relative.startswith("raw/"):
        raise CatalogueError("capture journal contains an invalid receipt path")
    manifest_path = source.path((PurePosixPath(relative).parent / "manifest.json").as_posix())
    manifest = _read_json(manifest_path, MANIFEST_LIMIT_BYTES)
    if not isinstance(manifest, dict):
        raise CatalogueError("receipt manifest is not an object")
    body_path = source.path((PurePosixPath(relative).parent / "body.bin").as_posix())
    if body_path.is_symlink() or not body_path.is_file():
        raise CatalogueError("sample receipt body is missing or unsafe")
    body_size = body_path.stat().st_size
    decoded_size = manifest.get("decoded_size", manifest.get("size"))
    generation = manifest.get("task_generation", manifest.get("request_generation", 0))
    endpoint = manifest.get("endpoint")
    if (
        isinstance(decoded_size, bool)
        or not isinstance(decoded_size, int)
        or decoded_size < 0
        or isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 0
        or not isinstance(endpoint, str)
    ):
        raise CatalogueError("receipt metadata lacks endpoint, size or generation")
    return {
        **row,
        "order": order,
        "endpoint": endpoint,
        "generation": generation,
        "encoded_bytes": body_size,
        "decoded_bytes": decoded_size,
        "status": manifest.get("status"),
        "manifest_bytes": manifest_path.stat().st_size,
    }


def _stratified_sample(pages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Choose deterministic position and size tails from every endpoint generation."""
    groups: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for page in pages:
        if page["status"] == 200:
            groups[(page["endpoint"], page["generation"])].append(page)
    selected: dict[str, dict[str, Any]] = {}
    for group in groups.values():
        ordered = sorted(group, key=lambda page: page["order"])
        by_size = sorted(ordered, key=lambda page: (page["decoded_bytes"], page["order"]))
        for page in (
            ordered[0],
            ordered[len(ordered) // 2],
            ordered[-1],
            by_size[0],
            by_size[len(by_size) // 2],
            by_size[-1],
        ):
            selected[page["relative"]] = page
    return sorted(selected.values(), key=lambda page: page["order"])


def _row_profile(rows: list[dict[str, Any]], payload: Any) -> dict[str, Any]:
    from kalshi_events.common import json_bytes

    unicode_scalars = 0
    max_scalar_bytes = 0
    max_combo_legs = 0
    wide_rule_fields = 0
    high_entropy_rule_fields = 0
    decimal_strings = 0

    def visit(value: Any, field: str = "") -> None:
        nonlocal unicode_scalars, max_scalar_bytes, max_combo_legs
        nonlocal wide_rule_fields, high_entropy_rule_fields, decimal_strings
        if isinstance(value, dict):
            for key, child in value.items():
                visit(child, key)
        elif isinstance(value, list):
            if field == "mve_selected_legs":
                max_combo_legs = max(max_combo_legs, len(value))
            for child in value:
                visit(child, field)
        elif isinstance(value, str):
            encoded = value.encode("utf-8")
            max_scalar_bytes = max(max_scalar_bytes, len(encoded))
            unicode_scalars += sum(ord(char) > 127 for char in value)
            if field in {"rules_primary", "rules_secondary"} and len(encoded) >= 4096:
                wide_rule_fields += 1
            if field in {"rules_primary", "rules_secondary"} and len(value) >= 256:
                high_entropy_rule_fields += len(set(value)) / len(value) >= 0.5
            decimal_strings += re.fullmatch(r"-?\d+\.\d+", value) is not None

    visit(payload)
    rules = (
        [
            row[key]
            for value in payload.values()
            if isinstance(value, list)
            for row in value
            if isinstance(row, dict)
            for key in ("rules_primary", "rules_secondary")
            if isinstance(row.get(key), str)
        ]
        if isinstance(payload, dict)
        else []
    )
    row_bytes = sum(
        len(value.encode("utf-8"))
        for row in rows
        for value in row.values()
        if isinstance(value, str)
    )
    return {
        "observations": len(rows),
        "landing_utf8_bytes": row_bytes,
        "payload_json_utf8_bytes": sum(len(row["payload_json"].encode("utf-8")) for row in rows),
        "max_scalar_utf8_bytes": max_scalar_bytes,
        "unicode_scalars": unicode_scalars,
        "max_combo_legs": max_combo_legs,
        "wide_rule_fields": wide_rule_fields,
        "high_entropy_rule_fields": high_entropy_rule_fields,
        "repeated_rule_values": sum(count - 1 for count in Counter(rules).values() if count > 1),
        "decimal_strings": decimal_strings,
        "canonical_payload_sha256": hashlib.sha256(json_bytes(payload)).hexdigest(),
    }


def _verify_and_copy(
    source: ReadOnlyStore,
    state: dict[str, Any],
    selected: list[dict[str, Any]],
    fixture_roots: list[Path],
) -> list[dict[str, Any]]:
    checkout = _checkout()
    if str(checkout / "src") not in sys.path:
        sys.path.insert(0, str(checkout / "src"))
    from kalshi_events import capture, warehouse
    from kalshi_events.common import CatalogueError, atomic_json
    from kalshi_events.normalize import observations
    from kalshi_events.transport import read_page

    sample_rows = []
    sample_bytes = 0
    observed_rows = 0
    entity_counts: dict[tuple[str, str], int] = defaultdict(int)
    page_rows = []
    for page in selected:
        source.check_deadline()
        manifest, payload = read_page(
            source,
            page["relative"],
            state["max_response_bytes"],
            state.get("max_decoded_response_bytes", state["max_response_bytes"]),
        )
        if not capture._receipt_matches_state_protocol(manifest, state):
            raise CatalogueError("sample receipt protocol differs from its capture")
        capture._validate_receipt_task_binding(manifest, state, page)
        if manifest.get("status") != 200:
            raise CatalogueError("sample contains a non-success receipt")
        scoped = warehouse._scope_manifest(state, manifest)
        rows = list(observations(((scoped, payload),)))
        observed_rows += len(rows)
        if observed_rows > MAX_SAMPLE_ROWS:
            raise CatalogueError("sample contains too many observations to profile safely")
        for row in rows:
            entity_counts[(row["entity_kind"], row["entity_ticker"])] += 1
        profile = _row_profile(rows, payload)
        sample_rows.append(
            {
                "endpoint": page["endpoint"],
                "generation": page["generation"],
                "position": page["position"],
                "encoded_bytes": page["encoded_bytes"],
                "decoded_bytes": page["decoded_bytes"],
                **profile,
            }
        )
        sample_bytes += page["encoded_bytes"]
        page_rows.append({key: page[key] for key in ("index", "relative", "request_id")})
        for fixture_root in fixture_roots:
            destination = fixture_root / page["relative"]
            destination.parent.mkdir(parents=True, exist_ok=True)
            source_manifest = source.path(
                (PurePosixPath(page["relative"]).parent / "manifest.json").as_posix()
            )
            source_body = source.path(
                (PurePosixPath(page["relative"]).parent / "body.bin").as_posix()
            )
            shutil.copyfile(source_manifest, destination.with_name("manifest.json"))
            shutil.copyfile(source_body, destination.with_name("body.bin"))

    if sample_bytes > MAX_SAMPLE_BYTES or len(selected) > MAX_SAMPLE_PAGES:
        raise CatalogueError("selected receipt sample exceeds its fixed size bound")
    if not sample_rows or not any(row["observations"] for row in sample_rows):
        raise CatalogueError("verified receipt sample contains no observations")
    sample_rows.append(
        {
            "sample_duplicate_entities": sum(
                count - 1 for count in entity_counts.values() if count > 1
            )
        }
    )

    fixture_state = dict(state)
    fixture_state.pop("journal_version", None)
    fixture_state.pop("_journal", None)
    for fixture_root in fixture_roots:
        fixture_root.mkdir(parents=True, exist_ok=True)
        for name in ("root.json", "root-id.json"):
            source_file = source.root / name
            if source_file.is_file():
                shutil.copyfile(source_file, fixture_root / name)
        capture_directory = fixture_root / "captures" / state["capture_id"]
        capture_directory.mkdir(parents=True, exist_ok=True)
        fixture_state["complete"] = False
        atomic_json(fixture_root / "diagnostic-state.json", fixture_state)
        atomic_json(
            fixture_root / ".diagnostic-sample.json",
            {"kind": "verified-receipt-subset", "source_capture_id": state["capture_id"]},
        )
        journal = b"".join(
            json.dumps(row, sort_keys=True, separators=(",", ":")).encode() + b"\n"
            for row in page_rows
        )
        (capture_directory / "pages.jsonl").write_bytes(journal)
    return sample_rows


def _tree_bytes(root: Path) -> tuple[int, int]:
    if not root.exists():
        return 0, 0
    total = entries = 0
    pending = [root]
    while pending:
        directory = pending.pop()
        with os.scandir(directory) as children:
            for child in children:
                entries += 1
                if entries > 1_000_000 or child.is_symlink():
                    raise RuntimeError(
                        "benchmark artifact tree is unsafe or exceeds its entry bound"
                    )
                info = child.stat(follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    pending.append(Path(child.path))
                elif stat.S_ISREG(info.st_mode):
                    total += info.st_size
                else:
                    raise RuntimeError("benchmark artifacts must be regular files and directories")
    return total, entries


def _terminate_group(process: subprocess.Popen[Any]) -> None:
    if process.poll() is not None:
        return
    with suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=2)


def _run_child(
    method: str,
    fixture_root: Path,
    report_root: Path,
    deadline: float,
    capture_id: str,
    task_indices: set[int],
) -> dict[str, Any]:
    checkout = _checkout()
    child_report = report_root / f"{method}.json"
    log_path = report_root / f"{method}.log"
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        method,
        "--fixture-root",
        str(fixture_root),
        "--worker-report",
        str(child_report),
        "--capture-id",
        capture_id,
        "--task-indices",
        *(str(index) for index in sorted(task_indices)),
    ]
    env = {
        key: os.environ[key]
        for key in ("PATH", "LANG", "LC_ALL", "TMPDIR", "VIRTUAL_ENV")
        if key in os.environ
    }
    env["PYTHONNOUSERSITE"] = "1"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PYTHONPATH"] = str(checkout / "src")
    with log_path.open("wb") as log:
        process = subprocess.Popen(
            command,
            cwd=checkout,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        started = time.monotonic()
        peak_temp_bytes = 0
        while process.poll() is None:
            if time.monotonic() >= deadline:
                _terminate_group(process)
                raise TimeoutError("aggregate one-hour benchmark window expired")
            if time.monotonic() - started > MAX_CHILD_SECONDS:
                _terminate_group(process)
                raise TimeoutError(f"{method} worker exceeded 180 seconds")
            size, _ = _tree_bytes(report_root)
            if size > MAX_ARTIFACT_BYTES:
                _terminate_group(process)
                raise RuntimeError("benchmark artifacts exceeded 16 GiB")
            peak_temp_bytes = max(peak_temp_bytes, _tree_bytes(fixture_root / "tmp")[0])
            time.sleep(0.25)
        if process.returncode:
            raise RuntimeError(f"{method} worker failed; inspect {log_path}")
    result = _read_json(child_report, 1024**2)
    result["peak_temporary_bytes"] = peak_temp_bytes
    return result


def _compare_databases(legacy_path: Path, native_path: Path) -> dict[str, Any]:
    import duckdb

    from kalshi_events.warehouse import RELATIONS

    connection = duckdb.connect(str(native_path), read_only=True)
    try:
        escaped = str(legacy_path).replace("'", "''")
        connection.execute(f"attach '{escaped}' as legacy (read_only)")
        relations = {
            "landing.observations": ("landing", "observations"),
            **{f"main.{table}": ("main", table) for table in RELATIONS.values()},
        }
        comparison = {}
        for label, (schema_name, table) in relations.items():
            native_schema = connection.execute(
                f"describe select * from main.{schema_name}.{table}"
            ).fetchall()
            legacy_schema = connection.execute(
                f"describe select * from legacy.{schema_name}.{table}"
            ).fetchall()
            if native_schema != legacy_schema:
                raise RuntimeError(f"legacy and native schemas differ for {label}")
            names = [row[0] for row in native_schema]
            projection = ", ".join(
                f'encode("{name}") as "{name}"' if "VARCHAR" in row[1].upper() else f'"{name}"'
                for name, row in zip(names, native_schema, strict=True)
            )
            differences = {}
            for left, right in (("main", "legacy"), ("legacy", "main")):
                differences[f"{left}_minus_{right}"] = connection.execute(
                    f"select count(*) from (select {projection} from {left}.{schema_name}.{table} "
                    f"except all select {projection} from {right}.{schema_name}.{table})"
                ).fetchone()[0]
            counts = {
                database: connection.execute(
                    f"select count(*) from {database}.{schema_name}.{table}"
                ).fetchone()[0]
                for database in ("main", "legacy")
            }
            comparison[label] = {
                "schema": native_schema,
                "row_counts": counts,
                "except_all_utf8_differences": differences,
                "equal": counts["main"] == counts["legacy"] and not any(differences.values()),
            }
        return {
            "relations": comparison,
            "equal": all(item["equal"] for item in comparison.values()),
        }
    finally:
        connection.close()


def _quantile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(fraction * len(ordered)) - 1)]


def _retained_baseline(root: Path) -> dict[str, int]:
    """Stat published database and export files without opening their contents."""
    from kalshi_events.common import CatalogueError, safe_path

    databases = exports = entries = 0
    for name in ("builds", "builds-historical", "releases"):
        base = root / name
        if not base.exists():
            continue
        base = safe_path(base)
        pending = [base]
        while pending:
            directory = pending.pop()
            with os.scandir(directory) as children:
                for child in children:
                    entries += 1
                    if entries > 100_000 or child.is_symlink():
                        raise CatalogueError("retained baseline inventory exceeds its safe bound")
                    details = child.stat(follow_symlinks=False)
                    if stat.S_ISDIR(details.st_mode):
                        pending.append(Path(child.path))
                    elif stat.S_ISREG(details.st_mode):
                        if child.name.endswith((".duckdb", ".wal")):
                            databases += details.st_size
                        elif child.name.endswith(".parquet"):
                            exports += details.st_size
                    else:
                        raise CatalogueError("retained baseline has a nonregular entry")
    return {
        "database_bytes": databases,
        "export_bytes": exports,
        "total_bytes": databases + exports,
    }


def _protected_index_bytes(root: Path) -> int:
    from kalshi_events.common import CatalogueError

    directory = root / "tmp"
    if not directory.exists():
        return 0
    total = 0
    for path in directory.glob("parent-index-*/index.sqlite"):
        if path.is_symlink() or not path.is_file():
            raise CatalogueError("protected parent index is missing or unsafe")
        total += path.stat().st_size
    return total


def _abandoned_staging_bytes(root: Path) -> int:
    from kalshi_events.common import CatalogueError

    directory = root / "tmp"
    if not directory.exists():
        return 0
    total = 0
    for path in directory.glob("build-historical-*/warehouse.duckdb"):
        if path.is_symlink() or not path.is_file():
            raise CatalogueError("abandoned historical stage is missing or unsafe")
        total += path.stat().st_size
    return total


def _sampling_coverage(
    candidates: list[dict[str, Any]], selected: list[dict[str, Any]]
) -> dict[str, Any]:
    groups: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    sampled = {page["relative"] for page in selected}
    for page in candidates:
        if page["status"] == 200:
            groups[(page["endpoint"], page["generation"])].append(page)
    missing = []
    for key, pages in sorted(groups.items()):
        ordered = sorted(pages, key=lambda page: page["order"])
        size_order = sorted(ordered, key=lambda page: (page["decoded_bytes"], page["order"]))
        required = {
            ordered[0]["relative"],
            ordered[len(ordered) // 2]["relative"],
            ordered[-1]["relative"],
            size_order[0]["relative"],
            size_order[len(size_order) // 2]["relative"],
            size_order[-1]["relative"],
        }
        if not required.issubset(sampled):
            missing.append({"endpoint": key[0], "generation": key[1]})
    return {
        "candidate_pages": sum(len(group) for group in groups.values()),
        "sampled_pages": len(selected),
        "endpoint_generation_groups": [
            {"endpoint": endpoint, "generation": generation, "pages": len(group)}
            for (endpoint, generation), group in sorted(groups.items())
        ],
        "missing_position_or_size_tails": missing,
        "metadata_coverage_complete": bool(groups) and not missing,
    }


def _historical_task_plan(
    state: dict[str, Any],
) -> tuple[dict[str, Any], set[int], list[dict[str, Any]]]:
    from kalshi_events.common import CatalogueError
    from kalshi_events.historical import historical_selection, historical_task_indices
    from kalshi_events.publish import HISTORICAL_OMITTED_SCOPES

    selection = historical_selection(state, allow_legacy_incomplete=True)
    task_indices = historical_task_indices(state, allow_legacy_incomplete=True)
    expected_scopes = {"series", "events_live", "markets_history"}
    if set(selection) != expected_scopes:
        raise CatalogueError("historical sample selection does not match the publication contract")

    task_scopes = {int(task_index): scope for task_index, scope in state["task_scopes"].items()}
    retired_scopes = set(HISTORICAL_OMITTED_SCOPES)
    excluded_retired = [
        {
            "task_index": task_index,
            "scope": scope,
            "endpoint": state["tasks"][task_index]["endpoint"],
        }
        for task_index, scope in sorted(task_scopes.items())
        if scope in retired_scopes
    ]
    if any(item["task_index"] in task_indices for item in excluded_retired):
        raise CatalogueError("retired historical scopes entered the selected sample tasks")

    cutoff_indices = {
        index
        for index, task in enumerate(state["tasks"])
        if task.get("endpoint") == "/historical/cutoff" and task.get("singular") is True
    }
    expected_indices = {
        *(item["task_index"] for item in selection.values()),
        *cutoff_indices,
    }
    if len(cutoff_indices) != 1 or task_indices != expected_indices:
        raise CatalogueError("historical sample task indices differ from the selected contract")
    return selection, task_indices, excluded_retired


def _capacity_forecast(
    sample: list[dict[str, Any]],
    legacy: dict[str, Any],
    native: dict[str, Any],
    export_bytes: int,
    retained_baseline: dict[str, int],
    raw_root_baseline_bytes: int,
    protected_index_bytes: int,
    abandoned_staging_bytes: int,
    target_observations: int,
) -> dict[str, Any]:
    from kalshi_events.common import CatalogueError

    pages = [page for page in sample if "observations" in page]
    rows = sum(page["observations"] for page in pages)
    input_bytes = sum(page["landing_utf8_bytes"] for page in pages)
    native_db = native["database_bytes"]
    if rows <= 0 or input_bytes <= 0:
        raise CatalogueError("capacity forecast requires a nonempty verified sample")
    per_page = [
        page["landing_utf8_bytes"] / page["observations"] for page in pages if page["observations"]
    ]
    ratio = native_db / input_bytes
    mean_row_bytes = input_bytes / rows
    high_row_bytes = max(_quantile(per_page, 0.95), max(per_page))
    point_database = math.ceil(target_observations * mean_row_bytes * ratio)
    upper_observations = math.ceil(target_observations * UNKNOWN_POPULATION_MULTIPLIER)
    upper_database = math.ceil(upper_observations * high_row_bytes * ratio * 1.20)
    point_export = math.ceil(target_observations * export_bytes / rows)
    upper_export = math.ceil(point_export * UNKNOWN_POPULATION_MULTIPLIER * 1.20)
    point_temp = math.ceil(target_observations * native["peak_temporary_bytes"] / rows)
    measured_temp_upper = math.ceil(
        upper_observations * native["peak_temporary_bytes"] / rows * 1.20
    )
    upper_spill_reserve = 4 * 1024**3
    wal_reserve = 256 * 1024**2
    projected_database = retained_baseline["database_bytes"] + upper_database
    projected_temp = protected_index_bytes + max(
        measured_temp_upper,
        upper_database + upper_export + upper_spill_reserve + wal_reserve,
    )
    projected_root = raw_root_baseline_bytes + retained_baseline["total_bytes"] + projected_temp
    unknown_edges = [
        name
        for name, present in (
            ("wide_rules", sum(page["wide_rule_fields"] for page in pages) > 0),
            ("high_entropy_rules", sum(page["high_entropy_rule_fields"] for page in pages) > 0),
            ("repeated_rules", sum(page["repeated_rule_values"] for page in pages) > 0),
            ("combo_legs", max((page["max_combo_legs"] for page in pages), default=0) > 0),
            ("unicode", sum(page["unicode_scalars"] for page in pages) > 0),
            ("decimal_strings", sum(page["decimal_strings"] for page in pages) > 0),
            ("duplicate_entities", sample[-1].get("sample_duplicate_entities", 0) > 0),
        )
        if not present
    ]
    covered = sample[-1].get("metadata_coverage_complete") is True and not unknown_edges
    forecast_fits = (
        projected_database <= SAFE_CATEGORY_BYTES
        and projected_temp <= SAFE_CATEGORY_BYTES
        and projected_database <= CATEGORY_LIMIT_BYTES
        and projected_temp <= CATEGORY_LIMIT_BYTES
        and projected_root <= ROOT_AGGREGATE_LIMIT_BYTES
    )
    preconditions_clear = abandoned_staging_bytes == 0
    forecast_status = (
        "insufficient_sample_coverage"
        if not covered
        else "sensitivity_exceeds_capacity"
        if not forecast_fits
        else "sensitivity_fits; fresh admission still required"
    )
    return {
        "target_observations_estimate": target_observations,
        "population_sensitivity_multiplier": UNKNOWN_POPULATION_MULTIPLIER,
        "upper_observations": upper_observations,
        "sample_observations": rows,
        "legacy_database_bytes": legacy["database_bytes"],
        "sample_landing_utf8_bytes": input_bytes,
        "native_compression_ratio_to_landing_utf8": ratio,
        "sample_row_bytes_per_observation": {
            "mean": mean_row_bytes,
            "p95_page": _quantile(per_page, 0.95),
            "observed_max_page": max(per_page),
        },
        "projected_database_bytes": {
            "point": retained_baseline["database_bytes"] + point_database,
            "upper": projected_database,
            "retained_baseline": retained_baseline["database_bytes"],
        },
        "projected_temporary_peak_bytes": {
            "point": protected_index_bytes + point_temp,
            "upper": projected_temp,
            "components_upper": {
                "native_database": upper_database,
                "zstd6_export": upper_export,
                "protected_index_baseline": protected_index_bytes,
                "measured_temporary_peak_scaled": measured_temp_upper,
                "minimum_spill_reserve": upper_spill_reserve,
                "wal_reserve": wal_reserve,
            },
        },
        "projected_root_bytes": {
            "upper": projected_root,
            "raw_root_baseline_unscaled": raw_root_baseline_bytes,
            "retained_baseline": retained_baseline["total_bytes"],
        },
        "preexisting_abandoned_staging_bytes": abandoned_staging_bytes,
        "operational_preconditions_clear": preconditions_clear,
        "fresh_post_cleanup_admission_required": True,
        "category_limit_bytes": CATEGORY_LIMIT_BYTES,
        "admission_threshold_bytes": SAFE_CATEGORY_BYTES,
        "root_aggregate_limit_bytes": ROOT_AGGREGATE_LIMIT_BYTES,
        "sample_coverage_sufficient": covered,
        "unknown_sample_strata": unknown_edges,
        "forecast_fits": forecast_fits,
        "forecast_status": forecast_status,
        "capacity_admitted": False,
        "production_admitted": False,
        "limitations": [
            "the catalog row estimate is uncertain; capacity sensitivity doubles the population estimate",
            "the sampled maximum page size and 20% mix margin form a conservative sensitivity range, not a formal confidence interval",
            "the temporary peak is sampled during a bounded diagnostic run and scaled by the twofold population sensitivity",
            "database size includes compact views and key tables; exports use measured ZSTD level 6 Parquet output",
        ],
    }


def _worker(args: argparse.Namespace) -> int:
    checkout = _checkout()
    sys.path.insert(0, str(checkout / "src"))
    from kalshi_events import publish, warehouse
    from kalshi_events.common import Limits, Store, atomic_json, decode, regular_bytes
    from kalshi_events.normalize import DiskParentIndex

    fixture_root = args.fixture_root.resolve()
    report_path = args.worker_report.absolute()
    data_root = (checkout / "data").resolve()
    if (
        not fixture_root.is_relative_to(data_root)
        or not report_path.is_relative_to(data_root)
        or fixture_root == report_path
        or report_path.is_relative_to(fixture_root)
        or any(path.is_symlink() for path in (report_path, *report_path.parents))
    ):
        raise RuntimeError(
            "worker fixture and report must be separate paths beneath checkout data/"
        )
    state = decode(regular_bytes(fixture_root / "diagnostic-state.json", STATE_LIMIT_BYTES))
    task_indices = set(args.task_indices)
    store = Store(
        fixture_root,
        Limits(
            max_duration_seconds=MAX_CHILD_SECONDS,
            max_root_bytes=MAX_ARTIFACT_BYTES,
            max_temp_bytes=MAX_ARTIFACT_BYTES,
            max_database_bytes=MAX_ARTIFACT_BYTES,
        ),
    )
    started = time.monotonic()
    cpu_started = time.process_time()
    native_loader = warehouse._land_historical_rows
    selected_loader = warehouse._land if args.worker == "legacy" else native_loader
    directory = store.path("tmp/compact-benchmark")
    directory.mkdir(parents=True)
    database = directory / "warehouse.duckdb"
    verified_pages: dict[str, str] = {}
    index = DiskParentIndex(store, collect_winners=False)
    loader_started = time.monotonic()
    count = selected_loader(
        store,
        state,
        directory,
        verified_pages=verified_pages,
        parent_index=index,
        task_indices=task_indices,
    )
    loader_seconds = time.monotonic() - loader_started
    database_relative = database.relative_to(store.root).as_posix()
    with (
        warehouse._duckdb_work_config(store, directory) as config,
        warehouse.duckdb.connect(str(database), config=config) as connection,
    ):
        warehouse._projection_membership(store, state, index, connection)
        warehouse._write_historical_coverage(connection, state)
    store.sync_file(database_relative)
    warehouse.dbt(
        store,
        ["build"],
        database=database_relative,
        artifacts=f"{directory.relative_to(store.root).as_posix()}/build-artifacts.json",
        variables={
            "catalogue_version": 2,
            "incremental_update": False,
            "current_capture_id": args.capture_id,
            "delta_capture_id": args.capture_id,
            "historical_compact": True,
        },
    )
    store.sync(directory.relative_to(store.root).as_posix())
    export_dir = store.path("tmp/compact-exports")
    export_dir.mkdir(parents=True)
    with publish._export_connection(store, database, export_dir) as connection:
        for name, table in publish.RELATIONS.items():
            destination = export_dir / f"{name}.parquet"
            connection.execute(
                f"copy (select * from main.{table} order by {publish.ORDER_KEYS[name]}) "
                "to ? (format parquet, compression zstd, compression_level 6)",
                [str(destination)],
            )
    store.sync("tmp/compact-exports")
    total_bytes, _ = _tree_bytes(fixture_root)
    export_bytes = sum(path.stat().st_size for path in export_dir.glob("*.parquet"))
    verified = verified_pages
    import resource

    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform != "darwin":
        rss *= 1024
    report = {
        "method": args.worker,
        "scope": "diagnostic-only subset; uncertified and unpublished",
        "certified": False,
        "published": False,
        "observations": count,
        "loader_seconds": loader_seconds,
        "verified_pages": len(verified),
        "verified_evidence_sha256": verified,
        "database_bytes": database.stat().st_size,
        "wal_bytes": database.with_name(database.name + ".wal").stat().st_size
        if database.with_name(database.name + ".wal").exists()
        else 0,
        "export_bytes": export_bytes,
        "export_files": sorted(path.name for path in export_dir.glob("*.parquet")),
        "fixture_and_artifact_bytes": total_bytes,
        "elapsed_seconds": time.monotonic() - started,
        "timing_interpretation": (
            "loader_seconds compares the selected landing paths; elapsed_seconds also includes "
            "the shared compact projection, dbt build and ZSTD level 6 diagnostic exports"
        ),
        "cpu_seconds": time.process_time() - cpu_started,
        "peak_rss_bytes": int(rss),
        "database": str(database),
    }
    index.close()
    atomic_json(report_path, report)
    return 0


def _benchmark(args: argparse.Namespace) -> int:
    checkout = _checkout()
    if str(checkout / "src") not in sys.path:
        sys.path.insert(0, str(checkout / "src"))
    from kalshi_events import capture
    from kalshi_events.common import CatalogueError, atomic_json

    started_utc = datetime.now(UTC).isoformat()
    started_monotonic = time.monotonic()
    deadline = started_monotonic + MAX_TOTAL_SECONDS
    checkout = _checkout()
    source_root = args.operator_root.absolute()
    output = args.output_dir.absolute()
    data_root = (checkout / "data").resolve()
    if not output.is_relative_to(data_root) or output == data_root:
        raise CatalogueError("benchmark output must be a descendant of checkout data/")
    if (
        source_root == output
        or source_root.is_relative_to(output)
        or output.is_relative_to(source_root)
    ):
        raise CatalogueError("benchmark output must be separate from the read-only operator root")
    if output.exists() or any(path.is_symlink() for path in (output, *output.parents)):
        raise CatalogueError("benchmark output must be a new path without symlink ancestors")
    output.mkdir(parents=True)
    report_path = output / "report.json"
    report: dict[str, Any] = {
        "version": 1,
        "status": "running",
        "scope": "diagnostic-only subset; uncertified and unpublished",
        "certified": False,
        "published": False,
        "started_at_utc": started_utc,
        "started_monotonic_ns": time.monotonic_ns(),
        "operator_root": str(source_root),
        "capture_id": args.capture_id,
        "revision": subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=checkout, capture_output=True, text=True, check=False
        ).stdout.strip(),
        "artifacts": {"directory": str(output), "limit_bytes": MAX_ARTIFACT_BYTES},
    }

    def save(status: str, **fields: Any) -> None:
        report.update(fields)
        report["status"] = status
        report["finished_at_utc"] = datetime.now(UTC).isoformat()
        report["elapsed_seconds"] = time.monotonic() - started_monotonic
        atomic_json(report_path, report)

    save("running")
    try:
        capture_id = _capture_id(source_root, args.capture_id)
        report["capture_id"] = capture_id
        source = ReadOnlyStore(source_root)
        state = capture.read_capture(source, capture_id)
        selection, task_indices, retired_scopes_excluded = _historical_task_plan(state)
        candidates = []
        bytes_scanned = 0
        if len(state.page_rows) > MAX_CAPTURE_ENTRIES:
            raise CatalogueError("capture has too many receipt entries for bounded sampling")
        for order, row in enumerate(state.page_rows):
            source.check_deadline()
            if row["index"] not in task_indices:
                continue
            meta = _page_metadata(source, row, order)
            bytes_scanned += meta["manifest_bytes"]
            if bytes_scanned > MAX_CAPTURE_BYTES:
                raise CatalogueError("receipt metadata scan exceeds its byte bound")
            candidates.append(meta)
        selected = _stratified_sample(candidates)
        if (
            len(selected) > MAX_SAMPLE_PAGES
            or sum(page["encoded_bytes"] for page in selected) > MAX_SAMPLE_BYTES
        ):
            raise CatalogueError("stratified sample exceeds its fixed page or byte bound")
        # Record the position within each endpoint/generation used by the sample.
        group_positions: dict[tuple[str, int], int] = defaultdict(int)
        for page in candidates:
            key = (page["endpoint"], page["generation"])
            page["position"] = group_positions[key]
            group_positions[key] += 1
        for page in selected:
            group = [
                candidate
                for candidate in candidates
                if (candidate["endpoint"], candidate["generation"])
                == (page["endpoint"], page["generation"])
            ]
            page["position"] = group.index(page)
        coverage = _sampling_coverage(candidates, selected)
        report.update(
            capture_id=capture_id,
            capture_revision=state.get("revision"),
            capture_state_sha256=getattr(state, "verified_snapshot_sha256", None),
            selected_task_indices=sorted(task_indices),
            historical_selection=selection,
            retired_scopes_excluded=retired_scopes_excluded,
            sampled_receipts=len(selected),
            sampled_encoded_bytes=sum(page["encoded_bytes"] for page in selected),
            sampled_strata=sorted(
                {f"{page['endpoint']}#g{page['generation']}" for page in selected}
            ),
        )
        fixture_roots = [output / "fixture-legacy", output / "fixture-native"]
        profiles = _verify_and_copy(source, state, selected, fixture_roots)
        duplicate_summary = profiles.pop()
        profiles.append(
            {
                **duplicate_summary,
                "metadata_coverage_complete": coverage["metadata_coverage_complete"],
            }
        )
        report["sampling_coverage"] = coverage
        report["sample_profiles"] = profiles
        after_fixture_bytes, _ = _tree_bytes(output)
        if after_fixture_bytes > MAX_ARTIFACT_BYTES:
            raise CatalogueError("sample fixtures exceed the 16 GiB artifact limit")
        save("sampled")

        worker_reports = []
        for method, fixture_root in zip(("legacy", "native"), fixture_roots, strict=True):
            worker_reports.append(
                _run_child(method, fixture_root, output, deadline, capture_id, task_indices)
            )
        report["workers"] = worker_reports
        legacy = next(item for item in worker_reports if item["method"] == "legacy")
        native = next(item for item in worker_reports if item["method"] == "native")
        parity = _compare_databases(Path(legacy["database"]), Path(native["database"]))
        report["parity"] = parity
        export_bytes = native["export_bytes"]
        report["historical_zstd6_export_bytes"] = export_bytes
        retained_baseline = _retained_baseline(source.root)
        report["retained_baseline"] = retained_baseline
        report["raw_root_baseline_bytes"] = RAW_ROOT_BASELINE_BYTES
        protected_index_bytes = _protected_index_bytes(source.root)
        report["protected_index_baseline_bytes"] = protected_index_bytes
        abandoned_staging_bytes = _abandoned_staging_bytes(source.root)
        report["preexisting_abandoned_staging_bytes"] = abandoned_staging_bytes
        target = _read_json(
            checkout / "data/testing/historical-compact-20261030/reference/baseline.json", 16 * 1024
        )
        target_observations = target.get("catalog_row_estimate")
        if (
            target.get("catalog_rows_are_estimated") is not True
            or isinstance(target_observations, bool)
            or not isinstance(target_observations, int)
            or target_observations <= 0
        ):
            raise CatalogueError("archived catalog row estimate is invalid")
        report["target_observations"] = target
        report["capacity"] = _capacity_forecast(
            profiles,
            legacy,
            native,
            export_bytes,
            retained_baseline,
            RAW_ROOT_BASELINE_BYTES,
            protected_index_bytes,
            abandoned_staging_bytes,
            target_observations,
        )
        report["timing_interpretation"] = (
            "loader_seconds compares legacy dlt merge landing with native landing; both workers "
            "use the current compact projection, dbt build and ZSTD level 6 diagnostic exports"
        )
        total_bytes, _ = _tree_bytes(output)
        if total_bytes > MAX_ARTIFACT_BYTES:
            raise CatalogueError("benchmark artifacts exceed the 16 GiB artifact limit")
        if time.monotonic() > deadline:
            raise TimeoutError("aggregate one-hour benchmark window expired")
        diagnostic_complete = parity["equal"]
        save(
            "diagnostic_complete" if diagnostic_complete else "parity_failed",
            artifact_bytes=total_bytes,
            production_admitted=False,
        )
        return 0 if diagnostic_complete else 2
    except Exception as exc:
        save("not_admitted", error=f"{type(exc).__name__}: {exc}")
        return 2


def _parser() -> argparse.ArgumentParser:
    checkout = _checkout()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--operator-root",
        type=Path,
        default=checkout / "data/kalshi-production/operator",
        help="read-only operator root containing the saved capture receipts",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="new report and fixture directory beneath checkout data/",
    )
    parser.add_argument("--worker", choices=("legacy", "native"), help=argparse.SUPPRESS)
    parser.add_argument("--fixture-root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--worker-report", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--task-indices", type=int, nargs="*", default=[], help=argparse.SUPPRESS)
    parser.add_argument("--capture-id", default=DEFAULT_CAPTURE_ID)
    return parser


def main() -> int:
    parser = _parser()
    args = parser.parse_args()
    if args.worker:
        if args.fixture_root is None or args.worker_report is None:
            parser.error("worker mode requires fixture and report paths")
        report_path = args.worker_report.absolute()
        data_root = (_checkout() / "data").resolve()
        if not report_path.is_relative_to(data_root) or any(
            path.is_symlink() for path in (report_path, *report_path.parents)
        ):
            parser.error("worker report must be a new safe path beneath checkout data/")
        try:
            return _worker(args)
        except Exception as exc:
            if args.worker_report is not None:
                from kalshi_events.common import atomic_json

                atomic_json(report_path, {"error": f"{type(exc).__name__}: {exc}"})
            return 2
    if args.output_dir is None:
        args.output_dir = (
            _checkout()
            / "data/testing/historical-compact-20261030/runs"
            / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        )
    return _benchmark(args)


if __name__ == "__main__":
    raise SystemExit(main())
