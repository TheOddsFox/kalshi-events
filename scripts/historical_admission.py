"""Bounded, diagnostic-only capacity admission for a saved historical capture."""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import signal
import stat
import subprocess
import sys
import time
from collections import defaultdict
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

INDEX_PAGE_FIELDS = {
    "decoded_bytes",
    "encoded_bytes",
    "endpoint",
    "generation",
    "index",
    "order",
    "relative",
    "request_id",
    "status",
}
GIB = 1024**3


class AdmissionChildFailure(RuntimeError):
    def __init__(self, message: str, partial_record: dict[str, Any]):
        super().__init__(message)
        self.partial_record = partial_record


AUTHORIZED_CAPTURE_ID = "19caddd934f74d13ad3d585b915601c9"
AUTHORIZED_ROOT_ID = "68d479ec30b54e4fb6a62129c0527c7a"
AUTHORIZED_STATE_SHA256 = "15d4ad2e733e4f012695238c487467f39056eaadd7317e3e3c573a1b20a5e059"
AUTHORIZED_INDEX_SHA256 = "e9de8868b7f23d9cd411713651e389e80c2ddaee8ae74d5f9232fab50dc3bc6c"
AUTHORIZED_POPULATION = {
    "candidate_pages": 83_397,
    "observations_by_endpoint": {
        "/events": 911_884,
        "/historical/markets": 78_835_045,
        "/series": 15_394,
    },
    "nested_event_markets": 0,
    "exact_observations": 79_762_323,
}


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _is_size(value: Any) -> bool:
    return type(value) is int and value >= 0


def _is_metric(value: Any) -> bool:
    return type(value) in {int, float} and math.isfinite(value) and value >= 0


def _admission_census_binding(
    source: Any,
    state: dict[str, Any],
    population_path: Path,
    receipt_index_path: Path,
    benchmark: Any,
    *,
    deadline: float,
) -> dict[str, Any]:
    from kalshi_events.common import CatalogueError

    population_raw = benchmark._read_regular_bytes(population_path, 1024**2)
    index_raw = benchmark._read_regular_bytes(receipt_index_path, 256 * 1024**2)
    population = json.loads(population_raw.decode("utf-8"))
    receipt_index = json.loads(index_raw.decode("utf-8"))
    state_sha = getattr(state, "verified_snapshot_sha256", None)
    if (
        not isinstance(population, dict)
        or not isinstance(receipt_index, dict)
        or population.get("status") != "complete"
        or receipt_index.get("capture_id") != state.get("capture_id")
        or population.get("capture_id") != state.get("capture_id")
        or not isinstance(state_sha, str)
        or population.get("capture_state_sha256") != state_sha
        or receipt_index.get("capture_state_sha256") != state_sha
        or population.get("metadata_index_sha256") != _sha256(index_raw)
    ):
        raise CatalogueError(
            "population census is not bound to the current capture and receipt index"
        )

    # The full acquisition may be incomplete because excluded scopes are unfinished;
    # this plan validates that each selected historical scope is complete.
    selection, task_indices, retired = benchmark._historical_task_plan(state)
    if (
        receipt_index.get("historical_selection") != selection
        or receipt_index.get("selected_task_indices") != sorted(task_indices)
        or receipt_index.get("retired_scopes_excluded") != retired
    ):
        raise CatalogueError("receipt index selection or task bindings differ from the capture")

    root_identity = benchmark._read_json(source.root / "root-id.json", 1024**2)
    if (
        not isinstance(root_identity, dict)
        or root_identity.get("version") != 1
        or not isinstance(root_identity.get("root_id"), str)
        or len(root_identity["root_id"]) != 32
        or any(char not in "0123456789abcdef" for char in root_identity["root_id"])
    ):
        raise CatalogueError("current operator root identity is missing or invalid")

    indexed = receipt_index.get("pages")
    page_rows = getattr(state, "page_rows", None)
    if (
        not isinstance(indexed, list)
        or not isinstance(page_rows, list)
        or len(indexed) != receipt_index.get("data_candidate_pages")
        or len(indexed) != population.get("candidate_pages")
        or population.get("pages_processed") != len(indexed)
        or population.get("next_index") != len(indexed)
    ):
        raise CatalogueError("receipt census page inventory is incomplete")
    indexed_by_order: dict[int, dict[str, Any]] = {}
    for entry in indexed:
        if (
            not isinstance(entry, dict)
            or set(entry) != INDEX_PAGE_FIELDS
            or not _is_size(entry.get("order"))
            or entry["order"] >= len(page_rows)
            or entry["order"] in indexed_by_order
            or type(entry.get("index")) is not int
            or entry["index"] < 0
            or not isinstance(entry.get("relative"), str)
            or not isinstance(entry.get("request_id"), str)
            or len(entry["request_id"]) != 64
            or any(char not in "0123456789abcdef" for char in entry["request_id"])
            or not isinstance(entry.get("endpoint"), str)
            or not _is_size(entry.get("generation"))
            or not _is_size(entry.get("encoded_bytes"))
            or not _is_size(entry.get("decoded_bytes"))
            or type(entry.get("status")) is not int
        ):
            raise CatalogueError("receipt census has an invalid or duplicate journal order")
        journal_row = page_rows[entry["order"]]
        if (
            not isinstance(journal_row, dict)
            or entry.get("index") != journal_row.get("index")
            or entry.get("relative") != journal_row.get("relative")
            or entry.get("request_id") != journal_row.get("request_id")
        ):
            raise CatalogueError("receipt census entry does not match its current journal order")
        indexed_by_order[entry["order"]] = entry

    data_pages = []
    controls = []
    selected_pages = 0
    manifest_bytes = 0
    for order, row in enumerate(page_rows):
        if row.get("index") not in task_indices:
            continue
        if time.monotonic() >= deadline:
            raise TimeoutError("admission deadline expired while rebinding receipt metadata")
        source.check_deadline()
        page = benchmark._page_metadata(source, row, order, state)
        selected_pages += 1
        manifest_bytes += page["manifest_bytes"]
        if benchmark._is_control_page(page):
            controls.append(page)
            if order in indexed_by_order:
                raise CatalogueError("control receipt was incorrectly included in the data index")
            continue
        entry = indexed_by_order.pop(order, None)
        if entry is None:
            raise CatalogueError("data receipt is missing from the census index")
        actual = {key: page[key] for key in INDEX_PAGE_FIELDS}
        if actual != entry:
            raise CatalogueError("indexed receipt metadata differs from the current operator root")
        data_pages.append(page)
    if indexed_by_order:
        raise CatalogueError("receipt census contains a foreign or unselected journal order")

    current_population = benchmark._receipt_population_metadata([*data_pages, *controls])
    data_strata = current_population["data_strata"]
    progress_strata = population.get("candidate_population", {}).get("data_strata")
    data_encoded = sum(page["encoded_bytes"] for page in data_pages)
    data_decoded = sum(page["decoded_bytes"] for page in data_pages)
    page_counts = {
        endpoint: count
        for endpoint, count in sorted(
            __import__("collections").Counter(page["endpoint"] for page in data_pages).items()
        )
    }
    if (
        progress_strata != data_strata
        or receipt_index.get("manifest_metadata_bytes") != manifest_bytes
        or population.get("candidate_population", {}).get("control_receipts") not in ([], None)
        or population.get("encoded_bytes_verified") != data_encoded
        or population.get("decoded_bytes_verified") != data_decoded
        or population.get("page_counts") != page_counts
    ):
        raise CatalogueError("population totals differ from the revalidated receipt census")

    observations = population.get("observation_counts")
    if not isinstance(observations, dict) or any(
        not _is_size(value) for value in observations.values()
    ):
        raise CatalogueError("population observation totals are missing or malformed")
    expected_endpoints = {row["endpoint"] for row in data_strata}
    if {key for key in observations if key.startswith("/")} != expected_endpoints or set(
        observations
    ) - expected_endpoints - {"nested_event_markets"}:
        raise CatalogueError("population observation endpoints differ from the capture strata")
    nested = observations.get("nested_event_markets", 0)
    if not _is_size(nested):
        raise CatalogueError("nested event-market population count is malformed")
    exact_observations = sum(value for key, value in observations.items() if key.startswith("/"))
    if exact_observations <= 0:
        raise CatalogueError("population census has no source observations")

    positions: dict[tuple[str, int], int] = defaultdict(int)
    for page in sorted(data_pages, key=lambda item: item["order"]):
        key = (page["endpoint"], page["generation"])
        page["position"] = positions[key]
        positions[key] += 1
    control_positions: dict[tuple[str, int, int], int] = defaultdict(int)
    for page in sorted(controls, key=lambda item: item["order"]):
        key = (page["endpoint"], page["generation"], page["status"])
        page["position"] = control_positions[key]
        control_positions[key] += 1

    return {
        "capture_id": state["capture_id"],
        "capture_state_sha256": state_sha,
        "root_id": root_identity["root_id"],
        "selection": selection,
        "task_indices": sorted(task_indices),
        "retired_scopes_excluded": retired,
        "candidates": data_pages,
        "controls": controls,
        "population": {
            "exact_observations": exact_observations,
            "observations_by_endpoint": {
                key: value for key, value in observations.items() if key.startswith("/")
            },
            "nested_event_markets": nested,
            "candidate_pages": len(data_pages),
            "encoded_bytes": data_encoded,
            "decoded_bytes": data_decoded,
            "data_strata": data_strata,
        },
        "control_receipt_counts": benchmark._control_receipt_counts(controls),
        "selected_task_receipts": selected_pages,
        "manifest_metadata_bytes": manifest_bytes,
        "fingerprints": {
            "population_report_sha256": _sha256(population_raw),
            "receipt_index_sha256": _sha256(index_raw),
            "capture_state_sha256": state_sha,
            "root_id": root_identity["root_id"],
            "selection_sha256": _sha256(
                json.dumps(
                    {
                        "selection": selection,
                        "task_indices": sorted(task_indices),
                        "orders": [page["order"] for page in data_pages],
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ),
        },
    }


def _admission_sample_order(
    candidates: list[dict[str, Any]], capture_id: str, hints: set[str] | None = None
) -> tuple[list[dict[str, Any]], dict[str, list[str]], list[dict[str, Any]]]:
    from kalshi_events.common import CatalogueError

    groups: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    by_relative = {page["relative"]: page for page in candidates}
    if len(by_relative) != len(candidates):
        raise CatalogueError("candidate receipt paths are not unique")
    hints = set() if hints is None else set(hints)
    if not hints.issubset(by_relative):
        raise CatalogueError("prior sample hint refers to a foreign receipt")
    for page in candidates:
        groups[(page["endpoint"], page["generation"])].append(page)

    reasons: dict[str, list[str]] = defaultdict(list)

    def seed(page: dict[str, Any], reason: str) -> None:
        if reason not in reasons[page["relative"]]:
            reasons[page["relative"]].append(reason)

    sorted_groups = {
        key: sorted(values, key=lambda page: page["order"])
        for key, values in sorted(groups.items())
    }
    for _key, values in sorted_groups.items():
        by_decoded = sorted(values, key=lambda page: (page["decoded_bytes"], page["order"]))
        by_compression = sorted(
            values,
            key=lambda page: (
                page["encoded_bytes"] / max(page["decoded_bytes"], 1),
                page["order"],
            ),
        )
        for page, reason in zip(
            (
                values[0],
                values[len(values) // 2],
                values[-1],
                by_decoded[0],
                by_decoded[len(values) // 2],
                by_decoded[-1],
                by_compression[-1],
            ),
            (
                "first_position",
                "middle_position",
                "last_position",
                "smallest_decoded",
                "middle_decoded",
                "largest_decoded",
                "poorest_compression",
            ),
            strict=True,
        ):
            seed(page, reason)
    for relative in sorted(hints):
        seed(by_relative[relative], "prior_combo_or_repeat_tail")

    extension: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for key, values in sorted_groups.items():
        shuffled = sorted(
            values,
            key=lambda page: hashlib.sha256(
                f"{capture_id}\0{key[0]}\0{key[1]}\0{page['request_id']}".encode()
            ).digest(),
        )
        extension[key] = [page for page in shuffled if page["relative"] not in reasons]

    ordered = [
        by_relative[relative]
        for relative in sorted(
            reasons,
            key=lambda value: (
                by_relative[value]["endpoint"],
                by_relative[value]["generation"],
                by_relative[value]["order"],
            ),
        )
    ]
    selected_counts = {
        key: sum(1 for page in ordered if (page["endpoint"], page["generation"]) == key)
        for key in sorted_groups
    }
    positions = {key: 0 for key in sorted_groups}
    while any(positions[key] < len(extension[key]) for key in sorted_groups):
        available = [key for key in sorted_groups if positions[key] < len(extension[key])]
        key = min(
            available,
            key=lambda candidate: (
                selected_counts[candidate] / len(sorted_groups[candidate]),
                candidate,
            ),
        )
        page = extension[key][positions[key]]
        positions[key] += 1
        ordered.append(page)
        selected_counts[key] += 1

    strata = [
        {
            "endpoint": endpoint,
            "generation": generation,
            "candidate_pages": len(values),
            "seeded_pages": sum(page["relative"] in reasons for page in values),
        }
        for (endpoint, generation), values in sorted_groups.items()
    ]
    return ordered, dict(reasons), strata


def _validated_stride_hints(
    report_path: Path,
    candidates: list[dict[str, Any]],
    binding: dict[str, Any],
    benchmark: Any,
) -> tuple[set[str], dict[str, Any]]:
    from kalshi_events.common import CatalogueError

    raw = benchmark._read_regular_bytes(report_path, 32 * 1024**2)
    report = json.loads(raw.decode("utf-8"))
    if (
        not isinstance(report, dict)
        or report.get("capture_id") != binding["capture_id"]
        or report.get("capture_state_sha256") != binding["capture_state_sha256"]
        or report.get("historical_selection") != binding["selection"]
        or report.get("selected_task_indices") != binding["task_indices"]
        or report.get("sampling_coverage", {}).get("metadata_coverage_complete") is not True
        or report.get("sampling_coverage", {}).get("candidate_pages") != len(candidates)
        or report.get("certified") is not False
        or report.get("published") is not False
    ):
        raise CatalogueError("prior stride sample is not bound to the current capture census")
    groups: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for page in candidates:
        groups[(page["endpoint"], page["generation"])].append(page)
    positions = {
        key: {page["position"]: page for page in sorted(values, key=lambda item: item["order"])}
        for key, values in groups.items()
    }
    hints: set[str] = set()
    combo_hints = repeat_hints = 0
    for profile in report.get("sample_profiles", []):
        if not isinstance(profile, dict) or "observations" not in profile:
            continue
        endpoint, generation = profile.get("endpoint"), profile.get("generation")
        position = profile.get("position")
        candidate = positions.get((endpoint, generation), {}).get(position)
        if (
            candidate is None
            or profile.get("encoded_bytes") != candidate["encoded_bytes"]
            or profile.get("decoded_bytes") != candidate["decoded_bytes"]
        ):
            raise CatalogueError("prior stride profile does not identify its current receipt")
        if profile.get("max_combo_legs", 0) > 0 or profile.get("repeated_rule_values", 0) > 0:
            hints.add(candidate["relative"])
            combo_hints += profile.get("max_combo_legs", 0) > 0
            repeat_hints += profile.get("repeated_rule_values", 0) > 0
    return hints, {
        "path": str(report_path),
        "sha256": _sha256(raw),
        "bound": True,
        "hint_receipts": len(hints),
        "combo_hint_profiles": combo_hints,
        "repeat_hint_profiles": repeat_hints,
        "combo_hint_covered": combo_hints > 0,
        "repeat_hint_covered": repeat_hints > 0,
    }


def _validated_old_stress(
    report_path: Path, binding: dict[str, Any], benchmark: Any
) -> dict[str, Any]:
    from kalshi_events.common import CatalogueError

    raw = benchmark._read_regular_bytes(report_path, 16 * 1024**2)
    report = json.loads(raw.decode("utf-8"))
    capture = report.get("capture") if isinstance(report, dict) else None
    forecast = report.get("forecast") if isinstance(report, dict) else None
    stress = (
        forecast.get("native_database_all_sampled_page_max_plus_20pct_stress")
        if isinstance(forecast, dict)
        else None
    )
    if (
        not isinstance(capture, dict)
        or capture.get("capture_id") != binding["capture_id"]
        or capture.get("state_sha256") != binding["capture_state_sha256"]
        or capture.get("metadata_index_sha256") != binding["fingerprints"]["receipt_index_sha256"]
        or capture.get("candidate_data_pages") != binding["population"]["candidate_pages"]
        or capture.get("exact_observation_rows") != binding["population"]["exact_observations"]
        or capture.get("status") != "complete"
        or not isinstance(stress, dict)
        or stress.get("bytes") != 109_613_465_550
        or stress.get("gib") != round(109_613_465_550 / GIB, 3)
    ):
        raise CatalogueError("prior all-strata stress is not bound to the current exact census")
    return {
        "path": str(report_path),
        "sha256": _sha256(raw),
        "bound": True,
        "bytes": stress["bytes"],
        "gib": float(stress["gib"]),
        "scenario": "old sampled-page maximum applied to every exact source row plus 20%",
    }


def _measure_tree(
    root: Path,
    *,
    device: int | None = None,
    allocated_inodes: set[tuple[int, int]] | None = None,
    exclude: set[Path] | None = None,
    entry_limit: int = 1_000_000,
) -> dict[str, Any]:
    root = root.absolute()
    exclude = set() if exclude is None else {path.absolute() for path in exclude}

    def walk(
        directory: Path,
        parts: tuple[str, ...],
        totals: dict[str, Any],
        attempt_allocated_inodes: set[tuple[int, int]],
    ) -> None:
        with os.scandir(directory) as children:
            for child in children:
                path = Path(child.path).absolute()
                if path in exclude:
                    continue
                totals["entries"] += 1
                if totals["entries"] > entry_limit:
                    raise RuntimeError("admission inventory exceeded its bounded entry count")
                info = child.stat(follow_symlinks=False)
                if info.st_dev != totals["device"] or stat.S_ISLNK(info.st_mode):
                    raise RuntimeError("admission inventory contains a link or crossed filesystem")
                if stat.S_ISDIR(info.st_mode):
                    walk(path, (*parts, child.name), totals, attempt_allocated_inodes)
                    continue
                if not stat.S_ISREG(info.st_mode):
                    raise RuntimeError("admission inventory contains a nonregular artifact")
                totals["logical_bytes"] += info.st_size
                identity = (info.st_dev, info.st_ino)
                allocated = 0
                if identity not in attempt_allocated_inodes:
                    allocated = info.st_blocks * 512
                    totals["allocated_bytes"] += allocated
                    attempt_allocated_inodes.add(identity)
                if child.name.endswith((".duckdb", ".wal")):
                    totals["database_bytes"] += info.st_size
                    totals["database_allocated_bytes"] += allocated
                if "tmp" in parts:
                    totals["temporary_bytes"] += info.st_size
                    totals["temporary_allocated_bytes"] += allocated
                if "index" in "/".join(parts).lower() or child.name.startswith("index.sqlite"):
                    totals["index_bytes"] += info.st_size
                    totals["index_allocated_bytes"] += allocated
                if (
                    any(part in {"duckdb-spill", "spill"} for part in parts)
                    or "spill" in child.name.lower()
                ):
                    totals["spill_bytes"] += info.st_size
                    totals["spill_allocated_bytes"] += allocated
                if child.name.endswith(".parquet"):
                    totals["export_bytes"] += info.st_size
                    totals["export_allocated_bytes"] += allocated
                if "verification" in parts or child.name.startswith("verification"):
                    totals["verification_bytes"] += info.st_size
                    totals["verification_allocated_bytes"] += allocated

    for attempt in range(3):
        totals = {
            "logical_bytes": 0,
            "allocated_bytes": 0,
            "database_bytes": 0,
            "database_allocated_bytes": 0,
            "temporary_bytes": 0,
            "temporary_allocated_bytes": 0,
            "index_bytes": 0,
            "index_allocated_bytes": 0,
            "spill_bytes": 0,
            "spill_allocated_bytes": 0,
            "export_bytes": 0,
            "export_allocated_bytes": 0,
            "verification_bytes": 0,
            "verification_allocated_bytes": 0,
            "entries": 0,
            "device": device,
        }
        attempt_allocated_inodes = set(allocated_inodes or ())
        try:
            root_info = root.lstat()
        except FileNotFoundError:
            if attempt:
                raise
            totals["logical_minus_allocated_bytes"] = 0
            return totals
        if device is None:
            device = root_info.st_dev
        if not stat.S_ISDIR(root_info.st_mode) or root_info.st_dev != device:
            raise RuntimeError(f"admission inventory root is unsafe or crossed devices: {root}")
        if totals["device"] is None:
            totals["device"] = root_info.st_dev

        try:
            walk(root, (), totals, attempt_allocated_inodes)
        except FileNotFoundError:
            if attempt == 2:
                raise
            continue

        totals["logical_minus_allocated_bytes"] = (
            totals["logical_bytes"] - totals["allocated_bytes"]
        )
        if allocated_inodes is not None:
            allocated_inodes.update(attempt_allocated_inodes)
        return totals
    raise RuntimeError("admission inventory did not complete")


def _measure_admission_storage(
    operator_root: Path,
    production_root: Path,
    output_root: Path,
    *,
    device: int,
) -> dict[str, Any]:
    inodes: set[tuple[int, int]] = set()
    operator = _measure_tree(operator_root, device=device, allocated_inodes=inodes)
    external = _measure_tree(
        production_root,
        device=device,
        allocated_inodes=inodes,
        exclude={operator_root, output_root},
    )
    output = _measure_tree(output_root, device=device, allocated_inodes=inodes)
    return {
        "operator": operator,
        "external": external,
        "isolated_artifacts": output,
        "aggregate_logical_bytes": sum(
            item["logical_bytes"] for item in (operator, external, output)
        ),
        "aggregate_allocated_bytes": sum(
            item["allocated_bytes"] for item in (operator, external, output)
        ),
        "aggregate_logical_minus_allocated_bytes": sum(
            item["logical_minus_allocated_bytes"] for item in (operator, external, output)
        ),
        "filesystem_free_bytes": shutil.disk_usage(operator_root).free,
    }


def _admission_forecast(
    population: dict[str, Any],
    stages: list[dict[str, Any]],
    baseline: dict[str, Any],
    old_stress: dict[str, Any] | None,
) -> dict[str, Any]:
    blockers: list[str] = []
    exact_rows = population.get("exact_observations")
    endpoint_rows = population.get("observations_by_endpoint")
    strata = population.get("data_strata")
    if (
        not _is_size(exact_rows)
        or exact_rows <= 0
        or not isinstance(endpoint_rows, dict)
        or not endpoint_rows
        or any(
            not isinstance(key, str) or not _is_size(value) for key, value in endpoint_rows.items()
        )
        or sum(endpoint_rows.values()) != exact_rows
    ):
        return {
            "supported": False,
            "blockers": ["exact census population is unknown or inconsistent"],
        }
    if not isinstance(strata, list) or not strata:
        return {"supported": False, "blockers": ["exact source decoded-byte weights are unknown"]}
    source_strata: dict[tuple[str, int], dict[str, int]] = {}
    for item in strata:
        if (
            not isinstance(item, dict)
            or not isinstance(item.get("endpoint"), str)
            or not _is_size(item.get("generation"))
            or not _is_size(item.get("candidate_pages"))
            or item["candidate_pages"] <= 0
            or not _is_size(item.get("decoded_bytes"))
            or item["decoded_bytes"] <= 0
            or not _is_size(item.get("encoded_bytes"))
        ):
            return {
                "supported": False,
                "blockers": ["exact source decoded-byte weights are malformed"],
            }
        key = (item["endpoint"], item["generation"])
        if key in source_strata:
            return {"supported": False, "blockers": ["exact source strata are duplicated"]}
        source_strata[key] = item
    if {endpoint for endpoint, _ in source_strata} != set(endpoint_rows):
        return {
            "supported": False,
            "blockers": ["exact source strata differ from observation endpoints"],
        }

    required_targets = (250_000, 1_000_000, 3_000_000)
    stage_by_target = {
        stage.get("target_observations"): stage for stage in stages if isinstance(stage, dict)
    }
    if len(stage_by_target) != len(stages):
        blockers.append("sample target stages are duplicate or malformed")
    if any(target not in stage_by_target for target in required_targets):
        blockers.append("one or more nested whole-receipt sample targets were not completed")
    full_stage = stage_by_target.get(required_targets[-1])
    if not isinstance(full_stage, dict):
        return {"supported": False, "blockers": [*blockers, "3,000,000-row sample is missing"]}

    stage_samples: dict[int, dict[tuple[str, int], dict[str, Any]]] = {}
    stage_metrics: dict[int, dict[str, float]] = {}
    required_seed_reasons = {
        "first_position",
        "middle_position",
        "last_position",
        "smallest_decoded",
        "middle_decoded",
        "largest_decoded",
        "poorest_compression",
    }
    for target in required_targets:
        stage = stage_by_target.get(target)
        if not isinstance(stage, dict):
            continue
        actual_rows = stage.get("observations")
        if not _is_size(actual_rows) or actual_rows < target:
            blockers.append(f"{target:,}-observation whole-receipt stage is below its target")
            continue
        grouped: dict[tuple[str, int], dict[str, Any]] = defaultdict(
            lambda: {
                "rows": 0,
                "landing_bytes": 0,
                "decoded_bytes": 0,
                "max_ratio": 0.0,
                "reasons": set(),
            }
        )
        profiles = stage.get("profiles")
        if not isinstance(profiles, list) or not profiles:
            blockers.append(f"{target:,}-observation stage has no receipt profiles")
            continue
        malformed = False
        for profile in profiles:
            if (
                not isinstance(profile, dict)
                or not isinstance(profile.get("endpoint"), str)
                or not _is_size(profile.get("generation"))
                or not _is_size(profile.get("observations"))
                or not _is_size(profile.get("landing_utf8_bytes"))
                or not _is_size(profile.get("decoded_bytes"))
            ):
                malformed = True
                break
            key = (profile["endpoint"], profile["generation"])
            if key not in source_strata:
                malformed = True
                break
            sample = grouped[key]
            sample["rows"] += profile["observations"]
            sample["landing_bytes"] += profile["landing_utf8_bytes"]
            sample["decoded_bytes"] += profile["decoded_bytes"]
            if profile["decoded_bytes"]:
                sample["max_ratio"] = max(
                    sample["max_ratio"],
                    profile["landing_utf8_bytes"] / profile["decoded_bytes"],
                )
            reasons = profile.get("seed_reasons", [])
            if not isinstance(reasons, list) or any(
                not isinstance(reason, str) for reason in reasons
            ):
                malformed = True
                break
            sample["reasons"].update(reasons)
        if malformed:
            blockers.append(f"{target:,}-observation stage has malformed receipt profile metrics")
            continue
        missing = set(source_strata) - set(grouped)
        if missing:
            blockers.append(
                f"{target:,}-observation stage lacks one or more endpoint-generation strata"
            )
        if target == required_targets[-1]:
            incomplete_seed = [
                key
                for key, sample in grouped.items()
                if not required_seed_reasons.issubset(sample["reasons"])
            ]
            if incomplete_seed:
                blockers.append(
                    "full sample lacks position, size, or compression-tail seeds for every stratum"
                )
        stage_samples[target] = grouped
        if sum(sample["rows"] for sample in grouped.values()) != actual_rows:
            blockers.append(f"{target:,}-observation stage profile rows differ from landed rows")

        metrics = {
            "database_peak_component_bytes": stage.get("build_database_component_bytes"),
            "database_peak_component_allocated_bytes": stage.get(
                "build_database_allocated_component_bytes"
            ),
            "retained_database_phase_bytes": stage.get("database_phase_bytes"),
            "retained_database_phase_allocated_bytes": stage.get("database_phase_allocated_bytes"),
            "export_bytes": stage.get("export_bytes"),
            "build_peak_temporary_bytes": stage.get("build_peak_temporary_bytes"),
            "build_peak_temporary_allocated_bytes": stage.get(
                "build_peak_temporary_allocated_bytes"
            ),
            "export_peak_temporary_bytes": stage.get("export_peak_temporary_bytes"),
            "export_peak_temporary_allocated_bytes": stage.get(
                "export_peak_temporary_allocated_bytes"
            ),
            "verification_peak_temporary_bytes": stage.get("verification_peak_temporary_bytes"),
            "verification_peak_temporary_allocated_bytes": stage.get(
                "verification_peak_temporary_allocated_bytes"
            ),
            "spill_peak_bytes": stage.get("spill_peak_bytes"),
            "spill_peak_allocated_bytes": stage.get("spill_peak_allocated_bytes"),
            "elapsed_seconds": stage.get("elapsed_seconds"),
            "verification_seconds": stage.get("verification_seconds"),
        }
        if any(not _is_metric(value) for value in metrics.values()):
            blockers.append(
                f"{target:,}-observation resource, allocated, spill or timing metrics are unknown"
            )
        stage_metrics[target] = metrics  # values are validated below before arithmetic
        if stage.get("spill_forecast_supported") is not True:
            blockers.append("build/export spill forecast is unsupported or incompletely measured")
        if (
            stage.get("telemetry_complete") is not True
            or stage.get("verification_complete") is not True
        ):
            blockers.append(
                "supervisor or semantic/hash/schema verification telemetry is incomplete"
            )

    def scale_profile(metric: str) -> dict[str, Any]:
        values = []
        for target in required_targets:
            metrics = stage_metrics.get(target)
            rows = stage_by_target.get(target, {}).get("observations")
            measured = metrics.get(metric) if metrics else None
            if not _is_size(rows) or rows <= 0 or not _is_metric(measured):
                return {"complete": False, "per_observation": None, "max_to_min_ratio": None}
            values.append(measured / rows)
        low, high = min(values), max(values)
        ratio = high / low if low > 0 else (1.0 if high == 0 else None)
        return {
            "complete": True,
            "per_observation": values,
            "max_to_min_ratio": ratio,
            "within_fixed_20pct": ratio is not None and ratio <= 1.20,
        }

    linearity = {
        metric: scale_profile(metric)
        for metric in (
            "database_peak_component_bytes",
            "database_peak_component_allocated_bytes",
            "retained_database_phase_bytes",
            "retained_database_phase_allocated_bytes",
            "export_bytes",
            "build_peak_temporary_bytes",
            "build_peak_temporary_allocated_bytes",
            "export_peak_temporary_bytes",
            "export_peak_temporary_allocated_bytes",
            "verification_peak_temporary_bytes",
            "verification_peak_temporary_allocated_bytes",
            "spill_peak_bytes",
            "spill_peak_allocated_bytes",
            "elapsed_seconds",
            "verification_seconds",
        )
    }
    if any(item.get("complete") is not True for item in linearity.values()):
        blockers.append("multi-scale resource telemetry is incomplete")
    elif any(item.get("within_fixed_20pct") is not True for item in linearity.values()):
        blockers.append("measured scale growth is nonlinear beyond the fixed 20% scenario")

    # Each source stratum contributes its exact decoded-byte population weight.
    full_samples = stage_samples.get(required_targets[-1], {})
    weighted_landing = 0.0
    weighted_max_landing = 0.0
    weighted_source: dict[str, Any] = {}
    for key, source in sorted(source_strata.items()):
        sample = full_samples.get(key)
        if (
            sample is None
            or sample["decoded_bytes"] <= 0
            or sample["landing_bytes"] <= 0
            or sample["rows"] <= 0
        ):
            blockers.append(
                f"full sample lacks decoded-byte/row weights for {key[0]} generation {key[1]}"
            )
            continue
        source_decoded = source["decoded_bytes"]
        weighted = source_decoded * sample["landing_bytes"] / sample["decoded_bytes"]
        stress = source_decoded * sample["max_ratio"]
        weighted_landing += weighted
        weighted_max_landing += stress
        endpoint = key[0]
        record = weighted_source.setdefault(
            endpoint,
            {
                "source_decoded_bytes": 0,
                "source_observations": endpoint_rows[endpoint],
                "weighted_landing_bytes": 0,
                "sample_strata": [],
            },
        )
        record["source_decoded_bytes"] += source_decoded
        record["weighted_landing_bytes"] += math.ceil(weighted)
        record["sample_strata"].append(
            {
                "generation": key[1],
                "source_decoded_bytes": source_decoded,
                "sample_decoded_bytes": sample["decoded_bytes"],
                "sample_landing_bytes": sample["landing_bytes"],
                "sample_observations": sample["rows"],
                "weighted_landing_bytes": math.ceil(weighted),
            }
        )
    sample_rows = sum(sample["rows"] for sample in full_samples.values())
    sample_landing = sum(sample["landing_bytes"] for sample in full_samples.values())
    exact_scale = exact_rows / sample_rows if sample_rows else None
    if sample_rows <= 0 or sample_landing <= 0:
        blockers.append("database, export, or landing measurements are unavailable")
    full_metrics = stage_metrics.get(required_targets[-1], {})
    database_phase = full_metrics.get("database_peak_component_bytes")
    export_bytes = full_metrics.get("export_bytes")
    if (
        not _is_metric(database_phase)
        or not _is_metric(export_bytes)
        or database_phase <= 0
        or export_bytes <= 0
        or sample_landing <= 0
    ):
        blockers.append("database, export, or landing measurements are unavailable")
        database_ratio = export_per_row = 0.0
    else:
        database_ratio = database_phase / sample_landing
        export_per_row = export_bytes / sample_rows
    database_point = math.ceil(weighted_landing * database_ratio)
    database_growth_scenario = math.ceil(database_point * 1.20)
    max_stress_scenario = math.ceil(weighted_max_landing * database_ratio * 1.20)
    export_scenario = math.ceil(exact_rows * export_per_row * 1.20)
    scale = exact_scale
    row_scale_20pct = scale * 1.20 if scale is not None else None

    before = baseline.get("baseline", baseline)
    after = baseline.get("observed", baseline)
    operator = before.get("operator", {}) if isinstance(before, dict) else {}
    external = before.get("external", {}) if isinstance(before, dict) else {}
    artifacts = after.get("isolated_artifacts", {}) if isinstance(after, dict) else {}
    baseline_free = after.get("filesystem_free_bytes") if isinstance(after, dict) else None
    required_bucket_fields = ("logical_bytes", "allocated_bytes")
    if (
        any(
            not _is_size(operator.get(name))
            for name in (
                *required_bucket_fields,
                "database_bytes",
                "database_allocated_bytes",
                "temporary_bytes",
                "temporary_allocated_bytes",
            )
        )
        or any(not _is_size(external.get(name)) for name in required_bucket_fields)
        or any(not _is_size(artifacts.get(name)) for name in required_bucket_fields)
        or not _is_size(baseline_free)
    ):
        blockers.append(
            "fresh logical/allocated root or aggregate storage accounting is incomplete"
        )

    build_tmp = full_metrics.get("build_peak_temporary_bytes")
    export_tmp = full_metrics.get("export_peak_temporary_bytes")
    build_tmp_allocated = full_metrics.get("build_peak_temporary_allocated_bytes")
    export_tmp_allocated = full_metrics.get("export_peak_temporary_allocated_bytes")
    verification_tmp = full_metrics.get("verification_peak_temporary_bytes")
    verification_tmp_allocated = full_metrics.get("verification_peak_temporary_allocated_bytes")
    if row_scale_20pct is None or any(
        not _is_metric(value)
        for value in (
            build_tmp,
            export_tmp,
            build_tmp_allocated,
            export_tmp_allocated,
            verification_tmp,
            verification_tmp_allocated,
        )
    ):
        blockers.append(
            "full sample logical/allocated build, export, or verification temporary high-water is unavailable"
        )
        build_growth = export_growth = verification_growth = None
        build_alloc_growth = export_alloc_growth = verification_alloc_growth = None
    else:
        build_growth = math.ceil(build_tmp * row_scale_20pct)
        export_growth = math.ceil(export_tmp * row_scale_20pct)
        verification_growth = math.ceil(verification_tmp * row_scale_20pct)
        build_alloc_growth = math.ceil(build_tmp_allocated * row_scale_20pct)
        export_alloc_growth = math.ceil(export_tmp_allocated * row_scale_20pct)
        verification_alloc_growth = math.ceil(verification_tmp_allocated * row_scale_20pct)
    full_db_phase = full_metrics.get("database_peak_component_bytes")
    full_db_allocated = full_metrics.get("database_peak_component_allocated_bytes")
    db_phase_estimate = (
        math.ceil(full_db_phase / sample_landing * weighted_landing * 1.20)
        if _is_metric(full_db_phase) and sample_landing > 0
        else None
    )
    db_alloc_estimate = (
        math.ceil(full_db_allocated / sample_landing * weighted_landing * 1.20)
        if _is_metric(full_db_allocated) and sample_landing > 0
        else None
    )
    retained_db_phase = full_metrics.get("retained_database_phase_bytes")
    retained_db_growth = (
        math.ceil(retained_db_phase / sample_landing * weighted_landing * 1.20)
        if _is_metric(retained_db_phase) and sample_landing > 0
        else None
    )
    retained_db_allocated = full_metrics.get("retained_database_phase_allocated_bytes")
    retained_db_alloc_growth = (
        math.ceil(retained_db_allocated / sample_landing * weighted_landing * 1.20)
        if _is_metric(retained_db_allocated) and sample_landing > 0
        else None
    )
    simultaneous_logical = (
        max(
            build_growth,
            retained_db_growth + export_growth,
            retained_db_growth + verification_growth,
        )
        if all(
            _is_size(item)
            for item in (build_growth, retained_db_growth, export_growth, verification_growth)
        )
        else None
    )
    simultaneous_allocated = (
        max(
            build_alloc_growth,
            retained_db_alloc_growth + export_alloc_growth,
            retained_db_alloc_growth + verification_alloc_growth,
        )
        if all(
            _is_size(item)
            for item in (
                build_alloc_growth,
                retained_db_alloc_growth,
                export_alloc_growth,
                verification_alloc_growth,
            )
        )
        else None
    )
    tmp_scenario = (
        operator["temporary_bytes"] + max(build_growth, export_growth, verification_growth)
        if all(
            _is_size(item)
            for item in (
                operator.get("temporary_bytes"),
                build_growth,
                export_growth,
                verification_growth,
            )
        )
        else None
    )
    tmp_allocated_scenario = (
        operator["temporary_allocated_bytes"]
        + max(build_alloc_growth, export_alloc_growth, verification_alloc_growth)
        if all(
            _is_size(item)
            for item in (
                operator.get("temporary_allocated_bytes"),
                build_alloc_growth,
                export_alloc_growth,
                verification_alloc_growth,
            )
        )
        else None
    )
    existing_database = operator.get("database_bytes")
    existing_database_allocated = operator.get("database_allocated_bytes")
    database_scenario = (
        existing_database + database_growth_scenario
        if _is_size(existing_database) and _is_size(database_growth_scenario)
        else None
    )
    database_allocated_scenario = (
        existing_database_allocated + db_alloc_estimate
        if _is_size(existing_database_allocated) and _is_size(db_alloc_estimate)
        else None
    )
    root_scenario = (
        operator["logical_bytes"] + simultaneous_logical
        if _is_size(operator.get("logical_bytes")) and _is_size(simultaneous_logical)
        else None
    )
    root_allocated_scenario = (
        operator["allocated_bytes"] + simultaneous_allocated
        if _is_size(operator.get("allocated_bytes")) and _is_size(simultaneous_allocated)
        else None
    )
    aggregate_scenario = (
        root_scenario + external["logical_bytes"] + artifacts["logical_bytes"]
        if all(
            _is_size(item)
            for item in (
                root_scenario,
                external.get("logical_bytes"),
                artifacts.get("logical_bytes"),
            )
        )
        else None
    )
    aggregate_allocated_scenario = (
        root_allocated_scenario + external["allocated_bytes"] + artifacts["allocated_bytes"]
        if all(
            _is_size(item)
            for item in (
                root_allocated_scenario,
                external.get("allocated_bytes"),
                artifacts.get("allocated_bytes"),
            )
        )
        else None
    )
    if database_scenario is None or database_scenario > 96 * GIB:
        blockers.append(
            "database baseline plus exact-census 20% scenario exceeds 96 GiB or is unknown"
        )
    if tmp_scenario is None or tmp_scenario > 144 * GIB:
        blockers.append("temporary exact-census plus 20% scenario exceeds 144 GiB or is unknown")
    if root_scenario is None or root_scenario > 350 * GIB:
        blockers.append("operator root exact-census scenario exceeds 350 GiB or is unknown")
    if aggregate_scenario is None or aggregate_scenario > 400 * GIB:
        blockers.append("aggregate exact-census scenario exceeds 400 GiB or is unknown")
    remaining_free = (
        baseline_free - simultaneous_logical
        if _is_size(baseline_free) and _is_size(simultaneous_logical)
        else None
    )
    if remaining_free is None or remaining_free < 150 * GIB:
        blockers.append("projected filesystem free space falls below 150 GiB or is unknown")

    full_stage = stage_by_target.get(required_targets[-1], {})
    full_elapsed = full_metrics.get("elapsed_seconds")
    verification_seconds = full_metrics.get("verification_seconds")
    if _is_metric(full_elapsed) and _is_metric(verification_seconds) and scale is not None:
        work_scenario_seconds = math.ceil(
            max(0.0, full_elapsed - verification_seconds) * scale * 1.20
        )
        verification_scenario_seconds = math.ceil(verification_seconds * scale * 1.20)
        if work_scenario_seconds > 22 * 60 * 60:
            blockers.append("22-hour work scenario is exceeded")
        if verification_scenario_seconds > 2 * 60 * 60:
            blockers.append("2-hour verification scenario is exceeded")
    else:
        work_scenario_seconds = verification_scenario_seconds = None
        blockers.append("work or verification timing telemetry is unknown")

    full_profiles = full_stage.get("profiles")
    if not isinstance(full_profiles, list):
        full_profiles = []
    actual_tail_coverage = {
        "combo": any(
            _is_size(profile.get("max_combo_legs")) and profile["max_combo_legs"] > 0
            for profile in full_profiles
            if isinstance(profile, dict)
        ),
        "repeated_rules": any(
            _is_size(profile.get("repeated_rule_values")) and profile["repeated_rule_values"] > 0
            for profile in full_profiles
            if isinstance(profile, dict)
        ),
        "profiles_checked": len(full_profiles),
        "source": "fresh selected-receipt payload normalization in the 3,000,000-row nested sample",
    }
    old_stress_bound = isinstance(old_stress, dict) and old_stress.get("bound") is True
    if not old_stress_bound:
        blockers.append("the prior 102.08 GiB all-strata stress is not bound to this census")
    if not actual_tail_coverage["combo"] or not actual_tail_coverage["repeated_rules"]:
        blockers.append(
            "freshly normalized full-sample combo and repeated-rule tails are not both observed"
        )
    old_stress_resolution = (
        "source decoded-byte weights, per-phase DB/WAL/index/temp/export evidence, three nested scales, and revalidated combo/repeated-rule tails support the narrower 20% scenario; the historical 102.08 GiB maximum remains a stress case, not an exact population estimate"
        if old_stress_bound
        and actual_tail_coverage["combo"]
        and actual_tail_coverage["repeated_rules"]
        and not any(item.get("within_fixed_20pct") is not True for item in linearity.values())
        and all(
            stage_by_target.get(target, {}).get("spill_forecast_supported") is True
            for target in required_targets
        )
        and all(
            stage_by_target.get(target, {}).get("verification_complete") is True
            for target in required_targets
        )
        and database_scenario is not None
        and database_scenario <= 96 * GIB
        else None
    )
    if old_stress_resolution is None:
        blockers.append(
            "the prior all-strata maximum stress has not been resolved by the measured scenario"
        )

    return {
        "supported": not blockers,
        "blockers": list(dict.fromkeys(blockers)),
        "population": {
            "exact_observations": exact_rows,
            "observations_by_endpoint": endpoint_rows,
            "exact_source_decoded_bytes_by_endpoint": {
                endpoint: sum(
                    item["decoded_bytes"]
                    for (name, _), item in source_strata.items()
                    if name == endpoint
                )
                for endpoint in sorted(endpoint_rows)
            },
            "sample_observations": sample_rows,
            "sample_landing_utf8_bytes": sample_landing,
            "exact_scale": scale,
            "fixed_scenario_margin": 0.20,
        },
        "endpoint_weighted": {
            "endpoint_evidence": weighted_source,
            "database_phase_bytes_per_landing_byte": database_ratio,
            "database_growth_point_bytes": database_point,
            "database_growth_exact_plus_20pct_bytes": database_growth_scenario,
            "database_baseline_bytes": existing_database,
            "database_scenario_total_bytes": database_scenario,
            "database_allocated_scenario_bytes": database_allocated_scenario,
            "zstd6_export_growth_exact_plus_20pct_bytes": export_scenario,
            "projected_database_component_peak_bytes": db_phase_estimate,
            "projected_database_allocated_peak_bytes": db_alloc_estimate,
            "build_temporary_growth_bytes": build_growth,
            "export_temporary_growth_bytes": export_growth,
            "verification_temporary_growth_bytes": verification_growth,
            "temporary_scenario_total_bytes": tmp_scenario,
            "temporary_allocated_scenario_bytes": tmp_allocated_scenario,
            "operator_root_scenario_bytes": root_scenario,
            "operator_root_allocated_scenario_bytes": root_allocated_scenario,
            "aggregate_scenario_bytes": aggregate_scenario,
            "aggregate_allocated_scenario_bytes": aggregate_allocated_scenario,
            "filesystem_free_after_scenario_bytes": remaining_free,
            "peak_model": "database limit includes inventoried baseline DB/WAL plus projected DB/WAL/index; root phases compare build, retained DB plus export tmp, and retained DB plus verification tmp separately",
        },
        "old_all_strata_max_stress": {
            **(old_stress or {"bound": False, "bytes": None, "gib": None}),
            "new_sample_max_page_plus_20pct_database_bytes": max_stress_scenario,
            "resolution": old_stress_resolution,
        },
        "scale_linearity": linearity,
        "content_tail_coverage": actual_tail_coverage,
        "spill_forecast": {
            "supported": all(
                stage_by_target.get(target, {}).get("spill_forecast_supported") is True
                for target in required_targets
            ),
            "measurement": "supervised logical/allocated high-water by build/export/verification phase; Store spill limit is enforced; 20% scaling is a supported scenario, not a hard filesystem quota",
            "scenario_is_formal_upper_bound": False,
        },
        "work_scenario_seconds": work_scenario_seconds,
        "verification_scenario_seconds": verification_scenario_seconds,
        "verification": {
            "shared_hash_schema_coverage_and_bounded_sql": "measured_at_each_sample_scale"
            if all(
                stage_by_target.get(target, {}).get("verification_complete") is True
                for target in required_targets
            )
            else "incomplete",
            "standalone_release_verify_wrapper": "not_called_for_incomplete_subset",
            "material_cost_unknown": False
            if all(
                stage_by_target.get(target, {}).get("verification_complete") is True
                for target in required_targets
            )
            else None,
        },
        "scenario_is_formal_upper_bound": False,
        "limitations": [
            "the fixed 20% margin is a supported scenario, not a formal upper bound or operating-system hard quota",
            "the 102.08 GiB prior all-strata page-maximum stress remains visible; the narrower estimate uses exact decoded-byte source weights, bounded sample tails and measured multi-scale behavior",
            "standalone release verification is not called on the incomplete diagnostic subset; shared receipt hashes, export hashes, schemas, coverage and bounded SQL checks are measured",
        ],
    }


def _admission_decision(
    forecast: dict[str, Any], preflight: dict[str, Any], old_stress: dict[str, Any] | None
) -> dict[str, Any]:
    if not isinstance(forecast, dict):
        forecast = {}
    if not isinstance(preflight, dict):
        preflight = {}
    blockers = []
    forecast_blockers = forecast.get("blockers") if isinstance(forecast, dict) else None
    if not isinstance(forecast_blockers, list) or any(
        not isinstance(item, str) for item in forecast_blockers
    ):
        blockers.append("capacity forecast blocker evidence is malformed or unknown")
    else:
        blockers.extend(forecast_blockers)
    preflight_blockers = preflight.get("blockers") if isinstance(preflight, dict) else None
    if not isinstance(preflight_blockers, list) or any(
        not isinstance(item, str) for item in preflight_blockers
    ):
        blockers.append("preflight blocker evidence is malformed or unknown")
        preflight_blockers = []
    for blocker in preflight_blockers:
        if blocker not in blockers:
            blockers.append(blocker)
    memory = preflight.get("memory") if isinstance(preflight, dict) else None
    if forecast.get("supported") is not True:
        blockers.append("capacity forecast is not explicitly supported")
    if not isinstance(forecast.get("endpoint_weighted"), dict) or not isinstance(
        forecast.get("scale_linearity"), dict
    ):
        blockers.append(
            "capacity forecast does not contain its weighted scenario and scale evidence"
        )
    if preflight.get("preflight_complete") is not True:
        blockers.append("preflight evidence is incomplete")
    if preflight.get("scope_complete") is not True:
        blockers.append("source and aggregate accounting scope is incomplete")
    if preflight.get("no_writer_lock") is not True:
        blockers.append("canonical source writer absence is not verified")
    if not isinstance(memory, dict) or memory.get("system_memory_pressure") != "normal":
        blockers.append("normal system memory pressure is not verified")
    if not _is_size(preflight.get("free_bytes")) or preflight["free_bytes"] < 150 * GIB:
        blockers.append("preflight filesystem free space is below 150 GiB or unknown")
    if (
        not isinstance(preflight.get("operator_volume_uuid"), str)
        or not preflight["operator_volume_uuid"]
        or preflight.get("output_volume_uuid") != preflight["operator_volume_uuid"]
    ):
        blockers.append("operator/output volume identity is missing or mismatched")
    if (
        not isinstance(old_stress, dict) or old_stress.get("bound") is not True
    ) and "prior all-strata stress is unbound" not in blockers:
        blockers.append("prior all-strata stress is unbound")
    return {
        "decision": "ADMIT" if not blockers else "NO_ADMIT",
        "launch_allowed": not blockers,
        "capacity_admitted": not blockers,
        "production_admitted": False,
        "blockers": blockers,
    }


def _check_authorized_census(binding: dict[str, Any]) -> dict[str, Any]:
    from kalshi_events.common import CatalogueError

    population = binding.get("population") if isinstance(binding, dict) else None
    fingerprints = binding.get("fingerprints") if isinstance(binding, dict) else None
    if (
        not isinstance(population, dict)
        or not isinstance(fingerprints, dict)
        or binding.get("capture_id") != AUTHORIZED_CAPTURE_ID
        or binding.get("root_id") != AUTHORIZED_ROOT_ID
        or binding.get("capture_state_sha256") != AUTHORIZED_STATE_SHA256
        or fingerprints.get("receipt_index_sha256") != AUTHORIZED_INDEX_SHA256
        or population.get("candidate_pages") != AUTHORIZED_POPULATION["candidate_pages"]
        or population.get("observations_by_endpoint")
        != AUTHORIZED_POPULATION["observations_by_endpoint"]
        or population.get("nested_event_markets") != AUTHORIZED_POPULATION["nested_event_markets"]
        or population.get("exact_observations") != AUTHORIZED_POPULATION["exact_observations"]
    ):
        raise CatalogueError(
            "revalidated source does not match the approved capture, root, and exact population"
        )
    return {
        "capture_id": AUTHORIZED_CAPTURE_ID,
        "root_id": AUTHORIZED_ROOT_ID,
        "capture_state_sha256": AUTHORIZED_STATE_SHA256,
        "receipt_index_sha256": AUTHORIZED_INDEX_SHA256,
        **AUTHORIZED_POPULATION,
        "checked": True,
        "binding": "current journal orders, task/protocol context, receipt metadata index, root identity and report-to-index hash were revalidated",
    }


def _preflight(
    operator_root: Path,
    output_root: Path,
    runner: Any,
    *,
    device: int,
) -> dict[str, Any]:
    blockers = []
    try:
        operator_volume = runner._volume_uuid(operator_root)
        output_volume = runner._volume_uuid(output_root)
    except Exception as exc:
        operator_volume = output_volume = None
        blockers.append(f"filesystem identity is unknown: {type(exc).__name__}")
    if operator_root.stat().st_dev != device or output_root.stat().st_dev != device:
        blockers.append("operator and isolated output roots are not on the same device")
    if operator_volume is None or output_volume is None or operator_volume != output_volume:
        blockers.append("operator and isolated output roots are not on the same verified volume")
    try:
        memory = runner._sample_memory(None)
    except Exception as exc:
        memory = None
        blockers.append(f"system memory telemetry is unknown: {type(exc).__name__}")
    if not isinstance(memory, dict) or memory.get("system_memory_pressure") != "normal":
        blockers.append("system memory pressure is not verified normal")
    free_bytes = shutil.disk_usage(operator_root).free
    if free_bytes < 150 * GIB:
        blockers.append("filesystem free space is below 150 GiB")
    return {
        "preflight_complete": not blockers
        and isinstance(memory, dict)
        and memory.get("system_memory_pressure") == "normal"
        and operator_volume is not None
        and output_volume is not None
        and operator_volume == output_volume,
        "scope_complete": False,
        "no_writer_lock": True,
        "operator_volume_uuid": operator_volume,
        "output_volume_uuid": output_volume,
        "device": device,
        "free_bytes": free_bytes,
        "minimum_free_bytes": 150 * GIB,
        "memory": memory,
        "blockers": blockers,
        "monitoring_is_hard_quota": False,
    }


def _dependency_fingerprint(checkout: Path, benchmark: Any, runner: Any) -> dict[str, Any]:
    names = (
        Path(benchmark.__file__).resolve(),
        Path(__file__).resolve(),
        checkout / "src/kalshi_events/capture.py",
        checkout / "src/kalshi_events/normalize.py",
        checkout / "src/kalshi_events/transport.py",
        checkout / "src/kalshi_events/warehouse.py",
        checkout / "src/kalshi_events/publish.py",
        checkout / "uv.lock",
    )
    files = {}
    for path in names:
        if path.is_symlink() or not path.is_file():
            raise RuntimeError(f"source/dependency fingerprint file is missing or unsafe: {path}")
        body = path.read_bytes()
        files[str(path.relative_to(checkout))] = {
            "sha256": _sha256(body),
            "size": len(body),
        }
    packages = runner._package_versions()
    package_bytes = json.dumps(packages, sort_keys=True, separators=(",", ":")).encode()
    return {
        "source_files": files,
        "packages": packages,
        "uv_lock_sha256": files["uv.lock"]["sha256"],
        "dependency_sha256": _sha256(package_bytes),
        "fingerprint_sha256": _sha256(
            json.dumps(
                {"files": files, "packages": packages}, sort_keys=True, separators=(",", ":")
            ).encode()
        ),
    }


def _profile_receipt(
    source: Any, state: dict[str, Any], page: dict[str, Any], benchmark: Any, *, data: bool
) -> tuple[dict[str, Any] | None, dict[str, str]]:
    from kalshi_events import capture, warehouse
    from kalshi_events.common import CatalogueError
    from kalshi_events.normalize import observations
    from kalshi_events.transport import read_page

    evidence: dict[str, str] = {}
    manifest, payload = read_page(
        source,
        page["relative"],
        state["max_response_bytes"],
        state.get("max_decoded_response_bytes", state["max_response_bytes"]),
        decode_json=data,
        inventory=evidence,
    )
    if manifest.get("capture_id") != state[
        "capture_id"
    ] or not capture._receipt_matches_state_protocol(manifest, state):
        raise CatalogueError("sample receipt failed current capture/protocol verification")
    capture._validate_receipt_task_binding(manifest, state, page)
    encoded_size = manifest.get("encoded_size", manifest.get("size"))
    decoded_size = manifest.get("decoded_size", manifest.get("size"))
    generation = manifest.get("task_generation", manifest.get("request_generation", 0))
    if (
        manifest.get("endpoint") != page["endpoint"]
        or manifest.get("status") != page["status"]
        or generation != page["generation"]
        or encoded_size != page["encoded_bytes"]
        or decoded_size != page["decoded_bytes"]
    ):
        raise CatalogueError("sample receipt context or size differs from the bound census")
    manifest_raw = benchmark._read_regular_bytes(
        source.path((PurePosixPath(page["relative"]).parent / "manifest.json").as_posix()),
        benchmark.MANIFEST_LIMIT_BYTES,
    )
    page["manifest_sha256"] = _sha256(manifest_raw)
    page["body_sha256"] = manifest.get("sha256")
    page["verified_evidence"] = evidence
    if not data:
        return None, evidence
    if manifest.get("status") != 200:
        raise CatalogueError("data sample includes a non-success receipt")
    scoped = warehouse._scope_manifest(state, manifest)
    rows = list(observations(((scoped, payload),)))
    profile = benchmark._row_profile(rows, payload)
    profile.update(
        endpoint=page["endpoint"],
        generation=page["generation"],
        position=page["position"],
        encoded_bytes=page["encoded_bytes"],
        decoded_bytes=page["decoded_bytes"],
    )
    return profile, evidence


def _copy_receipt(source: Any, page: dict[str, Any], fixture_root: Path, benchmark: Any) -> None:
    from kalshi_events.common import CatalogueError

    relative = PurePosixPath(page["relative"])
    manifest_source = source.path((relative.parent / "manifest.json").as_posix())
    body_source = source.path((relative.parent / "body.bin").as_posix())
    manifest_bytes = benchmark._read_regular_bytes(manifest_source, benchmark.MANIFEST_LIMIT_BYTES)
    if _sha256(manifest_bytes) != page.get("manifest_sha256"):
        raise CatalogueError("source receipt manifest changed after verification")
    body_info = body_source.lstat()
    if not stat.S_ISREG(body_info.st_mode) or body_info.st_size != page["encoded_bytes"]:
        raise CatalogueError("source receipt body changed after verification")
    destination = fixture_root.joinpath(*relative.parts)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(manifest_source, destination.with_name("manifest.json"))
    shutil.copyfile(body_source, destination.with_name("body.bin"))
    if (
        _sha256(
            benchmark._read_regular_bytes(
                destination.with_name("manifest.json"), benchmark.MANIFEST_LIMIT_BYTES
            )
        )
        != page["manifest_sha256"]
        or _file_sha256(destination.with_name("body.bin"), page["encoded_bytes"])
        != page["body_sha256"]
    ):
        raise CatalogueError("copied sample receipt does not match verified source evidence")


def _file_sha256(path: Path, limit: int) -> str:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    checksum = hashlib.sha256()
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
            raise RuntimeError("sample artifact is nonregular or exceeds its byte bound")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            while chunk := stream.read(1024 * 1024):
                checksum.update(chunk)
    finally:
        os.close(descriptor)
    return checksum.hexdigest()


def _clear_worker_outputs(fixture_root: Path, capture_id: str) -> None:
    for relative in (
        "tmp/compact-benchmark",
        "tmp/compact-exports",
        f"diagnostic-builds/{capture_id}",
        "tmp/verification",
    ):
        path = fixture_root / relative
        if path.is_symlink():
            raise RuntimeError("admission worker output became a symlink")
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()


def _terminate_admission_group(process: subprocess.Popen[Any], runner: Any) -> bool:
    """Require both the worker leader and every descendant to be gone."""
    try:
        group_exists = runner._process_group_exists(process.pid)
    except OSError:
        return False
    if group_exists:
        return runner._terminate(process)
    if process.poll() is None:
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            return False
    try:
        return process.poll() is not None and not runner._process_group_exists(process.pid)
    except OSError:
        return False


def _prepare_fixture(
    source: Any,
    state: dict[str, Any],
    fixture_root: Path,
    data_pages: list[dict[str, Any]],
    controls: list[dict[str, Any]],
    copied: set[str],
    target: int,
    benchmark: Any,
) -> None:
    from kalshi_events.common import atomic_json

    fixture_root.mkdir(parents=True, exist_ok=True)
    for name in ("root.json", "root-id.json"):
        source_file = source.root / name
        if source_file.is_file() and not source_file.is_symlink():
            destination = fixture_root / name
            if not destination.exists():
                shutil.copyfile(source_file, destination)
    selected = sorted([*data_pages, *controls], key=lambda page: page["order"])
    for page in selected:
        if page["relative"] not in copied:
            _copy_receipt(source, page, fixture_root, benchmark)
            copied.add(page["relative"])
    fixture_state = dict(state)
    fixture_state.pop("journal_version", None)
    fixture_state.pop("_journal", None)
    fixture_state["complete"] = False
    atomic_json(fixture_root / "diagnostic-state.json", fixture_state)
    atomic_json(
        fixture_root / ".diagnostic-sample.json",
        {
            "kind": "historical-admission-subset",
            "source_capture_id": state["capture_id"],
            "target_observations": target,
            "selected_data_receipts": len(data_pages),
            "selected_control_receipts": len(controls),
            "complete": False,
            "certified": False,
            "published": False,
        },
    )
    sample_rows = [
        {key: page[key] for key in ("index", "relative", "request_id")} for page in selected
    ]
    atomic_json(fixture_root / "diagnostic-page-index.json", sample_rows)
    if (fixture_root / "captures").exists() or (fixture_root / "genesis.json").exists():
        raise RuntimeError("admission fixture must not create a capture journal or genesis")


def _run_admission_child(
    *,
    benchmark: Any,
    output_root: Path,
    fixture_root: Path,
    capture_id: str,
    task_indices: list[int],
    target: int,
    start: float,
    total_deadline: float,
    device: int,
    operator_root: Path,
    source_sample: dict[str, Any] | None = None,
) -> dict[str, Any]:
    import production_run

    def stop_and_confirm(process: subprocess.Popen[Any]) -> None:
        if not _terminate_admission_group(process, production_run):
            raise RuntimeError("admission child process group was not confirmed reaped")

    report_root = output_root / "children"
    report_root.mkdir(exist_ok=True)
    child_report = report_root / f"native-{target}.json"
    log_path = report_root / f"native-{target}.log"
    command = [
        sys.executable,
        str(Path(benchmark.__file__).resolve()),
        "--worker",
        "native",
        "--admission-worker",
        "--fixture-root",
        str(fixture_root),
        "--worker-report",
        str(child_report),
        "--capture-id",
        capture_id,
        "--task-indices",
        *(str(index) for index in task_indices),
    ]
    env = {
        key: os.environ[key]
        for key in ("PATH", "LANG", "LC_ALL", "VIRTUAL_ENV")
        if key in os.environ
    }
    env.update(
        PYTHONNOUSERSITE="1",
        PYTHONDONTWRITEBYTECODE="1",
        PYTHONPATH=str(Path(benchmark._checkout()) / "src"),
        TMPDIR=str(output_root / "tmp"),
        DLT_TELEMETRY="0",
        DLT_SEND_ANONYMOUS_USAGE_STATS="false",
        DBT_SEND_ANONYMOUS_USAGE_STATS="false",
    )
    Path(env["TMPDIR"]).mkdir(exist_ok=True)
    child_started = time.monotonic()
    child_deadline = min(
        total_deadline - 15 * 60,
        start + 105 * 60,
        child_started + 20 * 60,
    )
    deadline_expired_before_spawn = child_deadline <= child_started
    command.extend(["--admission-deadline-monotonic", format(child_deadline, ".9f")])
    phase_path = fixture_root / ".benchmark-phase"
    phase_totals: dict[str, dict[str, Any]] = {}
    sample_count = 0
    max_sample_gap = 0.0
    last_sample = None
    aggregate_high_water = {
        "logical_bytes": 0,
        "allocated_bytes": 0,
        "database_bytes": 0,
        "database_allocated_bytes": 0,
        "index_bytes": 0,
        "index_allocated_bytes": 0,
        "spill_bytes": 0,
        "spill_allocated_bytes": 0,
        "export_bytes": 0,
        "export_allocated_bytes": 0,
        "verification_bytes": 0,
        "verification_allocated_bytes": 0,
        "logical_minus_allocated_min_bytes": None,
        "logical_minus_allocated_max_bytes": None,
    }
    tmp_high_water: dict[str, dict[str, Any]] = {}
    lowest_free = shutil.disk_usage(operator_root).free

    def make_partial_record(
        primary_error: BaseException,
        *,
        process: subprocess.Popen[Any] | None,
        worker_result: dict[str, Any] | None = None,
        worker_report_error: str | None = None,
        cleanup_error: BaseException | None = None,
    ) -> dict[str, Any]:
        if worker_result is None and worker_report_error is None:
            try:
                candidate = benchmark._read_json(child_report, 4 * 1024**2)
                worker_result = candidate if isinstance(candidate, dict) else None
                if worker_result is None:
                    worker_report_error = "worker report is not a JSON object"
            except Exception as exc:
                worker_report_error = f"{type(exc).__name__}: {exc}"

        worker_progress: dict[str, Any] = {}
        if isinstance(worker_result, dict):
            reported_progress = worker_result.get("worker_progress")
            if isinstance(reported_progress, dict):
                worker_progress = reported_progress
            else:
                progress_fields = {
                    "phase",
                    "source_capture_id",
                    "target_observations",
                    "selected_data_receipts",
                    "selected_control_receipts",
                    "source_receipt_identities",
                    "native_observations_landed",
                    "native_landing_complete",
                    "verified_evidence_sha256",
                    "verified_evidence_count",
                    "export_current_relation",
                    "export_completed_relations",
                    "verification_complete",
                    "verification_peak_temporary_bytes",
                    "verification_peak_temporary_allocated_bytes",
                }
                worker_progress = {
                    key: worker_result[key] for key in progress_fields if key in worker_result
                }
        worker_error = (
            worker_result.get("error")
            if isinstance(worker_result, dict) and isinstance(worker_result.get("error"), str)
            else None
        )
        primary_message = f"{type(primary_error).__name__}: {primary_error}"
        phase_coverage = {
            name: phase_totals.get(name, {}).get("samples", 0)
            for name in ("build", "export", "verification")
        }

        def partial_peak(phase: str, metric: str) -> int | None:
            phase_values = tmp_high_water.get(phase)
            if not isinstance(phase_values, dict):
                return None
            value = phase_values.get(metric)
            return value if _is_size(value) else None

        isolated_high_water = (
            aggregate_high_water
            if sample_count
            else {
                key: None if isinstance(value, int) else value
                for key, value in aggregate_high_water.items()
            }
        )
        blockers = [worker_error or primary_message]
        if cleanup_error is not None:
            blockers.append(f"cleanup uncertainty: {type(cleanup_error).__name__}: {cleanup_error}")
        partial_record = {
            "status": "failed_incomplete",
            "error": worker_error or primary_message,
            "primary_error": primary_message,
            "worker_report_error": worker_report_error,
            "cleanup_error": (
                f"{type(cleanup_error).__name__}: {cleanup_error}"
                if cleanup_error is not None
                else None
            ),
            "child_exit_code": process.returncode if process is not None else None,
            "child_log": str(log_path),
            "source_sample": source_sample,
            "worker_progress": worker_progress,
            "supervisor": {
                "sample_count": sample_count,
                "max_sample_gap_seconds": max_sample_gap if sample_count else None,
                "phase_high_water": phase_totals,
                "phase_sample_coverage": phase_coverage,
                "isolated_artifacts_high_water": isolated_high_water,
                "temporary_phase_high_water": tmp_high_water,
                "lowest_filesystem_free_bytes": lowest_free,
                "telemetry_complete": False,
            },
            "target_observations": target,
            "observations": worker_progress.get("native_observations_landed"),
            "build_peak_temporary_bytes": partial_peak("build", "logical_bytes"),
            "build_peak_temporary_allocated_bytes": partial_peak("build", "allocated_bytes"),
            "export_peak_temporary_bytes": partial_peak("export", "logical_bytes"),
            "export_peak_temporary_allocated_bytes": partial_peak("export", "allocated_bytes"),
            "verification_peak_temporary_bytes": partial_peak("verification", "logical_bytes"),
            "verification_peak_temporary_allocated_bytes": partial_peak(
                "verification", "allocated_bytes"
            ),
            "phase_coverage": phase_coverage,
            "complete": False,
            "certified": False,
            "published": False,
            "verification_complete": False,
            "decision": {
                "decision": "NO_ADMIT",
                "launch_allowed": False,
                "capacity_admitted": False,
                "production_admitted": False,
                "blockers": blockers,
            },
        }
        from kalshi_events.common import atomic_json

        atomic_json(child_report, partial_record)
        return partial_record

    def abort_with_partial(
        primary_error: BaseException,
        *,
        process: subprocess.Popen[Any] | None,
        worker_result: dict[str, Any] | None = None,
        worker_report_error: str | None = None,
    ) -> None:
        partial_record = make_partial_record(
            primary_error,
            process=process,
            worker_result=worker_result,
            worker_report_error=worker_report_error,
        )
        raise AdmissionChildFailure(str(primary_error), partial_record) from primary_error

    if deadline_expired_before_spawn:
        abort_with_partial(
            TimeoutError("admission children must finish by minute 105"),
            process=None,
        )

    with log_path.open("wb") as log:
        process: subprocess.Popen[Any] | None = None
        try:
            production_run._SPAWNING_CHILD = True
            spawn_error: BaseException | None = None
            try:
                process = subprocess.Popen(
                    command,
                    cwd=benchmark._checkout(),
                    env=env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            except BaseException as exc:
                spawn_error = exc
            finally:
                production_run._SPAWNING_CHILD = False
            if production_run._PENDING_STOP_SIGNAL is not None:
                signum = production_run._PENDING_STOP_SIGNAL
                production_run._PENDING_STOP_SIGNAL = None
                raise production_run.RunnerInterrupted(
                    f"received {signal.Signals(signum).name}; stopping admission child"
                )
            if spawn_error is not None:
                raise spawn_error.with_traceback(spawn_error.__traceback__)
            if process is None:
                raise RuntimeError("admission child did not start")
            while process.poll() is None:
                now = time.monotonic()
                if now >= child_deadline:
                    raise TimeoutError("admission child exceeded its 20-minute or global deadline")
                current = _measure_tree(output_root, device=device)
                temporary = _measure_tree(fixture_root / "tmp", device=device)
                sample_count += 1
                if last_sample is not None:
                    max_sample_gap = max(max_sample_gap, now - last_sample)
                last_sample = now
                for metric in aggregate_high_water:
                    if metric.startswith("logical_minus_allocated_"):
                        continue
                    aggregate_high_water[metric] = max(
                        aggregate_high_water[metric], current.get(metric, 0)
                    )
                current_gap = current["logical_minus_allocated_bytes"]
                aggregate_high_water["logical_minus_allocated_min_bytes"] = (
                    current_gap
                    if aggregate_high_water["logical_minus_allocated_min_bytes"] is None
                    else min(aggregate_high_water["logical_minus_allocated_min_bytes"], current_gap)
                )
                aggregate_high_water["logical_minus_allocated_max_bytes"] = (
                    current_gap
                    if aggregate_high_water["logical_minus_allocated_max_bytes"] is None
                    else max(aggregate_high_water["logical_minus_allocated_max_bytes"], current_gap)
                )
                phase = phase_path.read_text(encoding="ascii") if phase_path.is_file() else ""
                if phase:
                    current_phase = phase_totals.setdefault(
                        phase,
                        {
                            "samples": 0,
                            "logical_high_water_bytes": 0,
                            "allocated_high_water_bytes": 0,
                            "database_component_high_water_bytes": 0,
                            "database_component_allocated_high_water_bytes": 0,
                            "temporary_high_water_bytes": 0,
                            "temporary_allocated_high_water_bytes": 0,
                            "spill_high_water_bytes": 0,
                            "spill_allocated_high_water_bytes": 0,
                            "logical_minus_allocated_min_bytes": None,
                            "logical_minus_allocated_max_bytes": None,
                            "temporary_gap_min_bytes": None,
                            "temporary_gap_max_bytes": None,
                        },
                    )
                    current_phase["samples"] += 1
                    current_gap = current["logical_minus_allocated_bytes"]
                    current_phase["logical_minus_allocated_min_bytes"] = (
                        current_gap
                        if current_phase["logical_minus_allocated_min_bytes"] is None
                        else min(current_phase["logical_minus_allocated_min_bytes"], current_gap)
                    )
                    current_phase["logical_minus_allocated_max_bytes"] = (
                        current_gap
                        if current_phase["logical_minus_allocated_max_bytes"] is None
                        else max(current_phase["logical_minus_allocated_max_bytes"], current_gap)
                    )
                    temporary_gap = temporary["logical_minus_allocated_bytes"]
                    current_phase["temporary_gap_min_bytes"] = (
                        temporary_gap
                        if current_phase["temporary_gap_min_bytes"] is None
                        else min(current_phase["temporary_gap_min_bytes"], temporary_gap)
                    )
                    current_phase["temporary_gap_max_bytes"] = (
                        temporary_gap
                        if current_phase["temporary_gap_max_bytes"] is None
                        else max(current_phase["temporary_gap_max_bytes"], temporary_gap)
                    )
                    current_phase["logical_high_water_bytes"] = max(
                        current_phase["logical_high_water_bytes"], current["logical_bytes"]
                    )
                    current_phase["allocated_high_water_bytes"] = max(
                        current_phase["allocated_high_water_bytes"], current["allocated_bytes"]
                    )
                    current_phase["database_component_high_water_bytes"] = max(
                        current_phase["database_component_high_water_bytes"],
                        current["database_bytes"] + current["index_bytes"],
                    )
                    current_phase["database_component_allocated_high_water_bytes"] = max(
                        current_phase["database_component_allocated_high_water_bytes"],
                        current["database_allocated_bytes"] + current["index_allocated_bytes"],
                    )
                    current_phase["temporary_high_water_bytes"] = max(
                        current_phase["temporary_high_water_bytes"], temporary["logical_bytes"]
                    )
                    current_phase["temporary_allocated_high_water_bytes"] = max(
                        current_phase["temporary_allocated_high_water_bytes"],
                        temporary["allocated_bytes"],
                    )
                    current_phase["spill_high_water_bytes"] = max(
                        current_phase["spill_high_water_bytes"], temporary["spill_bytes"]
                    )
                    current_phase["spill_allocated_high_water_bytes"] = max(
                        current_phase["spill_allocated_high_water_bytes"],
                        temporary["spill_allocated_bytes"],
                    )
                    tmp_high_water[phase] = {
                        "logical_bytes": current_phase["temporary_high_water_bytes"],
                        "allocated_bytes": current_phase["temporary_allocated_high_water_bytes"],
                        "spill_bytes": current_phase["spill_high_water_bytes"],
                        "spill_allocated_bytes": current_phase["spill_allocated_high_water_bytes"],
                        "logical_minus_allocated_min_bytes": current_phase[
                            "temporary_gap_min_bytes"
                        ],
                        "logical_minus_allocated_max_bytes": current_phase[
                            "temporary_gap_max_bytes"
                        ],
                    }
                lowest_free = min(lowest_free, shutil.disk_usage(operator_root).free)
                if current["logical_bytes"] > 16 * GIB or temporary["logical_bytes"] > 16 * GIB:
                    raise RuntimeError("admission artifacts exceeded the 16 GiB diagnostic ceiling")
                if lowest_free < 150 * GIB:
                    raise RuntimeError("filesystem free space fell below the 150 GiB floor")
                time.sleep(0.25)
            stop_and_confirm(process)
        except BaseException as exc:
            cleanup_error = None
            if process is not None:
                try:
                    stop_and_confirm(process)
                except Exception as failure:
                    cleanup_error = failure
            partial_record = make_partial_record(
                exc,
                process=process,
                cleanup_error=cleanup_error,
            )
            with suppress(Exception):
                exc.partial_record = partial_record
            if cleanup_error is not None:
                failure = RuntimeError(
                    f"admission child cleanup failed; stopping the admission run: {cleanup_error}"
                )
                failure.partial_record = partial_record
                failure.runner_interrupted = isinstance(exc, production_run.RunnerInterrupted)
                raise failure from exc
            raise
    worker_report_error = None
    try:
        result = benchmark._read_json(child_report, 4 * 1024**2)
    except Exception as exc:
        result = None
        worker_report_error = f"{type(exc).__name__}: {exc}"
    if process.returncode != 0 or not isinstance(result, dict):
        if isinstance(result, dict) and isinstance(result.get("error"), str):
            primary_message = result["error"]
        elif process.returncode != 0:
            primary_message = f"native admission child exited with status {process.returncode}"
        else:
            primary_message = "native admission child returned an invalid worker report"
        primary_error = RuntimeError(primary_message)
        abort_with_partial(
            primary_error,
            process=process,
            worker_result=result if isinstance(result, dict) else None,
            worker_report_error=worker_report_error,
        )
    if not isinstance(result, dict):
        abort_with_partial(
            RuntimeError("native admission child returned an invalid report"),
            process=process,
            worker_report_error=worker_report_error,
        )

    def finalize_result(result: dict[str, Any]) -> dict[str, Any]:
        effective_deadline = result.get("effective_deadline_monotonic")
        entry_remaining = result.get("effective_deadline_remaining_at_worker_entry_seconds")
        effective_store_limit = result.get("effective_store_limit_seconds")
        if (
            result.get("effective_deadline_kind") != "absolute_monotonic_from_supervisor"
            or not _is_metric(effective_deadline)
            or abs(effective_deadline - child_deadline) > 1e-6
            or not _is_metric(entry_remaining)
            or entry_remaining <= 0
            or entry_remaining > 20 * 60
            or not _is_metric(effective_store_limit)
            or effective_store_limit <= 0
            or effective_store_limit > entry_remaining
        ):
            abort_with_partial(
                RuntimeError(
                    "admission worker did not honor the supervisor's effective monotonic deadline"
                ),
                process=process,
                worker_result=result,
            )
        if (
            result.get("certified") is not False
            or result.get("published") is not False
            or result.get("complete") is not False
        ):
            abort_with_partial(
                RuntimeError("native admission child produced a complete or certified artifact"),
                process=process,
                worker_result=result,
            )
        phase_coverage = {
            name: phase_totals.get(name, {}).get("samples", 0)
            for name in ("build", "export", "verification")
        }
        result.update(
            supervisor={
                "sample_count": sample_count,
                "max_sample_gap_seconds": max_sample_gap,
                "phase_high_water": phase_totals,
                "phase_sample_coverage": phase_coverage,
                "isolated_artifacts_high_water": aggregate_high_water,
                "temporary_phase_high_water": tmp_high_water,
                "lowest_filesystem_free_bytes": lowest_free,
                "telemetry_complete": sample_count > 0
                and max_sample_gap <= 2.0
                and all(phase_coverage[name] > 0 for name in ("build", "export", "verification")),
            },
            target_observations=target,
            child_elapsed_seconds=time.monotonic() - child_started,
            child_deadline_seconds=child_deadline - child_started,
            worker_entry_deadline_remaining_seconds=entry_remaining,
            certified=False,
            published=False,
            complete=False,
        )
        build_phase = phase_totals.get("build", {})
        build_database_peak = build_phase.get("database_component_high_water_bytes")
        build_database_allocated_peak = build_phase.get(
            "database_component_allocated_high_water_bytes"
        )
        terminal_database_component_bytes = result.get("build_database_component_bytes")
        terminal_database_allocated_component_bytes = result.get(
            "build_database_allocated_component_bytes"
        )
        result["build_database_component_bytes"] = (
            max(terminal_database_component_bytes, build_database_peak)
            if build_phase.get("samples", 0) > 0
            and _is_size(terminal_database_component_bytes)
            and _is_size(build_database_peak)
            else None
        )
        result["build_database_allocated_component_bytes"] = (
            max(terminal_database_allocated_component_bytes, build_database_allocated_peak)
            if build_phase.get("samples", 0) > 0
            and _is_size(terminal_database_allocated_component_bytes)
            and _is_size(build_database_allocated_peak)
            else None
        )
        result["telemetry_complete"] = result["supervisor"]["telemetry_complete"]
        result["verification_complete"] = result.get("verification", {}).get("complete") is True
        result["build_peak_temporary_bytes"] = max(
            result.get("build_phase_temporary_bytes", 0),
            tmp_high_water.get("build", {}).get("logical_bytes", 0),
        )
        result["build_peak_temporary_allocated_bytes"] = max(
            result.get("build_phase_temporary_allocated_bytes", 0),
            tmp_high_water.get("build", {}).get("allocated_bytes", 0),
        )
        result["export_peak_temporary_bytes"] = max(
            result.get("export_phase_temporary_bytes", 0),
            tmp_high_water.get("export", {}).get("logical_bytes", 0),
        )
        result["export_peak_temporary_allocated_bytes"] = max(
            result.get("export_phase_temporary_allocated_bytes", 0),
            tmp_high_water.get("export", {}).get("allocated_bytes", 0),
        )
        result["verification_peak_temporary_bytes"] = max(
            result.get("verification_phase_temporary_bytes", 0),
            tmp_high_water.get("verification", {}).get("logical_bytes", 0),
        )
        result["verification_peak_temporary_allocated_bytes"] = max(
            result.get("verification_phase_temporary_allocated_bytes", 0),
            tmp_high_water.get("verification", {}).get("allocated_bytes", 0),
        )
        result["spill_peak_bytes"] = max(
            tmp_high_water.get(name, {}).get("spill_bytes", 0)
            for name in ("build", "export", "verification")
        )
        result["spill_peak_allocated_bytes"] = max(
            tmp_high_water.get(name, {}).get("spill_allocated_bytes", 0)
            for name in ("build", "export", "verification")
        )
        result["spill_measurement_complete"] = all(
            name in phase_coverage and phase_coverage[name] > 0
            for name in ("build", "export", "verification")
        )
        result["spill_forecast_supported"] = (
            result.get("spill_limit_enforced_by_store") is True
            and result.get("spill_directory_configured") is True
            and _is_size(result.get("spill_limit_bytes"))
            and result["spill_measurement_complete"]
            and result["spill_peak_bytes"] <= result["spill_limit_bytes"]
        )
        return result

    try:
        return finalize_result(result)
    except AdmissionChildFailure:
        raise
    except BaseException as exc:
        partial_record = make_partial_record(exc, process=process, worker_result=result)
        with suppress(Exception):
            exc.partial_record = partial_record
        raise


def run(args: Any, benchmark: Any) -> int:
    from kalshi_events import capture
    from kalshi_events.common import CatalogueError, atomic_json, safe_path
    from kalshi_events.paths import PathPolicyError, persistent_path

    started = time.monotonic()
    started_utc = datetime.now(UTC).isoformat()
    checkout = benchmark._checkout()
    total_deadline = started + 2 * 60 * 60
    try:
        operator_root = safe_path(args.operator_root)
        output_root = persistent_path(args.output_dir, checkout=checkout, label="admission output")
        population_path = persistent_path(
            args.population_report, checkout=checkout, label="population report"
        )
        receipt_index_path = persistent_path(
            args.receipt_index, checkout=checkout, label="receipt index"
        )
        stride_report_path = persistent_path(
            args.stride_report, checkout=checkout, label="stride hint report"
        )
        old_stress_path = persistent_path(
            args.old_stress_report, checkout=checkout, label="prior stress report"
        )
    except PathPolicyError as exc:
        raise CatalogueError(str(exc)) from exc
    if benchmark._paths_overlap(operator_root, output_root) or output_root.exists():
        raise CatalogueError(
            "admission output must be a new path outside the read-only operator root"
        )
    if any(
        benchmark._paths_overlap(output_root, path)
        for path in (population_path, receipt_index_path, stride_report_path, old_stress_path)
    ):
        raise CatalogueError("admission output must not overlap an input report")
    output_root.mkdir(parents=True)
    report_path = output_root / "report.json"
    report: dict[str, Any] = {
        "version": 1,
        "mode": "admission_check",
        "status": "running",
        "scope": "bounded native diagnostic subset; incomplete, uncertified, unpublished",
        "complete": False,
        "certified": False,
        "published": False,
        "production_admitted": False,
        "started_at_utc": started_utc,
        "operator_root": str(operator_root),
        "capture_id": args.capture_id,
        "input_reports": {
            "population_report": str(population_path),
            "receipt_index": str(receipt_index_path),
            "stride_hints": str(stride_report_path),
            "prior_stress": str(old_stress_path),
        },
        "limits": {
            "child_seconds": 20 * 60,
            "children_finish_by_seconds": 105 * 60,
            "report_by_seconds": 120 * 60,
            "sample_targets": [250_000, 1_000_000, 3_000_000],
            "max_pages": 4096,
            "max_encoded_bytes": 2 * GIB,
            "max_generated_bytes": 16 * GIB,
            "database_bytes": 96 * GIB,
            "temporary_bytes": 144 * GIB,
            "operator_root_bytes": 350 * GIB,
            "aggregate_bytes": 400 * GIB,
            "minimum_free_bytes": 150 * GIB,
            "work_seconds": 22 * 60 * 60,
            "verification_seconds": 2 * 60 * 60,
        },
        "diagnostic_marker": ".diagnostic-sample.json",
        "phase_ledger": [],
    }

    def save(status: str, **fields: Any) -> None:
        report.update(fields)
        report["status"] = status
        report["elapsed_seconds"] = time.monotonic() - started
        report["finished_at_utc"] = datetime.now(UTC).isoformat()
        atomic_json(report_path, report)

    def record_phase(name: str, start: float, started_at: str, *, error: str | None = None) -> None:
        report["phase_ledger"].append(
            {
                "phase": name,
                "started_at_utc": started_at,
                "elapsed_seconds": time.monotonic() - start,
                "complete": error is None,
                **({"error": error} if error is not None else {}),
            }
        )

    def phase_start(name: str) -> tuple[float, str]:
        if time.monotonic() >= total_deadline:
            raise TimeoutError("two-hour admission report deadline expired")
        report["phase"] = name
        save("running")
        return time.monotonic(), datetime.now(UTC).isoformat()

    save("running")
    preflight: dict[str, Any] = {"blockers": []}
    old_stress = None
    stages: list[dict[str, Any]] = []
    production_run = None
    signal_handlers = None
    try:
        if str(checkout / "src") not in sys.path:
            sys.path.insert(0, str(checkout / "src"))
        scripts = str(checkout / "scripts")
        if scripts not in sys.path:
            sys.path.insert(0, scripts)
        import production_run

        signal_handlers = production_run._graceful_stop_handlers()
        signal_handlers.__enter__()

        source = benchmark.ReadOnlyStore(operator_root, max_seconds=2 * 60 * 60)
        with production_run._operator_writer_lock(operator_root) as lock_identity:
            root_device = operator_root.stat().st_dev
            preflight_started, preflight_utc = phase_start("preflight")
            preflight = _preflight(operator_root, output_root, production_run, device=root_device)
            preflight.update(
                writer_lock_identity=list(lock_identity),
                output_device=output_root.stat().st_dev,
                operator_device=root_device,
            )
            record_phase("preflight", preflight_started, preflight_utc)
            report["preflight"] = preflight
            if preflight["blockers"]:
                decision = _admission_decision(
                    {"supported": False, "blockers": []}, preflight, None
                )
                save("not_admitted", decision=decision)
                return 2

            source_started, source_utc = phase_start("source_and_census_binding")
            state = capture.read_capture(source, args.capture_id)
            binding = _admission_census_binding(
                source,
                state,
                population_path,
                receipt_index_path,
                benchmark,
                deadline=total_deadline,
            )
            approved_census = _check_authorized_census(binding)
            fingerprints = _dependency_fingerprint(checkout, benchmark, production_run)
            report["source"] = {
                "root_id": binding["root_id"],
                "capture_id": binding["capture_id"],
                "capture_state_sha256": binding["capture_state_sha256"],
                "population": binding["population"],
                "approved_census_contract": approved_census,
                "control_receipt_counts": binding["control_receipt_counts"],
                "selected_task_receipts": binding["selected_task_receipts"],
                "retired_scopes_excluded": binding["retired_scopes_excluded"],
                "fingerprints": binding["fingerprints"],
            }
            report["dependencies"] = fingerprints
            report["historical_selection"] = binding["selection"]
            report["selected_task_indices"] = binding["task_indices"]
            record_phase("source_and_census_binding", source_started, source_utc)
            save("running")

            hint_started, hint_utc = phase_start("prior_tail_binding")
            hints, hint_report = _validated_stride_hints(
                stride_report_path, binding["candidates"], binding, benchmark
            )
            old_stress = _validated_old_stress(old_stress_path, binding, benchmark)
            report["prior_stride_hints"] = hint_report
            report["prior_all_strata_stress"] = old_stress
            record_phase("prior_tail_binding", hint_started, hint_utc)
            save("running")

            storage_started, storage_utc = phase_start("fresh_storage_baseline")
            storage = _measure_admission_storage(
                operator_root, operator_root.parent, output_root, device=root_device
            )
            initial_storage = storage
            preflight["scope_complete"] = True
            preflight["accounting_scope"] = (
                "source census fully rebound; operator root, sibling production root, and isolated output tree inventoried"
            )
            report["preflight"] = preflight
            report["storage_baseline"] = storage
            record_phase("fresh_storage_baseline", storage_started, storage_utc)
            save("running")

            selection_order, seed_reasons, sample_strata = _admission_sample_order(
                binding["candidates"], binding["capture_id"], hints
            )
            report["sampling_plan"] = {
                "strata": sample_strata,
                "seeded_receipts": len(seed_reasons),
                "seed_reasons": seed_reasons,
                "extension_order": "stable request-id hash with population-proportional stratum round-robin",
            }
            controls_started, controls_utc = phase_start("control_receipt_revalidation")
            controls = benchmark._control_sample(binding["controls"])
            verified_controls = []
            encoded_total = decoded_total = 0
            for page in controls:
                _profile, evidence = _profile_receipt(source, state, page, benchmark, data=False)
                verified_controls.append(
                    {
                        "relative": page["relative"],
                        "endpoint": page["endpoint"],
                        "generation": page["generation"],
                        "status": page["status"],
                        "body_sha256": page["body_sha256"],
                        "verified_evidence_sha256": evidence,
                    }
                )
                encoded_total += page["encoded_bytes"]
                decoded_total += page["decoded_bytes"]
            record_phase("control_receipt_revalidation", controls_started, controls_utc)
            report["verified_control_receipts"] = verified_controls
            save("running")

            fixture_root = output_root / "fixture-native"
            copied: set[str] = set()
            if len(controls) > 4096 or encoded_total > 2 * GIB or decoded_total > 16 * GIB:
                raise CatalogueError("control receipt sample exceeds admission limits")
            sample_profiles: list[dict[str, Any]] = []
            selected_data: list[dict[str, Any]] = []
            selected_rows = 0
            current_encoded = encoded_total
            current_decoded = decoded_total
            target_index = 0
            sample_started, sample_utc = phase_start("nested_receipt_sampling")
            sample_extensions: dict[str, int] = defaultdict(int)
            for page in selection_order:
                if time.monotonic() >= min(total_deadline, started + 105 * 60):
                    raise TimeoutError("admission child window closed before all sample targets")
                profile, evidence = _profile_receipt(source, state, page, benchmark, data=True)
                if profile is None:
                    raise CatalogueError("data sample did not return a profile")
                profile["seed_reasons"] = seed_reasons.get(page["relative"], [])
                profile["verified_evidence_sha256"] = evidence
                sample_profiles.append(profile)
                selected_data.append(page)
                selected_rows += profile["observations"]
                current_encoded += page["encoded_bytes"]
                current_decoded += page["decoded_bytes"]
                if not profile["seed_reasons"]:
                    key = f"{page['endpoint']}#g{page['generation']}"
                    sample_extensions[key] += 1
                if (
                    len(selected_data) + len(controls) > 4096
                    or current_encoded > 2 * GIB
                    or current_decoded > 16 * GIB
                ):
                    raise CatalogueError(
                        "nested receipt sample exceeded its fixed page/byte limits"
                    )
                if target_index >= len(benchmark.ADMISSION_SAMPLE_TARGETS):
                    break
                target = benchmark.ADMISSION_SAMPLE_TARGETS[target_index]
                if selected_rows < target:
                    continue
                if selected_rows < target:
                    raise CatalogueError(
                        f"whole-receipt sample stopped below the {target:,}-observation target"
                    )
                record_phase("nested_receipt_sampling", sample_started, sample_utc)
                fixture_started, fixture_utc = phase_start(f"fixture_{target}")
                if target_index:
                    _clear_worker_outputs(fixture_root, binding["capture_id"])
                _prepare_fixture(
                    source,
                    state,
                    fixture_root,
                    selected_data,
                    controls,
                    copied,
                    target,
                    benchmark,
                )
                fixture_measure = _measure_tree(output_root, device=root_device)
                if fixture_measure["logical_bytes"] > 16 * GIB:
                    raise CatalogueError("diagnostic fixture/artifacts exceed 16 GiB")
                record_phase(f"fixture_{target}", fixture_started, fixture_utc)
                save("running")
                source_sample = {
                    "target_observations": target,
                    "normalized_source_observations": selected_rows,
                    "selected_data_receipts": len(selected_data),
                    "selected_control_receipts": len(controls),
                    "encoded_bytes": current_encoded,
                    "decoded_bytes": current_decoded,
                    "source_profile_complete": True,
                    "native_landing_complete": False,
                    "verification_complete": False,
                    "receipt_identities": [
                        {
                            key: page.get(key)
                            for key in ("relative", "request_id", "endpoint", "order", "index")
                        }
                        for page in [*selected_data, *controls]
                    ],
                }
                report["active_source_sample"] = source_sample
                save("running")
                child_started, child_utc = phase_start(f"native_child_{target}")
                child = _run_admission_child(
                    benchmark=benchmark,
                    output_root=output_root,
                    fixture_root=fixture_root,
                    capture_id=binding["capture_id"],
                    task_indices=binding["task_indices"],
                    target=target,
                    start=started,
                    total_deadline=total_deadline,
                    device=root_device,
                    operator_root=operator_root,
                    source_sample=source_sample,
                )
                report.pop("active_source_sample", None)
                if child.get("observations") != selected_rows:
                    raise CatalogueError(
                        "native sample landed an observation count different from the normalized whole-receipt sample"
                    )
                record_phase(f"native_child_{target}", child_started, child_utc)
                stage = {
                    **child,
                    "target_observations": target,
                    "observations": selected_rows,
                    "data_receipts": len(selected_data),
                    "control_receipts": len(controls),
                    "encoded_bytes": current_encoded,
                    "decoded_bytes": current_decoded,
                    "profiles": [dict(item) for item in sample_profiles],
                    "sample_duplicate_entities": child.get("sample_duplicate_entities"),
                    "spill_measurement_complete": child.get("spill_measurement_complete") is True,
                    "spill_forecast_supported": child.get("spill_forecast_supported") is True,
                    "telemetry_complete": child.get("telemetry_complete") is True,
                    "verification_complete": child.get("verification_complete") is True,
                    "extension_receipts_by_stratum": dict(sample_extensions),
                }
                stages.append(stage)
                report["sample_stages"] = stages
                report["verified_control_receipts"] = verified_controls
                report["sampled_receipts"] = len(selected_data) + len(controls)
                report["sampled_encoded_bytes"] = current_encoded
                report["sampled_decoded_bytes"] = current_decoded
                save("running")
                target_index += 1
                if target_index == 3:
                    break
                sample_started, sample_utc = phase_start("nested_receipt_sampling")
            if target_index < 3:
                raise CatalogueError(
                    "the 3,000,000-observation whole-receipt target was not reached"
                )

            final_storage_started, final_storage_utc = phase_start("fresh_final_storage_accounting")
            final_storage = _measure_admission_storage(
                operator_root, operator_root.parent, output_root, device=root_device
            )
            final_storage["filesystem_free_bytes"] = min(
                final_storage["filesystem_free_bytes"],
                preflight["free_bytes"],
                min(stage["supervisor"]["lowest_filesystem_free_bytes"] for stage in stages),
            )
            report["storage_final"] = final_storage
            record_phase("fresh_final_storage_accounting", final_storage_started, final_storage_utc)
            save("running")

            forecast_started, forecast_utc = phase_start("capacity_forecast")
            forecast = _admission_forecast(
                binding["population"],
                stages,
                {"baseline": initial_storage, "observed": final_storage},
                old_stress,
            )
            decision = _admission_decision(forecast, preflight, old_stress)
            report["capacity_forecast"] = forecast
            report["decision"] = decision
            report["selection_fingerprint_sha256"] = binding["fingerprints"]["selection_sha256"]
            report["sample_extension_receipts_by_stratum"] = dict(sample_extensions)
            record_phase("capacity_forecast", forecast_started, forecast_utc)
            if time.monotonic() > total_deadline:
                raise TimeoutError("admission report exceeded its two-hour wall-clock limit")
            save(
                "capacity_admitted_diagnostic_only"
                if decision["decision"] == "ADMIT"
                else "not_admitted"
            )
            return 0 if decision["decision"] == "ADMIT" else 2
    except KeyboardInterrupt as exc:
        partial_record = getattr(exc, "partial_record", None)
        if isinstance(partial_record, dict):
            report.pop("active_source_sample", None)
            report["failed_sample"] = partial_record
        report.setdefault("decision", {"decision": "NO_ADMIT", "launch_allowed": False})
        save("cancelled", error="KeyboardInterrupt: admission diagnostic stopped before completion")
        return 2
    except Exception as exc:
        runner_interrupted = (
            production_run is not None and isinstance(exc, production_run.RunnerInterrupted)
        ) or getattr(exc, "runner_interrupted", False) is True
        partial_record = getattr(exc, "partial_record", None)
        if isinstance(partial_record, dict):
            report.pop("active_source_sample", None)
            report["failed_sample"] = partial_record
        if runner_interrupted:
            report.setdefault("decision", {"decision": "NO_ADMIT", "launch_allowed": False})
            save("cancelled", error=f"{type(exc).__name__}: {exc}")
            return 2
        report.setdefault("decision", {"decision": "NO_ADMIT", "launch_allowed": False})
        current = report.get("decision", {})
        blockers = list(current.get("blockers", [])) if isinstance(current, dict) else []
        blockers.append(f"{type(exc).__name__}: {exc}")
        report["decision"] = {
            "decision": "NO_ADMIT",
            "launch_allowed": False,
            "capacity_admitted": False,
            "production_admitted": False,
            "blockers": blockers,
        }
        save("not_admitted", error=f"{type(exc).__name__}: {exc}")
        return 2
    finally:
        if signal_handlers is not None:
            signal_handlers.__exit__(None, None, None)
