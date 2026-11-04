"""Prepare a disposable pinned runtime and seal a real historical fixture."""

from __future__ import annotations

import ast
import hashlib
import importlib
import importlib.util
import json
import os
import pprint
import re
import shutil
import stat
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_HELPER_SHA256 = "ad86b68e48b352d2b05ed181ee47059d9da23deb8712d57a8e2ab9eb577fc9eb"
RUNTIME_HELPER_SOURCE = (
    ROOT / "data/runtime/kalshi-historical-completion-20261107/controls/runtime_manifest.py"
)
_CENSUS_CONSTANTS = {
    "AUTHORIZED_CAPTURE_ID",
    "AUTHORIZED_ROOT_ID",
    "AUTHORIZED_STATE_SHA256",
    "AUTHORIZED_INDEX_SHA256",
    "AUTHORIZED_POPULATION",
}
_CACHE_PARTS = {
    ".git",
    ".hypothesis",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".tox",
    ".venv",
    "build",
    "data",
    "dist",
}
_SOURCE_SNAPSHOTS: dict[Path, dict[str, tuple[str, str, str]]] = {}


def _sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _source_record(path: Path) -> tuple[str, str, str]:
    info = path.lstat()
    mode = oct(stat.S_IFMT(info.st_mode) | stat.S_IMODE(info.st_mode))
    if stat.S_ISLNK(info.st_mode):
        return ("symlink", mode, os.readlink(path))
    if not stat.S_ISREG(info.st_mode):
        raise RuntimeError(f"tracked source is not a regular file or symlink: {path}")
    return ("file", mode, _sha256(path.read_bytes()))


def _tracked_source_paths() -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    paths = []
    for raw in result.stdout.split(b"\0"):
        if not raw:
            continue
        relative = os.fsdecode(raw)
        path = Path(relative)
        if (
            path.is_absolute()
            or ".." in path.parts
            or path.parts[0] in _CACHE_PARTS
            or any(part in _CACHE_PARTS or part == "__pycache__" for part in path.parts)
            or path.name.endswith(".pyc")
        ):
            continue
        paths.append(relative)
    return sorted(paths)


def _private_control_root(control_root: Path) -> Path:
    root = Path(control_root)
    if (
        not root.is_absolute()
        or root.resolve(strict=True) != root
        or root.parent != ROOT / "data/runtime"
        or re.fullmatch(r"kalshi-historical-completion-\d{8}", root.name) is None
    ):
        raise RuntimeError("rehearsal controls must use a fresh managed runtime directory")
    info = root.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or stat.S_ISLNK(info.st_mode)
        or info.st_uid != os.geteuid()
        or stat.S_IMODE(info.st_mode) & 0o077
    ):
        raise RuntimeError("rehearsal control root must be a private real directory")
    return root


def _runtime_helper(path: Path) -> bytes:
    path = Path(path)
    if not path.is_absolute():
        raise RuntimeError("trusted runtime helper path must be absolute")
    for candidate in (RUNTIME_HELPER_SOURCE, Path(path)):
        try:
            info = candidate.lstat()
        except OSError as exc:
            raise RuntimeError(f"trusted runtime helper is unavailable: {candidate}") from exc
        if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode) or info.st_nlink != 1:
            raise RuntimeError(f"trusted runtime helper is unsafe: {candidate}")
        body = candidate.read_bytes()
        if _sha256(body) != RUNTIME_HELPER_SHA256:
            raise RuntimeError(f"trusted runtime helper checksum changed: {candidate}")
    return body


def _copy_tracked_source(source_root: Path) -> dict[str, tuple[str, str, str]]:
    source_root.mkdir(mode=0o700)
    records: dict[str, tuple[str, str, str]] = {}
    for relative in _tracked_source_paths():
        source = ROOT / relative
        target = source_root / relative
        record = _source_record(source)
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if record[0] == "symlink":
            os.symlink(record[2], target)
        else:
            target.write_bytes(source.read_bytes())
            target.chmod(stat.S_IMODE(source.lstat().st_mode))
        if _source_record(target) != record:
            raise RuntimeError(f"tracked source copy differs from checkout: {relative}")
        records[relative] = record
    actual = {
        path.relative_to(source_root).as_posix()
        for path in source_root.rglob("*")
        if path.is_file() or path.is_symlink()
    }
    if actual != set(records):
        raise RuntimeError("disposable source tree contains missing or untracked files")
    return records


def _offline_env(venv_root: Path) -> dict[str, str]:
    env = {
        name: os.environ[name]
        for name in ("PATH", "LANG", "LC_ALL", "UV_CACHE_DIR")
        if name in os.environ
    }
    env.update(
        PYTHONDONTWRITEBYTECODE="1",
        PYTHONNOUSERSITE="1",
        UV_OFFLINE="1",
        UV_LINK_MODE="copy",
        UV_PROJECT_ENVIRONMENT=str(venv_root),
    )
    return env


def _run(command: list[str], *, env: dict[str, str], timeout: float = 3600) -> str:
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )
    if result.returncode:
        raise RuntimeError(
            f"rehearsal setup command failed ({result.returncode}): {command[0]}\n"
            f"{result.stdout[-4000:]}{result.stderr[-4000:]}"
        )
    return result.stdout.strip()


def prepare_source(control_root: Path, runtime_helper: Path, wheel: Path) -> Path:
    """Copy the tracked checkout and build an independent locked runtime for it."""
    root = _private_control_root(control_root)
    helper_body = _runtime_helper(Path(runtime_helper))
    wheel = Path(wheel)
    try:
        wheel_info = wheel.lstat()
    except OSError as exc:
        raise RuntimeError("rehearsal wheel is unavailable") from exc
    if (
        not wheel.is_absolute()
        or not stat.S_ISREG(wheel_info.st_mode)
        or stat.S_ISLNK(wheel_info.st_mode)
        or wheel_info.st_nlink != 1
        or wheel.suffix != ".whl"
    ):
        raise RuntimeError("rehearsal wheel must be an absolute private regular wheel file")

    source_root = root / "source"
    records = _copy_tracked_source(source_root)
    _SOURCE_SNAPSHOTS[root] = records
    controls = root / "controls"
    controls.mkdir(mode=0o700)
    for name in ("grants", "phases", "activations"):
        (root / name).mkdir(mode=0o700)
    helper = controls / "runtime_manifest.py"
    helper.write_bytes(helper_body)
    helper.chmod(0o400)

    uv = shutil.which("uv")
    host_python = Path(getattr(sys, "_base_executable", sys.executable)).resolve(strict=True)
    if uv is None:
        raise RuntimeError("rehearsal requires uv")
    venv_root = root / ".venv"
    env = _offline_env(venv_root)
    _run(
        [
            uv,
            "venv",
            "--no-project",
            "--no-python-downloads",
            "--python",
            str(host_python),
            str(venv_root),
        ],
        env=env,
    )
    _run(
        [
            uv,
            "sync",
            "--project",
            str(source_root),
            "--frozen",
            "--offline",
            "--no-dev",
            "--no-install-project",
            "--link-mode",
            "copy",
            "--python",
            str(venv_root / "bin/python"),
        ],
        env=env,
    )
    _run(
        [
            uv,
            "pip",
            "install",
            "--python",
            str(venv_root / "bin/python"),
            "--offline",
            "--no-deps",
            "--link-mode",
            "copy",
            str(wheel),
        ],
        env=env,
    )
    if (source_root / ".venv").exists():
        raise RuntimeError("dependency sync escaped the independent runtime root")
    _run(
        [
            str(venv_root / "bin/python"),
            "-I",
            "-B",
            "-c",
            "import kalshi_events, pathlib, sys; "
            "installed=pathlib.Path(kalshi_events.__file__).resolve().parent; "
            "assert installed.is_relative_to(pathlib.Path(sys.prefix).resolve()), installed; "
            "source=pathlib.Path(sys.argv[1]); "
            "assert all((installed / p.relative_to(source)).read_bytes() == p.read_bytes() "
            "for p in source.rglob('*') if p.is_file()), installed",
            str(source_root / "src/kalshi_events"),
        ],
        env=env,
    )
    return source_root


def _assignment_nodes(source: bytes) -> dict[str, ast.Assign]:
    tree = ast.parse(source.decode("utf-8"))
    found: dict[str, ast.Assign] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        names = [target.id for target in node.targets if isinstance(target, ast.Name)]
        if len(names) == 1 and names[0] in _CENSUS_CONSTANTS:
            found[names[0]] = node
    if set(found) != _CENSUS_CONSTANTS:
        raise RuntimeError("historical admission census constants changed shape")
    return found


def _assignment_expressions(source: bytes) -> dict[str, bytes]:
    nodes = _assignment_nodes(source)
    offsets = [0]
    for line in source.splitlines(keepends=True):
        offsets.append(offsets[-1] + len(line))
    return {
        name: source[
            offsets[node.value.lineno - 1] + node.value.col_offset : offsets[
                node.value.end_lineno - 1
            ]
            + node.value.end_col_offset
        ]
        for name, node in nodes.items()
    }


def _replace_assignment_values(source: bytes, replacements: dict[str, Any]) -> bytes:
    nodes = _assignment_nodes(source)
    if set(nodes) != set(replacements):
        raise RuntimeError("historical admission census assignment set changed")
    offsets = [0]
    for line in source.splitlines(keepends=True):
        offsets.append(offsets[-1] + len(line))
    edits = []
    for name, node in nodes.items():
        start = offsets[node.value.lineno - 1] + node.value.col_offset
        end = offsets[node.value.end_lineno - 1] + node.value.end_col_offset
        value = replacements[name]
        expression = (
            value
            if isinstance(value, bytes)
            else pprint.pformat(value, sort_dicts=True, width=100).encode("utf-8")
        )
        edits.append((start, end, expression))
    for start, end, expression in sorted(edits, reverse=True):
        source = source[:start] + expression + source[end:]
    return source


def _patch_census(source: bytes, census: dict[str, Any]) -> bytes:
    previous = _assignment_expressions(source)
    population = {
        key: census[key]
        for key in (
            "candidate_pages",
            "observations_by_endpoint",
            "nested_event_markets",
            "exact_observations",
        )
    }
    replacements = {
        "AUTHORIZED_CAPTURE_ID": census["capture_id"],
        "AUTHORIZED_ROOT_ID": census["root_id"],
        "AUTHORIZED_STATE_SHA256": census["capture_state_sha256"],
        "AUTHORIZED_INDEX_SHA256": census["receipt_index_sha256"],
        "AUTHORIZED_POPULATION": population,
    }
    patched = _replace_assignment_values(source, replacements)
    assert _replace_assignment_values(patched, previous) == source
    values = {
        name: ast.literal_eval(node.value) for name, node in _assignment_nodes(patched).items()
    }
    assert all(values[name] == value for name, value in replacements.items())
    return patched


@contextmanager
def _frozen_modules(source_root: Path) -> Iterator[tuple[Any, Any, Any]]:
    names = ("historical_grants", "historical_admission")
    previous_modules = {name: sys.modules.pop(name, None) for name in names}
    previous_path = sys.path[:]
    previous_bytecode = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    scripts = source_root / "scripts"
    sys.path.insert(0, str(source_root / "src"))
    sys.path.insert(0, str(scripts))
    try:
        grants = importlib.import_module("historical_grants")
        admission = importlib.import_module("historical_admission")
        spec = importlib.util.spec_from_file_location(
            "historical_rehearsal_production_run", scripts / "production_run.py"
        )
        if spec is None or spec.loader is None:
            raise RuntimeError("frozen historical runner cannot be loaded")
        production = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(production)
        yield grants, admission, production
    finally:
        sys.dont_write_bytecode = previous_bytecode
        sys.path[:] = previous_path
        for name in names:
            sys.modules.pop(name, None)
            if previous_modules[name] is not None:
                sys.modules[name] = previous_modules[name]


def _write_runtime_manifest(control_root: Path, production: Any) -> str:
    helper = control_root / "controls/runtime_manifest.py"
    manifest = control_root / "runtime-manifest.json"
    if manifest.exists() or manifest.is_symlink():
        raise RuntimeError("runtime manifest already exists")
    env = {key: os.environ[key] for key in ("PATH", "LANG", "LC_ALL") if key in os.environ}
    env.update(PYTHONDONTWRITEBYTECODE="1", PYTHONNOUSERSITE="1")
    raw_sha = _run(
        [
            str(production.HISTORICAL_HOST_PYTHON),
            "-I",
            "-S",
            "-B",
            str(helper),
            "--write",
            "--root",
            str(control_root / ".venv"),
            "--manifest",
            str(manifest),
        ],
        env=env,
        timeout=1800,
    )
    manifest.chmod(0o400)
    if not re.fullmatch(r"[0-9a-f]{64}", raw_sha) or _sha256(manifest.read_bytes()) != raw_sha:
        raise RuntimeError("runtime helper returned an invalid manifest checksum")
    return raw_sha


def seal_fixture(control_root: Path, directory: Path, fixture: dict[str, Any]) -> dict[str, Any]:
    """Bind the disposable capture, census, runtime and source proof to frozen controls."""
    root = _private_control_root(control_root)
    source_root = root / "source"
    if root not in _SOURCE_SNAPSHOTS:
        raise RuntimeError("prepare_source must run before fixture sealing")
    required = {
        "capture_id",
        "root_id",
        "stage_name",
        "report_path",
        "census",
        "prior_attempts",
        "owned_spill",
        "unrelated_spill",
    }
    if not isinstance(fixture, dict) or not required.issubset(fixture):
        raise RuntimeError("historical rehearsal fixture is incomplete")

    from kalshi_events import warehouse

    census = fixture["census"]
    report_path = Path(fixture["report_path"])
    directory = Path(directory).resolve(strict=True)
    if not report_path.resolve(strict=True).is_relative_to(directory):
        raise RuntimeError("fixture report must be inside its disposable directory")

    admission_path = source_root / "scripts/historical_admission.py"
    for name, record in _SOURCE_SNAPSHOTS[root].items():
        target = source_root / name
        if _source_record(target) != record:
            raise RuntimeError(f"source copy changed before census sealing: {name}")
    original_admission = admission_path.read_bytes()
    patched_admission = _patch_census(original_admission, census)
    admission_path.write_bytes(patched_admission)
    admission_path.chmod(stat.S_IMODE((ROOT / "scripts/historical_admission.py").lstat().st_mode))
    if (
        _replace_assignment_values(patched_admission, _assignment_expressions(original_admission))
        != original_admission
    ):
        raise RuntimeError("census patch changed more than its five literal values")

    for name, record in _SOURCE_SNAPSHOTS[root].items():
        if name == "scripts/historical_admission.py":
            continue
        if _source_record(source_root / name) != record:
            raise RuntimeError(f"sealed source differs from its tracked snapshot: {name}")

    if _source_record(admission_path) == _SOURCE_SNAPSHOTS[root]["scripts/historical_admission.py"]:
        raise RuntimeError("fixture census did not update the copied admission constants")
    if set(
        path.relative_to(source_root).as_posix()
        for path in source_root.rglob("*")
        if path.is_file() or path.is_symlink()
    ) != set(_SOURCE_SNAPSHOTS[root]):
        raise RuntimeError("sealed source tree contains an untracked or missing file")

    with _frozen_modules(source_root) as (grants, _admission, production):
        if grants._validate_authorized_census_binding(census) != census:
            raise RuntimeError("fixture census failed the production control schema")

        report_raw = production._historical_session_report_bytes(report_path)
        report = json.loads(report_raw)
        capture_id = fixture["capture_id"]
        root_id = fixture["root_id"]
        stage_name = fixture["stage_name"]
        if (
            report.get("phase") != "stopped"
            or report.get("active_process_group") is not None
            or report.get("capture_id") != capture_id
            or report.get("root_id") != root_id
            or report.get("session_id") != report_path.parent.name
            or census.get("capture_id") != capture_id
            or census.get("root_id") != root_id
        ):
            raise RuntimeError("fixture report and census do not bind the same stopped session")
        session_id = report["session_id"]
        prior_attempts = fixture["prior_attempts"]
        template_identity = {
            "capture_id": capture_id,
            "session_id": session_id,
            "root_id": root_id,
            "stage_name": stage_name,
            "failed_attempts_sha256": prior_attempts["bundle_sha256"],
            "prior_session_sha256": prior_attempts["failed_session_sha256"],
            "state_sha256": None,
            "source_sha256": None,
            "runtime_sha256": None,
        }

        operator = directory / "operator"
        store = production._historical_store(operator, max_duration_seconds=600)
        state = warehouse.read_capture(store, capture_id)
        from kalshi_events.common import digest, json_bytes

        state_sha256 = digest(json_bytes(dict(state)))
        if census.get("capture_state_sha256") != state.verified_snapshot_sha256:
            raise RuntimeError("fixture census does not bind the real capture state")
        source_snapshot = warehouse.historical_readiness_snapshot(store, capture_id, stage_name)
        if (
            source_snapshot.get("binding", {}).get("capture_state_sha256") != state_sha256
            or source_snapshot.get("phase") != "historical-source-snapshot"
        ):
            raise RuntimeError("production source snapshot does not bind the fixture capture")
        grants.write_once_json(root / "source/source-snapshot.json", source_snapshot)

        runtime_manifest_sha256 = _write_runtime_manifest(root, production)
        operational_source_sha256 = production._historical_operational_source_sha256(root)
        runtime_sha256 = grants.sha256_json(
            {
                "runtime_manifest_sha256": runtime_manifest_sha256,
                "operational_source_sha256": operational_source_sha256,
            }
        )
        identity = {
            **template_identity,
            "state_sha256": state_sha256,
            "source_sha256": source_snapshot["result"]["source_sha256"],
            "runtime_sha256": runtime_sha256,
        }
        template = {
            "version": 1,
            "state": "inert",
            "identity": template_identity,
            "prior_attempts": prior_attempts,
            "fresh_pins": {"source": None, "state": None, "runtime": None, "approval": None},
            "phase_seals": [],
            "authorized_census": census,
        }
        control_sha256 = grants.write_once_json(root / "control.json", template)
        root_device = directory.stat(follow_symlinks=False).st_dev
        if (
            root_device != operator.stat(follow_symlinks=False).st_dev
            or root_device != root.stat(follow_symlinks=False).st_dev
        ):
            raise RuntimeError("fixture and controls do not share one production device")
        root_volume_uuid = production._volume_uuid(directory).upper()
        freeze = {
            "version": 1,
            "control_sha256": control_sha256,
            "identity": identity,
            "state_sha256": state_sha256,
            "source_sha256": identity["source_sha256"],
            "runtime_sha256": runtime_sha256,
            "source_snapshot_sha256": source_snapshot["sha256"],
            "receipt_evidence_sha256": source_snapshot["result"]["receipt_evidence_sha256"],
            "scope_sha256": source_snapshot["binding"]["scope_sha256"],
            "code_sha256": source_snapshot["binding"]["code_sha256"],
            "runtime_manifest_sha256": runtime_manifest_sha256,
            "operational_source_sha256": operational_source_sha256,
            "root_volume_uuid": root_volume_uuid,
            "root_device": root_device,
            "frozen_at_epoch": time.time(),
        }
        grants.write_once_json(root / "freeze.json", freeze)
        grants.validate_inert_control(template, template_identity)
        grants.validate_frozen_control(
            freeze,
            control_sha256=control_sha256,
            expected_identity=identity,
        )
        frozen, _ = grants.read_frozen_control(root, expected_identity=identity)
        production._historical_verify_runtime(root, freeze)
        if production._historical_operational_source_sha256(root) != operational_source_sha256:
            raise RuntimeError("frozen operational source changed during sealing")

        mount = production._historical_current_mount(
            directory, operator, root, report, identity, freeze
        )
        verified_mount = {
            "root_path": str(store.root.resolve()),
            "root_id": identity["root_id"],
            "capture_id": capture_id,
            "current_device": mount["device"],
            "device_lineage": mount["device_lineage"],
        }
        checked_snapshot = production._historical_source_snapshot(
            store,
            root,
            identity,
            freeze,
            grant_id="0" * 32,
            approval={},
            control=template,
            verified_mount=verified_mount,
        )
        if checked_snapshot != source_snapshot:
            raise RuntimeError("production source snapshot validation changed its frozen proof")
        census_result = production._historical_authorized_census(
            {
                "directory": directory,
                "operator": operator,
                "control": template,
                "grant": {"workflow_deadline_monotonic": time.monotonic() + 600},
            },
            full_revalidation=True,
        )
        expected_census = {
            key: census[key]
            for key in (
                "population_report_sha256",
                "receipt_index_sha256",
                "candidate_pages",
                "observations_by_endpoint",
                "nested_event_markets",
                "exact_observations",
            )
        }
        if census_result != expected_census:
            raise RuntimeError("production authorized-census validation changed its frozen input")
        return frozen["identity"]
