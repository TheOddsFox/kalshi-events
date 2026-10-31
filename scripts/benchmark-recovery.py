#!/usr/bin/env python3
"""Opt-in offline recovery comparison; generated roots and reports stay in checkout data."""

from __future__ import annotations

import argparse
import functools
import hashlib
import importlib.util
import inspect
import json
import os
import shutil
import stat
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

CHILD_TIMEOUT_SECONDS = 180
TOTAL_TIMEOUT_SECONDS = 3600
MAX_FIXTURE_ENTRIES = 600_000
MAX_FIXTURE_BYTES = 8 * 1024**3
MAX_MARKER_BYTES = 64 * 1024
MAX_ROOT_ID_BYTES = 1024**2
MAX_JOURNAL_BYTES = 8 * 1024**3
CHILD_ENV_KEYS = {
    "HOME",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "PATH",
    "PATHEXT",
    "SYSTEMROOT",
    "TEMP",
    "TMP",
    "TMPDIR",
    "VIRTUAL_ENV",
    "WINDIR",
}
FIXTURE_MARKER = {
    "version": 1,
    "fixture": "kalshi-events-recovery",
    "capture_id": "d" * 32,
    "root_id": "b" * 32,
}


@functools.cache
def _pipeline_tools() -> Any:
    path = Path(__file__).with_name("benchmark-pipeline.py")
    spec = importlib.util.spec_from_file_location("benchmark_pipeline_helpers", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load the existing benchmark supervisor")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _external(path: Path, protected: list[Path]) -> Path:
    candidate = path if path.is_absolute() else Path.cwd() / path
    cursor = Path(candidate.anchor)
    for part in candidate.parts[1:]:
        if part == "..":
            cursor = cursor.parent
            continue
        if part != ".":
            cursor /= part
        if cursor.is_symlink():
            raise ValueError(f"benchmark path cannot pass through a symlink: {cursor}")
    resolved = candidate.resolve()
    checkout = _pipeline_tools()._checkout_root()
    data = checkout / "data" if checkout is not None else None
    if any(
        resolved == base
        or (
            resolved.is_relative_to(base)
            and not (base == checkout and data is not None and resolved.is_relative_to(data))
        )
        for base in protected
    ):
        raise ValueError(f"benchmark input overlaps source checkouts: {resolved}")
    if any(
        (ancestor / ".git").exists() and ancestor != checkout
        for ancestor in (resolved, *resolved.parents)
    ):
        raise ValueError(f"benchmark data cannot be inside a Git checkout: {resolved}")
    return resolved


def _output(path: Path, protected: list[Path], *, label: str) -> Path:
    output = _pipeline_tools()._persistent_output(path, label)
    checkout = _pipeline_tools()._checkout_root()
    for source in protected:
        source = source.resolve()
        if source != checkout and (
            output == source or output.is_relative_to(source) or source.is_relative_to(output)
        ):
            raise ValueError(f"{label} overlaps a source input: {source}")
    return output


def _write_new(path: Path, value: dict[str, Any]) -> None:
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True, indent=2, allow_nan=False)
        stream.write("\n")


def _write_report(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    if path.is_symlink() or (
        path.exists() and not stat.S_ISREG(path.stat(follow_symlinks=False).st_mode)
    ):
        raise ValueError("benchmark report path must be a regular file")
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(temporary, flags, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, sort_keys=True, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def _source_provenance(source_root: Path, fixture_root: Path) -> dict[str, Any]:
    environment = _pipeline_tools()._environment(source_root, None)
    files = environment.pop("runtime_source_sha256")
    for label, path in (
        ("benchmark-recovery.py", Path(__file__).resolve()),
        ("benchmark-pipeline.py", Path(__file__).with_name("benchmark-pipeline.py")),
        ("recovery_fixtures.py", fixture_root / "tests/recovery_fixtures.py"),
        ("conftest.py", fixture_root / "tests/conftest.py"),
    ):
        files[label] = hashlib.sha256(path.read_bytes()).hexdigest()
    encoded = json.dumps(files, sort_keys=True, separators=(",", ":")).encode("utf-8")
    environment["source_files_sha256"] = files
    environment["source_inventory_sha256"] = hashlib.sha256(encoded).hexdigest()
    return environment


def _capture_id(root: Path) -> str:
    captures = root / "captures"
    if (
        root.is_symlink()
        or (root / "root.json").is_symlink()
        or not (root / "root.json").is_file()
        or captures.is_symlink()
        or not captures.is_dir()
    ):
        raise ValueError("fixture template is not a complete operator root")
    ids = []
    entry_count = 0
    with os.scandir(captures) as children:
        for child in children:
            entry_count += 1
            if entry_count > MAX_FIXTURE_ENTRIES:
                raise ValueError("fixture capture directory exceeds the entry limit")
            if child.is_symlink():
                raise ValueError("fixture capture entries cannot be symlinks")
            if stat.S_ISDIR(child.stat(follow_symlinks=False).st_mode):
                ids.append(child.name)
    ids.sort()
    if len(ids) != 1 or len(ids[0]) != 32 or any(char not in "0123456789abcdef" for char in ids[0]):
        raise ValueError("fixture template must contain exactly one capture")
    return ids[0]


def _read_json_bounded(path: Path, limit: int) -> Any:
    if path.is_symlink():
        raise ValueError("fixture identity files cannot be symlinks")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode):
            raise ValueError("fixture identity files must be regular files")
        if details.st_size > limit:
            raise ValueError(f"fixture identity file exceeds the {limit}-byte limit")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            content = stream.read(limit + 1)
        if len(content) > limit:
            raise ValueError(f"fixture identity file exceeds the {limit}-byte limit")
        return json.loads(content.decode("utf-8"))
    finally:
        os.close(descriptor)


def _sha256_bounded(path: Path, limit: int) -> str:
    if path.is_symlink():
        raise ValueError("fixture journal cannot be a symlink")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode):
            raise ValueError("fixture journal must be a regular file")
        if details.st_size > limit:
            raise ValueError(f"fixture journal exceeds the {limit}-byte limit")
        checksum = hashlib.sha256()
        total = 0
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                total += len(block)
                if total > limit:
                    raise ValueError(f"fixture journal exceeds the {limit}-byte limit")
                checksum.update(block)
        return checksum.hexdigest()
    finally:
        os.close(descriptor)


def _cache_present(root: Path) -> bool:
    capture_dir = root / "captures" / _capture_id(root)
    cache_path = root / "tmp" / "recovery-index" / capture_dir.name / "index.sqlite"
    return cache_path.is_file() and not cache_path.is_symlink()


def _validate_fixture_root(root: Path, *, tree_checked: bool = False) -> str:
    if root.is_symlink() or not root.is_dir():
        raise ValueError("fixture template must be a real directory")
    if not tree_checked:
        _reject_fixture_symlinks(root)
    marker_path = root / ".recovery-fixture.json"
    root_id_path = root / "root-id.json"
    try:
        marker = _read_json_bounded(marker_path, MAX_MARKER_BYTES)
        identity = _read_json_bounded(root_id_path, MAX_ROOT_ID_BYTES)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("fixture template is not a marked synthetic recovery root") from exc
    expected_keys = {
        *FIXTURE_MARKER,
        "scenario",
        "repeats",
        "num_pages",
        "wide_bytes",
        "hydration_tasks",
        "journal_sha256",
    }
    if (
        not isinstance(marker, dict)
        or set(marker) != expected_keys
        or any(marker.get(key) != value for key, value in FIXTURE_MARKER.items())
        or identity != {"version": 1, "root_id": FIXTURE_MARKER["root_id"]}
    ):
        raise ValueError("fixture template identity does not match the synthetic fixture marker")
    if marker["scenario"] not in {"primary", "hydration"}:
        raise ValueError("fixture marker has an unsupported scenario")
    if (
        isinstance(marker["repeats"], bool)
        or not isinstance(marker["repeats"], int)
        or not 0 <= marker["repeats"] <= 3
        or isinstance(marker["num_pages"], bool)
        or not isinstance(marker["num_pages"], int)
        or marker["num_pages"] < 1
        or isinstance(marker["wide_bytes"], bool)
        or not isinstance(marker["wide_bytes"], int)
        or marker["wide_bytes"] < 0
    ):
        raise ValueError("fixture marker has invalid generation settings")
    tasks = marker["hydration_tasks"]
    if marker["scenario"] == "hydration":
        if isinstance(tasks, bool) or not isinstance(tasks, int) or tasks < 1:
            raise ValueError("hydration fixture marker needs a positive task count")
    elif tasks is not None:
        raise ValueError("primary fixture marker cannot declare hydration tasks")
    capture_id = _capture_id(root)
    if capture_id != FIXTURE_MARKER["capture_id"]:
        raise ValueError("fixture template capture ID does not match the synthetic fixture marker")
    journal = root / "captures" / capture_id / "pages.jsonl"
    if not journal.is_file():
        raise ValueError("fixture journal is missing")
    checksum = _sha256_bounded(journal, MAX_JOURNAL_BYTES)
    if (
        not isinstance(marker["journal_sha256"], str)
        or len(marker["journal_sha256"]) != 64
        or marker["journal_sha256"] != checksum
    ):
        raise ValueError("fixture marker journal checksum does not match the fixture")
    return capture_id


def _reject_fixture_symlinks(root: Path) -> tuple[int, int]:
    if root.is_symlink() or not root.is_dir():
        raise ValueError("fixture root must be a real directory")
    entries_seen = 0
    byte_count = 0
    pending = [root]
    while pending:
        directory = pending.pop()
        with os.scandir(directory) as children:
            for child in children:
                entries_seen += 1
                if entries_seen > MAX_FIXTURE_ENTRIES:
                    raise ValueError("fixture template exceeds the entry limit")
                if child.is_symlink():
                    raise ValueError("fixture template cannot contain symlinks")
                details = child.stat(follow_symlinks=False)
                if stat.S_ISDIR(details.st_mode):
                    pending.append(Path(child.path))
                    continue
                if not stat.S_ISREG(details.st_mode):
                    raise ValueError(
                        "fixture template can contain only regular files and directories"
                    )
                byte_count += details.st_size
                if byte_count > MAX_FIXTURE_BYTES:
                    raise ValueError("fixture template exceeds the synthetic fixture size limit")
    return entries_seen, byte_count


def _worker_imports(source_root: Path, fixture_root: Path) -> None:
    source = source_root.resolve()
    paths = [str(source / "src"), str(fixture_root.resolve() / "tests")]
    sys.path[:] = [value for value in sys.path if value not in paths]
    sys.path[:0] = paths
    for name in tuple(sys.modules):
        if name == "kalshi_events" or name.startswith("kalshi_events."):
            del sys.modules[name]
    package = __import__("kalshi_events")
    if not Path(package.__file__).resolve().is_relative_to((source / "src").resolve()):
        raise RuntimeError(f"selected source import escaped {source / 'src'}")


def _child_env(source_root: Path, fixture_root: Path) -> dict[str, str]:
    environment = {key: os.environ[key] for key in CHILD_ENV_KEYS if key in os.environ}
    environment["PYTHONPATH"] = os.pathsep.join(
        (str(source_root / "src"), str(fixture_root / "tests"))
    )
    environment["PYTHONNOUSERSITE"] = "1"
    pipeline = _pipeline_tools()
    return pipeline._path_policy().child_environment(environment, pipeline._checkout_root())


def _seed_worker(args: argparse.Namespace) -> int:
    _worker_imports(args.source_root.resolve(), args.fixture_root.resolve())
    from recovery_fixtures import build_recovery_root

    capture_id = build_recovery_root(
        args.root,
        args.scenario,
        repeats=args.resets,
        num_pages=args.pages,
    )
    _write_new(args.result, {"capture_id": capture_id})
    return 0


class _StopAfterFirstDataPage(Exception):
    pass


def _resume_worker(args: argparse.Namespace) -> int:
    worker_started = time.perf_counter()
    _worker_imports(args.source_root.resolve(), args.fixture_root.resolve())
    from conftest import FakeClient

    from kalshi_events import capture, metrics
    from kalshi_events.common import Store

    first_attempt: dict[str, Any] = {}
    first_page: dict[str, float] = {}
    final_hydrate_started: float | None = None
    final_closure: float | None = None
    old_body_reads_before_first_attempt = 0
    new_receipts: set[str] = set()
    cutoff_attempts = 0
    lock = __import__("threading").Lock()
    capture.MAX_CONCURRENT_REQUESTS = 1
    original_read_page = capture.read_page
    original_commit = capture._commit_page_v2
    original_hydrate = capture._hydrate
    original_checkpoint = capture._checkpoint_v2

    def observed_read_page(store, relative, *read_args, **read_kwargs):
        nonlocal old_body_reads_before_first_attempt
        with lock:
            if relative not in new_receipts and not first_attempt:
                old_body_reads_before_first_attempt += 1
        return original_read_page(store, relative, *read_args, **read_kwargs)

    def observed_commit(store, state, index, relative, request_id, identities, **commit_kwargs):
        was_new = relative not in getattr(state, "processed", set())
        if "first_data_timing" in commit_kwargs:
            parameters = inspect.signature(original_commit).parameters
            if "first_data_timing" not in parameters and not any(
                parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()
            ):
                commit_kwargs.pop("first_data_timing")
        result = original_commit(
            store, state, index, relative, request_id, identities, **commit_kwargs
        )
        if was_new and relative not in first_page:
            manifest = json.loads(store.path(relative).read_text(encoding="utf-8"))
            if manifest.get("endpoint") != capture.CUTOFF_ENDPOINT:
                first_page["at"] = time.perf_counter() - worker_started
                if not args.complete:
                    raise _StopAfterFirstDataPage(manifest["endpoint"])
        return result

    def observed_hydrate(*hydrate_args, **hydrate_kwargs):
        nonlocal final_hydrate_started
        started = time.perf_counter()
        more = original_hydrate(*hydrate_args, **hydrate_kwargs)
        if not more:
            final_hydrate_started = started
        return more

    def observed_checkpoint(store, state):
        nonlocal final_closure
        result = original_checkpoint(store, state)
        if state.get("complete") and final_hydrate_started is not None:
            final_closure = time.perf_counter() - final_hydrate_started
        return result

    capture.read_page = observed_read_page
    capture._commit_page_v2 = observed_commit
    capture._hydrate = observed_hydrate
    capture._checkpoint_v2 = observed_checkpoint

    def client_factory(store, budget, **options):
        client = FakeClient(store, budget, **options)
        original_get = client.get

        def get(endpoint, params=None, context=None):
            nonlocal cutoff_attempts
            if endpoint == capture.CUTOFF_ENDPOINT:
                cutoff_attempts += 1
            else:
                with lock:
                    if not first_attempt:
                        first_attempt.update(
                            endpoint=endpoint,
                            elapsed_seconds=time.perf_counter() - worker_started,
                            cutoff_attempts=cutoff_attempts,
                        )
            relative = original_get(endpoint, params, context)
            with lock:
                new_receipts.add(relative)
            return relative

        client.get = get
        return client

    root = args.root.resolve()
    capture_id = args.capture_id
    cache_before = _cache_present(root)
    progress_path = args.result.with_name(args.result.stem + ".progress.json")
    complete = False
    collect = (
        metrics.collect(progress_path=progress_path)
        if "progress_path" in inspect.signature(metrics.collect).parameters
        else metrics.collect()
    )
    try:
        with collect as measured:
            store = Store(root)
            try:
                state = capture.capture(
                    store,
                    resume=capture_id,
                    client_factory=client_factory,
                )
                complete = bool(state.get("complete"))
            except _StopAfterFirstDataPage:
                if hasattr(measured, "progress"):
                    measured.progress("stopped")
            metrics_report = measured.report()
            usage = store.usage()
    finally:
        capture.read_page = original_read_page
        capture._commit_page_v2 = original_commit
        capture._hydrate = original_hydrate
        capture._checkpoint_v2 = original_checkpoint

    progress = (
        json.loads(progress_path.read_text(encoding="utf-8")) if progress_path.exists() else None
    )
    _write_new(
        args.result,
        {
            "capture_complete": complete,
            "first_new_data_attempt_endpoint": first_attempt.get("endpoint"),
            "first_new_data_attempt_elapsed_seconds": first_attempt.get("elapsed_seconds"),
            "cutoff_attempts_before_first_data_attempt": first_attempt.get("cutoff_attempts"),
            "old_prefix_body_reads_before_first_data_attempt": old_body_reads_before_first_attempt,
            "first_committed_new_page_elapsed_seconds": first_page.get("at"),
            "final_closure_elapsed_seconds": final_closure if complete else None,
            "recovery_elapsed_seconds": round(time.perf_counter() - worker_started, 6),
            "recovery_cache_mode": _cache_mode(root, metrics_report),
            "recovery_cache_file_present_before": cache_before,
            "storage_bytes": usage.get("retained_bytes"),
            "peak_rss_bytes": metrics_report.get("peak_rss_bytes", {}).get("self"),
            "metrics": metrics_report,
            "recovery_progress": progress,
        },
    )
    return 0


def _cache_mode(root: Path, report: dict[str, Any]) -> str:
    root_id_path = root / "root-id.json"
    if root_id_path.is_symlink() or not root_id_path.is_file():
        return "ineligible-root-id"
    try:
        identity = json.loads(root_id_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "ineligible-root-id"
    if not isinstance(identity, dict) or not isinstance(identity.get("root_id"), str):
        return "ineligible-root-id"
    counters = report.get("counters", {})
    if counters.get("recovery_cache_reused", 0):
        return "warm"
    if counters.get("recovery_cache_enabled", 0):
        return "cold-supported"
    return "unsupported"


def _warm_cache_worker(args: argparse.Namespace) -> int:
    _worker_imports(args.source_root.resolve(), args.fixture_root.resolve())
    from kalshi_events import capture
    from kalshi_events.common import Store
    from kalshi_events.incremental import root_id
    from kalshi_events.recovery_index import RecoveryParentIndex, sealed_settings_sha256

    store = Store(args.root.resolve())
    with store.writer_guard():
        state = capture.read_capture(
            store, args.capture_id, _writer_resume=True, _parent_index=None
        )
        if (
            state.get("acquisition_protocol") != capture.CURRENT_ACQUISITION_PROTOCOL
            or state.get("selected") is not None
            or state.get("incremental") is True
            or state.get("complete")
            or not state.page_rows
            or state.genesis_anchor is None
        ):
            raise ValueError("warm-cache setup requires an unfinished full protocol-5 capture")
        identities = RecoveryParentIndex(
            store,
            args.capture_id,
            root_id(store),
            sealed_settings_sha256(state),
            page_rows=state.page_rows,
            page_anchors=state.page_anchors,
            genesis_anchor=state.genesis_anchor,
        )
        try:
            for row in capture.page_receipts(store, state):
                capture._index_receipt(store, state, row, identities)
            identities.flush(force=True)
        finally:
            identities.close()
    _write_new(
        args.result,
        {"capture_id": args.capture_id, "indexed_pages": identities.watermark_pages},
    )
    return 0


def _validate_worker(args: argparse.Namespace) -> int:
    _validate_fixture_root(args.root.resolve())
    _worker_imports(args.source_root.resolve(), args.fixture_root.resolve())
    from kalshi_events import capture
    from kalshi_events.common import Store

    store = Store(args.root.resolve())
    state = capture.read_capture(store, args.capture_id)
    _write_new(
        args.result,
        {
            "capture_id": state["capture_id"],
            "complete": bool(state.get("complete")),
            "page_references": len(capture.page_receipts(store, state)),
        },
    )
    return 0


def _copy_worker(args: argparse.Namespace) -> int:
    capture_id = _copy_fixture(args.fixture_root.resolve(), args.root.resolve())
    _write_new(args.result, {"capture_id": capture_id})
    return 0


def _command(
    args: list[str], *, env: dict[str, str], timeout: float
) -> subprocess.CompletedProcess[str]:
    result = _pipeline_tools()._run_supervised(
        args,
        timeout=timeout,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(result.stderr[-4000:] or f"child exited {result.returncode}")
    return result


def _median(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [row[key] for row in rows if isinstance(row.get(key), (int, float))]
    return round(statistics.median(values), 6) if values else None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-source-root", type=Path)
    parser.add_argument("--reference-revision")
    parser.add_argument("--source-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--report-dir", type=Path)
    parser.add_argument("--fixture-template", type=Path)
    parser.add_argument("--scenario", choices=("primary", "hydration"), default="primary")
    parser.add_argument("--cache-mode", choices=("cold", "warm"), default="cold")
    parser.add_argument(
        "--pages", type=int, default=100, help="pages per cursor generation, up to 163,000"
    )
    parser.add_argument(
        "--resets", type=int, default=0, help="cursor reset generations in a primary fixture"
    )
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--runs", type=int, default=3, help="alternating reference/candidate pairs")
    parser.add_argument(
        "--complete", action="store_true", help="continue offline through capture closure"
    )
    parser.add_argument("--worker-seed", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--worker-resume", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--worker-validate", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--worker-copy", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--worker-warm-cache", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--fixture-root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--capture-id", help=argparse.SUPPRESS)
    parser.add_argument("--result", type=Path, help=argparse.SUPPRESS)
    return parser


def _worker(args: argparse.Namespace) -> int | None:
    modes = (
        args.worker_seed,
        args.worker_resume,
        args.worker_validate,
        args.worker_copy,
        args.worker_warm_cache,
    )
    if not any(modes):
        return None
    required = (args.root, args.result, args.fixture_root)
    if any(path is None for path in required) or args.source_root is None:
        raise ValueError("worker mode is missing required paths")
    protected = [args.source_root.resolve(), Path(__file__).resolve().parents[1]]
    if not args.worker_copy:
        protected.append(args.fixture_root.resolve())
    outputs_protected = protected[:]
    args.root = _output(args.root, outputs_protected, label="recovery worker root")
    args.result = _output(args.result, outputs_protected, label="recovery worker result")
    if sum(modes) != 1:
        raise ValueError("select exactly one worker mode")
    if args.worker_seed:
        return _seed_worker(args)
    if args.worker_copy:
        source = _external(args.fixture_root, protected)
        destination = args.root.resolve()
        if (
            source == destination
            or source.is_relative_to(destination)
            or destination.is_relative_to(source)
        ):
            raise ValueError("fixture copy destination cannot contain its source")
        return _copy_worker(args)
    if not isinstance(args.capture_id, str) or len(args.capture_id) != 32:
        raise ValueError("capture worker needs a capture ID")
    if args.worker_validate:
        return _validate_worker(args)
    if args.worker_warm_cache:
        return _warm_cache_worker(args)
    return _resume_worker(args)


def _revision(root: Path) -> str | None:
    result = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def _worktree_dirty(root: Path) -> bool | None:
    result = subprocess.run(
        ["git", "-C", str(root), "status", "--porcelain", "--untracked-files=normal"],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    return bool(result.stdout.strip()) if result.returncode == 0 else None


def _copy_fixture(source: Path, destination: Path) -> str:
    capture_id = _validate_fixture_root(source)
    shutil.copytree(source, destination, symlinks=True)
    _reject_fixture_symlinks(destination)
    _validate_fixture_root(destination, tree_checked=True)
    return capture_id


def _run_one(
    *,
    role: str,
    source_root: Path,
    fixture_root: Path,
    seed_root: Path,
    report_dir: Path,
    name: str,
    args: argparse.Namespace,
    started: float,
    source_inventory_sha256: str,
) -> dict[str, Any]:
    root = report_dir / "roots" / name
    root.parent.mkdir(exist_ok=True)
    result_path = report_dir / f"{name}.json"
    copy_result = report_dir / f"{name}-copy.json"
    deadline = started + TOTAL_TIMEOUT_SECONDS
    if time.perf_counter() >= deadline:
        raise TimeoutError("aggregate benchmark time reached one hour")
    copy_started = time.perf_counter()
    copy_command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker-copy",
        "--source-root",
        str(source_root),
        "--fixture-root",
        str(seed_root),
        "--root",
        str(root),
        "--result",
        str(copy_result),
    ]
    remaining = deadline - time.perf_counter()
    if remaining <= 0:
        raise TimeoutError("aggregate benchmark time reached one hour")
    _command(
        copy_command,
        env=_child_env(source_root, seed_root),
        timeout=min(CHILD_TIMEOUT_SECONDS, remaining),
    )
    capture_id = json.loads(copy_result.read_text(encoding="utf-8"))["capture_id"]
    copy_seconds = time.perf_counter() - copy_started
    env = _child_env(source_root, fixture_root)
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker-resume",
        "--source-root",
        str(source_root),
        "--fixture-root",
        str(fixture_root),
        "--root",
        str(root),
        "--capture-id",
        capture_id,
        "--result",
        str(result_path),
    ]
    if args.complete:
        command.append("--complete")
    child_started = time.perf_counter()
    remaining = deadline - child_started
    if remaining <= 0:
        raise TimeoutError("aggregate benchmark time reached one hour")
    _command(command, env=env, timeout=min(CHILD_TIMEOUT_SECONDS, remaining))
    child_seconds = time.perf_counter() - child_started
    measured = json.loads(result_path.read_text(encoding="utf-8"))
    if role == "reference":
        measured["recovery_cache_mode"] = "unsupported"
    return {
        "role": role,
        "name": name,
        "fixture_copy_seconds": round(copy_seconds, 6),
        "subprocess_seconds": round(child_seconds, 6),
        "source_inventory_sha256": source_inventory_sha256,
        **measured,
    }


def _benchmark(args: argparse.Namespace) -> int:
    overall_started = time.perf_counter()
    reference_root = args.reference_source_root.resolve()
    candidate_root = args.source_root.resolve()
    for label, source in (("reference", reference_root), ("candidate", candidate_root)):
        if not source.is_dir() or not (source / "src/kalshi_events/__init__.py").is_file():
            raise ValueError(f"{label} source root must contain src/kalshi_events")
    if reference_root == candidate_root:
        raise ValueError("reference and candidate source roots must be different")
    reference_revision = _revision(reference_root)
    if (
        args.reference_revision
        and reference_revision
        and not reference_revision.startswith(args.reference_revision)
    ):
        raise ValueError("reference source revision does not match --reference-revision")
    fixture_root = candidate_root
    protected = [candidate_root, reference_root, Path(__file__).resolve().parents[1]]
    fixture_template = None
    if args.fixture_template:
        fixture_template = _external(args.fixture_template, protected)
        checkout = _pipeline_tools()._checkout_root()
        local_data = checkout / "data" if checkout is not None else None
        for source in protected:
            if (
                source == checkout
                and local_data is not None
                and fixture_template.is_relative_to(local_data)
            ):
                continue
            if (
                fixture_template == source
                or fixture_template.is_relative_to(source)
                or source.is_relative_to(fixture_template)
            ):
                raise ValueError("fixture template must be disjoint from source checkouts")
        protected.append(fixture_template)
    if args.report_dir is None:
        suffix = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        pipeline = _pipeline_tools()
        checkout = pipeline._checkout_root()
        if checkout is not None:
            parent = pipeline._path_policy().data_directory(checkout, "benchmarks")
        else:
            parent = Path(tempfile.gettempdir()).resolve()
        report_dir = parent / f"kalshi-recovery-benchmark-{suffix}-{os.getpid()}"
    else:
        report_dir = args.report_dir
    report_dir = _output(
        report_dir,
        [candidate_root, reference_root, *([fixture_template] if fixture_template else [])],
        label="recovery benchmark report directory",
    )
    if fixture_template and fixture_template.is_relative_to(report_dir):
        raise ValueError("report directory and fixture template must be disjoint")
    if report_dir.exists():
        raise FileExistsError(f"report directory must be new: {report_dir}")
    report_dir.mkdir(parents=True)
    (report_dir / "roots").mkdir()
    report_path = report_dir / "report.json"
    fixture_page_references = (
        args.pages * (args.resets + 1) + args.resets
        if args.scenario == "primary"
        else args.pages + 7
    )
    summary: dict[str, Any] = {
        "version": 1,
        "status": "running",
        "current_step": "source_inventory",
        "scenario": args.scenario,
        "pages": args.pages,
        "fixture_page_references": fixture_page_references,
        "resets": args.resets,
        "complete_capture_mode": args.complete,
        "cache_mode_requested": args.cache_mode,
        "warmups": args.warmups,
        "run_pairs": args.runs,
        "child_timeout_seconds": CHILD_TIMEOUT_SECONDS,
        "reference": {
            "root": str(reference_root),
            "revision": reference_revision,
            "declared_archive_revision": args.reference_revision,
            "revision_source": "git" if reference_revision else "operator_metadata",
            "working_tree_dirty": _worktree_dirty(reference_root),
        },
        "candidate": {
            "root": str(candidate_root),
            "revision": _revision(candidate_root),
            "working_tree_dirty": _worktree_dirty(candidate_root),
        },
        "runs": [],
        "limitations": [
            "All requests are served by FakeClient. Workers receive an allowlisted environment but execute trusted source code without OS network isolation; do not use untrusted source roots.",
            "Production supervisor accounting is not included, so these timings cannot establish a five-minute end-to-end recovery target.",
            "Default mode stops after the first committed new data page; closure timing is reported only with --complete.",
        ],
    }
    _write_report(report_path, summary)
    setup_started = time.perf_counter()
    seed_root = report_dir / "seed"
    try:
        source_environment = {
            "reference": _source_provenance(reference_root, fixture_root),
            "candidate": _source_provenance(candidate_root, fixture_root),
        }
        summary["source_environment"] = source_environment
        summary["current_step"] = "fixture_setup"
        _write_report(report_path, summary)
        if fixture_template:
            copy_result = report_dir / "seed-copy.json"
            copy_command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "--worker-copy",
                "--source-root",
                str(candidate_root),
                "--fixture-root",
                str(fixture_template),
                "--root",
                str(seed_root),
                "--result",
                str(copy_result),
            ]
            remaining = TOTAL_TIMEOUT_SECONDS - (time.perf_counter() - overall_started)
            if remaining <= 0:
                raise TimeoutError("aggregate benchmark time reached one hour")
            _command(
                copy_command,
                env=_child_env(candidate_root, fixture_template),
                timeout=min(CHILD_TIMEOUT_SECONDS, remaining),
            )
            seed_capture_id = json.loads(copy_result.read_text(encoding="utf-8"))["capture_id"]
            validate_result = report_dir / "seed-validation.json"
            env = _child_env(candidate_root, fixture_root)
            validate_command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "--worker-validate",
                "--source-root",
                str(candidate_root),
                "--fixture-root",
                str(fixture_root),
                "--root",
                str(seed_root),
                "--capture-id",
                seed_capture_id,
                "--result",
                str(validate_result),
            ]
            remaining = TOTAL_TIMEOUT_SECONDS - (time.perf_counter() - overall_started)
            if remaining <= 0:
                raise TimeoutError("aggregate benchmark time reached one hour")
            _command(validate_command, env=env, timeout=min(CHILD_TIMEOUT_SECONDS, remaining))
            fixture_page_references = json.loads(validate_result.read_text(encoding="utf-8"))[
                "page_references"
            ]
        else:
            env = _child_env(candidate_root, fixture_root)
            seed_result = report_dir / "seed.json"
            seed_command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "--worker-seed",
                "--source-root",
                str(candidate_root),
                "--fixture-root",
                str(fixture_root),
                "--root",
                str(seed_root),
                "--result",
                str(seed_result),
                "--scenario",
                args.scenario,
                "--pages",
                str(args.pages),
                "--resets",
                str(args.resets),
            ]
            remaining = TOTAL_TIMEOUT_SECONDS - (time.perf_counter() - overall_started)
            if remaining <= 0:
                raise TimeoutError("aggregate benchmark time reached one hour")
            _command(seed_command, env=env, timeout=min(CHILD_TIMEOUT_SECONDS, remaining))
            seed_capture_id = json.loads(seed_result.read_text(encoding="utf-8"))["capture_id"]
        summary["fixture_page_references"] = fixture_page_references
        summary["fixture_setup_seconds"] = round(time.perf_counter() - setup_started, 6)
        summary["fixture_capture_id"] = seed_capture_id
        fixture_identity = json.loads(
            (seed_root / ".recovery-fixture.json").read_text(encoding="utf-8")
        )
        fixture_identity_json = json.dumps(
            fixture_identity, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        summary["fixture_identity"] = fixture_identity
        summary["fixture_identity_sha256"] = hashlib.sha256(fixture_identity_json).hexdigest()
        summary["requested_scenario"] = summary["scenario"]
        summary["scenario"] = fixture_identity["scenario"]
        summary["pages"] = fixture_identity["num_pages"]
        summary["resets"] = fixture_identity["repeats"]
        summary["current_step"] = "cache_seed" if args.cache_mode == "warm" else "runs"
        _write_report(report_path, summary)

        if args.cache_mode == "warm":
            if not (candidate_root / "src/kalshi_events/recovery_index.py").is_file():
                raise ValueError("--cache-mode warm requires the candidate recovery index API")
            warm_result = report_dir / "warm-cache.json"
            summary["current_step"] = "cache_seed"
            _write_report(report_path, summary)
            warm_command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "--worker-warm-cache",
                "--source-root",
                str(candidate_root),
                "--fixture-root",
                str(fixture_root),
                "--root",
                str(seed_root),
                "--capture-id",
                seed_capture_id,
                "--result",
                str(warm_result),
            ]
            warm_env = _child_env(candidate_root, fixture_root)
            remaining = TOTAL_TIMEOUT_SECONDS - (time.perf_counter() - overall_started)
            if remaining <= 0:
                raise TimeoutError("aggregate benchmark time reached one hour")
            warm_started = time.perf_counter()
            _command(
                warm_command,
                env=warm_env,
                timeout=min(CHILD_TIMEOUT_SECONDS, remaining),
            )
            warm_result_data = json.loads(warm_result.read_text(encoding="utf-8"))
            if warm_result_data.get("indexed_pages") != fixture_page_references:
                raise ValueError("warm-cache setup did not index the complete fixture prefix")
            summary["cache_seed_seconds"] = round(time.perf_counter() - warm_started, 6)

        sources = [("reference", reference_root), ("candidate", candidate_root)]

        def record_run(
            role: str, source: Path, name: str, *, warmup: bool, pair: int | None = None
        ) -> None:
            inventory = _source_provenance(source, fixture_root)["source_inventory_sha256"]
            summary["current_step"] = name
            summary["active_source_inventory_sha256"] = inventory
            _write_report(report_path, summary)
            row = _run_one(
                role=role,
                source_root=source,
                fixture_root=fixture_root,
                seed_root=seed_root,
                report_dir=report_dir,
                name=name,
                args=args,
                started=overall_started,
                source_inventory_sha256=inventory,
            )
            row["warmup"] = warmup
            if pair is not None:
                row["pair"] = pair
            summary["runs"].append(row)
            summary.pop("active_source_inventory_sha256", None)
            summary["current_step"] = "runs"
            _write_report(report_path, summary)

        for warmup in range(args.warmups):
            for role, source in sources:
                name = f"warmup-{warmup + 1}-{role}"
                record_run(role, source, name, warmup=True)
        for pair in range(args.runs):
            ordered = sources if pair % 2 == 0 else list(reversed(sources))
            for role, source in ordered:
                name = f"pair-{pair + 1}-{role}"
                record_run(role, source, name, warmup=False, pair=pair + 1)
        summary["current_step"] = "final_source_inventory"
        _write_report(report_path, summary)
        final_environment = {
            "reference": _source_provenance(reference_root, fixture_root),
            "candidate": _source_provenance(candidate_root, fixture_root),
        }
        summary["final_source_inventory_sha256"] = {
            role: value["source_inventory_sha256"] for role, value in final_environment.items()
        }
        summary["source_inventory_stable"] = {
            role: final_environment[role]["source_inventory_sha256"]
            == source_environment[role]["source_inventory_sha256"]
            for role in source_environment
        }
        summary["source_inventory_stable"] = {
            role: was_stable
            and all(
                row.get("source_inventory_sha256")
                == source_environment[role]["source_inventory_sha256"]
                for row in summary["runs"]
                if row.get("role") == role
            )
            for role, was_stable in summary["source_inventory_stable"].items()
        }
        summary["status"] = "complete"
    except (OSError, RuntimeError, TimeoutError, ValueError, subprocess.TimeoutExpired) as exc:
        summary.setdefault("fixture_setup_seconds", round(time.perf_counter() - setup_started, 6))
        summary["stopped_elapsed_seconds"] = round(time.perf_counter() - overall_started, 6)
        summary["status"] = "failed"
        summary["error"] = str(exc)
        summary["current_step"] = "failed"
        _write_report(report_path, summary)
    candidate_runs = [
        row for row in summary["runs"] if row.get("role") == "candidate" and not row.get("warmup")
    ]
    reference_runs = [
        row for row in summary["runs"] if row.get("role") == "reference" and not row.get("warmup")
    ]
    summary["median_seconds"] = {
        role: {
            "first_new_data_attempt": _median(rows, "first_new_data_attempt_elapsed_seconds"),
            "first_committed_new_page": _median(rows, "first_committed_new_page_elapsed_seconds"),
            "recovery_total": _median(rows, "recovery_elapsed_seconds"),
            "final_closure": _median(rows, "final_closure_elapsed_seconds"),
        }
        for role, rows in (("reference", reference_runs), ("candidate", candidate_runs))
    }
    summary["aggregate_performance_seconds"] = round(time.perf_counter() - overall_started, 6)
    summary["current_step"] = None
    _write_report(report_path, summary)
    print(report_path)
    return 0 if summary["status"] == "complete" else 2


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    worker_result = _worker(args)
    if worker_result is not None:
        return worker_result
    if args.reference_source_root is None:
        parser.error("--reference-source-root is required for a benchmark run")
    if args.pages < 1 or args.pages > 163_000:
        parser.error("--pages must be between 1 and 163000")
    if args.resets < 0 or args.resets > 3:
        parser.error("--resets must be between 0 and 3")
    if args.warmups < 0 or args.runs < 1:
        parser.error("--warmups must be nonnegative and --runs must be positive")
    if args.complete and args.pages > 100:
        parser.error("--complete is limited to 100-page small fixtures")
    return _benchmark(args)


if __name__ == "__main__":
    raise SystemExit(main())
