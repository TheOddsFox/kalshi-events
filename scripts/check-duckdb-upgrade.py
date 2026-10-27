from __future__ import annotations

import argparse
import importlib.util
import os
import subprocess
import sys
import tempfile
from pathlib import Path

CHILD_TIMEOUT_SECONDS = 180
COMPATIBILITY_TESTS = (
    "tests/test_duckdb_upgrade.py::test_harness_preserves_venv_symlink_and_resolves_relative_paths",
    "tests/test_duckdb_upgrade.py::test_harness_imports_application_from_expected_source",
    "tests/test_duckdb_upgrade.py::test_harness_timeout_reaps_child_group_and_saves_logs",
    "tests/test_arrow_landing.py::test_arrow_merge_replays_dict_landed_database_with_text_coercion",
    "tests/test_arrow_landing.py::test_duplicate_ids_across_arrow_batches_match_legacy_and_replay",
    "tests/test_series_warehouse.py::test_scoped_full_build_is_certified_as_internal_v2_"
    "with_exact_values",
    "tests/test_capture_process_recovery.py::test_process_exit_capture_resumes_from_"
    "durable_evidence",
)


def _absolute_path(path: Path, base: Path) -> Path:
    return Path(os.path.abspath(path if path.is_absolute() else base / path))


def _interpreter_path(path: Path, base: Path) -> Path:
    return _absolute_path(path, base)


def _text(value: str | bytes | None) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value or ""


def _load_benchmark_supervisor():
    script = Path(__file__).with_name("benchmark-pipeline.py")
    spec = importlib.util.spec_from_file_location("duckdb_upgrade_benchmark", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _test_environment(source: Path, repo: Path, fixture_root: Path) -> dict[str, str]:
    environment = os.environ.copy()
    environment["KALSHI_DUCKDB_UPGRADE_ROOT"] = str(fixture_root / "reference-root")
    environment["KALSHI_DUCKDB_UPGRADE_APPLICATION_SOURCE"] = str(source)
    environment["PYTHONPATH"] = os.pathsep.join((str(source / "src"), str(repo / "tests")))
    return environment


def _run(
    label: str,
    interpreter: Path,
    tests: tuple[str, ...],
    environment: dict[str, str],
    log_dir: Path,
    run_supervised,
    timeout: float = CHILD_TIMEOUT_SECONDS,
) -> int:
    log_dir.mkdir(parents=True, exist_ok=True)
    stem = label.replace(" ", "-")
    stdout_path = log_dir / f"{stem}.stdout.log"
    stderr_path = log_dir / f"{stem}.stderr.log"
    command = [str(interpreter), "-m", "pytest", "-q", "-x", *tests]
    try:
        result = run_supervised(
            command,
            timeout=timeout,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = _text(exc.stdout)
        stderr = _text(exc.stderr) + f"\nChild timed out after {timeout:g} seconds.\n"
        stdout_path.write_text(stdout)
        stderr_path.write_text(stderr)
        print(f"{label} timed out after {timeout:g}s; logs: {log_dir}", flush=True)
        return 124

    stdout_path.write_text(result.stdout)
    stderr_path.write_text(result.stderr)
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    if result.returncode:
        print(f"{label} failed; logs: {log_dir}", flush=True)
    return result.returncode


def main() -> int:
    project = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Run focused DuckDB compatibility tests serially in two Python environments."
    )
    parser.add_argument("--reference-python", required=True, type=Path)
    parser.add_argument("--candidate-python", required=True, type=Path)
    parser.add_argument("--reference-source", required=True, type=Path)
    parser.add_argument("--repo", type=Path, default=project)
    parser.add_argument("--log-dir", type=Path)
    arguments = parser.parse_args()
    invocation_dir = Path.cwd()
    repo = _absolute_path(arguments.repo, invocation_dir).resolve()
    reference_source = _absolute_path(arguments.reference_source, invocation_dir).resolve()
    interpreters = {
        label: _interpreter_path(path, invocation_dir)
        for label, path in (
            ("reference", arguments.reference_python),
            ("candidate", arguments.candidate_python),
        )
    }
    for label, interpreter in interpreters.items():
        if not interpreter.is_file():
            parser.error(f"{label} Python interpreter does not exist: {interpreter}")
    if not (reference_source / "src/kalshi_events").is_dir():
        parser.error(f"reference source is missing src/kalshi_events: {reference_source}")

    log_dir = arguments.log_dir
    if log_dir is None:
        log_dir = Path(tempfile.mkdtemp(prefix="duckdb-upgrade-compat-logs-"))
    else:
        log_dir = _absolute_path(log_dir, invocation_dir)
    os.chdir(repo)
    print(f"Logs: {log_dir}", flush=True)

    benchmark = _load_benchmark_supervisor()
    fixture_root = Path(tempfile.mkdtemp(prefix="duckdb-upgrade-compat-"))
    print(f"Fixture root: {fixture_root}", flush=True)
    reference_environment = _test_environment(reference_source, repo, fixture_root)
    candidate_environment = _test_environment(repo, repo, fixture_root)
    try:
        phases = (
            (
                "reference baseline setup",
                interpreters["reference"],
                (
                    "tests/test_duckdb_upgrade.py::test_write_reference_dictionary_baseline",
                    "tests/test_duckdb_upgrade.py::test_write_reference_series_baselines",
                ),
                reference_environment,
            ),
            (
                "reference compatibility",
                interpreters["reference"],
                COMPATIBILITY_TESTS,
                reference_environment,
            ),
            (
                "candidate dictionary handoff",
                interpreters["candidate"],
                (
                    "tests/test_duckdb_upgrade.py::"
                    "test_candidate_arrow_merge_replays_reference_dictionary_baseline",
                ),
                candidate_environment,
            ),
            (
                "candidate frozen v1 public-schema handoff",
                interpreters["candidate"],
                (
                    "tests/test_duckdb_upgrade.py::"
                    "test_candidate_rebuild_preserves_reference_v1_public_schemas",
                ),
                candidate_environment,
            ),
            (
                "candidate native storage conversion",
                interpreters["candidate"],
                ("tests/test_duckdb_storage.py",),
                candidate_environment,
            ),
            (
                "candidate series handoff",
                interpreters["candidate"],
                (
                    "tests/test_duckdb_upgrade.py::"
                    "test_candidate_bootstraps_v2_and_deltas_reference_baselines",
                ),
                candidate_environment,
            ),
            (
                "reference native storage rejection",
                interpreters["reference"],
                (
                    "tests/test_duckdb_upgrade.py::"
                    "test_reference_reader_rejects_candidate_native_v2_baseline",
                ),
                reference_environment,
            ),
            (
                "candidate compatibility",
                interpreters["candidate"],
                COMPATIBILITY_TESTS,
                candidate_environment,
            ),
        )
        for label, interpreter, tests, test_environment in phases:
            print(f"\n=== {label}: {interpreter} ===", flush=True)
            status = _run(
                label,
                interpreter,
                tests,
                test_environment,
                log_dir,
                benchmark._run_supervised,
            )
            if status:
                print(f"Stopped after {label}; dependent phases were skipped.", flush=True)
                return 1
    except benchmark._UnreapedWorkerGroup as exc:
        print(f"Worker descendants remain; preserved fixture root {fixture_root}: {exc}")
        return 2

    print("All focused DuckDB compatibility phases passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
