"""Operator-invoked catalogue commands. JSON stdout is the delegated-refresh protocol.

Exit codes: 0 success, 1 failed invariant or invalid input, 3 budget exhausted before completion
(the JSON on stdout carries `complete: false` and the capture resumes with `--capture-id`).
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from contextlib import nullcontext
from pathlib import Path
from typing import Any

from kalshi_events import capture as acquisition
from kalshi_events import metadata, metrics, publish, recovery, refresh, warehouse
from kalshi_events.common import (
    MAX_BUNDLE_BYTES,
    BudgetExceeded,
    CatalogueError,
    Limits,
    Store,
    json_bytes,
    safe_path,
)
from kalshi_events.historical import HISTORICAL_VERSION, is_historical_capture
from kalshi_events.paths import (
    PathPolicyError,
    data_directory,
)
from kalshi_events.paths import (
    operator_root as _policy_operator_root,
)
from kalshi_events.paths import (
    persistent_path as _policy_persistent_path,
)
from kalshi_events.paths import (
    project_root as _policy_project_root,
)
from kalshi_events.transport import accounting

EXIT_INCOMPLETE = 3


def _checkout_root() -> Path | None:
    try:
        return _policy_project_root(code_root=Path(__file__).resolve().parents[2])
    except PathPolicyError as exc:
        raise CatalogueError(str(exc)) from exc


def _operator_root(explicit: Path | None, *, checkout: Path | None) -> Path:
    try:
        return _policy_operator_root(explicit, checkout=checkout)
    except PathPolicyError as exc:
        raise CatalogueError(str(exc)) from exc


def _persistent_path(path: Path, *, checkout: Path | None, label: str) -> Path:
    try:
        return _policy_persistent_path(path, checkout=checkout, label=label)
    except PathPolicyError as exc:
        raise CatalogueError(str(exc)) from exc


def _data_directory(checkout: Path, relative: str) -> Path:
    try:
        return data_directory(checkout, relative)
    except PathPolicyError as exc:
        raise CatalogueError(str(exc)) from exc


FLOAT_LIMITS = {"max_rate", "max_duration_seconds"}


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(prog="kalshi-events")
    cli.add_argument(
        "--root", type=Path, default=None, help="persistent producer-owned operator root"
    )
    cli.add_argument(
        "--metrics-file", type=Path, help="write opt-in phase metrics outside command JSON"
    )
    commands = cli.add_subparsers(dest="command", required=True)
    for name in (
        "capture",
        "refresh",
        "load",
        "build",
        "publish",
        "replay",
        "rebuild",
        "status",
        "current",
        "verify",
        "dbt",
        "upgrade-root",
        "discard",
    ):
        command = commands.add_parser(name)
        if name in {"capture", "load", "build", "publish", "replay"}:
            command.add_argument("--capture-id")
        if name == "capture":
            command.add_argument("--market-ticker", action="append")
            command.add_argument("--compression", choices=("identity", "gzip"), default=None)
            command.add_argument(
                "--rate-policy", choices=("fixed", "decrease-on-429"), default=None
            )
            command.add_argument("--incremental", action="store_true")
            command.add_argument(
                "--catalogue-profile",
                choices=("historical", "exchange"),
                default=None,
                help="historical full capture by default; exchange preserves the legacy broad plan",
            )
        if name == "refresh":
            command.add_argument("--full", action="store_true")
            command.add_argument("--capture-id")
            command.add_argument("--compression", choices=("identity", "gzip"), default=None)
            command.add_argument(
                "--rate-policy", choices=("fixed", "decrease-on-429"), default=None
            )
        if name in {"current", "replay", "rebuild", "publish"}:
            command.add_argument("--catalogue-version", type=int, choices=(1, 2), default=None)
        if name in {"refresh", "build", "publish", "current", "replay", "rebuild"}:
            command.add_argument(
                "--historical",
                action="store_true",
                help="use the historical catalogue; an explicit capture can use saved evidence only",
            )
        if name == "discard":
            command.add_argument("--capture-id", required=True)
        if name == "verify":
            command.add_argument("--release", type=Path, required=True)
        if name == "dbt":
            command.add_argument("dbt_args", nargs=argparse.REMAINDER)
        _limits(command)
    bundle = commands.add_parser("metadata")
    actions = bundle.add_subparsers(dest="metadata_action", required=True)
    for name in ("refresh", "export", "verify"):
        action = actions.add_parser(name)
        action.add_argument("--output", type=Path, required=True)
        if name != "verify":
            action.add_argument("--market-ticker", action="append", required=True)
            action.add_argument("--max-output-bytes", type=int, default=MAX_BUNDLE_BYTES)
            if name == "refresh":
                action.add_argument("--compression", choices=("identity", "gzip"), default=None)
                action.add_argument(
                    "--rate-policy", choices=("fixed", "decrease-on-429"), default=None
                )
        if name == "export":
            action.add_argument("--capture-id")
        _limits(action)
    series = commands.add_parser("series", help="operate on one dedicated series catalogue")
    series_actions = series.add_subparsers(dest="series_action", required=True)
    refresh_action = series_actions.add_parser("refresh")
    refresh_action.add_argument("--series-ticker")
    refresh_action.add_argument("--full", action="store_true")
    refresh_action.add_argument("--capture-id")
    refresh_action.add_argument("--compression", choices=("identity", "gzip"), default=None)
    refresh_action.add_argument("--rate-policy", choices=("fixed", "decrease-on-429"), default=None)
    for name in ("current", "replay", "rebuild"):
        action = series_actions.add_parser(name)
        if name == "replay":
            action.add_argument("--capture-id")
    for action in series_actions.choices.values():
        _limits(action)
    return cli


def _limits(command: argparse.ArgumentParser) -> None:
    defaults = Limits()
    for field in defaults.__dataclass_fields__:
        command.add_argument(
            "--" + field.replace("_", "-"),
            type=float if field in FLOAT_LIMITS else int,
            default=(None if field == "max_decoded_response_bytes" else getattr(defaults, field)),
        )


def execute(args: argparse.Namespace) -> dict[str, Any]:
    limits = Limits(**{field: getattr(args, field) for field in Limits.__dataclass_fields__})
    if args.command == "metadata" and args.metadata_action == "verify":
        return metadata.verify(args.output)
    if args.command == "verify":
        return publish.verify_release(args.release, limits.max_root_bytes)
    checkout = _checkout_root()
    if args.command == "metadata":
        args.output = _persistent_path(args.output, checkout=checkout, label="metadata output")
    if args.command == "series":
        root = _operator_root(args.root, checkout=checkout)
        if args.series_action != "refresh" and not _has_operator_marker(root):
            raise CatalogueError("series commands require an existing scoped operator root")
        if (
            args.series_action == "refresh"
            and not _has_operator_marker(root)
            and args.series_ticker is None
        ):
            raise CatalogueError("a new series root requires --series-ticker")
        store = Store(
            root,
            limits,
            series_ticker=(args.series_ticker if args.series_action == "refresh" else None),
        )
        operation_error = None
        result = None
        try:
            if store.scope is None:
                raise CatalogueError("series command requires a scoped operator root")
            result = _run_series(store, args)
        except BaseException as exc:
            operation_error = exc
            raise
        finally:
            try:
                store.reconcile()
            except BaseException as exc:
                if operation_error is None:
                    if isinstance(exc, BudgetExceeded) and result is not None:
                        result["complete"] = False
                        result["error"] = result.get("error") or str(exc)
                    else:
                        raise
        assert result is not None
        return result
    if args.command == "dbt" and args.root is None:
        temporary_parent = _data_directory(checkout, "tmp") if checkout else None
        with tempfile.TemporaryDirectory(
            prefix="kalshi-events-dbt-", dir=temporary_parent
        ) as directory:
            store = Store(Path(directory).resolve() / "root", limits)
            arguments = args.dbt_args[1:] if args.dbt_args[:1] == ["--"] else args.dbt_args
            result = warehouse.dbt(store, arguments or ["parse"])
            return {"returncode": result.returncode}
    root = _operator_root(args.root, checkout=checkout)
    if getattr(args, "historical", False) and not _has_operator_marker(safe_path(root)):
        raise CatalogueError("saved historical operations require an existing operator root")
    store = Store(root, limits)
    operation_error = None
    result = None
    try:
        result = _run(store, args)
    except BaseException as exc:
        operation_error = exc
        raise
    finally:
        # One full walk at invocation start (in Store) and one at its end.
        try:
            store.reconcile()
        except BaseException as exc:
            if operation_error is None:
                if isinstance(exc, BudgetExceeded) and result is not None:
                    result["complete"] = False
                    result["error"] = result.get("error") or str(exc)
                else:
                    raise
    assert result is not None
    return result


def _catalogue_version(store: Store, args: argparse.Namespace) -> int:
    version = getattr(args, "catalogue_version", None)
    if getattr(args, "historical", False):
        if version is not None:
            raise CatalogueError("--historical cannot be combined with --catalogue-version")
        return HISTORICAL_VERSION
    if version is not None:
        return version
    capture_id = getattr(args, "capture_id", None)
    if capture_id is not None:
        if is_historical_capture(acquisition.read_capture(store, capture_id)):
            return HISTORICAL_VERSION
    elif (
        args.command in {"current", "replay", "rebuild", "publish"}
        and store.path("current-historical.json").exists()
    ):
        return HISTORICAL_VERSION
    if args.command in {"build", "publish"} and capture_id is None:
        latest = acquisition.latest_catalogue_capture(store)
        if is_historical_capture(acquisition.read_capture(store, latest)):
            return HISTORICAL_VERSION
    return 1


def _run(store: Store, args: argparse.Namespace) -> dict[str, Any]:
    if store.scope is not None and args.command not in {"status", "discard", "upgrade-root"}:
        raise CatalogueError("catalogue commands cannot operate on a scoped series root")
    if args.command == "current":
        return publish.current(store, catalogue_version=_catalogue_version(store, args))
    if args.command == "status":
        return _status(store)
    if args.command == "upgrade-root":
        with store.lock(allow_legacy=True) as writer_lock:
            store.upgrade_root(writer_lock)
        return {"storage_version": store.storage_version, "upgraded": True}
    with store.lock():
        from kalshi_events.incremental import finish_pending_retirement

        finish_pending_retirement(store)
        if args.command == "capture":
            with metrics.phase("capture"):
                state = acquisition.capture(
                    store,
                    selected=args.market_ticker,
                    resume=args.capture_id,
                    compression=args.compression,
                    rate_policy=args.rate_policy,
                    incremental=args.incremental,
                    **(
                        {"catalogue_profile": args.catalogue_profile}
                        if args.catalogue_profile is not None
                        else {}
                    ),
                )
            result = {
                key: state[key]
                for key in (
                    "capture_id",
                    "complete",
                    "error",
                    "requests",
                    "downloaded_bytes",
                    "accounting_complete",
                )
            }
            if is_historical_capture(state):
                result["coverage_scope"] = "historical"
                result["omitted_scopes"] = ["events_combo", "markets_live", "markets_combo"]
            return result
        if args.command == "load":
            capture_id = args.capture_id or acquisition.latest_catalogue_capture(store)
            return warehouse.load(store, capture_id)
        if args.command == "build":
            capture_id = args.capture_id or acquisition.latest_catalogue_capture(store)
            if _catalogue_version(store, args) == HISTORICAL_VERSION:
                return warehouse.build_historical(store, capture_id)
            return warehouse.build(store, capture_id)
        if args.command == "replay":
            return recovery.replay(
                store, args.capture_id, catalogue_version=_catalogue_version(store, args)
            )
        if args.command == "publish":
            return publish.publish(
                store, args.capture_id, catalogue_version=_catalogue_version(store, args)
            )
        if args.command == "rebuild":
            return recovery.rebuild(store, catalogue_version=_catalogue_version(store, args))
        if args.command == "discard":
            from kalshi_events.incremental import discard

            return discard(store, args.capture_id)
        if args.command == "dbt":
            arguments = args.dbt_args[1:] if args.dbt_args[:1] == ["--"] else args.dbt_args
            result = warehouse.dbt(store, arguments or ["parse"])
            return {"returncode": result.returncode}
        if args.command == "metadata":
            if args.metadata_action == "refresh":
                return metadata.refresh(
                    store,
                    args.market_ticker,
                    args.output,
                    max_output_bytes=args.max_output_bytes,
                    compression=args.compression,
                    rate_policy=args.rate_policy,
                )
            return metadata.export(
                store,
                args.market_ticker,
                args.output,
                capture_id=args.capture_id,
                max_output_bytes=args.max_output_bytes,
            )
    raise CatalogueError("unknown command")


def _run_series(store: Store, args: argparse.Namespace) -> dict[str, Any]:
    if args.series_action == "current":
        result = publish.current(store, catalogue_version=2)
    elif args.series_action == "replay":
        result = recovery.replay(store, args.capture_id, catalogue_version=2)
    elif args.series_action == "rebuild":
        result = recovery.rebuild(store, catalogue_version=2)
    else:
        raise CatalogueError("series refresh must use the refresh workflow")
    return {**result, "series_scope": store.scope}


def _has_operator_marker(root: Path) -> bool:
    return root.is_dir() and (root / "root.json").is_file()


def _status(store: Store) -> dict[str, Any]:
    captures = []
    if store.path("captures").exists():
        for child in sorted(store.path("captures").iterdir()):
            state = acquisition.read_capture(store, child.name)
            captures.append(
                {
                    "incremental": state.get("incremental") is True,
                    **{
                        key: state[key]
                        for key in (
                            "capture_id",
                            "selected",
                            "complete",
                            "started_at",
                            "completed_at",
                            "error",
                        )
                    },
                }
            )
    result = {
        "root": str(store.root),
        "captures": captures,
        "accounting": accounting(store),
        "current": (
            publish.current(store)
            if store.scope is None and store.path("current.json").exists()
            else None
        ),
        "current_v2": (
            publish.current(store, catalogue_version=2)
            if store.scope is None and store.path("current-v2.json").exists()
            else None
        ),
        "current_historical": (
            publish.current(store, catalogue_version=HISTORICAL_VERSION)
            if store.scope is None and store.path("current-historical.json").exists()
            else None
        ),
    }
    if store.scope is not None:
        result["series_scope"] = store.scope
        result["current_series"] = (
            publish.current(store, catalogue_version=2)
            if store.path("current-series.json").exists()
            else None
        )
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.command == "refresh" or (args.command == "series" and args.series_action == "refresh"):
        return _refresh_main(args)
    try:
        if args.metrics_file is None:
            result = execute(args)
        else:
            checkout = _checkout_root()
            root = _operator_root(args.root, checkout=checkout)
            metrics_path = _persistent_path(
                args.metrics_file, checkout=checkout, label="metrics report"
            )
            progress_path = metrics_path.with_name(f"{metrics_path.name}.progress.json")
            _validate_metrics_root_overlap(root, metrics_path, progress_path)
            _validate_metrics_output_overlap(args, metrics_path, progress_path)
            if metrics_path.exists():
                raise CatalogueError("metrics report path already exists")
            metrics.check_progress_path(progress_path)
            report_stream = metrics.reserve_report(metrics_path)
            operation_error: BaseException | None = None
            with report_stream, metrics.collect(progress_path=progress_path) as measured:
                try:
                    result = execute(args)
                    if result.get("complete") is False:
                        measured.progress("stopped")
                except BaseException as exc:
                    operation_error = exc
                    raise
                finally:
                    try:
                        metrics.write_report(metrics_path, measured, report_stream)
                    except OSError:
                        if operation_error is None:
                            raise
                    finally:
                        report_stream.close()
    except BudgetExceeded as exc:
        print(json_bytes({"complete": False, "error": str(exc)}).decode().strip())
        return EXIT_INCOMPLETE
    except (CatalogueError, OSError, ValueError) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 1
    print(json_bytes(result).decode().strip())
    return EXIT_INCOMPLETE if result.get("complete") is False else 0


def _refresh_main(args: argparse.Namespace) -> int:
    """Keep refresh failures in its single machine-readable result."""
    result = refresh.result_template()
    result["capture_id"] = args.capture_id
    series_command = args.command == "series"
    if series_command:
        result["series_scope"] = None
    exit_code = 1
    report_stream = None
    measured = None
    try:
        if args.full and args.capture_id is not None:
            raise CatalogueError("--full and --capture-id cannot be used together")
        limits = Limits(**{field: getattr(args, field) for field in Limits.__dataclass_fields__})
        checkout = _checkout_root()
        root = _operator_root(args.root, checkout=checkout)
        if getattr(args, "historical", False) and not args.full and not _has_operator_marker(root):
            raise CatalogueError("saved historical refresh requires an existing operator root")
        if series_command and not _has_operator_marker(root) and args.series_ticker is None:
            raise CatalogueError("a new series root requires --series-ticker")
        if args.metrics_file is not None:
            metrics_path = _persistent_path(
                args.metrics_file, checkout=checkout, label="metrics report"
            )
            progress_path = metrics_path.with_name(f"{metrics_path.name}.progress.json")
            _validate_metrics_root_overlap(root, metrics_path, progress_path)
            if metrics_path.exists():
                raise CatalogueError("metrics report path already exists")
            metrics.check_progress_path(progress_path)
            report_stream = metrics.reserve_report(metrics_path)
        context = (
            metrics.collect(progress_path=progress_path)
            if report_stream is not None
            else nullcontext(None)
        )
        with context as measured:
            store = Store(
                root,
                limits,
                series_ticker=(args.series_ticker if series_command else None),
            )
            if not series_command and store.scope is not None:
                raise CatalogueError("catalogue commands cannot operate on a scoped series root")
            if series_command:
                if store.scope is None:
                    raise CatalogueError("series refresh requires a scoped operator root")
                result["series_scope"] = store.scope
            with store.lock():
                result, exit_code = refresh.run(
                    store,
                    full=args.full,
                    capture_id=args.capture_id,
                    compression=args.compression,
                    rate_policy=args.rate_policy,
                    **({"historical": True} if getattr(args, "historical", False) else {}),
                )
                if series_command:
                    result["series_scope"] = store.scope
                try:
                    store.reconcile()
                except Exception as exc:
                    result["complete"] = False
                    if result["publication_state"] == "verified":
                        result["stage"] = "complete"
                    result["error"] = (
                        result["error"] or f"final storage reconciliation failed: {exc}"
                    )
                    if exit_code == 0:
                        exit_code = EXIT_INCOMPLETE if isinstance(exc, BudgetExceeded) else 1
            if measured is not None:
                measured.progress("complete" if exit_code == 0 else "stopped")
    except BudgetExceeded as exc:
        result["complete"] = False
        result["error"] = result["error"] or str(exc)
        exit_code = EXIT_INCOMPLETE
    except Exception as exc:
        result["complete"] = False
        result["error"] = result["error"] or str(exc)
        exit_code = 1
    finally:
        if report_stream is not None:
            try:
                if measured is not None:
                    measured.progress("complete" if exit_code == 0 else "stopped")
                    metrics.write_report(args.metrics_file, measured, report_stream)
            except Exception as exc:
                result["complete"] = False
                result["error"] = result["error"] or f"metrics report failed: {exc}"
                exit_code = 1
            try:
                report_stream.close()
            except Exception as exc:
                result["complete"] = False
                result["error"] = result["error"] or f"metrics report close failed: {exc}"
                exit_code = 1
    print(json_bytes(result).decode().strip())
    return exit_code


def _validate_metrics_output_overlap(
    args: argparse.Namespace, metrics_path: Path, progress_path: Path
) -> None:
    outputs: list[Path] = []
    if args.command == "verify":
        outputs.append(safe_path(args.release))
    elif args.command == "metadata":
        outputs.append(safe_path(args.output))

    for metrics_output in (metrics_path, progress_path):
        for output in outputs:
            if metrics_output.is_relative_to(output) or output.is_relative_to(metrics_output):
                raise CatalogueError(
                    "metrics report or progress file must not overlap a verified or metadata bundle"
                )


def _validate_metrics_root_overlap(root: Path, metrics_path: Path, progress_path: Path) -> None:
    for metrics_output in (metrics_path, progress_path):
        if metrics_output.is_relative_to(root) or root.is_relative_to(metrics_output):
            raise CatalogueError("metrics report must be outside the operator root")


if __name__ == "__main__":
    raise SystemExit(main())
