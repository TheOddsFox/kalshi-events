"""Operator-invoked catalogue commands. JSON stdout is the delegated-refresh protocol.

Exit codes: 0 success, 1 failed invariant or invalid input, 3 budget exhausted before completion
(the JSON on stdout carries `complete: false` and the capture resumes with `--capture-id`).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

from kalshi_events import capture as acquisition
from kalshi_events import metadata, publish, recovery, warehouse
from kalshi_events.common import (
    MAX_BUNDLE_BYTES,
    BudgetExceeded,
    CatalogueError,
    Limits,
    Store,
    json_bytes,
)
from kalshi_events.transport import accounting

EXIT_INCOMPLETE = 3
FLOAT_LIMITS = {"max_rate", "max_duration_seconds"}


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(prog="kalshi-events")
    cli.add_argument(
        "--root", type=Path, default=None, help="persistent producer-owned operator root"
    )
    commands = cli.add_subparsers(dest="command", required=True)
    for name in (
        "capture",
        "load",
        "build",
        "publish",
        "replay",
        "rebuild",
        "status",
        "current",
        "verify",
        "dbt",
    ):
        command = commands.add_parser(name)
        if name in {"capture", "load", "build", "publish", "replay"}:
            command.add_argument("--capture-id")
        if name == "capture":
            command.add_argument("--market-ticker", action="append")
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
        if name == "export":
            action.add_argument("--capture-id")
        _limits(action)
    return cli


def _limits(command: argparse.ArgumentParser) -> None:
    defaults = Limits()
    for field in defaults.__dataclass_fields__:
        command.add_argument(
            "--" + field.replace("_", "-"),
            type=float if field in FLOAT_LIMITS else int,
            default=getattr(defaults, field),
        )


def execute(args: argparse.Namespace) -> dict[str, Any]:
    limits = Limits(**{field: getattr(args, field) for field in Limits.__dataclass_fields__})
    if args.command == "metadata" and args.metadata_action == "verify":
        return metadata.verify(args.output)
    if args.command == "verify":
        return publish.verify_release(args.release, limits.max_root_bytes)
    if args.command == "dbt" and args.root is None:
        with tempfile.TemporaryDirectory(prefix="kalshi-events-dbt-") as directory:
            store = Store(Path(directory).resolve() / "root", limits)
            arguments = args.dbt_args[1:] if args.dbt_args[:1] == ["--"] else args.dbt_args
            result = warehouse.dbt(store, arguments or ["parse"])
            return {"returncode": result.returncode}
    root = args.root or Path(os.environ.get("KALSHI_EVENTS_ROOT", "data"))
    store = Store(root, limits)
    try:
        return _run(store, args)
    finally:
        # One full walk at invocation start (in Store) and one at its end.
        store.reconcile()


def _run(store: Store, args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "current":
        return publish.current(store)
    if args.command == "status":
        return _status(store)
    with store.lock():
        if args.command == "capture":
            state = acquisition.capture(store, selected=args.market_ticker, resume=args.capture_id)
            return {
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
        if args.command == "load":
            capture_id = args.capture_id or acquisition.latest_catalogue_capture(store)
            return warehouse.load(store, capture_id)
        if args.command == "build":
            capture_id = args.capture_id or acquisition.latest_catalogue_capture(store)
            return warehouse.build(store, capture_id)
        if args.command == "replay":
            capture_id = args.capture_id or publish.current(store)["capture_id"]
            return warehouse.build(store, capture_id)
        if args.command == "publish":
            return publish.publish(store, args.capture_id)
        if args.command == "rebuild":
            return recovery.rebuild(store)
        if args.command == "dbt":
            arguments = args.dbt_args[1:] if args.dbt_args[:1] == ["--"] else args.dbt_args
            result = warehouse.dbt(store, arguments or ["parse"])
            return {"returncode": result.returncode}
        if args.command == "metadata":
            if args.metadata_action == "refresh":
                return metadata.refresh(
                    store, args.market_ticker, args.output, max_output_bytes=args.max_output_bytes
                )
            return metadata.export(
                store,
                args.market_ticker,
                args.output,
                capture_id=args.capture_id,
                max_output_bytes=args.max_output_bytes,
            )
    raise CatalogueError("unknown command")


def _status(store: Store) -> dict[str, Any]:
    captures = []
    if store.path("captures").exists():
        for child in sorted(store.path("captures").iterdir()):
            state = acquisition.read_capture(store, child.name)
            captures.append(
                {
                    key: state[key]
                    for key in (
                        "capture_id",
                        "selected",
                        "complete",
                        "started_at",
                        "completed_at",
                        "error",
                    )
                }
            )
    return {
        "root": str(store.root),
        "captures": captures,
        "accounting": accounting(store),
        "current": publish.current(store) if store.path("current.json").exists() else None,
    }


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        result = execute(args)
    except BudgetExceeded as exc:
        print(json_bytes({"complete": False, "error": str(exc)}).decode().strip())
        return EXIT_INCOMPLETE
    except (CatalogueError, OSError, ValueError) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 1
    print(json_bytes(result).decode().strip())
    return EXIT_INCOMPLETE if result.get("complete") is False else 0


if __name__ == "__main__":
    raise SystemExit(main())
