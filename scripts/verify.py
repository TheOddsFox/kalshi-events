"""Run the repository verification gates from native Python on any platform."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
from pathlib import Path


def verification_commands(*, fast: bool, uv: str) -> list[tuple[list[str], dict[str, str]]]:
    commands = [
        (["git", "diff", "--check"], {}),
        (["git", "diff", "--cached", "--check"], {}),
    ]
    if not fast:
        commands.extend(
            [
                ([uv, "lock", "--check"], {"UV_LINK_MODE": "copy"}),
                ([uv, "sync", "--locked"], {"UV_LINK_MODE": "copy"}),
            ]
        )
    commands.extend(
        [
            ([uv, "run", "--locked", "ruff", "check", "."], {"UV_LINK_MODE": "copy"}),
            (
                [uv, "run", "--locked", "ruff", "format", "--check", "."],
                {"UV_LINK_MODE": "copy"},
            ),
        ]
    )
    if not fast:
        commands.append(
            (
                [uv, "run", "--locked", "kalshi-events", "dbt", "--", "parse"],
                {"UV_LINK_MODE": "copy"},
            )
        )
    commands.append(
        (
            [uv, "run", "--locked", "pytest", "-q"],
            {
                "UV_LINK_MODE": "copy",
                "KALSHI_EVENTS_API_URL": "http://127.0.0.1:9/trade-api/v2",
                "KALSHI_EVENTS_ALLOW_LOOPBACK": "1",
            },
        )
    )
    if not fast:
        commands.append(([uv, "build"], {"UV_LINK_MODE": "copy"}))
    return commands


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fast", action="store_true", help="run the development gate instead of the full gate"
    )
    arguments = parser.parse_args()
    repository = Path(__file__).resolve().parent.parent
    uv = shutil.which("uv")
    if uv is None:
        parser.error("uv is required and must be on PATH")
    base_environment = os.environ.copy()
    for command, overrides in verification_commands(fast=arguments.fast, uv=uv):
        subprocess.run(
            command,
            cwd=repository,
            env={**base_environment, **overrides},
            check=True,
        )


if __name__ == "__main__":
    main()
