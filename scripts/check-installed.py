"""Install a locked wheel outside the checkout; adapted from market-data's harness."""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def venv_executable(venv: Path, command: str) -> Path:
    return venv / "bin" / command


def main():
    if not __debug__:
        raise SystemExit("installed acceptance requires assertions")
    repository = Path(__file__).resolve().parent.parent
    wheels = sorted((repository / "dist").glob("*.whl"))
    if len(wheels) != 1:
        raise ValueError("exactly one freshly built wheel required")
    uv = shutil.which("uv")
    if uv is None:
        raise ValueError("uv required")
    environment = {
        key: value
        for key, value in os.environ.items()
        if key
        not in {
            "PYTHONPATH",
            "PYTHONHOME",
            "UV_PROJECT",
            "UV_PROJECT_ENVIRONMENT",
            "UV_WORKING_DIR",
        }
        and not key.startswith(("KALSHI_EVENTS_", "DLT_", "DBT_"))
    }
    with tempfile.TemporaryDirectory(prefix="kalshi-events-installed-") as temporary:
        directory = Path(temporary).resolve()
        venv = directory / "venv"
        subprocess.run(
            [uv, "venv", "--python", sys.executable, str(venv)], check=True, env=environment
        )
        subprocess.run(
            [uv, "sync", "--locked", "--offline", "--no-dev", "--no-install-project"],
            cwd=repository,
            env={**environment, "UV_PROJECT_ENVIRONMENT": str(venv)},
            check=True,
        )
        requirement = directory / "wheel.txt"
        requirement.write_text(
            f"oddsfox-kalshi-events @ {wheels[0].as_uri()} --hash=sha256:{hashlib.sha256(wheels[0].read_bytes()).hexdigest()}\n"
        )
        python = venv_executable(venv, "python")
        subprocess.run(
            [
                uv,
                "pip",
                "install",
                "--offline",
                "--no-deps",
                "--python",
                str(python),
                "--require-hashes",
                "--requirements",
                str(requirement),
            ],
            check=True,
            env=environment,
        )
        subprocess.run(
            [
                str(python),
                "-c",
                "import kalshi_events; from pathlib import Path; p=Path(kalshi_events.__file__); assert 'site-packages' in p.parts, p",
            ],
            cwd=directory,
            env=environment,
            check=True,
        )
        runtime_packages = (
            "duckdb",
            "dlt",
            "dbt-duckdb",
            "dbt-core",
            "pyarrow",
            "httpx",
            "httpcore",
        )
        runtime_check = """import importlib.metadata, sys, tomllib
from pathlib import Path
lock = tomllib.loads(Path(sys.argv[1]).read_text())
names = sys.argv[2:]
expected = {p["name"]: p["version"] for p in lock["package"] if p["name"] in names}
actual = {name: importlib.metadata.version(name) for name in names}
assert expected == actual, (expected, actual)
"""
        subprocess.run(
            [str(python), "-c", runtime_check, str(repository / "uv.lock"), *runtime_packages],
            cwd=directory,
            env=environment,
            check=True,
        )
        subprocess.run(
            [str(venv_executable(venv, "kalshi-events")), "dbt", "--", "parse"],
            cwd=directory,
            env=environment,
            check=True,
        )
    print("Installed wheel import origin and packaged dbt parse verified outside checkout")


if __name__ == "__main__":
    main()
