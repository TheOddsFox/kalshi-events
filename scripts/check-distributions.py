"""Check wheel/sdist boundaries; adapted from kalshi-market-data's archive check."""

import sys
import tarfile
import zipfile
from pathlib import Path, PurePosixPath

FORBIDDEN = {
    "data",
    ".state",
    "artifacts",
    "captures",
    ".pad",
    ".pad.toml",
    ".git",
    ".venv",
    "__pycache__",
}
EXTENSIONS = {".parquet", ".sqlite", ".duckdb", ".pem", ".key", ".pyc"}
REQUIRED = {
    "kalshi_events/cli.py",
    "kalshi_events/dbt/dbt_project.yml",
    "kalshi_events/dbt/models/schema.yml",
    "kalshi_events/dbt/models/current_markets.sql",
}


def validate(name):
    path = PurePosixPath(name)
    if (
        path.is_absolute()
        or ".." in path.parts
        or FORBIDDEN.intersection(path.parts)
        or path.suffix in EXTENSIONS
        or any(part.startswith(".env") for part in path.parts)
    ):
        raise ValueError("operator data or unsafe member in distribution: " + name)


for argument in sys.argv[1:] or ["dist"]:
    files = sorted(Path(argument).iterdir())
    if not any(p.suffix == ".whl" for p in files) or not any(
        p.name.endswith(".tar.gz") for p in files
    ):
        raise ValueError("wheel and sdist required")
    for file in files:
        if file.suffix == ".whl":
            with zipfile.ZipFile(file) as archive:
                names = set(archive.namelist())
                for name in names:
                    validate(name)
                if not REQUIRED.issubset(names):
                    raise ValueError("packaged dbt resources missing from wheel")
        elif file.name.endswith(".tar.gz"):
            with tarfile.open(file) as archive:
                names = set()
                for member in archive.getmembers():
                    validate(member.name)
                    names.add(member.name)
                    if member.issym() or member.islnk() or member.isdev():
                        raise ValueError("non-regular distribution member")
                if not all(
                    any(name.endswith("src/" + required) for name in names) for required in REQUIRED
                ):
                    raise ValueError("packaged dbt resources missing from sdist")
print("Distribution boundaries and packaged dbt resources verified")
