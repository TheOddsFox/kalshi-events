"""Checkout-aware locations for command-owned persistent data."""

from __future__ import annotations

import os
import stat
import tomllib
from pathlib import Path
from typing import Any

PROJECT_NAME = "oddsfox-kalshi-events"
PROJECT_ROOT_ENV = "KALSHI_EVENTS_PROJECT_ROOT"
ROOT_ENV = "KALSHI_EVENTS_ROOT"


class PathPolicyError(ValueError):
    """A checkout identity or persistent-data path violates the local policy."""


def _safe_path(path: Path) -> Path:
    path = path.absolute()
    try:
        for part in (path, *path.parents):
            try:
                details = part.lstat()
            except FileNotFoundError:
                continue
            if stat.S_ISLNK(details.st_mode):
                raise PathPolicyError(f"symlink is forbidden: {part}")
        return path.resolve()
    except OSError as exc:
        raise PathPolicyError(f"unsafe or unavailable path: {path}") from exc


def validate_project_root(path: Path | str) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute() or ".." in candidate.parts:
        raise PathPolicyError("project root must be an absolute normalized path")
    root = _safe_path(candidate)
    marker = root / ".git"
    project_file = root / "pyproject.toml"
    try:
        marker_info = marker.lstat()
        project_info = project_file.lstat()
        if stat.S_ISLNK(marker_info.st_mode) or not (
            stat.S_ISDIR(marker_info.st_mode) or stat.S_ISREG(marker_info.st_mode)
        ):
            raise PathPolicyError("project root has an invalid .git marker")
        if not stat.S_ISREG(project_info.st_mode):
            raise PathPolicyError("project root pyproject.toml is not a regular file")
        with project_file.open("rb") as stream:
            project: dict[str, Any] = tomllib.load(stream).get("project", {})
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise PathPolicyError("project root markers are unavailable or invalid") from exc
    if project.get("name") != PROJECT_NAME:
        raise PathPolicyError("project root does not identify oddsfox-kalshi-events")
    return root


def project_root(start: Path | None = None, *, code_root: Path | None = None) -> Path | None:
    """Find a real checkout from cwd, or validate an explicit child-process binding."""
    configured = os.environ.get(PROJECT_ROOT_ENV)
    if configured is not None:
        if not configured:
            raise PathPolicyError(f"{PROJECT_ROOT_ENV} cannot be empty")
        return validate_project_root(Path(configured))
    if code_root is not None:
        source = _safe_path(Path(code_root).expanduser().absolute())
        marker = source / ".git"
        if marker.is_file() or marker.is_dir():
            return validate_project_root(source)
    current = _safe_path((Path.cwd() if start is None else Path(start)).expanduser().absolute())
    if current.is_file():
        current = current.parent
    for candidate in (current, *current.parents):
        project_file = candidate / "pyproject.toml"
        if not project_file.is_file() or project_file.is_symlink():
            continue
        try:
            with project_file.open("rb") as stream:
                name = tomllib.load(stream).get("project", {}).get("name")
        except (OSError, tomllib.TOMLDecodeError):
            continue
        if name == PROJECT_NAME:
            marker = candidate / ".git"
            if marker.is_file() or marker.is_dir():
                return validate_project_root(candidate)
    if code_root is not None:
        source = _safe_path(Path(code_root).expanduser().absolute())
        project_file = source / "pyproject.toml"
        package_file = source / "src" / "kalshi_events" / "__init__.py"
        if project_file.is_file() and package_file.is_file() and not project_file.is_symlink():
            try:
                with project_file.open("rb") as stream:
                    name = tomllib.load(stream).get("project", {}).get("name")
            except (OSError, tomllib.TOMLDecodeError):
                name = None
            if name == PROJECT_NAME:
                raise PathPolicyError(
                    "frozen project code requires checkout cwd or KALSHI_EVENTS_PROJECT_ROOT"
                )
    return None


def persistent_path(
    path: Path | str, *, checkout: Path | None, label: str = "persistent output"
) -> Path:
    """Canonicalize a write path and confine checkout-mode writes to strict data descendants."""
    candidate = Path(path).expanduser()
    result = _safe_path(candidate if candidate.is_absolute() else Path.cwd() / candidate)
    if checkout is not None:
        data = _safe_path(checkout / "data")
        if result == data or not result.is_relative_to(data):
            raise PathPolicyError(f"{label} must be a strict descendant of {data}")
        for ancestor in (result, *result.parents):
            if ancestor == checkout:
                break
            marker = ancestor / ".git"
            if marker.exists() or marker.is_symlink():
                raise PathPolicyError(f"{label} cannot be inside a nested Git checkout")
    return result


def operator_root(
    explicit: Path | None, *, checkout: Path | None, environ: dict[str, str] | None = None
) -> Path:
    environment = os.environ if environ is None else environ
    configured = environment.get(ROOT_ENV)
    if explicit is not None:
        candidate = explicit
    elif configured is not None:
        if not configured:
            raise PathPolicyError(f"{ROOT_ENV} cannot be empty")
        candidate = Path(configured)
    elif checkout is not None:
        candidate = checkout / "data" / "operator"
    else:
        candidate = Path("data")
    return persistent_path(candidate, checkout=checkout, label="operator root")


def data_directory(checkout: Path, relative: str) -> Path:
    path = Path(relative)
    if not relative or path.is_absolute() or ".." in path.parts or "." in path.parts:
        raise PathPolicyError("data directory must be a safe relative path")
    result = persistent_path(checkout / "data" / path, checkout=checkout, label="data directory")
    result.mkdir(parents=True, exist_ok=True)
    return _safe_path(result)


def child_environment(environment: dict[str, str], checkout: Path | None) -> dict[str, str]:
    if checkout is not None:
        environment[PROJECT_ROOT_ENV] = str(checkout)
        environment["TMPDIR"] = str(data_directory(checkout, "tmp"))
    return environment
