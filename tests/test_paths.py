from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from kalshi_events.common import CatalogueError
from kalshi_events.paths import (
    PROJECT_ROOT_ENV,
    PathPolicyError,
    data_directory,
    operator_root,
    persistent_path,
    project_root,
)

CHECKOUT = Path(__file__).resolve().parents[1]


def _checkout(path: Path) -> Path:
    path.mkdir(parents=True)
    (path / ".git").mkdir()
    (path / "pyproject.toml").write_text('[project]\nname = "oddsfox-kalshi-events"\n')
    return path


def test_checkout_source_context_is_independent_of_invocation_directory(tmp_path: Path):
    assert project_root(start=tmp_path, code_root=CHECKOUT) == CHECKOUT


def test_frozen_project_layout_requires_checkout_context(tmp_path: Path, monkeypatch):
    frozen = tmp_path / "frozen-source"
    (frozen / "src/kalshi_events").mkdir(parents=True)
    (frozen / "src/kalshi_events/__init__.py").touch()
    (frozen / "pyproject.toml").write_text('[project]\nname = "oddsfox-kalshi-events"\n')
    monkeypatch.delenv(PROJECT_ROOT_ENV, raising=False)

    with pytest.raises(PathPolicyError, match="requires checkout cwd"):
        project_root(start=Path("/nonexistent-checkout-context"), code_root=frozen)

    checkout = _checkout(tmp_path / "checkout")
    assert project_root(start=checkout / "nested", code_root=frozen) == checkout


def test_explicit_project_root_is_validated_and_propagatable(tmp_path: Path, monkeypatch):
    checkout = _checkout(tmp_path / "checkout")
    monkeypatch.setenv(PROJECT_ROOT_ENV, str(checkout))
    assert project_root(start=tmp_path) == checkout

    invalid = tmp_path / "not-checkout"
    invalid.mkdir()
    monkeypatch.setenv(PROJECT_ROOT_ENV, str(invalid))
    with pytest.raises(PathPolicyError, match="markers"):
        project_root(start=tmp_path)


def test_operator_root_and_persistent_paths_are_confined_to_data(tmp_path: Path):
    checkout = _checkout(tmp_path / "checkout")
    assert operator_root(None, checkout=checkout) == checkout / "data/operator"
    assert (
        persistent_path(checkout / "data" / "metadata" / "bundle.json", checkout=checkout)
        == checkout / "data/metadata/bundle.json"
    )
    assert data_directory(checkout, "tmp").is_dir()

    for rejected in (checkout / "data", checkout / "src/output", tmp_path / "outside"):
        with pytest.raises(PathPolicyError):
            persistent_path(rejected, checkout=checkout)


def test_operator_root_rejects_external_environment_override(tmp_path: Path):
    checkout = _checkout(tmp_path / "checkout")
    with pytest.raises(PathPolicyError, match="strict descendant"):
        operator_root(
            None,
            checkout=checkout,
            environ={"KALSHI_EVENTS_ROOT": str(tmp_path / "external")},
        )


def test_symlinked_data_and_nested_checkouts_are_rejected(tmp_path: Path):
    checkout = _checkout(tmp_path / "checkout")
    outside = tmp_path / "outside"
    outside.mkdir()
    (checkout / "data").symlink_to(outside, target_is_directory=True)
    with pytest.raises(PathPolicyError):
        persistent_path(checkout / "data/root", checkout=checkout)

    (checkout / "data").unlink()
    nested = checkout / "data/nested"
    nested.mkdir(parents=True)
    (nested / ".git").mkdir()
    with pytest.raises(PathPolicyError, match="nested Git"):
        persistent_path(nested / "output", checkout=checkout)


def test_installed_code_without_project_tree_keeps_caller_owned_paths(tmp_path: Path):
    site_packages = tmp_path / "site-packages"
    (site_packages / "kalshi_events").mkdir(parents=True)
    assert (
        project_root(start=Path("/nonexistent-checkout-context"), code_root=site_packages) is None
    )


@pytest.mark.parametrize("marker", [".git", "data"])
def test_test_checkout_rejects_symlinked_checkout_markers(tmp_path: Path, monkeypatch, marker):
    import conftest

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "pyproject.toml").write_text('[project]\nname = "oddsfox-kalshi-events"\n')
    outside = tmp_path / "outside"
    outside.mkdir()
    if marker == ".git":
        (checkout / ".git").symlink_to(outside, target_is_directory=True)
        (checkout / "data").mkdir()
    else:
        (checkout / ".git").mkdir()
        (checkout / "data").symlink_to(outside, target_is_directory=True)
    monkeypatch.setenv(PROJECT_ROOT_ENV, str(checkout))

    with pytest.raises(RuntimeError, match="unsafe or invalid checkout data path"):
        conftest._test_checkout()
    with pytest.raises(RuntimeError, match="unsafe or invalid checkout data path"):
        conftest.pytest_configure(SimpleNamespace(option=SimpleNamespace(basetemp=None)))
    assert list(outside.iterdir()) == []


def test_test_checkout_fallback_includes_cwd_itself(tmp_path: Path, monkeypatch):
    import conftest

    checkout = _checkout(tmp_path / "checkout")
    monkeypatch.delenv(PROJECT_ROOT_ENV, raising=False)
    monkeypatch.setattr(conftest, "__file__", str(tmp_path / "archive/tests/conftest.py"))
    monkeypatch.setattr(Path, "cwd", lambda: checkout)

    assert conftest._test_checkout() == checkout


def test_pytest_configure_rejects_symlinked_testing_directory(tmp_path: Path, monkeypatch):
    import conftest

    checkout = _checkout(tmp_path / "checkout")
    (checkout / "data").mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (checkout / "data" / "testing").symlink_to(outside, target_is_directory=True)
    monkeypatch.setenv(PROJECT_ROOT_ENV, str(checkout))

    with pytest.raises(RuntimeError, match="unsafe symlink"):
        conftest.pytest_configure(SimpleNamespace(option=SimpleNamespace(basetemp=None)))
    assert list(outside.iterdir()) == []


def test_archive_source_fails_closed_for_unsafe_cwd_checkout(tmp_path: Path, monkeypatch):
    import conftest

    archive = tmp_path / "archive"
    (archive / "tests").mkdir(parents=True)
    (archive / "pyproject.toml").write_text('[project]\nname = "oddsfox-kalshi-events"\n')
    checkout = _checkout(tmp_path / "checkout")
    outside = tmp_path / "outside"
    outside.mkdir()
    (checkout / "data").symlink_to(outside, target_is_directory=True)
    monkeypatch.delenv(PROJECT_ROOT_ENV, raising=False)
    monkeypatch.setattr(conftest, "__file__", str(archive / "tests/conftest.py"))
    monkeypatch.setattr(Path, "cwd", lambda: checkout)
    prior_tempdir = conftest.tempfile.tempdir
    config = SimpleNamespace(option=SimpleNamespace(basetemp=None))

    with pytest.raises(RuntimeError, match="unsafe or invalid checkout data path"):
        conftest.pytest_configure(config)

    assert config.option.basetemp is None
    assert conftest.tempfile.tempdir == prior_tempdir
    assert list(outside.iterdir()) == []


def test_pytest_temp_root_uses_checkout_data_without_forcing_basetemp(tmp_path: Path, monkeypatch):
    import conftest

    checkout = _checkout(tmp_path / "checkout")
    monkeypatch.setenv(PROJECT_ROOT_ENV, str(checkout))
    monkeypatch.setattr(conftest.tempfile, "tempdir", conftest.tempfile.tempdir)
    prior_tmpdir = os.environ.get("TMPDIR")
    if prior_tmpdir is None:
        monkeypatch.delenv("TMPDIR", raising=False)
    else:
        monkeypatch.setenv("TMPDIR", prior_tmpdir)
    config = SimpleNamespace(option=SimpleNamespace(basetemp=None))

    conftest.pytest_configure(config)

    testing_root = checkout / "data" / "testing"
    assert config.option.basetemp is None
    assert os.environ["TMPDIR"] == str(testing_root)
    assert conftest.tempfile.gettempdir() == str(testing_root)
    assert testing_root.is_dir()
    assert list(testing_root.iterdir()) == []


def test_standalone_archive_keeps_portable_temp_behavior(tmp_path: Path, monkeypatch):
    import conftest

    archive = tmp_path / "archive"
    (archive / "tests").mkdir(parents=True)
    (archive / "pyproject.toml").write_text('[project]\nname = "oddsfox-kalshi-events"\n')
    monkeypatch.delenv(PROJECT_ROOT_ENV, raising=False)
    monkeypatch.setattr(conftest, "__file__", str(archive / "tests/conftest.py"))
    monkeypatch.setattr(Path, "cwd", lambda: Path("/standalone-installed-tests"))
    monkeypatch.setattr(conftest.tempfile, "tempdir", conftest.tempfile.tempdir)
    prior_tempdir = conftest.tempfile.tempdir
    prior_tmpdir = os.environ.get("TMPDIR")
    if prior_tmpdir is None:
        monkeypatch.delenv("TMPDIR", raising=False)
    else:
        monkeypatch.setenv("TMPDIR", prior_tmpdir)
    config = SimpleNamespace(option=SimpleNamespace(basetemp=None))

    conftest.pytest_configure(config)

    assert config.option.basetemp is None
    assert conftest.tempfile.tempdir == prior_tempdir
    assert os.environ.get("TMPDIR") == prior_tmpdir


def test_pytest_default_temp_factory_retains_only_configured_sessions(tmp_path: Path):
    checkout = tmp_path / "checkout"
    (checkout / ".git").mkdir(parents=True)
    (checkout / "tests").mkdir()
    (checkout / "pyproject.toml").write_text(
        '[project]\nname = "oddsfox-kalshi-events"\n\n'
        "[tool.pytest.ini_options]\n"
        "tmp_path_retention_count = 1\n"
        'tmp_path_retention_policy = "all"\n'
    )
    (checkout / "tests" / "conftest.py").write_text(
        (Path(__file__).with_name("conftest.py")).read_text()
    )
    (checkout / "tests" / "test_temp_root.py").write_text(
        "from pathlib import Path\n"
        "\n"
        "def test_tmp_path_is_checkout_local(tmp_path):\n"
        "    assert tmp_path.is_relative_to(Path(__file__).parents[1] / 'data' / 'testing')\n"
    )

    environment = os.environ.copy()
    environment["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    for name in ("PYTEST_ADDOPTS", "PYTEST_DEBUG_TEMPROOT", "PYTEST_PLUGINS", PROJECT_ROOT_ENV):
        environment.pop(name, None)
    source_root = str(CHECKOUT / "src")
    environment["PYTHONPATH"] = os.pathsep.join(
        value for value in (source_root, environment.get("PYTHONPATH")) if value
    )

    testing_root = checkout / "data" / "testing"
    for session_number in range(3):
        completed = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "tests/test_temp_root.py"],
            cwd=checkout,
            env=environment,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        assert completed.returncode == 0, completed.stdout + completed.stderr

        user_roots = list(testing_root.glob("pytest-of-*"))
        assert len(user_roots) == 1
        sessions = sorted(
            path
            for path in user_roots[0].iterdir()
            if path.is_dir()
            and path.name.startswith("pytest-")
            and path.name[len("pytest-") :].isdigit()
        )
        assert [path.name for path in sessions] == [f"pytest-{session_number}"]


def test_cli_guards_operator_metrics_and_metadata_writes_before_creation(
    tmp_path: Path, monkeypatch, capsys
):
    from kalshi_events import cli

    checkout = _checkout(tmp_path / "checkout")
    monkeypatch.setenv(PROJECT_ROOT_ENV, str(checkout))
    external = tmp_path / "external"

    assert cli.main(["--root", str(external), "status"]) == 1
    assert not external.exists()
    capsys.readouterr()

    assert cli.main(["--metrics-file", str(external / "metrics.json"), "status"]) == 1
    assert not external.exists()
    capsys.readouterr()

    arguments = cli.parser().parse_args(
        ["metadata", "export", "--output", str(external), "--market-ticker", "T"]
    )
    with pytest.raises(CatalogueError, match="metadata output"):
        cli.execute(arguments)
    assert not external.exists()


def test_cli_read_only_verifiers_do_not_require_a_checkout_binding(monkeypatch):
    from kalshi_events import cli

    def no_binding(**_kwargs):
        raise AssertionError("read-only verification should not resolve a write context")

    monkeypatch.setattr(cli, "_policy_project_root", no_binding)
    monkeypatch.setattr(cli.publish, "verify_release", lambda path, _limit: {"release": str(path)})
    release_args = cli.parser().parse_args(["verify", "--release", "/external/release"])
    assert cli.execute(release_args) == {"release": "/external/release"}

    monkeypatch.setattr(cli.metadata, "verify", lambda path: {"bundle": str(path)})
    metadata_args = cli.parser().parse_args(["metadata", "verify", "--output", "/external/bundle"])
    assert cli.execute(metadata_args) == {"bundle": "/external/bundle"}
