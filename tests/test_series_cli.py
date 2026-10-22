from __future__ import annotations

import json

import pytest

from kalshi_events import cli, publish, recovery, refresh
from kalshi_events.common import CatalogueError, Store, series_scope

SCOPE = series_scope("KXCPI")


def test_series_parser_exposes_scoped_refresh_and_recovery_actions(tmp_path):
    root = str(tmp_path / "series")
    args = cli.parser().parse_args(
        ["--root", root, "series", "refresh", "--series-ticker", "KXCPI", "--full"]
    )
    assert (args.command, args.series_action, args.series_ticker, args.full) == (
        "series",
        "refresh",
        "KXCPI",
        True,
    )
    assert args.max_requests > 0

    assert cli.parser().parse_args(["--root", root, "series", "current"]).series_action == "current"
    replay = cli.parser().parse_args(["--root", root, "series", "replay", "--capture-id", "a" * 32])
    assert replay.capture_id == "a" * 32
    assert cli.parser().parse_args(["--root", root, "series", "rebuild"]).series_action == "rebuild"


def test_new_series_root_requires_ticker_before_store_creation(tmp_path, capsys, monkeypatch):
    root = tmp_path / "series"

    def unexpected_store(*_args, **_kwargs):
        pytest.fail("Store must not create an ordinary root for invalid series input")

    monkeypatch.setattr(cli, "Store", unexpected_store)
    code = cli.main(["--root", str(root), "series", "refresh"])
    output = capsys.readouterr()
    assert code == 1
    assert "--series-ticker" in json.loads(output.out)["error"]
    assert not root.exists()


@pytest.mark.parametrize("args", [["--full", "--capture-id", "a" * 32]])
def test_invalid_series_refresh_flags_fail_before_root_creation(tmp_path, capsys, args):
    root = tmp_path / "series"
    code = cli.main(["--root", str(root), "series", "refresh", *args, "--series-ticker", "KXCPI"])
    result = json.loads(capsys.readouterr().out)
    assert code == 1
    assert result["stage"] == "preflight"
    assert result["series_scope"] is None
    assert not root.exists()


def test_series_read_actions_do_not_create_missing_roots(tmp_path, monkeypatch):
    root = tmp_path / "missing"
    monkeypatch.setattr(cli, "Store", lambda *_a, **_k: pytest.fail("Store constructed"))
    with pytest.raises(CatalogueError, match="existing scoped operator root"):
        cli.execute(cli.parser().parse_args(["--root", str(root), "series", "current"]))
    assert not root.exists()


def test_existing_series_root_derives_scope_and_rejects_conflicts(tmp_path, monkeypatch):
    root = tmp_path / "series"
    Store(root, series_ticker="KXCPI")
    seen = {}

    def fake_run(store, **kwargs):
        seen["scope"] = store.scope
        seen["kwargs"] = kwargs
        return ({**refresh.result_template(), "series_scope": store.scope}, 0)

    monkeypatch.setattr(refresh, "run", fake_run)
    code = cli.main(["--root", str(root), "series", "refresh"])
    assert code == 0
    assert seen["scope"] == SCOPE
    assert seen["kwargs"] == {"full": False, "capture_id": None, "compression": None}

    with pytest.raises(CatalogueError, match="series scope requires"):
        cli.execute(
            cli.parser().parse_args(
                ["--root", str(root), "series", "refresh", "--series-ticker", "KXGDP"]
            )
        )


def test_series_refresh_emits_scoped_machine_result_and_uses_one_refresh_workflow(
    tmp_path, monkeypatch, capsys
):
    root = tmp_path / "series"
    Store(root, series_ticker="KXCPI")
    called = {}

    def fake_run(store, **kwargs):
        called["store"] = store
        called["kwargs"] = kwargs
        return (
            {
                **refresh.result_template(),
                "complete": True,
                "capture_id": "b" * 32,
                "stage": "complete",
                "publication_state": "verified",
                "series_scope": store.scope,
            },
            0,
        )

    monkeypatch.setattr(refresh, "run", fake_run)
    code = cli.main(
        [
            "--root",
            str(root),
            "series",
            "refresh",
            "--capture-id",
            "b" * 32,
            "--compression",
            "gzip",
        ]
    )
    result = json.loads(capsys.readouterr().out)
    assert code == 0
    assert called["store"].scope == SCOPE
    assert called["kwargs"] == {
        "full": False,
        "capture_id": "b" * 32,
        "compression": "gzip",
    }
    assert result["series_scope"] == SCOPE
    assert result["publication_state"] == "verified"


@pytest.mark.parametrize(
    "action",
    ["capture", "load", "build", "publish", "replay", "rebuild", "current", "dbt"],
)
def test_generic_catalogue_actions_reject_scoped_roots(tmp_path, action):
    root = tmp_path / "series"
    Store(root, series_ticker="KXCPI")
    parsed = cli.parser().parse_args(["--root", str(root), action])
    with pytest.raises(CatalogueError, match="cannot operate on a scoped series root"):
        cli.execute(parsed)


def test_upgrade_root_is_allowed_for_legacy_scoped_roots(tmp_path, capsys):
    root = tmp_path / "series"
    root.mkdir()
    marker = {"owner": "oddsfox.kalshi.events.v1", "storage_version": 4, "scope": SCOPE}
    (root / "root.json").write_text(json.dumps(marker))
    (root / "root-id.json").write_text(
        json.dumps({"version": 1, "root_id": "0123456789abcdef0123456789abcdef"})
    )
    store = Store(root)
    from kalshi_events.incremental import root_id

    identity = root_id(store)
    (root / "evidence.bin").write_bytes(b"unchanged")

    code = cli.main(["--root", str(root), "upgrade-root"])

    assert code == 0
    result = json.loads(capsys.readouterr().out)
    assert result == {"storage_version": 5, "upgraded": True}
    assert json.loads((root / "root.json").read_text()) == {
        "owner": "oddsfox.kalshi.events.v1",
        "storage_version": 5,
        "scope": SCOPE,
    }
    assert Store(root).scope == SCOPE
    assert (root / "evidence.bin").read_bytes() == b"unchanged"
    from kalshi_events.incremental import root_id as read_root_id

    assert read_root_id(Store(root)) == identity


def test_global_refresh_rejects_scoped_root_without_running_workflow(tmp_path, monkeypatch, capsys):
    root = tmp_path / "series"
    Store(root, series_ticker="KXCPI")
    monkeypatch.setattr(refresh, "run", lambda *_a, **_k: pytest.fail("refresh workflow called"))
    code = cli.main(["--root", str(root), "refresh", "--full"])
    result = json.loads(capsys.readouterr().out)
    assert code == 1
    assert result["stage"] == "preflight"
    assert "catalogue commands cannot operate" in result["error"]


def test_series_actions_route_to_internal_v2_and_add_scope(tmp_path, monkeypatch):
    root = tmp_path / "series"
    Store(root, series_ticker="KXCPI")
    seen = []

    monkeypatch.setattr(
        publish, "current", lambda actual, **kw: seen.append(("current", kw)) or {"x": 1}
    )
    monkeypatch.setattr(
        recovery,
        "replay",
        lambda actual, capture_id, **kw: seen.append(("replay", capture_id, kw)) or {"x": 2},
    )
    monkeypatch.setattr(
        recovery, "rebuild", lambda actual, **kw: seen.append(("rebuild", kw)) or {"x": 3}
    )
    for argv, expected in (
        (["series", "current"], {"x": 1, "series_scope": SCOPE}),
        (["series", "replay", "--capture-id", "c" * 32], {"x": 2, "series_scope": SCOPE}),
        (["series", "rebuild"], {"x": 3, "series_scope": SCOPE}),
    ):
        args = cli.parser().parse_args(["--root", str(root), *argv])
        assert cli.execute(args) == expected
    assert seen == [
        ("current", {"catalogue_version": 2}),
        ("replay", "c" * 32, {"catalogue_version": 2}),
        ("rebuild", {"catalogue_version": 2}),
    ]


def test_status_and_discard_remain_available_on_scoped_roots(tmp_path, monkeypatch):
    root = tmp_path / "series"
    Store(root, series_ticker="KXCPI")
    monkeypatch.setattr(
        publish,
        "current",
        lambda _store, *, catalogue_version=1: {"contract": f"v{catalogue_version}"},
    )
    status = cli.execute(cli.parser().parse_args(["--root", str(root), "status"]))
    assert status["series_scope"] == SCOPE
    assert status["current_series"] is None
    assert status["current"] is None and status["current_v2"] is None

    from kalshi_events import incremental

    monkeypatch.setattr(incremental, "discard", lambda actual, identity: {"capture_id": identity})
    discarded = cli.execute(
        cli.parser().parse_args(["--root", str(root), "discard", "--capture-id", "d" * 32])
    )
    assert discarded == {"capture_id": "d" * 32}
