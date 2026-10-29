from __future__ import annotations

import os
import stat
import tempfile
import threading
import time
import tomllib
import uuid
from pathlib import Path

import pytest

from kalshi_events.common import Store, digest, json_bytes, now


def _test_checkout() -> Path | None:
    configured = os.environ.get("KALSHI_EVENTS_PROJECT_ROOT")
    source = Path(__file__).absolute().parents[1]
    candidates = [Path(configured)] if configured else [source]
    if not configured:
        source_marker = source / ".git"
        if not source_marker.exists() and not source_marker.is_symlink():
            cwd = Path.cwd()
            candidates.extend((cwd, *cwd.parents))
    for candidate in candidates:
        if not candidate.is_absolute() or ".." in candidate.parts:
            continue
        marker = candidate / ".git"
        project_file = candidate / "pyproject.toml"
        data = candidate / "data"
        try:
            project_info = project_file.lstat()
            if not stat.S_ISREG(project_info.st_mode):
                continue
            with project_file.open("rb") as stream:
                name = tomllib.load(stream).get("project", {}).get("name")
        except (OSError, tomllib.TOMLDecodeError):
            continue
        if name != "oddsfox-kalshi-events":
            continue

        # A project marker establishes checkout context. Once it does, unsafe
        # paths must fail closed instead of falling back to the OS temp dir.
        try:
            marker_info = marker.lstat()
        except FileNotFoundError:
            continue
        except OSError as error:
            raise RuntimeError(
                "unsafe or invalid checkout data path for pytest temporary files"
            ) from error
        if (
            stat.S_ISLNK(marker_info.st_mode)
            or not (stat.S_ISREG(marker_info.st_mode) or stat.S_ISDIR(marker_info.st_mode))
            or any(path.is_symlink() for path in (candidate, *candidate.parents))
        ):
            raise RuntimeError("unsafe or invalid checkout data path for pytest temporary files")
        try:
            data_info = data.lstat()
        except FileNotFoundError:
            data_info = None
        except OSError as error:
            raise RuntimeError(
                "unsafe or invalid checkout data path for pytest temporary files"
            ) from error
        if data_info is not None and not stat.S_ISDIR(data_info.st_mode):
            raise RuntimeError("unsafe or invalid checkout data path for pytest temporary files")
        return candidate
    return None


def pytest_configure(config):
    """Keep pytest's synthetic roots and tempfile outputs in ignored checkout data."""
    checkout = _test_checkout()
    if checkout is None:
        source_checkout = Path(__file__).absolute().parents[1]
        marker = source_checkout / ".git"
        if os.environ.get("KALSHI_EVENTS_PROJECT_ROOT") or marker.exists() or marker.is_symlink():
            raise RuntimeError("unsafe or invalid checkout data path for pytest temporary files")
        return
    data = checkout / "data"
    parent = data / "testing"
    if any(path.is_symlink() for path in (parent, *parent.parents)):
        raise RuntimeError("unsafe symlink in pytest temporary-data path")
    if data.exists() and not data.is_dir():
        raise RuntimeError("pytest data path is not a directory")
    if parent.exists() and not parent.is_dir():
        raise RuntimeError("pytest testing-data path is not a directory")
    parent.mkdir(parents=True, exist_ok=True)
    # Leave basetemp unset so pytest retains its normal numbered, locked
    # session directories and bounded cleanup under this checkout-owned root.
    os.environ["TMPDIR"] = str(parent)
    tempfile.tempdir = str(parent)


@pytest.fixture
def advancing_clock(monkeypatch):
    """Advance mocked waits so limiter rechecks also consume operation deadlines."""
    clock = [time.monotonic()]
    waits = []
    lock = threading.Lock()

    def monotonic():
        with lock:
            return clock[0]

    def sleep(delay):
        assert delay >= 0
        with lock:
            clock[0] += delay
            waits.append(delay)

    monkeypatch.setattr("kalshi_events.transport.time.monotonic", monotonic)
    monkeypatch.setattr("kalshi_events.transport.time.sleep", sleep)
    return waits


SERIES = [
    {
        "ticker": "SERIES",
        "title": "Native contracts",
        "contract_url": "https://example.test/rules",
        "fee_multiplier": "1.25",
    }
]
EVENTS = [
    {
        "event_ticker": "EVENT",
        "series_ticker": "SERIES",
        "title": "Ordinary event",
        "mutually_exclusive": False,
    },
    {"event_ticker": "HISTORY", "series_ticker": "SERIES", "title": "Archived event"},
]
COMBO_EVENTS = [
    {
        "event_ticker": "COMBO",
        "series_ticker": "SERIES",
        "title": "Combo event",
        "product_metadata": {"native": True},
    }
]
MARKET = {
    "ticker": "MARKET",
    "event_ticker": "EVENT",
    "status": "active",
    "rules_primary": "Resolve YES only under native rules",
    "rules_secondary": "Details",
    "last_price_dollars": "0.00123",
    "volume_fp": "12.3456789",
    "open_interest_fp": "2.125",
    "price_level_structure": "deci_cent",
    "price_ranges": [{"start": "0", "end": "1", "step": "0.0001"}],
    "floor_strike": "3.123456789123456789",
    "result": "",
    "settlement_ts": None,
}
COMBO = {
    **MARKET,
    "ticker": "COMBO-MARKET",
    "event_ticker": "COMBO",
    "mve_collection_ticker": "COLLECTION",
    "mve_selected_legs": [
        {
            "event_ticker": "EVENT",
            "market_ticker": "MARKET",
            "side": "yes",
            "yes_settlement_value_dollars": "0.00001",
        }
    ],
}
HISTORICAL = {
    **MARKET,
    "ticker": "HISTORICAL",
    "event_ticker": "HISTORY",
    "status": "finalized",
    "result": "no",
    "settlement_ts": "2025-01-01T00:00:00Z",
    "settlement_value_dollars": "0.0000",
}
CUTOFF = {"market_settled_ts": "2026-01-01T00:00:00Z", "trades_created_ts": "2026-02-01T00:00:00Z"}


class FakeClient:
    """Offline stand-in for PublicClient with the same receipt and budget contract."""

    def __init__(self, store, budget, responses=None, **client_options):
        self.store = store
        self.budget = budget
        self.responses = responses or {}
        self.compression = client_options.get("compression", "identity")
        self.rate_policy = client_options.get("rate_policy", "fixed")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def response(self, endpoint, params):
        if endpoint in self.responses:
            value = self.responses[endpoint]
            result = value(params) if callable(value) else value
        else:
            defaults = {
                "/historical/cutoff": CUTOFF,
                "/series": {"series": SERIES},
                "/series/SERIES": {"series": SERIES[0]},
                "/events": {"events": EVENTS, "cursor": ""},
                "/events/multivariate": {"events": COMBO_EVENTS, "cursor": ""},
                "/markets": {"markets": [MARKET, COMBO], "cursor": ""},
                "/historical/markets": {"markets": [HISTORICAL], "cursor": ""},
                "/markets/MARKET": {"market": MARKET},
                "/markets/COMBO-MARKET": {"market": COMBO},
                "/historical/markets/HISTORICAL": {"market": HISTORICAL},
            }
            defaults.update(
                {
                    f"/events/{row['event_ticker']}": {"event": row}
                    for row in [*EVENTS, *COMBO_EVENTS]
                }
            )
            result = (200, defaults[endpoint]) if endpoint in defaults else (404, {})
        if endpoint == "/markets" and isinstance(params.get("tickers"), str):
            status, payload = result
            rows = payload.get("markets") if isinstance(payload, dict) else None
            if (
                status == 200
                and isinstance(rows, list)
                and all(isinstance(row, dict) for row in rows)
            ):
                requested = params["tickers"].split(",")
                selected = set(requested)
                payload = {
                    **payload,
                    "markets": [row for row in rows if row.get("ticker") in selected],
                }
                for value in requested:
                    direct = self.responses.get(f"/markets/{value}")
                    if not isinstance(direct, tuple) or len(direct) != 2:
                        continue
                    direct_status, direct_payload = direct
                    if direct_status == 404:
                        payload["markets"] = [
                            row for row in payload["markets"] if row.get("ticker") != value
                        ]
                    elif (
                        direct_status == 200
                        and isinstance(direct_payload, dict)
                        and isinstance(direct_payload.get("market"), dict)
                    ):
                        replacement = direct_payload["market"]
                        payload["markets"] = [
                            row for row in payload["markets"] if row.get("ticker") != value
                        ]
                        payload["markets"].append(replacement)
                result = (status, payload)
        return result

    def get(self, endpoint, params=None, context=None):
        allowance = self.budget.reserve()
        try:
            status, payload = self.response(endpoint, params or {})
            body = json_bytes(payload)
            attempt = uuid.uuid4().hex
            relative = f"raw/{attempt}/manifest.json"
            self.budget.add_download(len(body))
            self.store.write_bytes(f"raw/{attempt}/body.bin", body)
            self.store.write(
                relative,
                {
                    **(context or {}),
                    "page_id": attempt,
                    "version": 1,
                    "endpoint": endpoint,
                    "params": params or {},
                    "received_at": now(),
                    "status": status,
                    "size": len(body),
                    "sha256": digest(body),
                    "downloaded_bytes": len(body),
                    "accounting_complete": True,
                },
            )
            return relative
        finally:
            self.budget.settle(allowance)


@pytest.fixture
def store(tmp_path):
    return Store(tmp_path / "root")


@pytest.fixture
def factory():
    return FakeClient
