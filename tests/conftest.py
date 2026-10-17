from __future__ import annotations

import uuid

import pytest

from kalshi_events.common import Store, digest, json_bytes, now

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

    def __init__(self, store, budget, responses=None):
        self.store = store
        self.budget = budget
        self.responses = responses or {}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def response(self, endpoint, params):
        if endpoint in self.responses:
            value = self.responses[endpoint]
            return value(params) if callable(value) else value
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
            {f"/events/{row['event_ticker']}": {"event": row} for row in [*EVENTS, *COMBO_EVENTS]}
        )
        return (200, defaults[endpoint]) if endpoint in defaults else (404, {})

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
