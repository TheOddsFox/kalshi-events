from __future__ import annotations

import builtins
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts/benchmark-pipeline.py"
SPEC = importlib.util.spec_from_file_location("benchmark_pagination", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


@pytest.mark.parametrize("cursor", [None, "499800", "999800", "999999", "1000000"])
def test_unfiltered_page_consumes_only_returned_indexes(monkeypatch, cursor):
    """A million logical markets must not require a million-index scan per page."""
    consumed = 0

    class CountedRange:
        def __init__(self, values):
            self.values = values

        def __len__(self):
            return len(self.values)

        def __getitem__(self, key):
            value = self.values[key]
            return CountedRange(value) if isinstance(key, slice) else value

        def __iter__(self):
            nonlocal consumed
            for value in self.values:
                consumed += 1
                assert consumed <= benchmark.PAGE_SIZE, "scanned indexes outside the requested page"
                yield value

    monkeypatch.setattr(
        benchmark, "range", lambda *args: CountedRange(builtins.range(*args)), raising=False
    )
    status, payload = benchmark.FixtureClient(1_000_000).response(
        "/markets", {} if cursor is None else {"cursor": cursor}
    )

    start = int(cursor or "0")
    stop = min(start + 200, 1_000_000)
    assert status == 200
    assert [row["ticker"] for row in payload["markets"]] == [
        f"MKT-{index:06d}" for index in builtins.range(start, stop)
    ]
    assert payload["cursor"] == (str(stop) if stop < 1_000_000 else "")
    assert consumed == len(payload["markets"])


# Canonical status/payload hashes from committed f20f9c1, before lazy pagination.
# These freeze every native field, decimal, combo leg and cursor, independently
# of the index-consumption spy above.
@pytest.mark.parametrize(
    ("markets", "cursor", "expected_hash"),
    [
        (0, None, "59ec7241a9f2dd9a59d806803bbe39784287b90b4374fb395b6ecdfce5ae3e67"),
        (1, None, "f5569a5f03e3d9a402b379d8bcbb059c32c09735a1d5e979b82ecc92a6131091"),
        (200, None, "243e9fbf0885be0a625f775405d14a66649d246c18ec28cc0faae03aaed1b639"),
        (201, None, "316036ef66512d715d4b0acafe2656cf1ee21d704df165d9b3bdfcb524de8fff"),
        (201, "200", "1f2506209b5c7afc6b213444f744d623f6dfde1f884340602ddf9702a34f7115"),
        (1203, "1000", "5eb334f1b8209d9004777aa7e84a8b40d94a0ce876c66548bc56c5257dd6fd0e"),
        (1203, "1200", "0c7a7936ab6cdb55c8fb0b027ad59e5bd7ccc255f418aaabbee668e9cb8c89b5"),
        (1203, "1203", "59ec7241a9f2dd9a59d806803bbe39784287b90b4374fb395b6ecdfce5ae3e67"),
        (
            1_000_000,
            "999800",
            "ddd500006c1c7bd29cf31e6077dee04baae788cb509084c8b7042be0ffeeda9c",
        ),
    ],
)
def test_unfiltered_pages_preserve_frozen_payloads(markets, cursor, expected_hash):
    response = benchmark.FixtureClient(markets).response(
        "/markets", {} if cursor is None else {"cursor": cursor}
    )
    encoded = json.dumps(response, sort_keys=True, separators=(",", ":")).encode()
    assert hashlib.sha256(encoded).hexdigest() == expected_hash
