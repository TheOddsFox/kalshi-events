"""Diagnostic inventories distinguish native JSON numbers from provider strings."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from probe_fixtures import event_fixture
from test_throughput_probe import FixtureClient, _pilot_store

from kalshi_events import partition_probe


def _receipt_inventory(root: Path, value_json: str) -> str:
    fixture = event_fixture("balanced")
    fixture["partitions"] = [row for row in fixture["partitions"] if row["kind"] == "series"]
    for row in fixture["partitions"]:
        row["events"] = row["events"][:1]
    fixture["partitions"][0]["events"][0]["probe_value"] = "RAW-JSON-INVENTORY-VALUE"
    selected = [{"kind": row["kind"], "ticker": row["ticker"]} for row in fixture["partitions"]]
    store, budget = _pilot_store(root)
    client = FixtureClient(store, fixture)
    original_get = client.get

    def get(endpoint: str, params: Any = None, context: Any = None) -> str:
        relative = original_get(endpoint, params, context)
        manifest_path = store.path(relative)
        manifest = json.loads(manifest_path.read_text())
        body_relative = str(Path(relative).with_name("body.bin"))
        body = (
            store.path(body_relative)
            .read_bytes()
            .replace(b'"RAW-JSON-INVENTORY-VALUE"', value_json.encode("utf-8"))
        )
        store.write_bytes(body_relative, body)
        manifest.update(
            size=len(body), downloaded_bytes=len(body), sha256=hashlib.sha256(body).hexdigest()
        )
        store.write(relative, manifest)
        return relative

    client.get = get
    with store.writer_guard():
        result = partition_probe.acquire_partitions(
            store, client, budget, selected, collection_series={}, workers=1
        )
    assert result["completed"] is True
    assert result["observations"] == 2
    return result["partitions"][0]["inventory_sha256"]


@pytest.mark.parametrize(
    "number_json,string_json",
    [
        ("1.25", '"1.25"'),
        ('{"value":1.25}', '{"value":"1.25"}'),
        ("[1.25]", '["1.25"]'),
        ('[null,true,1,1.25,{"at":"1.25"}]', '[null,true,1,"1.25",{"at":"1.25"}]'),
        ('{"~":{"a/b":[1.25]}}', '{"~":{"a/b":["1.25"]}}'),
        (
            '{"decimal_paths":[["value"]],"$decimal":"1.25","value":1.25}',
            '{"decimal_paths":[["value"]],"$decimal":"1.25","value":"1.25"}',
        ),
    ],
)
def test_real_receipt_inventory_preserves_number_versus_string_types(
    tmp_path: Path, number_json: str, string_json: str
) -> None:
    assert _receipt_inventory(tmp_path / "number", number_json) != _receipt_inventory(
        tmp_path / "string", string_json
    )


@pytest.mark.parametrize(
    "left,right",
    [
        ("1.234567891234567891", "1.234567891234567892"),
        ("1.25", "1.250"),
        ("1", '"1"'),
        ("true", "1"),
        ("null", '"null"'),
        ("1.25", '{"__decimal__":"1.25"}'),
    ],
)
def test_real_receipt_inventory_preserves_precision_and_other_json_types(
    tmp_path: Path, left: str, right: str
) -> None:
    assert _receipt_inventory(tmp_path / "left", left) != _receipt_inventory(
        tmp_path / "right", right
    )


def test_real_receipt_inventory_ignores_object_key_order(tmp_path: Path) -> None:
    assert _receipt_inventory(
        tmp_path / "first", '{"z":0,"a":[1.25,"1.25"],"type":"Decimal"}'
    ) == _receipt_inventory(tmp_path / "second", '{"type":"Decimal","a":[1.25,"1.25"],"z":0}')


def test_decimal_paths_distinguish_array_positions_and_object_keys() -> None:
    digest = partition_probe._event_payload_digest
    decimal = Decimal("1.25")
    assert digest({"x": decimal}) != digest({"x": "1.25"})
    assert digest({"x": [decimal, "1.25"]}) != digest({"x": ["1.25", decimal]})
    assert digest({"x": {"0": decimal}, "y": ["1.25"]}) != digest(
        {"x": {"0": "1.25"}, "y": [decimal]}
    )
    assert digest({"x": Decimal("1.250")}) != digest({"x": decimal})
