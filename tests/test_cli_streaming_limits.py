from __future__ import annotations

import pytest

from kalshi_events import capture, publish, warehouse
from kalshi_events.cli import parser
from kalshi_events.common import OWNER_MARKER, CatalogueError, Limits, Store, json_bytes


def test_decoded_limit_defaults_to_selected_encoded_limit(tmp_path):
    args = parser().parse_args(
        ["--root", str(tmp_path / "root"), "capture", "--max-response-bytes", "1234"]
    )
    values = {field: getattr(args, field) for field in Limits.__dataclass_fields__}
    assert args.max_decoded_response_bytes is None
    assert Limits(**values).max_decoded_response_bytes == 1234


def test_legacy_root_direct_writers_fail_before_creating_work_files(tmp_path):
    root = tmp_path / "legacy"
    root.mkdir()
    (root / "root.json").write_bytes(json_bytes(OWNER_MARKER))
    store = Store(root)

    operations = (
        lambda: capture.capture(store),
        lambda: warehouse.load(store, "0" * 32),
        lambda: warehouse.dbt(store, ["parse"]),
        lambda: warehouse.build(store, "0" * 32),
        lambda: publish.publish(store, "0" * 32),
        lambda: publish.prune_superseded(store, {}, "0" * 64),
    )
    for operation in operations:
        with pytest.raises(CatalogueError, match="explicit upgrade-root"):
            operation()
    assert list(root.iterdir()) == [root / "root.json"]
