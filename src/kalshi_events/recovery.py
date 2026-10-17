"""Rebuild the published catalogue capture from retained raw evidence.

Rebuild is latest-only: it certifies a fresh warehouse for the capture named by the current
release and never touches published releases or other captures.
"""

from __future__ import annotations

from typing import Any

from kalshi_events.common import Store
from kalshi_events.publish import current
from kalshi_events.warehouse import build


def rebuild(store: Store) -> dict[str, Any]:
    capture_id = current(store)["capture_id"]
    return build(store, capture_id)
