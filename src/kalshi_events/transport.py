"""Public, single-worker HTTP with durable per-attempt evidence and bounded streaming."""

from __future__ import annotations

import os
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

from kalshi_events.common import (
    Budget,
    CatalogueError,
    Store,
    atomic_bytes,
    decode,
    digest,
    now,
    regular_bytes,
)

BASE_URL = "https://external-api.kalshi.com/trade-api/v2"


def source_url() -> str:
    value = os.environ.get("KALSHI_EVENTS_API_URL", BASE_URL).rstrip("/")
    parsed = urlsplit(value)
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise CatalogueError("source URL cannot include credentials, query or fragment")
    public = value == BASE_URL
    loopback = (
        os.environ.get("KALSHI_EVENTS_ALLOW_LOOPBACK") == "1"
        and parsed.hostname in {"127.0.0.1", "::1", "localhost"}
        and parsed.scheme == "http"
        and parsed.path == "/trade-api/v2"
    )
    if not (public or loopback):
        raise CatalogueError("source origin is not the approved public Kalshi API")
    return value


def read_page(store: Store, relative: str) -> tuple[dict[str, Any], Any]:
    manifest = decode(regular_bytes(store.path(relative), 1024**2))
    if not isinstance(manifest, dict) or manifest.get("version") != 1:
        raise CatalogueError("unsupported raw evidence manifest")
    body_path = store.path(str(Path(relative).parent / "body.bin"))
    body = regular_bytes(body_path, store.limits.max_response_bytes)
    if len(body) != manifest.get("size") or digest(body) != manifest.get("sha256"):
        raise CatalogueError("raw response evidence integrity mismatch")
    return manifest, decode(body) if manifest["status"] == 200 else None


class PublicClient:
    def __init__(self, store: Store, budget: Budget):
        self.store = store
        self.budget = budget
        self.base_url = source_url()
        self.last_attempt = 0.0
        self.request_context: dict[str, Any] = {}
        self.client = httpx.Client(
            trust_env=False,
            follow_redirects=False,
            headers={"Accept": "application/json", "Accept-Encoding": "identity"},
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
        )

    def __enter__(self) -> PublicClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self.client.close()

    def get(self, endpoint: str, params: dict[str, Any] | None = None) -> str:
        if (
            not endpoint.startswith("/")
            or ".." in endpoint.split("/")
            or "?" in endpoint
            or "#" in endpoint
        ):
            raise CatalogueError("invalid source endpoint")
        for retry in range(4):
            delay = max(0, 0.5 - (time.monotonic() - self.last_attempt))
            if delay >= self.budget.remaining:
                raise CatalogueError("deadline cannot accommodate request spacing")
            if delay:
                time.sleep(delay)
            allowance = self.budget.reserve()
            self.last_attempt = time.monotonic()
            attempt_id = uuid.uuid4().hex
            relative = f"raw/{attempt_id}/manifest.json"
            reservation = {
                **self.request_context,
                "page_id": attempt_id,
                "version": 1,
                "endpoint": endpoint,
                "params": params or {},
                "received_at": now(),
                "status": None,
                "reserved_bytes": allowance,
                "accounting_complete": False,
            }
            self.store.write(relative, reservation)
            body = bytearray()
            attempt_start_bytes = self.budget.downloaded_bytes
            status = None
            try:
                with self.client.stream(
                    "GET",
                    self.base_url + endpoint,
                    params=params,
                    timeout=min(30, self.budget.remaining),
                ) as response:
                    status = response.status_code
                    if response.headers.get("content-encoding", "identity") not in {"identity", ""}:
                        raise CatalogueError("compressed response is not permitted")
                    declared = response.headers.get("content-length")
                    if declared and int(declared) > allowance:
                        raise CatalogueError("response exceeds byte ceiling")
                    for chunk in response.iter_raw():
                        _ = self.budget.remaining
                        self.budget.downloaded_bytes += len(chunk)
                        if (
                            len(body) + len(chunk) > allowance
                            or self.budget.downloaded_bytes > self.store.limits.max_download_bytes
                        ):
                            raise CatalogueError("stream exceeded response/download byte ceiling")
                        body.extend(chunk)
                self.store.check_storage(len(body) + 4096)
                atomic_bytes(self.store.path(f"raw/{attempt_id}/body.bin"), bytes(body))
                self.store.write(
                    relative,
                    {
                        **reservation,
                        "status": status,
                        "size": len(body),
                        "sha256": digest(bytes(body)),
                        "received_at": now(),
                        "downloaded_bytes": self.budget.downloaded_bytes - attempt_start_bytes,
                        "accounting_complete": True,
                    },
                )
                if status in {429, 500, 502, 503, 504} and retry < 3:
                    if 2**retry >= self.budget.remaining:
                        raise CatalogueError("deadline cannot accommodate retry")
                    time.sleep(2**retry)
                    continue
                if status not in {200, 400, 404, 410}:
                    raise CatalogueError(f"source request failed with HTTP {status}")
                return relative
            except (httpx.HTTPError, ValueError) as exc:
                self.store.write(
                    relative,
                    {
                        **reservation,
                        "status": status,
                        "size": len(body),
                        "accounting_complete": True,
                        "error": type(exc).__name__,
                        "downloaded_bytes": self.budget.downloaded_bytes - attempt_start_bytes,
                    },
                )
                if retry == 3:
                    raise CatalogueError("bounded public source request failed") from exc
            except CatalogueError:
                self.store.write(
                    relative,
                    {
                        **reservation,
                        "status": status,
                        "size": len(body),
                        "accounting_complete": True,
                        "error": "rejected response",
                        "downloaded_bytes": self.budget.downloaded_bytes - attempt_start_bytes,
                    },
                )
                raise
        raise CatalogueError("source retries exhausted")


def accounting(store: Store) -> dict[str, Any]:
    attempts = downloaded_or_reserved = 0
    complete = True
    for path in store.path("raw").glob("*/manifest.json"):
        receipt = decode(regular_bytes(path, 1024**2))
        attempts += 1
        accounted = receipt.get("accounting_complete") is True
        downloaded_or_reserved += (
            receipt.get("downloaded_bytes", receipt.get("size", 0))
            if accounted
            else receipt.get("reserved_bytes", store.limits.max_response_bytes)
        )
        complete = complete and accounted
    return {
        "attempts": attempts,
        "downloaded_or_reserved_bytes": downloaded_or_reserved,
        "accounting_complete": complete,
    }
