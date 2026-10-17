"""Public, rate-limited HTTP with durable per-attempt evidence and bounded streaming."""

from __future__ import annotations

import email.utils
import math
import os
import random
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

from kalshi_events.common import (
    Budget,
    BudgetExceeded,
    CatalogueError,
    Store,
    decode,
    digest,
    now,
    regular_bytes,
)

BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
MAX_ATTEMPTS = 4
MAX_CONCURRENT_REQUESTS = 8
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
ACCEPTED_STATUSES = frozenset({200, 400, 404, 410})
MAX_BACKOFF_SECONDS = 30.0


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


def read_page(store: Store, relative: str, limit: int | None = None) -> tuple[dict[str, Any], Any]:
    """Verify one raw receipt and its body. `limit` is the capture's sealed response ceiling."""
    limit = store.limits.max_response_bytes if limit is None else limit
    manifest = decode(regular_bytes(store.path(relative), 1024**2))
    if not isinstance(manifest, dict) or manifest.get("version") != 1:
        raise CatalogueError("unsupported raw evidence manifest")
    body_path = store.path(str(Path(relative).parent / "body.bin"))
    body = regular_bytes(body_path, limit)
    if len(body) != manifest.get("size") or digest(body) != manifest.get("sha256"):
        raise CatalogueError("raw response evidence integrity mismatch")
    return manifest, decode(body) if manifest["status"] == 200 else None


class RateLimiter:
    """Start spacing of one slot per 1/max_rate seconds, shared by every traversal of a client."""

    def __init__(self, max_rate: float):
        self.interval = 1.0 / max_rate
        self._lock = threading.Lock()
        self._next_start = 0.0

    def acquire(self, budget: Budget) -> None:
        with self._lock:
            current = time.monotonic()
            start = max(current, self._next_start)
            self._next_start = start + self.interval
        wait = start - current
        if wait > 0:
            if wait >= budget.remaining:
                raise BudgetExceeded("deadline cannot accommodate request spacing")
            time.sleep(wait)

    def defer(self, seconds: float) -> None:
        """Hold every traversal back, for example after a Retry-After from the source."""
        with self._lock:
            self._next_start = max(self._next_start, time.monotonic() + seconds)


def retry_after_seconds(value: str | None, *, at: datetime | None = None) -> float | None:
    """Parse Retry-After as delta-seconds or an HTTP-date; malformed values are ignored."""
    if value is None or not value.strip():
        return None
    text = value.strip()
    try:
        seconds = float(text)
    except ValueError:
        try:
            when = email.utils.parsedate_to_datetime(text)
        except (TypeError, ValueError, IndexError):
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        seconds = (when - (at or datetime.now(UTC))).total_seconds()
    if not math.isfinite(seconds):
        return None
    return max(0.0, seconds)


def backoff_seconds(attempt: int, retry_after: float | None) -> float:
    """Equal-jitter exponential backoff (1s, 2s, 4s ... capped), never shorter than Retry-After."""
    ceiling = min(MAX_BACKOFF_SECONDS, 2.0**attempt)
    jittered = random.uniform(ceiling / 2, ceiling)
    return max(jittered, retry_after or 0.0)


class PublicClient:
    def __init__(self, store: Store, budget: Budget):
        self.store = store
        self.budget = budget
        self.base_url = source_url()
        self.limiter = RateLimiter(store.limits.max_rate)
        self.client = httpx.Client(
            trust_env=False,
            follow_redirects=False,
            headers={"Accept": "application/json", "Accept-Encoding": "identity"},
            limits=httpx.Limits(
                max_connections=MAX_CONCURRENT_REQUESTS,
                max_keepalive_connections=MAX_CONCURRENT_REQUESTS,
            ),
        )

    def __enter__(self) -> PublicClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self.client.close()

    def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
    ) -> str:
        """Fetch one endpoint and return the committed raw receipt path (relative to the root)."""
        if (
            not endpoint.startswith("/")
            or ".." in endpoint.split("/")
            or "?" in endpoint
            or "#" in endpoint
        ):
            raise CatalogueError("invalid source endpoint")
        for attempt in range(MAX_ATTEMPTS):
            last = attempt == MAX_ATTEMPTS - 1
            self.limiter.acquire(self.budget)
            allowance = self.budget.reserve()
            attempt_id = uuid.uuid4().hex
            relative = f"raw/{attempt_id}/manifest.json"
            reservation = {
                **(context or {}),
                "page_id": attempt_id,
                "version": 1,
                "endpoint": endpoint,
                "params": params or {},
                "received_at": now(),
                "status": None,
                "reserved_bytes": allowance,
                "accounting_complete": False,
            }
            body = bytearray()
            streamed = 0
            status: int | None = None
            retry_after: str | None = None
            try:
                self.store.write(relative, reservation)
                with self.client.stream(
                    "GET",
                    self.base_url + endpoint,
                    params=params,
                    timeout=min(30, self.budget.remaining),
                ) as response:
                    status = response.status_code
                    retry_after = response.headers.get("retry-after")
                    if response.headers.get("content-encoding", "identity") not in {
                        "identity",
                        "",
                    }:
                        raise CatalogueError("compressed response is not permitted")
                    declared = response.headers.get("content-length")
                    if declared and int(declared) > allowance:
                        raise CatalogueError("response exceeds byte ceiling")
                    for chunk in response.iter_raw():
                        _ = self.budget.remaining
                        streamed += len(chunk)
                        downloaded = self.budget.add_download(len(chunk))
                        if (
                            len(body) + len(chunk) > allowance
                            or downloaded > self.budget.limits.max_download_bytes
                        ):
                            raise CatalogueError("stream exceeded response/download byte ceiling")
                        body.extend(chunk)
                payload = bytes(body)
                self.store.write_bytes(f"raw/{attempt_id}/body.bin", payload)
                receipt = {
                    **reservation,
                    "status": status,
                    "size": len(payload),
                    "sha256": digest(payload),
                    "received_at": now(),
                    "downloaded_bytes": streamed,
                    "retry_after": retry_after,
                    "accounting_complete": True,
                }
                if status in RETRYABLE_STATUSES and not last:
                    delay = backoff_seconds(attempt, retry_after_seconds(retry_after))
                    receipt["retry_delay_seconds"] = round(delay, 3)
                    self.store.write(relative, receipt)
                    self._pause(delay)
                    continue
                self.store.write(relative, receipt)
                if status not in ACCEPTED_STATUSES:
                    raise CatalogueError(f"source request failed with HTTP {status}")
                return relative
            except (httpx.HTTPError, ValueError) as exc:
                delay = None if last else backoff_seconds(attempt, None)
                self._record_failure(
                    relative,
                    reservation,
                    status=status,
                    size=len(body),
                    downloaded=streamed,
                    extra={"error": type(exc).__name__},
                    delay=delay,
                )
                if last:
                    raise CatalogueError("bounded public source request failed") from exc
                self._pause(delay or 0.0)
            except CatalogueError as exc:
                label = (
                    "budget exhausted" if isinstance(exc, BudgetExceeded) else "rejected response"
                )
                self._record_failure(
                    relative,
                    reservation,
                    status=status,
                    size=len(body),
                    downloaded=streamed,
                    extra={"error": label},
                    delay=None,
                )
                raise
            finally:
                self.budget.settle(allowance)
        raise CatalogueError("source retries exhausted")

    def _record_failure(
        self,
        relative: str,
        reservation: dict[str, Any],
        *,
        status: int | None,
        size: int,
        downloaded: int,
        extra: dict[str, Any],
        delay: float | None,
    ) -> None:
        receipt = {
            **reservation,
            "status": status,
            "size": size,
            "accounting_complete": True,
            "downloaded_bytes": downloaded,
            **extra,
        }
        if delay is not None:
            receipt["retry_delay_seconds"] = round(delay, 3)
        self.store.write(relative, receipt)

    def _pause(self, delay: float) -> None:
        if delay >= self.budget.remaining:
            raise BudgetExceeded("deadline cannot accommodate retry")
        self.limiter.defer(delay)
        time.sleep(delay)


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
