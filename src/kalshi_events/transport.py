"""Public, rate-limited HTTP with durable per-attempt evidence and bounded streaming."""

from __future__ import annotations

import email.utils
import hashlib
import math
import os
import random
import threading
import time
import uuid
import zlib
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlsplit

import httpx

from kalshi_events import metrics
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


def _decode_gzip(body: bytes, limit: int, check_deadline: Any = None) -> tuple[bytes, str]:
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    output = bytearray()
    checksum = hashlib.sha256()
    metrics.add("hashes")
    try:
        with metrics.phase("decompression"):
            for offset in range(0, len(body), 64 * 1024):
                pending = body[offset : offset + 64 * 1024]
                while pending:
                    if check_deadline is not None:
                        check_deadline()
                    remaining = limit - len(output)
                    decoded = decoder.decompress(pending, remaining + 1)
                    if check_deadline is not None:
                        check_deadline()
                    if len(decoded) > remaining:
                        raise CatalogueError("decoded response exceeds byte ceiling")
                    output.extend(decoded)
                    with metrics.phase("decoded_hashing"):
                        checksum.update(decoded)
                    metrics.add("hash_bytes", len(decoded))
                    if decoder.unused_data:
                        raise CatalogueError("gzip response has trailing data or multiple members")
                    pending = decoder.unconsumed_tail
                    if not pending:
                        break
            if not decoder.eof:
                raise CatalogueError("truncated gzip response")
    except zlib.error as exc:
        raise CatalogueError("invalid gzip response") from exc
    return bytes(output), checksum.hexdigest()


def _checked_size(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CatalogueError(f"invalid raw evidence {name}")
    return value


def _checked_digest(value: Any, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(c not in "0123456789abcdef" for c in value)
    ):
        raise CatalogueError(f"invalid raw evidence {name}")
    return value


def _encoded_digest(body: bytes) -> str:
    with metrics.phase("encoded_hashing"):
        return digest(body)


def read_page(
    store: Store,
    relative: str,
    limit: int | None = None,
    decoded_limit: int | None = None,
    *,
    decode_json: bool = True,
    decode_endpoints: set[str] | None = None,
    inventory: dict[str, str] | None = None,
) -> tuple[dict[str, Any], Any]:
    """Verify one raw receipt. Limits may be the capture's sealed response ceilings."""
    limit = store.limits.max_response_bytes if limit is None else limit
    decoded_limit = (
        store.limits.max_decoded_response_bytes if decoded_limit is None else decoded_limit
    )
    manifest_body = regular_bytes(store.path(relative), 1024**2)
    manifest = decode(manifest_body)
    store.check_deadline()
    if (
        not isinstance(manifest, dict)
        or isinstance(manifest.get("version"), bool)
        or manifest.get("version") not in {1, 2}
    ):
        raise CatalogueError("unsupported raw evidence manifest")
    status = manifest.get("status")
    if isinstance(status, bool) or not isinstance(status, int):
        raise CatalogueError("invalid raw evidence status")
    body_path = store.path((PurePosixPath(relative).parent / "body.bin").as_posix())
    body = regular_bytes(body_path, limit)
    metrics.add("raw_body_reads")
    metrics.add("raw_body_read_bytes", len(body))
    version = manifest["version"]
    actual_encoded_sha: str
    if version == 1:
        actual_encoded_sha = _encoded_digest(body)
        if len(body) != _checked_size(
            manifest.get("size"), "size"
        ) or actual_encoded_sha != _checked_digest(manifest.get("sha256"), "sha256"):
            raise CatalogueError("raw response evidence integrity mismatch")
        decoded = body
    else:
        if manifest.get("content_encoding") != "gzip":
            raise CatalogueError("unsupported raw evidence content encoding")
        encoded_size = _checked_size(manifest.get("encoded_size"), "encoded size")
        decoded_size = _checked_size(manifest.get("decoded_size"), "decoded size")
        encoded_sha = _checked_digest(manifest.get("encoded_sha256"), "encoded checksum")
        decoded_sha = _checked_digest(manifest.get("decoded_sha256"), "decoded checksum")
        if (
            len(body) != encoded_size
            or len(body) != _checked_size(manifest.get("size"), "size")
            or (actual_encoded_sha := _encoded_digest(body)) != encoded_sha
            or manifest.get("sha256") != encoded_sha
        ):
            raise CatalogueError("raw response evidence integrity mismatch")
        decoded, actual_decoded_sha = _decode_gzip(body, decoded_limit, store.check_deadline)
        if len(decoded) != decoded_size or actual_decoded_sha != decoded_sha:
            raise CatalogueError("decoded response evidence integrity mismatch")
    if len(decoded) > decoded_limit:
        raise CatalogueError("decoded response exceeds byte ceiling")
    if version == 1 and len(body) > decoded_limit:
        raise CatalogueError("decoded response exceeds byte ceiling")
    if inventory is not None:
        inventory[relative] = digest(manifest_body)
        inventory[body_path.relative_to(store.root).as_posix()] = actual_encoded_sha
    should_decode = (
        status == 200
        and decode_json
        and (decode_endpoints is None or manifest.get("endpoint") in decode_endpoints)
    )
    if should_decode:
        metrics.add("raw_body_json_decodes")
    store.check_deadline()
    payload = decode(decoded) if should_decode else None
    store.check_deadline()
    return manifest, payload


class RateLimiter:
    """Start spacing of one slot per 1/max_rate seconds, shared by every traversal of a client."""

    def __init__(self, max_rate: float):
        self.interval = 1.0 / max_rate
        self._lock = threading.Lock()
        self._next_start = 0.0

    def acquire(self, budget: Budget) -> None:
        while True:
            _ = budget.remaining
            with self._lock:
                current = time.monotonic()
                start = max(current, self._next_start)
                if start <= current:
                    self._next_start = current + self.interval
                    return
                wait = start - current
            if wait >= budget.remaining:
                raise BudgetExceeded("deadline cannot accommodate request spacing")
            with metrics.phase("rate_wait"):
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
    def __init__(self, store: Store, budget: Budget, *, compression: str = "identity"):
        if compression not in {"identity", "gzip"}:
            raise CatalogueError("compression must be identity or gzip")
        self.store = store
        self.budget = budget
        self.compression = compression
        self.base_url = source_url()
        self.limiter = RateLimiter(store.limits.max_rate)
        self.client = httpx.Client(
            trust_env=False,
            follow_redirects=False,
            headers={"Accept": "application/json", "Accept-Encoding": compression},
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
            spool = None
            streamed = 0
            decoded_size = 0
            encoded_hash = hashlib.sha256()
            decoded_hash = hashlib.sha256()
            decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
            status: int | None = None
            retry_after: str | None = None
            committed: dict[str, Any] | None = None
            observation = metrics.endpoint_attempt(endpoint, self.compression)
            telemetry = observation.__enter__()
            try:
                self.store.write(relative, reservation)
                metrics.add("requests")
                spool = self.store.spool(f"raw/{attempt_id}/body.bin")
                with (
                    metrics.phase("request"),
                    metrics.http_request(),
                    ExitStack() as stack,
                ):
                    request = self.client.stream(
                        "GET",
                        self.base_url + endpoint,
                        params=params,
                        timeout=min(30, self.budget.remaining),
                    )
                    with metrics.phase("http_header_wait"):
                        response = stack.enter_context(request)
                    status = response.status_code
                    retry_after = response.headers.get("retry-after")
                    encoding = response.headers.get("content-encoding", "identity").strip().lower()
                    telemetry.response(status, encoding)
                    if encoding not in {"identity", "gzip"}:
                        raise CatalogueError("unsupported response content encoding")
                    if encoding == "gzip" and self.compression != "gzip":
                        raise CatalogueError("source returned unrequested gzip compression")
                    declared = response.headers.get("content-length")
                    if declared is not None:
                        if not declared.isdigit():
                            raise CatalogueError("invalid response content length")
                        if int(declared) > allowance:
                            raise self._ceiling_error(allowance)
                    iterator = iter(response.iter_raw())
                    while True:
                        with metrics.phase("response_iterator_wait"):
                            try:
                                chunk = next(iterator)
                            except StopIteration:
                                break
                        streamed += len(chunk)
                        downloaded = self.budget.add_download(len(chunk))
                        metrics.add("encoded_bytes", len(chunk))
                        telemetry.add_bytes(encoded=len(chunk))
                        _ = self.budget.remaining
                        if downloaded > self.budget.limits.max_download_bytes:
                            raise BudgetExceeded("download allowance exhausted")
                        if streamed > allowance:
                            raise self._ceiling_error(allowance)
                        spool.write(chunk, self.budget.consume_storage)
                        with metrics.phase("encoded_hashing"):
                            encoded_hash.update(chunk)
                        metrics.add("hash_bytes", len(chunk))
                        if encoding == "gzip":
                            decoded_chunk_size = self._consume_gzip(
                                decoder,
                                chunk,
                                decoded_hash,
                                decoded_size,
                                telemetry,
                            )
                            decoded_size += decoded_chunk_size
                            _ = self.budget.remaining
                        else:
                            decoded_size += len(chunk)
                            telemetry.add_bytes(decoded=len(chunk))
                            if decoded_size > self.budget.max_decoded_response_bytes:
                                raise CatalogueError("decoded response exceeds byte ceiling")
                telemetry.finish()
                if encoding == "gzip" and not decoder.eof:
                    raise CatalogueError("truncated gzip response")
                _ = self.budget.remaining
                spool.commit()
                payload_size = streamed
                metrics.add("hashes", 1 if encoding == "identity" else 2)
                encoded_checksum = encoded_hash.hexdigest()
                decoded_checksum = (
                    encoded_checksum if encoding == "identity" else decoded_hash.hexdigest()
                )
                receipt = {
                    **reservation,
                    "status": status,
                    "size": payload_size,
                    "sha256": encoded_checksum,
                    "received_at": now(),
                    "downloaded_bytes": streamed,
                    "retry_after": retry_after,
                    "accounting_complete": True,
                }
                if encoding == "gzip":
                    receipt.update(
                        version=2,
                        content_encoding="gzip",
                        encoded_size=payload_size,
                        encoded_sha256=encoded_checksum,
                        decoded_size=decoded_size,
                        decoded_sha256=decoded_checksum,
                    )
                metrics.add("decoded_bytes", decoded_size)
                if status in RETRYABLE_STATUSES and not last:
                    delay = backoff_seconds(attempt, retry_after_seconds(retry_after))
                    receipt["retry_delay_seconds"] = round(delay, 3)
                    self.store.write(relative, receipt)
                    committed = receipt
                    self._pause(delay)
                    continue
                self.store.write(relative, receipt)
                committed = receipt
                if status not in ACCEPTED_STATUSES:
                    raise CatalogueError(f"source request failed with HTTP {status}")
                return relative
            except (httpx.HTTPError, ValueError) as exc:
                telemetry.finish()
                delay = None if last else backoff_seconds(attempt, None)
                self._record_failure(
                    relative,
                    reservation,
                    status=status,
                    size=streamed,
                    downloaded=streamed,
                    extra={"error": type(exc).__name__},
                    delay=delay,
                )
                if last:
                    raise CatalogueError("bounded public source request failed") from exc
                self._pause(delay or 0.0)
            except CatalogueError as exc:
                telemetry.finish()
                label = (
                    "budget exhausted" if isinstance(exc, BudgetExceeded) else "rejected response"
                )
                if committed is not None:
                    # Response already hashed: keep its evidence and retry fields.
                    self.store.write(relative, {**committed, "error": label})
                else:
                    self._record_failure(
                        relative,
                        reservation,
                        status=status,
                        size=streamed,
                        downloaded=streamed,
                        extra={"error": label},
                        delay=None,
                    )
                raise
            finally:
                observation.__exit__(None, None, None)
                if spool is not None:
                    spool.abort()
                self.budget.settle(allowance)

    def _consume_gzip(
        self,
        decoder: Any,
        chunk: bytes,
        checksum: Any,
        current_size: int,
        telemetry: Any = None,
    ) -> int:
        pending = chunk
        emitted = 0
        while pending:
            remaining = self.budget.max_decoded_response_bytes - current_size - emitted
            try:
                with metrics.phase("decompression"):
                    decoded = decoder.decompress(pending, remaining + 1)
            except zlib.error as exc:
                raise CatalogueError("invalid gzip response") from exc
            emitted += len(decoded)
            if telemetry is not None:
                telemetry.add_bytes(decoded=len(decoded))
            if emitted > self.budget.max_decoded_response_bytes - current_size:
                raise CatalogueError("decoded response exceeds byte ceiling")
            with metrics.phase("decoded_hashing"):
                checksum.update(decoded)
            metrics.add("hash_bytes", len(decoded))
            if decoder.unused_data:
                raise CatalogueError("gzip response has trailing data or multiple members")
            pending = decoder.unconsumed_tail
        return emitted

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

    def _ceiling_error(self, allowance: int) -> CatalogueError:
        """Past the remaining download allowance is a budget stop; past the sealed ceiling is not."""
        if allowance < self.budget.max_response_bytes:
            return BudgetExceeded("download allowance exhausted")
        return CatalogueError("response exceeds byte ceiling")

    def _pause(self, delay: float) -> None:
        if delay >= self.budget.remaining:
            raise BudgetExceeded("deadline cannot accommodate retry")
        self.limiter.defer(delay)
        with metrics.phase("retry_wait"):
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
