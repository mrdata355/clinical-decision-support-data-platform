"""Resilient HTTP extraction utilities.

The implementation handles pagination, retryable failures, request correlation, bounded
updated-at filters, cursor-cycle detection, and source-side rate limits. It keeps transport
concerns independent from domain normalization so each API can reuse the same reliability
behavior while supplying only field mappings and endpoint configuration.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import random
import time
from collections.abc import Iterator, Mapping
from typing import Any
from urllib.parse import urljoin

import httpx

UTC = dt.UTC


class HttpExtractionError(RuntimeError):
    pass


class RetryableHttpError(HttpExtractionError):
    pass


class FatalHttpError(HttpExtractionError):
    pass


@dataclasses.dataclass(frozen=True, slots=True)
class RetryPolicy:
    attempts: int = 5
    initial_delay_seconds: float = 0.5
    maximum_delay_seconds: float = 20.0
    jitter_seconds: float = 0.5

    def delay(self, attempt: int) -> float:
        base = min(self.maximum_delay_seconds, self.initial_delay_seconds * (2 ** max(attempt - 1, 0)))
        return base + random.random() * self.jitter_seconds


@dataclasses.dataclass(frozen=True, slots=True)
class HttpSourceSpec:
    base_url: str
    resource_path: str
    items_field: str = "data"
    cursor_field: str = "next_cursor"
    cursor_param: str = "cursor"
    updated_from_param: str = "updated_from"
    updated_before_param: str = "updated_before"
    page_size_param: str = "limit"
    page_size: int = 500
    timeout_seconds: float = 30.0
    max_pages: int = 20_000


@dataclasses.dataclass(frozen=True, slots=True)
class HttpPage:
    page_number: int
    cursor_in: str | None
    cursor_out: str | None
    item_count: int
    items: tuple[Mapping[str, Any], ...]
    request_id: str | None
    elapsed_seconds: float


class HttpExtractor:
    def __init__(
        self,
        spec: HttpSourceSpec,
        *,
        headers: Mapping[str, str] | None = None,
        retry_policy: RetryPolicy | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        self.spec = spec
        self.headers = dict(headers or {})
        self.retry_policy = retry_policy or RetryPolicy()
        self.client = client or httpx.Client(timeout=spec.timeout_seconds)

    def _url(self) -> str:
        return urljoin(self.spec.base_url.rstrip("/") + "/", self.spec.resource_path.lstrip("/"))

    def _request(self, params: Mapping[str, Any]) -> httpx.Response:
        last_error: Exception | None = None
        for attempt in range(1, self.retry_policy.attempts + 1):
            try:
                response = self.client.get(self._url(), params=params, headers=self.headers)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                last_error = exc
                if attempt == self.retry_policy.attempts:
                    break
                time.sleep(self.retry_policy.delay(attempt))
                continue

            if response.status_code in {408, 425, 429, 500, 502, 503, 504}:
                last_error = RetryableHttpError(f"retryable status {response.status_code}")
                if attempt == self.retry_policy.attempts:
                    break
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = min(float(retry_after), self.retry_policy.maximum_delay_seconds)
                    except ValueError:
                        delay = self.retry_policy.delay(attempt)
                else:
                    delay = self.retry_policy.delay(attempt)
                time.sleep(delay)
                continue

            if response.status_code >= 400:
                raise FatalHttpError(
                    f"source returned status={response.status_code} body={response.text[:1000]}"
                )
            return response

        raise RetryableHttpError(f"request failed after retries: {last_error}")

    @staticmethod
    def _extract_cursor(body: Any, field: str) -> str | None:
        if isinstance(body, Mapping):
            direct = body.get(field)
            if direct not in (None, ""):
                return str(direct)
            meta = body.get("meta")
            if isinstance(meta, Mapping):
                nested = meta.get(field)
                if nested not in (None, ""):
                    return str(nested)
        return None

    @staticmethod
    def _extract_items(body: Any, field: str) -> list[Mapping[str, Any]]:
        if isinstance(body, list):
            return [item for item in body if isinstance(item, Mapping)]
        if isinstance(body, Mapping):
            value = body.get(field)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, Mapping)]
        raise FatalHttpError(f"response does not contain a list at field={field}")

    def pages(self, *, start: dt.datetime, end: dt.datetime) -> Iterator[HttpPage]:
        if start.tzinfo is None or end.tzinfo is None:
            raise ValueError("window timestamps must be timezone-aware")
        if end <= start:
            raise ValueError("end must be after start")

        cursor: str | None = None
        seen_cursors: set[str] = set()
        for page_number in range(1, self.spec.max_pages + 1):
            params: dict[str, Any] = {
                self.spec.updated_from_param: start.astimezone(UTC).isoformat(),
                self.spec.updated_before_param: end.astimezone(UTC).isoformat(),
                self.spec.page_size_param: self.spec.page_size,
            }
            if cursor:
                params[self.spec.cursor_param] = cursor

            started = time.monotonic()
            response = self._request(params)
            elapsed = time.monotonic() - started
            body = response.json()
            items = self._extract_items(body, self.spec.items_field)
            next_cursor = self._extract_cursor(body, self.spec.cursor_field)

            yield HttpPage(
                page_number=page_number,
                cursor_in=cursor,
                cursor_out=next_cursor,
                item_count=len(items),
                items=tuple(items),
                request_id=response.headers.get("x-request-id") or response.headers.get("request-id"),
                elapsed_seconds=elapsed,
            )

            if not next_cursor:
                break
            if next_cursor in seen_cursors:
                raise FatalHttpError(f"pagination cursor cycle detected: {next_cursor}")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        else:
            raise FatalHttpError("maximum page count exceeded")

    def extract(self, *, start: dt.datetime, end: dt.datetime) -> Iterator[Mapping[str, Any]]:
        for page in self.pages(start=start, end=end):
            yield from page.items
