"""Source connector patterns for application, content, SaaS, and integration systems.

Connectors implement bounded reads, source-watermark capture, pagination, retry, stable
record identity, and source audit metadata. Secrets are never embedded in source definitions;
callers provide credentials through environment-specific secret managers.
"""
from __future__ import annotations

import abc
import csv
import dataclasses
import datetime as dt
import hashlib
import io
import json
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urljoin

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

UTC = dt.UTC


class SourceConnectorError(RuntimeError):
    pass


class TransientSourceError(SourceConnectorError):
    pass


class PermanentSourceError(SourceConnectorError):
    pass


@dataclasses.dataclass(frozen=True, slots=True)
class ExtractionWindow:
    start: dt.datetime
    end: dt.datetime

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("timestamps must be timezone-aware")
        if self.end <= self.start:
            raise ValueError("end must be after start")


@dataclasses.dataclass(frozen=True, slots=True)
class SourceRecord:
    source_system: str
    source_object: str
    source_key: str
    source_updated_at: dt.datetime
    payload: Mapping[str, Any]
    extracted_at: dt.datetime
    source_cursor: str | None = None
    source_partition: str | None = None
    source_file: str | None = None
    source_row_number: int | None = None

    @property
    def payload_hash(self) -> str:
        canonical = json.dumps(self.payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class Connector(abc.ABC):
    source_system: str
    source_object: str

    @abc.abstractmethod
    def high_watermark(self) -> dt.datetime:
        raise NotImplementedError

    @abc.abstractmethod
    def extract(self, window: ExtractionWindow) -> Iterable[SourceRecord]:
        raise NotImplementedError


class TokenProvider(Protocol):
    def get_token(self) -> str: ...


@dataclasses.dataclass(slots=True)
class StaticTokenProvider:
    token: str

    def get_token(self) -> str:
        return self.token


@dataclasses.dataclass(slots=True)
class ApiConnectorConfig:
    base_url: str
    resource_path: str
    updated_at_param: str = "updated_at"
    page_param: str = "page"
    page_size_param: str = "per_page"
    page_size: int = 500
    timeout_seconds: float = 30.0
    max_pages: int = 20_000
    response_items_path: tuple[str, ...] = ("data",)
    response_next_cursor_path: tuple[str, ...] = ("meta", "next_cursor")
    cursor_param: str = "cursor"


class ApiConnector(Connector):
    """Cursor-aware HTTP extractor.

    The upper source watermark is captured before extraction and reused for every page.
    Pagination therefore cannot expand the logical batch window while the export is running.
    """

    def __init__(
        self,
        *,
        source_system: str,
        source_object: str,
        config: ApiConnectorConfig,
        token_provider: TokenProvider,
        key_field: str,
        updated_at_field: str,
        client: httpx.Client | None = None,
    ) -> None:
        self.source_system = source_system
        self.source_object = source_object
        self.config = config
        self.token_provider = token_provider
        self.key_field = key_field
        self.updated_at_field = updated_at_field
        self.client = client or httpx.Client(timeout=config.timeout_seconds)

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token_provider.get_token()}",
            "Accept": "application/json",
            "User-Agent": "clinical-data-platform/1.0",
        }

    @staticmethod
    def _dig(document: Mapping[str, Any], path: Sequence[str]) -> Any:
        value: Any = document
        for part in path:
            if not isinstance(value, Mapping):
                return None
            value = value.get(part)
        return value

    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError, TransientSourceError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=1, max=30),
        reraise=True,
    )
    def _get(self, path: str, params: Mapping[str, Any] | None = None) -> httpx.Response:
        url = urljoin(self.config.base_url.rstrip("/") + "/", path.lstrip("/"))
        response = self.client.get(url, params=params, headers=self._headers())
        if response.status_code == 429 or response.status_code >= 500:
            raise TransientSourceError(f"transient upstream status={response.status_code}")
        if response.status_code >= 400:
            raise PermanentSourceError(
                f"permanent upstream status={response.status_code} body={response.text[:500]}"
            )
        return response

    def high_watermark(self) -> dt.datetime:
        response = self._get("/health/time")
        payload = response.json()
        candidate = payload.get("current_time") or payload.get("server_time")
        if candidate:
            return dt.datetime.fromisoformat(str(candidate).replace("Z", "+00:00")).astimezone(UTC)
        return dt.datetime.now(UTC)

    def extract(self, window: ExtractionWindow) -> Iterator[SourceRecord]:
        cursor: str | None = None
        seen_cursors: set[str] = set()
        extracted_at = dt.datetime.now(UTC)
        for page_number in range(1, self.config.max_pages + 1):
            params: dict[str, Any] = {
                "updated_from": window.start.isoformat(),
                "updated_before": window.end.isoformat(),
                self.config.page_size_param: self.config.page_size,
            }
            if cursor:
                params[self.config.cursor_param] = cursor
            else:
                params[self.config.page_param] = page_number

            response = self._get(self.config.resource_path, params=params)
            body = response.json()
            items = self._dig(body, self.config.response_items_path)
            if items is None:
                items = body if isinstance(body, list) else []
            if not isinstance(items, list):
                raise PermanentSourceError("API items payload must be a list")

            for item in items:
                if not isinstance(item, Mapping):
                    continue
                key = item.get(self.key_field)
                updated = item.get(self.updated_at_field)
                if key in (None, "") or updated in (None, ""):
                    raise PermanentSourceError(
                        f"source contract missing key/update fields: key={self.key_field} updated={self.updated_at_field}"
                    )
                updated_at = dt.datetime.fromisoformat(str(updated).replace("Z", "+00:00")).astimezone(UTC)
                if not (window.start <= updated_at < window.end):
                    continue
                yield SourceRecord(
                    source_system=self.source_system,
                    source_object=self.source_object,
                    source_key=str(key),
                    source_updated_at=updated_at,
                    payload=dict(item),
                    extracted_at=extracted_at,
                    source_cursor=cursor,
                )

            next_cursor_raw = self._dig(body, self.config.response_next_cursor_path)
            next_cursor = str(next_cursor_raw) if next_cursor_raw not in (None, "") else None
            if next_cursor:
                if next_cursor in seen_cursors:
                    raise PermanentSourceError(f"pagination cursor cycle detected: {next_cursor}")
                seen_cursors.add(next_cursor)
                cursor = next_cursor
                continue

            if len(items) < self.config.page_size:
                break
        else:
            raise PermanentSourceError("maximum page count reached before source exhaustion")


@dataclasses.dataclass(slots=True)
class JsonLinesConnector(Connector):
    source_system: str
    source_object: str
    paths: Sequence[Path]
    key_field: str
    updated_at_field: str

    def high_watermark(self) -> dt.datetime:
        latest = [
            dt.datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
            for path in self.paths
            if path.exists()
        ]
        return max(latest, default=dt.datetime.now(UTC)) + dt.timedelta(microseconds=1)

    def extract(self, window: ExtractionWindow) -> Iterator[SourceRecord]:
        extracted_at = dt.datetime.now(UTC)
        for path in sorted(self.paths):
            if not path.exists():
                continue
            with path.open("r", encoding="utf-8") as handle:
                for row_number, line in enumerate(handle, start=1):
                    if not line.strip():
                        continue
                    payload = json.loads(line)
                    updated_at = dt.datetime.fromisoformat(
                        str(payload[self.updated_at_field]).replace("Z", "+00:00")
                    ).astimezone(UTC)
                    if not (window.start <= updated_at < window.end):
                        continue
                    yield SourceRecord(
                        source_system=self.source_system,
                        source_object=self.source_object,
                        source_key=str(payload[self.key_field]),
                        source_updated_at=updated_at,
                        payload=payload,
                        extracted_at=extracted_at,
                        source_file=str(path),
                        source_row_number=row_number,
                    )


@dataclasses.dataclass(slots=True)
class CsvConnector(Connector):
    source_system: str
    source_object: str
    paths: Sequence[Path]
    key_field: str
    updated_at_field: str
    delimiter: str = ","

    def high_watermark(self) -> dt.datetime:
        mtimes = [path.stat().st_mtime for path in self.paths if path.exists()]
        return dt.datetime.fromtimestamp(max(mtimes), tz=UTC) + dt.timedelta(microseconds=1) if mtimes else dt.datetime.now(UTC)

    def extract(self, window: ExtractionWindow) -> Iterator[SourceRecord]:
        extracted_at = dt.datetime.now(UTC)
        for path in sorted(self.paths):
            if not path.exists():
                continue
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle, delimiter=self.delimiter)
                for row_number, payload in enumerate(reader, start=2):
                    updated_raw = payload.get(self.updated_at_field)
                    key = payload.get(self.key_field)
                    if not updated_raw or not key:
                        raise PermanentSourceError(
                            f"required field missing in {path}:{row_number}"
                        )
                    updated_at = dt.datetime.fromisoformat(updated_raw.replace("Z", "+00:00")).astimezone(UTC)
                    if not (window.start <= updated_at < window.end):
                        continue
                    yield SourceRecord(
                        source_system=self.source_system,
                        source_object=self.source_object,
                        source_key=key,
                        source_updated_at=updated_at,
                        payload=dict(payload),
                        extracted_at=extracted_at,
                        source_file=str(path),
                        source_row_number=row_number,
                    )


class CursorStore(Protocol):
    def get(self, source_system: str, source_object: str) -> dt.datetime | None: ...
    def commit(self, source_system: str, source_object: str, value: dt.datetime, run_id: str) -> None: ...


class MemoryCursorStore:
    def __init__(self) -> None:
        self.values: dict[tuple[str, str], tuple[dt.datetime, str]] = {}

    def get(self, source_system: str, source_object: str) -> dt.datetime | None:
        item = self.values.get((source_system, source_object))
        return item[0] if item else None

    def commit(self, source_system: str, source_object: str, value: dt.datetime, run_id: str) -> None:
        key = (source_system, source_object)
        current = self.values.get(key)
        if current and value < current[0]:
            raise ValueError("watermark cannot move backwards")
        self.values[key] = (value, run_id)


@dataclasses.dataclass(slots=True)
class ExtractionResult:
    source_system: str
    source_object: str
    window: ExtractionWindow
    records: list[SourceRecord]
    duplicate_keys: list[str]

    @property
    def count(self) -> int:
        return len(self.records)

    @property
    def unique_key_count(self) -> int:
        return len({record.source_key for record in self.records})

    def audit(self) -> dict[str, Any]:
        return {
            "source_system": self.source_system,
            "source_object": self.source_object,
            "window_start": self.window.start.isoformat(),
            "window_end": self.window.end.isoformat(),
            "record_count": self.count,
            "unique_key_count": self.unique_key_count,
            "duplicate_keys": self.duplicate_keys,
        }


class ExtractionRunner:
    """Coordinates the high-watermark pattern without committing progress early."""

    def __init__(self, cursor_store: CursorStore) -> None:
        self.cursor_store = cursor_store

    def run(
        self,
        connector: Connector,
        *,
        initial_start: dt.datetime,
    ) -> ExtractionResult:
        lower = self.cursor_store.get(connector.source_system, connector.source_object) or initial_start
        upper = connector.high_watermark()
        window = ExtractionWindow(start=lower.astimezone(UTC), end=upper.astimezone(UTC))
        records = list(connector.extract(window))

        counts: dict[str, int] = {}
        duplicate_keys: list[str] = []
        for record in records:
            counts[record.source_key] = counts.get(record.source_key, 0) + 1
        for key, count in counts.items():
            if count > 1:
                duplicate_keys.append(key)

        return ExtractionResult(
            source_system=connector.source_system,
            source_object=connector.source_object,
            window=window,
            records=records,
            duplicate_keys=sorted(duplicate_keys),
        )

    def commit(self, result: ExtractionResult, *, run_id: str) -> None:
        # Commit is a separate action so callers can wait for RAW persistence, transformation,
        # quality gates, reconciliation, and serving publication before advancing the source.
        self.cursor_store.commit(
            result.source_system,
            result.source_object,
            result.window.end,
            run_id,
        )


def partition_records(records: Iterable[SourceRecord], max_records: int) -> Iterator[list[SourceRecord]]:
    if max_records <= 0:
        raise ValueError("max_records must be positive")
    batch: list[SourceRecord] = []
    for record in records:
        batch.append(record)
        if len(batch) >= max_records:
            yield batch
            batch = []
    if batch:
        yield batch


def to_raw_rows(records: Iterable[SourceRecord], run_id: str) -> Iterator[dict[str, Any]]:
    for record in records:
        yield {
            "ingest_run_id": run_id,
            "source_system": record.source_system,
            "source_object": record.source_object,
            "source_key": record.source_key,
            "source_updated_at": record.source_updated_at.isoformat(),
            "ingested_at": record.extracted_at.isoformat(),
            "source_cursor": record.source_cursor,
            "source_partition": record.source_partition,
            "source_file": record.source_file,
            "source_row_number": record.source_row_number,
            "payload_hash": record.payload_hash,
            "payload": dict(record.payload),
        }
