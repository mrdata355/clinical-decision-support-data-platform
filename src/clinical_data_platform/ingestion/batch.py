"""Bounded batch ingestion primitives."""
from __future__ import annotations

import dataclasses
import datetime as dt
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Any, Protocol

UTC = dt.UTC


@dataclasses.dataclass(frozen=True, slots=True)
class SourceWindow:
    low: dt.datetime
    high: dt.datetime

    def __post_init__(self) -> None:
        if self.low.tzinfo is None or self.high.tzinfo is None:
            raise ValueError("source window must be timezone aware")
        if self.high <= self.low:
            raise ValueError("high watermark must be greater than low watermark")


@dataclasses.dataclass(frozen=True, slots=True)
class IngestBatch:
    run_id: str
    source_name: str
    window: SourceWindow
    records: tuple[Mapping[str, Any], ...]

    @property
    def count(self) -> int:
        return len(self.records)


class WatermarkStore(Protocol):
    def read(self, source_name: str) -> dt.datetime | None: ...
    def commit(self, source_name: str, value: dt.datetime, run_id: str) -> None: ...


class BoundedSource(Protocol):
    name: str
    def high_watermark(self) -> dt.datetime: ...
    def read(self, window: SourceWindow) -> Iterable[Mapping[str, Any]]: ...


class InMemoryWatermarkStore:
    def __init__(self) -> None:
        self._values: dict[str, tuple[dt.datetime, str]] = {}

    def read(self, source_name: str) -> dt.datetime | None:
        current = self._values.get(source_name)
        return current[0] if current else None

    def commit(self, source_name: str, value: dt.datetime, run_id: str) -> None:
        prior = self._values.get(source_name)
        if prior and value < prior[0]:
            raise ValueError("watermark regression is not allowed")
        self._values[source_name] = (value, run_id)


class BatchPlanner:
    """Captures the upper watermark before extraction.

    A pipeline retry reuses the same closed-open interval. This prevents the data set from
    changing while the batch is in flight and makes source-to-target reconciliation repeatable.
    """

    def __init__(self, watermark_store: WatermarkStore) -> None:
        self.watermark_store = watermark_store

    def plan(self, source: BoundedSource, *, bootstrap: dt.datetime) -> SourceWindow:
        low = self.watermark_store.read(source.name) or bootstrap
        high = source.high_watermark()
        return SourceWindow(low=low.astimezone(UTC), high=high.astimezone(UTC))

    def extract(self, source: BoundedSource, window: SourceWindow) -> IngestBatch:
        return IngestBatch(
            run_id=str(uuid.uuid4()),
            source_name=source.name,
            window=window,
            records=tuple(source.read(window)),
        )

    def commit(self, batch: IngestBatch) -> None:
        self.watermark_store.commit(batch.source_name, batch.window.high, batch.run_id)


def chunked(records: Sequence[Mapping[str, Any]], size: int) -> Iterator[Sequence[Mapping[str, Any]]]:
    if size <= 0:
        raise ValueError("size must be positive")
    for offset in range(0, len(records), size):
        yield records[offset : offset + size]


def map_records(
    records: Iterable[Mapping[str, Any]],
    transform: Callable[[Mapping[str, Any]], Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    return [transform(record) for record in records]
