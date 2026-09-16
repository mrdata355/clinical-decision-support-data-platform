"""Structured metrics emitted by ingestion and transformation services."""
from __future__ import annotations

import contextlib
import dataclasses
import datetime as dt
import time
from collections import Counter, defaultdict
from collections.abc import Iterator, Mapping
from typing import Any

UTC = dt.UTC


@dataclasses.dataclass(slots=True)
class MetricPoint:
    name: str
    value: float
    timestamp: dt.datetime
    labels: dict[str, str]
    metric_type: str = "gauge"


@dataclasses.dataclass(slots=True)
class Span:
    name: str
    started_at: dt.datetime
    started_monotonic: float
    labels: dict[str, str]
    completed_at: dt.datetime | None = None
    duration_seconds: float | None = None
    status: str = "RUNNING"
    error: str | None = None


class MetricRegistry:
    def __init__(self) -> None:
        self.points: list[MetricPoint] = []
        self.counters: Counter[tuple[str, tuple[tuple[str, str], ...]]] = Counter()
        self.gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self.spans: list[Span] = []

    @staticmethod
    def _key(name: str, labels: Mapping[str, str] | None) -> tuple[str, tuple[tuple[str, str], ...]]:
        return name, tuple(sorted((labels or {}).items()))

    def increment(self, name: str, value: int = 1, labels: Mapping[str, str] | None = None) -> None:
        key = self._key(name, labels)
        self.counters[key] += value
        self.points.append(MetricPoint(name, float(value), dt.datetime.now(UTC), dict(labels or {}), "counter"))

    def gauge(self, name: str, value: float, labels: Mapping[str, str] | None = None) -> None:
        key = self._key(name, labels)
        self.gauges[key] = value
        self.points.append(MetricPoint(name, value, dt.datetime.now(UTC), dict(labels or {}), "gauge"))

    def timing(self, name: str, seconds: float, labels: Mapping[str, str] | None = None) -> None:
        self.points.append(MetricPoint(name, seconds, dt.datetime.now(UTC), dict(labels or {}), "timing"))

    @contextlib.contextmanager
    def span(self, name: str, labels: Mapping[str, str] | None = None) -> Iterator[Span]:
        span = Span(
            name=name,
            started_at=dt.datetime.now(UTC),
            started_monotonic=time.monotonic(),
            labels=dict(labels or {}),
        )
        self.spans.append(span)
        try:
            yield span
        except Exception as exc:
            span.status = "ERROR"
            span.error = f"{exc.__class__.__name__}: {exc}"
            raise
        else:
            span.status = "OK"
        finally:
            span.completed_at = dt.datetime.now(UTC)
            span.duration_seconds = time.monotonic() - span.started_monotonic
            self.timing(f"{name}.duration_seconds", span.duration_seconds, span.labels)

    def snapshot(self) -> dict[str, Any]:
        return {
            "counters": [
                {"name": name, "labels": dict(labels), "value": value}
                for (name, labels), value in sorted(self.counters.items())
            ],
            "gauges": [
                {"name": name, "labels": dict(labels), "value": value}
                for (name, labels), value in sorted(self.gauges.items())
            ],
            "spans": [
                {
                    "name": span.name,
                    "started_at": span.started_at.isoformat(),
                    "completed_at": span.completed_at.isoformat() if span.completed_at else None,
                    "duration_seconds": span.duration_seconds,
                    "status": span.status,
                    "error": span.error,
                    "labels": span.labels,
                }
                for span in self.spans
            ],
        }


GLOBAL_METRICS = MetricRegistry()
