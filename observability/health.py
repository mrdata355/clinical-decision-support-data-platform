"""Operational health evaluation for data products and pipelines.

The module evaluates freshness, duration, volume, error rate, and reconciliation health with
explicit SLO thresholds. It is transport-neutral so the same policy can drive logs, dashboards,
warehouse health marts, and paging integrations.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import statistics
from collections import Counter, defaultdict, deque
from collections.abc import Iterable, Mapping, Sequence
from enum import StrEnum
from typing import Any

UTC = dt.UTC


class HealthStatus(StrEnum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    UNHEALTHY = "UNHEALTHY"
    UNKNOWN = "UNKNOWN"


@dataclasses.dataclass(frozen=True, slots=True)
class PipelineSLO:
    pipeline_name: str
    max_freshness_minutes: float
    max_duration_minutes: float
    max_error_rate: float
    min_volume_ratio: float = 0.5
    max_volume_ratio: float = 2.0
    require_reconciliation: bool = True

    def __post_init__(self) -> None:
        if self.max_freshness_minutes <= 0 or self.max_duration_minutes <= 0:
            raise ValueError("time SLOs must be positive")
        if not 0 <= self.max_error_rate <= 1:
            raise ValueError("max_error_rate must be in [0,1]")
        if self.min_volume_ratio <= 0 or self.max_volume_ratio <= 0:
            raise ValueError("volume ratios must be positive")


@dataclasses.dataclass(frozen=True, slots=True)
class PipelineRun:
    run_id: str
    pipeline_name: str
    started_at: dt.datetime
    completed_at: dt.datetime | None
    status: str
    source_count: int
    quarantined_count: int = 0
    reconciliation_failed: int = 0
    watermark_high: dt.datetime | None = None

    @property
    def duration_minutes(self) -> float | None:
        if self.completed_at is None:
            return None
        return (self.completed_at - self.started_at).total_seconds() / 60

    @property
    def error_rate(self) -> float:
        return self.quarantined_count / self.source_count if self.source_count else 0.0


@dataclasses.dataclass(frozen=True, slots=True)
class HealthSignal:
    signal: str
    status: HealthStatus
    observed: Any
    threshold: Any
    message: str


@dataclasses.dataclass(slots=True)
class HealthReport:
    pipeline_name: str
    evaluated_at: dt.datetime
    status: HealthStatus
    signals: list[HealthSignal]
    latest_run_id: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "evaluated_at": self.evaluated_at.isoformat(),
            "status": self.status.value,
            "latest_run_id": self.latest_run_id,
            "signals": [
                {
                    "signal": signal.signal,
                    "status": signal.status.value,
                    "observed": signal.observed,
                    "threshold": signal.threshold,
                    "message": signal.message,
                }
                for signal in self.signals
            ],
        }


class BaselineWindow:
    """Rolling volume baseline using recent successful runs."""

    def __init__(self, max_runs: int = 28) -> None:
        self.values: dict[str, deque[int]] = defaultdict(lambda: deque(maxlen=max_runs))

    def add(self, pipeline_name: str, source_count: int) -> None:
        if source_count >= 0:
            self.values[pipeline_name].append(source_count)

    def median(self, pipeline_name: str) -> float | None:
        values = self.values.get(pipeline_name)
        return statistics.median(values) if values else None

    def ratio(self, pipeline_name: str, source_count: int) -> float | None:
        baseline = self.median(pipeline_name)
        if baseline in (None, 0):
            return None
        return source_count / baseline


class HealthEvaluator:
    def __init__(self, baseline: BaselineWindow | None = None) -> None:
        self.baseline = baseline or BaselineWindow()

    @staticmethod
    def _worst_status(signals: Sequence[HealthSignal]) -> HealthStatus:
        rank = {
            HealthStatus.UNKNOWN: 0,
            HealthStatus.HEALTHY: 1,
            HealthStatus.DEGRADED: 2,
            HealthStatus.UNHEALTHY: 3,
        }
        if not signals:
            return HealthStatus.UNKNOWN
        return max((signal.status for signal in signals), key=lambda value: rank[value])

    def evaluate(
        self,
        *,
        slo: PipelineSLO,
        latest_run: PipelineRun | None,
        now: dt.datetime | None = None,
    ) -> HealthReport:
        now = now or dt.datetime.now(UTC)
        if latest_run is None:
            return HealthReport(
                pipeline_name=slo.pipeline_name,
                evaluated_at=now,
                status=HealthStatus.UNKNOWN,
                signals=[
                    HealthSignal(
                        signal="latest_run",
                        status=HealthStatus.UNKNOWN,
                        observed=None,
                        threshold="run required",
                        message="no pipeline run available",
                    )
                ],
            )

        signals: list[HealthSignal] = []
        signals.extend(self._status_signal(latest_run))
        signals.extend(self._freshness_signal(slo, latest_run, now))
        signals.extend(self._duration_signal(slo, latest_run))
        signals.extend(self._error_rate_signal(slo, latest_run))
        signals.extend(self._volume_signal(slo, latest_run))
        signals.extend(self._reconciliation_signal(slo, latest_run))

        if latest_run.status.upper() == "SUCCESS":
            self.baseline.add(latest_run.pipeline_name, latest_run.source_count)

        return HealthReport(
            pipeline_name=slo.pipeline_name,
            evaluated_at=now,
            status=self._worst_status(signals),
            signals=signals,
            latest_run_id=latest_run.run_id,
        )

    def _status_signal(self, run: PipelineRun) -> list[HealthSignal]:
        status = run.status.upper()
        if status == "SUCCESS":
            health = HealthStatus.HEALTHY
        elif status in {"RUNNING", "RETRYING"}:
            health = HealthStatus.DEGRADED
        else:
            health = HealthStatus.UNHEALTHY
        return [
            HealthSignal(
                signal="run_status",
                status=health,
                observed=status,
                threshold="SUCCESS",
                message=f"latest pipeline state is {status}",
            )
        ]

    def _freshness_signal(
        self,
        slo: PipelineSLO,
        run: PipelineRun,
        now: dt.datetime,
    ) -> list[HealthSignal]:
        reference = run.watermark_high or run.completed_at
        if reference is None:
            return [
                HealthSignal(
                    signal="freshness",
                    status=HealthStatus.UNKNOWN,
                    observed=None,
                    threshold=slo.max_freshness_minutes,
                    message="no watermark or completion timestamp available",
                )
            ]
        freshness = (now - reference.astimezone(UTC)).total_seconds() / 60
        status = (
            HealthStatus.HEALTHY
            if freshness <= slo.max_freshness_minutes
            else HealthStatus.UNHEALTHY
        )
        return [
            HealthSignal(
                signal="freshness_minutes",
                status=status,
                observed=round(freshness, 2),
                threshold=slo.max_freshness_minutes,
                message="time since latest published source watermark",
            )
        ]

    def _duration_signal(self, slo: PipelineSLO, run: PipelineRun) -> list[HealthSignal]:
        duration = run.duration_minutes
        if duration is None:
            return [
                HealthSignal(
                    signal="duration_minutes",
                    status=HealthStatus.DEGRADED,
                    observed=None,
                    threshold=slo.max_duration_minutes,
                    message="run has not completed",
                )
            ]
        status = (
            HealthStatus.HEALTHY
            if duration <= slo.max_duration_minutes
            else HealthStatus.DEGRADED
        )
        return [
            HealthSignal(
                signal="duration_minutes",
                status=status,
                observed=round(duration, 2),
                threshold=slo.max_duration_minutes,
                message="pipeline elapsed time",
            )
        ]

    def _error_rate_signal(self, slo: PipelineSLO, run: PipelineRun) -> list[HealthSignal]:
        rate = run.error_rate
        status = HealthStatus.HEALTHY if rate <= slo.max_error_rate else HealthStatus.UNHEALTHY
        return [
            HealthSignal(
                signal="quarantine_rate",
                status=status,
                observed=round(rate, 6),
                threshold=slo.max_error_rate,
                message="fraction of source rows routed to quarantine",
            )
        ]

    def _volume_signal(self, slo: PipelineSLO, run: PipelineRun) -> list[HealthSignal]:
        ratio = self.baseline.ratio(run.pipeline_name, run.source_count)
        if ratio is None:
            return [
                HealthSignal(
                    signal="volume_ratio",
                    status=HealthStatus.UNKNOWN,
                    observed=None,
                    threshold=[slo.min_volume_ratio, slo.max_volume_ratio],
                    message="insufficient successful-run history for baseline",
                )
            ]
        if slo.min_volume_ratio <= ratio <= slo.max_volume_ratio:
            status = HealthStatus.HEALTHY
        elif ratio < slo.min_volume_ratio * 0.5 or ratio > slo.max_volume_ratio * 2:
            status = HealthStatus.UNHEALTHY
        else:
            status = HealthStatus.DEGRADED
        return [
            HealthSignal(
                signal="volume_ratio",
                status=status,
                observed=round(ratio, 4),
                threshold=[slo.min_volume_ratio, slo.max_volume_ratio],
                message="latest source volume divided by rolling successful-run median",
            )
        ]

    def _reconciliation_signal(self, slo: PipelineSLO, run: PipelineRun) -> list[HealthSignal]:
        if not slo.require_reconciliation:
            return []
        status = (
            HealthStatus.HEALTHY
            if run.reconciliation_failed == 0
            else HealthStatus.UNHEALTHY
        )
        return [
            HealthSignal(
                signal="reconciliation_failures",
                status=status,
                observed=run.reconciliation_failed,
                threshold=0,
                message="failed source-to-target controls",
            )
        ]


DEFAULT_SLOS = {
    "product_events": PipelineSLO(
        pipeline_name="product_events",
        max_freshness_minutes=30,
        max_duration_minutes=20,
        max_error_rate=0.01,
        min_volume_ratio=0.4,
        max_volume_ratio=2.5,
    ),
    "clinical_tool_catalog": PipelineSLO(
        pipeline_name="clinical_tool_catalog",
        max_freshness_minutes=24 * 60,
        max_duration_minutes=30,
        max_error_rate=0.001,
        min_volume_ratio=0.8,
        max_volume_ratio=1.2,
    ),
    "content_catalog": PipelineSLO(
        pipeline_name="content_catalog",
        max_freshness_minutes=24 * 60,
        max_duration_minutes=30,
        max_error_rate=0.001,
        min_volume_ratio=0.7,
        max_volume_ratio=1.3,
    ),
    "business_systems": PipelineSLO(
        pipeline_name="business_systems",
        max_freshness_minutes=180,
        max_duration_minutes=45,
        max_error_rate=0.01,
        min_volume_ratio=0.3,
        max_volume_ratio=3.0,
    ),
}


def summarize_reports(reports: Iterable[HealthReport]) -> dict[str, Any]:
    reports = list(reports)
    counts = Counter(report.status.value for report in reports)
    unhealthy = [report.pipeline_name for report in reports if report.status == HealthStatus.UNHEALTHY]
    degraded = [report.pipeline_name for report in reports if report.status == HealthStatus.DEGRADED]
    return {
        "evaluated_pipeline_count": len(reports),
        "status_counts": dict(counts),
        "unhealthy_pipelines": unhealthy,
        "degraded_pipelines": degraded,
        "overall_status": (
            HealthStatus.UNHEALTHY.value
            if unhealthy
            else HealthStatus.DEGRADED.value
            if degraded
            else HealthStatus.HEALTHY.value
            if reports
            else HealthStatus.UNKNOWN.value
        ),
    }
