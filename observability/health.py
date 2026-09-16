"""Operational health, SLO and continuous-query evaluation.

Health is evaluated across batch pipelines, streaming/continuous-query processors, serving
models and ML scoring services. The module is intentionally transport-neutral: the same
reports can be written to Snowflake OPS tables, emitted as structured logs, or rendered in
the Vercel evidence console.
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
    max_p95_lag_seconds: float | None = None
    max_restart_count_24h: int | None = None

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
    p95_lag_seconds: float | None = None
    restart_count_24h: int = 0

    @property
    def duration_minutes(self) -> float | None:
        if self.completed_at is None:
            return None
        return (self.completed_at - self.started_at).total_seconds() / 60

    @property
    def error_rate(self) -> float:
        return self.quarantined_count / self.source_count if self.source_count else 0.0


@dataclasses.dataclass(frozen=True, slots=True)
class ContinuousQueryState:
    query_name: str
    status: str
    last_checkpoint_at: dt.datetime | None
    last_input_at: dt.datetime | None
    last_output_at: dt.datetime | None
    input_rows: int
    output_rows: int
    p95_processing_lag_seconds: float
    restart_count_24h: int = 0
    backlog_rows: int = 0
    checkpoint_age_seconds: float | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class ModelServiceState:
    model_name: str
    model_version: str
    status: str
    request_count: int
    p95_latency_ms: float
    error_rate: float
    fallback_rate: float
    feature_null_rate: float
    feature_drift_score: float | None = None
    prediction_drift_score: float | None = None
    last_scored_at: dt.datetime | None = None


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
    component_type: str = "pipeline"

    def as_dict(self) -> dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "component_type": self.component_type,
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
    """Rolling successful-run baselines for volume and duration."""

    def __init__(self, max_runs: int = 28) -> None:
        self.values: dict[str, deque[int]] = defaultdict(lambda: deque(maxlen=max_runs))
        self.durations: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=max_runs))

    def add(self, pipeline_name: str, source_count: int, duration_minutes: float | None = None) -> None:
        if source_count >= 0:
            self.values[pipeline_name].append(source_count)
        if duration_minutes is not None and duration_minutes >= 0:
            self.durations[pipeline_name].append(duration_minutes)

    def median(self, pipeline_name: str) -> float | None:
        values = self.values.get(pipeline_name)
        return statistics.median(values) if values else None

    def duration_p95(self, pipeline_name: str) -> float | None:
        values = sorted(self.durations.get(pipeline_name, []))
        if not values:
            return None
        index = max(0, min(len(values) - 1, int(round(0.95 * (len(values) - 1)))))
        return values[index]

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
        rank = {HealthStatus.UNKNOWN: 0, HealthStatus.HEALTHY: 1, HealthStatus.DEGRADED: 2, HealthStatus.UNHEALTHY: 3}
        return max((signal.status for signal in signals), key=lambda value: rank[value]) if signals else HealthStatus.UNKNOWN

    def evaluate(self, *, slo: PipelineSLO, latest_run: PipelineRun | None, now: dt.datetime | None = None) -> HealthReport:
        now = now or dt.datetime.now(UTC)
        if latest_run is None:
            return HealthReport(slo.pipeline_name, now, HealthStatus.UNKNOWN, [HealthSignal("latest_run", HealthStatus.UNKNOWN, None, "run required", "no pipeline run available")])
        signals: list[HealthSignal] = []
        signals.extend(self._status_signal(latest_run))
        signals.extend(self._freshness_signal(slo, latest_run, now))
        signals.extend(self._duration_signal(slo, latest_run))
        signals.extend(self._error_rate_signal(slo, latest_run))
        signals.extend(self._volume_signal(slo, latest_run))
        signals.extend(self._reconciliation_signal(slo, latest_run))
        signals.extend(self._lag_signal(slo, latest_run))
        signals.extend(self._restart_signal(slo, latest_run))
        if latest_run.status.upper() == "SUCCESS":
            self.baseline.add(latest_run.pipeline_name, latest_run.source_count, latest_run.duration_minutes)
        return HealthReport(slo.pipeline_name, now, self._worst_status(signals), signals, latest_run.run_id)

    def evaluate_continuous_query(
        self,
        state: ContinuousQueryState,
        *,
        max_checkpoint_age_seconds: float = 180,
        max_lag_seconds: float = 120,
        max_backlog_rows: int = 100_000,
        max_restarts_24h: int = 5,
        now: dt.datetime | None = None,
    ) -> HealthReport:
        now = now or dt.datetime.now(UTC)
        status_value = state.status.upper()
        signals = [
            HealthSignal("query_status", HealthStatus.HEALTHY if status_value in {"RUNNING", "HEALTHY"} else HealthStatus.UNHEALTHY, status_value, "RUNNING", "continuous query runtime state"),
            HealthSignal("p95_processing_lag_seconds", HealthStatus.HEALTHY if state.p95_processing_lag_seconds <= max_lag_seconds else HealthStatus.UNHEALTHY, state.p95_processing_lag_seconds, max_lag_seconds, "event-to-serving processing lag"),
            HealthSignal("backlog_rows", HealthStatus.HEALTHY if state.backlog_rows <= max_backlog_rows else HealthStatus.DEGRADED, state.backlog_rows, max_backlog_rows, "unconsumed rows waiting for processing"),
            HealthSignal("restart_count_24h", HealthStatus.HEALTHY if state.restart_count_24h <= max_restarts_24h else HealthStatus.DEGRADED, state.restart_count_24h, max_restarts_24h, "query restarts in the trailing 24 hours"),
        ]
        checkpoint_age = state.checkpoint_age_seconds
        if checkpoint_age is None and state.last_checkpoint_at is not None:
            checkpoint_age = (now - state.last_checkpoint_at.astimezone(UTC)).total_seconds()
        signals.append(HealthSignal("checkpoint_age_seconds", HealthStatus.UNKNOWN if checkpoint_age is None else HealthStatus.HEALTHY if checkpoint_age <= max_checkpoint_age_seconds else HealthStatus.UNHEALTHY, checkpoint_age, max_checkpoint_age_seconds, "age of last committed continuous-query checkpoint"))
        return HealthReport(state.query_name, now, self._worst_status(signals), signals, component_type="continuous_query")

    def evaluate_model_service(
        self,
        state: ModelServiceState,
        *,
        max_p95_latency_ms: float = 250,
        max_error_rate: float = 0.01,
        max_fallback_rate: float = 0.10,
        max_feature_null_rate: float = 0.01,
        warn_drift_score: float = 0.15,
        fail_drift_score: float = 0.30,
        now: dt.datetime | None = None,
    ) -> HealthReport:
        now = now or dt.datetime.now(UTC)
        signals = [
            HealthSignal("model_status", HealthStatus.HEALTHY if state.status.upper() in {"READY", "HEALTHY", "SERVING"} else HealthStatus.UNHEALTHY, state.status, "SERVING", "model serving state"),
            HealthSignal("p95_latency_ms", HealthStatus.HEALTHY if state.p95_latency_ms <= max_p95_latency_ms else HealthStatus.DEGRADED, state.p95_latency_ms, max_p95_latency_ms, "online scoring latency"),
            HealthSignal("error_rate", HealthStatus.HEALTHY if state.error_rate <= max_error_rate else HealthStatus.UNHEALTHY, state.error_rate, max_error_rate, "failed scoring fraction"),
            HealthSignal("fallback_rate", HealthStatus.HEALTHY if state.fallback_rate <= max_fallback_rate else HealthStatus.DEGRADED, state.fallback_rate, max_fallback_rate, "fraction served by deterministic fallback"),
            HealthSignal("feature_null_rate", HealthStatus.HEALTHY if state.feature_null_rate <= max_feature_null_rate else HealthStatus.UNHEALTHY, state.feature_null_rate, max_feature_null_rate, "null rate across required online features"),
        ]
        for name, value in (("feature_drift_score", state.feature_drift_score), ("prediction_drift_score", state.prediction_drift_score)):
            status = HealthStatus.UNKNOWN if value is None else HealthStatus.UNHEALTHY if value > fail_drift_score else HealthStatus.DEGRADED if value > warn_drift_score else HealthStatus.HEALTHY
            signals.append(HealthSignal(name, status, value, {"warn": warn_drift_score, "fail": fail_drift_score}, "population drift score"))
        return HealthReport(f"{state.model_name}:{state.model_version}", now, self._worst_status(signals), signals, component_type="model_service")

    def _status_signal(self, run: PipelineRun) -> list[HealthSignal]:
        status = run.status.upper()
        health = HealthStatus.HEALTHY if status == "SUCCESS" else HealthStatus.DEGRADED if status in {"RUNNING", "RETRYING"} else HealthStatus.UNHEALTHY
        return [HealthSignal("run_status", health, status, "SUCCESS", f"latest pipeline state is {status}")]

    def _freshness_signal(self, slo: PipelineSLO, run: PipelineRun, now: dt.datetime) -> list[HealthSignal]:
        reference = run.watermark_high or run.completed_at
        if reference is None:
            return [HealthSignal("freshness", HealthStatus.UNKNOWN, None, slo.max_freshness_minutes, "no watermark or completion timestamp available")]
        freshness = (now - reference.astimezone(UTC)).total_seconds() / 60
        return [HealthSignal("freshness_minutes", HealthStatus.HEALTHY if freshness <= slo.max_freshness_minutes else HealthStatus.UNHEALTHY, round(freshness, 2), slo.max_freshness_minutes, "time since latest published source watermark")]

    def _duration_signal(self, slo: PipelineSLO, run: PipelineRun) -> list[HealthSignal]:
        duration = run.duration_minutes
        if duration is None:
            return [HealthSignal("duration_minutes", HealthStatus.DEGRADED, None, slo.max_duration_minutes, "run has not completed")]
        return [HealthSignal("duration_minutes", HealthStatus.HEALTHY if duration <= slo.max_duration_minutes else HealthStatus.DEGRADED, round(duration, 2), slo.max_duration_minutes, "pipeline elapsed time")]

    def _error_rate_signal(self, slo: PipelineSLO, run: PipelineRun) -> list[HealthSignal]:
        rate = run.error_rate
        return [HealthSignal("quarantine_rate", HealthStatus.HEALTHY if rate <= slo.max_error_rate else HealthStatus.UNHEALTHY, round(rate, 6), slo.max_error_rate, "fraction of source rows routed to quarantine")]

    def _volume_signal(self, slo: PipelineSLO, run: PipelineRun) -> list[HealthSignal]:
        ratio = self.baseline.ratio(run.pipeline_name, run.source_count)
        if ratio is None:
            return [HealthSignal("volume_ratio", HealthStatus.UNKNOWN, None, [slo.min_volume_ratio, slo.max_volume_ratio], "insufficient successful-run history for baseline")]
        status = HealthStatus.HEALTHY if slo.min_volume_ratio <= ratio <= slo.max_volume_ratio else HealthStatus.UNHEALTHY if ratio < slo.min_volume_ratio * 0.5 or ratio > slo.max_volume_ratio * 2 else HealthStatus.DEGRADED
        return [HealthSignal("volume_ratio", status, round(ratio, 4), [slo.min_volume_ratio, slo.max_volume_ratio], "latest source volume divided by rolling successful-run median")]

    def _reconciliation_signal(self, slo: PipelineSLO, run: PipelineRun) -> list[HealthSignal]:
        if not slo.require_reconciliation:
            return []
        return [HealthSignal("reconciliation_failures", HealthStatus.HEALTHY if run.reconciliation_failed == 0 else HealthStatus.UNHEALTHY, run.reconciliation_failed, 0, "failed source-to-target controls")]

    def _lag_signal(self, slo: PipelineSLO, run: PipelineRun) -> list[HealthSignal]:
        if slo.max_p95_lag_seconds is None:
            return []
        if run.p95_lag_seconds is None:
            return [HealthSignal("p95_lag_seconds", HealthStatus.UNKNOWN, None, slo.max_p95_lag_seconds, "processing lag is unavailable")]
        return [HealthSignal("p95_lag_seconds", HealthStatus.HEALTHY if run.p95_lag_seconds <= slo.max_p95_lag_seconds else HealthStatus.UNHEALTHY, run.p95_lag_seconds, slo.max_p95_lag_seconds, "pipeline p95 processing lag")]

    def _restart_signal(self, slo: PipelineSLO, run: PipelineRun) -> list[HealthSignal]:
        if slo.max_restart_count_24h is None:
            return []
        return [HealthSignal("restart_count_24h", HealthStatus.HEALTHY if run.restart_count_24h <= slo.max_restart_count_24h else HealthStatus.DEGRADED, run.restart_count_24h, slo.max_restart_count_24h, "processor restarts in trailing 24 hours")]


DEFAULT_SLOS = {
    "product_events": PipelineSLO("product_events", 20, 10, 0.01, 0.4, 2.5, True, 120, 5),
    "clinical_tool_catalog": PipelineSLO("clinical_tool_catalog", 24 * 60, 30, 0.001, 0.8, 1.2),
    "content_catalog": PipelineSLO("content_catalog", 24 * 60, 30, 0.001, 0.7, 1.3),
    "quality_rating_catalog": PipelineSLO("quality_rating_catalog", 24 * 60, 30, 0.001, 0.8, 1.2),
    "ehr_integration_events": PipelineSLO("ehr_integration_events", 15, 10, 0.005, 0.3, 3.0, True, 60, 5),
    "business_systems": PipelineSLO("business_systems", 180, 45, 0.01, 0.3, 3.0),
}


def summarize_reports(reports: Iterable[HealthReport]) -> dict[str, Any]:
    reports = list(reports)
    counts = Counter(report.status.value for report in reports)
    unhealthy = [r.pipeline_name for r in reports if r.status == HealthStatus.UNHEALTHY]
    degraded = [r.pipeline_name for r in reports if r.status == HealthStatus.DEGRADED]
    unknown = [r.pipeline_name for r in reports if r.status == HealthStatus.UNKNOWN]
    return {
        "evaluated_component_count": len(reports),
        "status_counts": dict(counts),
        "unhealthy_components": unhealthy,
        "degraded_components": degraded,
        "unknown_components": unknown,
        "overall_status": HealthStatus.UNHEALTHY.value if unhealthy else HealthStatus.DEGRADED.value if degraded else HealthStatus.UNKNOWN.value if unknown and not reports else HealthStatus.HEALTHY.value if reports else HealthStatus.UNKNOWN.value,
    }
