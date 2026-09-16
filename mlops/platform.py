"""Production-style MLOps control plane for product/operations models.

This module is intentionally framework-agnostic. It defines the contracts that real training
jobs, feature pipelines, registries, and serving systems must satisfy while keeping the portfolio
reference implementation runnable without heavyweight ML dependencies.

The governed model set optimizes tool discovery, search routing, integration confidence,
content engagement, and platform reliability. It does not make diagnoses or treatment decisions.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
import math
import statistics
import uuid
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from enum import StrEnum
from typing import Any, Protocol

UTC = dt.UTC

TOUCHPOINTS = (
    "dbt/models/core/fct_product_event.sql",
    "dbt/models/marts/product/fct_recommendation_performance_daily.sql",
    "dbt/models/marts/product/fct_search_funnel_daily.sql",
    "dbt/models/marts/product/fct_integration_health_daily.sql",
    "observability/health.py",
    "reconciliation/reconcile.py",
    "snowflake/streams_tasks/incremental_processing.sql",
    "site/index.html",
    "api/telemetry.js",
)


class ModelStage(StrEnum):
    CANDIDATE = "candidate"
    SHADOW = "shadow"
    CANARY = "canary"
    PRODUCTION = "production"
    RETIRED = "retired"


class GateStatus(StrEnum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


@dataclasses.dataclass(frozen=True, slots=True)
class DatasetSnapshot:
    dataset_name: str
    snapshot_id: str
    as_of: dt.datetime
    row_count: int
    min_event_ts: dt.datetime | None
    max_event_ts: dt.datetime | None
    feature_schema_hash: str
    source_model_versions: Mapping[str, str]
    query_hash: str

    def __post_init__(self) -> None:
        if self.as_of.tzinfo is None:
            raise ValueError("as_of must be timezone-aware")
        if self.row_count < 0:
            raise ValueError("row_count cannot be negative")


@dataclasses.dataclass(frozen=True, slots=True)
class FeatureSpec:
    name: str
    dtype: str
    source_model: str
    expression: str
    point_in_time_column: str | None = None
    ttl_hours: int | None = None
    contains_direct_identifier: bool = False
    contains_phi_payload: bool = False


@dataclasses.dataclass(frozen=True, slots=True)
class ModelArtifact:
    model_name: str
    version: str
    stage: ModelStage
    created_at: dt.datetime
    git_sha: str
    dataset_snapshot_id: str
    feature_schema_hash: str
    algorithm: str
    metrics: Mapping[str, float]
    artifact_uri: str
    preprocessing_uri: str
    training_manifest_uri: str
    model_card_uri: str


@dataclasses.dataclass(frozen=True, slots=True)
class PromotionGate:
    metric: str
    comparator: str
    threshold: float
    severity: GateStatus = GateStatus.FAIL

    def evaluate(self, metrics: Mapping[str, float]) -> GateStatus:
        value = metrics.get(self.metric)
        if value is None or math.isnan(value):
            return GateStatus.FAIL
        if self.comparator == ">=":
            passed = value >= self.threshold
        elif self.comparator == "<=":
            passed = value <= self.threshold
        elif self.comparator == ">":
            passed = value > self.threshold
        elif self.comparator == "<":
            passed = value < self.threshold
        else:
            raise ValueError(f"unsupported comparator {self.comparator}")
        return GateStatus.PASS if passed else self.severity


@dataclasses.dataclass(slots=True)
class EvaluationReport:
    model_name: str
    version: str
    evaluated_at: dt.datetime
    metrics: dict[str, float]
    gates: dict[str, GateStatus]
    segment_metrics: dict[str, dict[str, float]]

    @property
    def passed(self) -> bool:
        return not any(status == GateStatus.FAIL for status in self.gates.values())

    def as_dict(self) -> dict[str, Any]:
        return {
            "model_name": self.model_name,
            "version": self.version,
            "evaluated_at": self.evaluated_at.isoformat(),
            "metrics": self.metrics,
            "gates": {name: status.value for name, status in self.gates.items()},
            "segment_metrics": self.segment_metrics,
            "passed": self.passed,
        }


class Registry(Protocol):
    def register(self, artifact: ModelArtifact) -> None: ...
    def latest(self, model_name: str, stage: ModelStage | None = None) -> ModelArtifact | None: ...
    def promote(self, model_name: str, version: str, stage: ModelStage) -> ModelArtifact: ...


class InMemoryRegistry:
    """Deterministic reference registry used by tests and the generated demo."""

    def __init__(self) -> None:
        self._artifacts: dict[tuple[str, str], ModelArtifact] = {}

    def register(self, artifact: ModelArtifact) -> None:
        key = (artifact.model_name, artifact.version)
        if key in self._artifacts:
            raise ValueError(f"artifact already registered: {key}")
        self._artifacts[key] = artifact

    def latest(self, model_name: str, stage: ModelStage | None = None) -> ModelArtifact | None:
        candidates = [a for a in self._artifacts.values() if a.model_name == model_name]
        if stage is not None:
            candidates = [a for a in candidates if a.stage == stage]
        return max(candidates, key=lambda a: a.created_at, default=None)

    def promote(self, model_name: str, version: str, stage: ModelStage) -> ModelArtifact:
        key = (model_name, version)
        artifact = self._artifacts[key]
        promoted = dataclasses.replace(artifact, stage=stage)
        self._artifacts[key] = promoted
        return promoted


@dataclasses.dataclass(frozen=True, slots=True)
class TrainingManifest:
    run_id: str
    model_name: str
    algorithm: str
    dataset: DatasetSnapshot
    feature_specs: tuple[FeatureSpec, ...]
    train_window: tuple[dt.datetime, dt.datetime]
    validation_window: tuple[dt.datetime, dt.datetime]
    test_window: tuple[dt.datetime, dt.datetime]
    label_definition: str
    git_sha: str
    parameters: Mapping[str, Any]
    created_at: dt.datetime

    def fingerprint(self) -> str:
        payload = {
            "model_name": self.model_name,
            "algorithm": self.algorithm,
            "dataset_snapshot_id": self.dataset.snapshot_id,
            "feature_schema_hash": self.dataset.feature_schema_hash,
            "train_window": [value.isoformat() for value in self.train_window],
            "validation_window": [value.isoformat() for value in self.validation_window],
            "test_window": [value.isoformat() for value in self.test_window],
            "label_definition": self.label_definition,
            "git_sha": self.git_sha,
            "parameters": dict(sorted(self.parameters.items())),
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def validate_feature_boundary(features: Sequence[FeatureSpec]) -> None:
    violations = [
        feature.name
        for feature in features
        if feature.contains_direct_identifier or feature.contains_phi_payload
    ]
    if violations:
        raise ValueError(f"disallowed feature boundary: {violations}")


def validate_time_split(
    train: tuple[dt.datetime, dt.datetime],
    validation: tuple[dt.datetime, dt.datetime],
    test: tuple[dt.datetime, dt.datetime],
    *,
    leakage_gap: dt.timedelta = dt.timedelta(hours=1),
) -> None:
    for start, end in (train, validation, test):
        if start.tzinfo is None or end.tzinfo is None or end <= start:
            raise ValueError("all split windows must be timezone-aware and increasing")
    if train[1] + leakage_gap > validation[0]:
        raise ValueError("train/validation leakage gap violated")
    if validation[1] + leakage_gap > test[0]:
        raise ValueError("validation/test leakage gap violated")


def population_stability_index(expected: Sequence[float], actual: Sequence[float], bins: int = 10) -> float:
    if len(expected) < bins or len(actual) < bins:
        return 0.0
    ordered = sorted(expected)
    cuts = [ordered[min(len(ordered) - 1, int(len(ordered) * i / bins))] for i in range(1, bins)]

    def bucketize(values: Sequence[float]) -> list[int]:
        counts = [0] * bins
        for value in values:
            idx = 0
            while idx < len(cuts) and value > cuts[idx]:
                idx += 1
            counts[idx] += 1
        return counts

    e = bucketize(expected)
    a = bucketize(actual)
    result = 0.0
    for expected_count, actual_count in zip(e, a, strict=True):
        expected_ratio = max(expected_count / len(expected), 1e-6)
        actual_ratio = max(actual_count / len(actual), 1e-6)
        result += (actual_ratio - expected_ratio) * math.log(actual_ratio / expected_ratio)
    return result


def unseen_category_rate(reference: Iterable[str], current: Iterable[str]) -> float:
    reference_set = set(reference)
    current_values = list(current)
    if not current_values:
        return 0.0
    return sum(value not in reference_set for value in current_values) / len(current_values)


@dataclasses.dataclass(slots=True)
class DriftSignal:
    feature_name: str
    metric: str
    value: float
    status: GateStatus


class DriftMonitor:
    def __init__(self, warning: float = 0.15, critical: float = 0.25) -> None:
        if not 0 <= warning <= critical:
            raise ValueError("drift thresholds are invalid")
        self.warning = warning
        self.critical = critical

    def _status(self, value: float) -> GateStatus:
        if value >= self.critical:
            return GateStatus.FAIL
        if value >= self.warning:
            return GateStatus.WARN
        return GateStatus.PASS

    def numeric(self, feature_name: str, reference: Sequence[float], current: Sequence[float]) -> DriftSignal:
        value = population_stability_index(reference, current)
        return DriftSignal(feature_name, "psi", round(value, 6), self._status(value))

    def categorical(self, feature_name: str, reference: Sequence[str], current: Sequence[str]) -> DriftSignal:
        value = unseen_category_rate(reference, current)
        return DriftSignal(feature_name, "unseen_category_rate", round(value, 6), self._status(value))


class PromotionController:
    def __init__(self, registry: Registry) -> None:
        self.registry = registry

    def evaluate(
        self,
        artifact: ModelArtifact,
        gates: Sequence[PromotionGate],
        *,
        segment_metrics: Mapping[str, Mapping[str, float]] | None = None,
    ) -> EvaluationReport:
        statuses = {gate.metric: gate.evaluate(artifact.metrics) for gate in gates}
        return EvaluationReport(
            model_name=artifact.model_name,
            version=artifact.version,
            evaluated_at=dt.datetime.now(UTC),
            metrics=dict(artifact.metrics),
            gates=statuses,
            segment_metrics={k: dict(v) for k, v in (segment_metrics or {}).items()},
        )

    def promote_after_gate(
        self,
        artifact: ModelArtifact,
        report: EvaluationReport,
        target_stage: ModelStage,
    ) -> ModelArtifact:
        if not report.passed:
            raise ValueError("promotion blocked by evaluation gates")
        allowed = {
            ModelStage.CANDIDATE: {ModelStage.SHADOW},
            ModelStage.SHADOW: {ModelStage.CANARY, ModelStage.RETIRED},
            ModelStage.CANARY: {ModelStage.PRODUCTION, ModelStage.RETIRED},
            ModelStage.PRODUCTION: {ModelStage.RETIRED},
            ModelStage.RETIRED: set(),
        }
        if target_stage not in allowed[artifact.stage]:
            raise ValueError(f"invalid promotion {artifact.stage} -> {target_stage}")
        return self.registry.promote(artifact.model_name, artifact.version, target_stage)


def build_training_manifest(
    *,
    model_name: str,
    algorithm: str,
    dataset: DatasetSnapshot,
    features: Sequence[FeatureSpec],
    train_window: tuple[dt.datetime, dt.datetime],
    validation_window: tuple[dt.datetime, dt.datetime],
    test_window: tuple[dt.datetime, dt.datetime],
    label_definition: str,
    git_sha: str,
    parameters: Mapping[str, Any],
) -> TrainingManifest:
    validate_feature_boundary(features)
    validate_time_split(train_window, validation_window, test_window, leakage_gap=dt.timedelta(hours=1))
    return TrainingManifest(
        run_id=str(uuid.uuid4()),
        model_name=model_name,
        algorithm=algorithm,
        dataset=dataset,
        feature_specs=tuple(features),
        train_window=train_window,
        validation_window=validation_window,
        test_window=test_window,
        label_definition=label_definition,
        git_sha=git_sha,
        parameters=dict(parameters),
        created_at=dt.datetime.now(UTC),
    )


def summarize_service_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    """Aggregate inference logs for the operations mart/site without exposing payloads."""
    if not rows:
        return {"request_count": 0.0, "error_rate": 0.0, "p50_latency_ms": 0.0, "p95_latency_ms": 0.0}
    latencies = sorted(float(row.get("latency_ms") or 0) for row in rows)
    errors = sum(bool(row.get("error")) for row in rows)

    def percentile(p: float) -> float:
        index = min(len(latencies) - 1, max(0, math.ceil(p * len(latencies)) - 1))
        return latencies[index]

    return {
        "request_count": float(len(rows)),
        "error_rate": round(errors / len(rows), 6),
        "p50_latency_ms": round(percentile(0.50), 2),
        "p95_latency_ms": round(percentile(0.95), 2),
    }


def segment_metric_gap(segment_values: Mapping[str, float]) -> float:
    values = [value for value in segment_values.values() if not math.isnan(value)]
    return max(values) - min(values) if values else 0.0


def model_inventory(artifacts: Sequence[ModelArtifact]) -> dict[str, Any]:
    by_stage = Counter(artifact.stage.value for artifact in artifacts)
    by_model: dict[str, list[str]] = defaultdict(list)
    for artifact in artifacts:
        by_model[artifact.model_name].append(artifact.version)
    return {
        "artifact_count": len(artifacts),
        "stage_counts": dict(by_stage),
        "model_versions": {name: sorted(versions) for name, versions in sorted(by_model.items())},
        "touchpoints": list(TOUCHPOINTS),
    }
