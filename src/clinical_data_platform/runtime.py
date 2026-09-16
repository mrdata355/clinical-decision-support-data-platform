"""Core pipeline runtime.

The module keeps ingestion behavior deterministic and observable. Source adapters emit a
common envelope, validation separates malformed records from usable records, normalization
produces stable business keys, and merge decisions are based on source version plus event
ordering rather than arrival order. The same abstractions are reusable for API, file, event,
and warehouse sources.
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import logging
import os
import uuid
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping, MutableMapping, Sequence
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol

import orjson
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LOGGER = logging.getLogger(__name__)
UTC = dt.UTC


class RuntimeSettings(BaseSettings):
    """Runtime configuration loaded from environment variables.

    Configuration is intentionally externalized so deployment environments can alter
    warehouse, schema, batching, and quality thresholds without changing transformation code.
    """

    model_config = SettingsConfigDict(env_prefix="CDP_", env_file=".env", extra="ignore")

    environment: str = "dev"
    warehouse: str = "COMPUTE_WH"
    database: str = "CLINICAL_ANALYTICS"
    raw_schema: str = "RAW"
    staging_schema: str = "STAGING"
    core_schema: str = "CORE"
    mart_schema: str = "MART"
    ops_schema: str = "OPS"
    batch_size: int = 5_000
    max_bad_record_ratio: float = 0.02
    max_source_lag_minutes: int = 30
    retain_raw_days: int = 90
    query_tag_prefix: str = "clinical-data-platform"


class SourceKind(StrEnum):
    APPLICATION = "application"
    WEB = "web"
    MOBILE = "mobile"
    CONTENT = "content"
    INTEGRATION = "integration"
    THIRD_PARTY = "third_party"
    BUSINESS_SYSTEM = "business_system"


class RecordStatus(StrEnum):
    ACCEPTED = "accepted"
    QUARANTINED = "quarantined"
    DUPLICATE = "duplicate"
    STALE = "stale"
    INSERTED = "inserted"
    UPDATED = "updated"
    UNCHANGED = "unchanged"


class EventType(StrEnum):
    TOOL_VIEW = "tool_view"
    TOOL_START = "tool_start"
    TOOL_COMPLETE = "tool_complete"
    SEARCH = "search"
    FAVORITE_ADD = "favorite_add"
    FAVORITE_REMOVE = "favorite_remove"
    CONTENT_VIEW = "content_view"
    INTEGRATION_LAUNCH = "integration_launch"
    SESSION_START = "session_start"
    SESSION_END = "session_end"


class PipelineError(RuntimeError):
    """Base pipeline exception."""


class QualityGateError(PipelineError):
    """Raised when the quality threshold makes a batch unsafe to publish."""


class SourceReadError(PipelineError):
    """Raised when a source cannot be read consistently."""


class SinkWriteError(PipelineError):
    """Raised when a sink cannot commit a deterministic batch."""


@dataclasses.dataclass(frozen=True, slots=True)
class BatchWindow:
    """Closed-open source window used for repeatable extraction.

    A window is captured before reading. Replays use the exact same lower and upper bound,
    preventing a retry from silently including records that arrived after the original run.
    """

    start: dt.datetime
    end: dt.datetime

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("BatchWindow timestamps must be timezone-aware")
        if self.end <= self.start:
            raise ValueError("BatchWindow end must be after start")

    @property
    def duration_seconds(self) -> float:
        return (self.end - self.start).total_seconds()

    def contains(self, value: dt.datetime) -> bool:
        return self.start <= value < self.end


class RecordEnvelope(BaseModel):
    """Transport-neutral envelope retained through RAW and CLEAN layers."""

    model_config = ConfigDict(extra="allow", frozen=True)

    event_id: str = Field(min_length=8, max_length=128)
    event_type: EventType
    source_kind: SourceKind
    source_system: str = Field(min_length=2, max_length=100)
    schema_version: int = Field(ge=1)
    event_ts: dt.datetime
    source_updated_at: dt.datetime
    ingested_at: dt.datetime
    business_key: str = Field(min_length=1, max_length=256)
    source_version: int = Field(ge=1)
    payload: dict[str, Any]
    trace_id: str | None = None
    account_token: str | None = None
    user_token: str | None = None

    @field_validator("event_ts", "source_updated_at", "ingested_at")
    @classmethod
    def require_timezone(cls, value: dt.datetime) -> dt.datetime:
        if value.tzinfo is None:
            raise ValueError("timestamp must include timezone")
        return value.astimezone(UTC)


class QualityIssue(BaseModel):
    code: str
    field: str | None = None
    message: str
    severity: str = "error"


class ValidatedRecord(BaseModel):
    envelope: RecordEnvelope
    status: RecordStatus
    issues: list[QualityIssue] = Field(default_factory=list)
    normalized_payload: dict[str, Any] = Field(default_factory=dict)
    canonical_hash: str = ""


@dataclasses.dataclass(slots=True)
class PipelineMetrics:
    run_id: str
    pipeline_name: str
    started_at: dt.datetime
    source_count: int = 0
    accepted_count: int = 0
    quarantined_count: int = 0
    duplicate_count: int = 0
    stale_count: int = 0
    inserted_count: int = 0
    updated_count: int = 0
    unchanged_count: int = 0
    published_count: int = 0
    completed_at: dt.datetime | None = None
    counters: Counter[str] = dataclasses.field(default_factory=Counter)

    @property
    def bad_record_ratio(self) -> float:
        if self.source_count == 0:
            return 0.0
        return self.quarantined_count / self.source_count

    @property
    def explained_count(self) -> int:
        return (
            self.inserted_count
            + self.updated_count
            + self.unchanged_count
            + self.duplicate_count
            + self.stale_count
            + self.quarantined_count
        )

    def finish(self) -> None:
        self.completed_at = dt.datetime.now(UTC)

    def as_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["started_at"] = self.started_at.isoformat()
        result["completed_at"] = self.completed_at.isoformat() if self.completed_at else None
        result["bad_record_ratio"] = round(self.bad_record_ratio, 6)
        return result


class SourceAdapter(Protocol):
    """Source contract for bounded extraction."""

    def high_watermark(self) -> dt.datetime: ...

    def read(self, window: BatchWindow) -> Iterable[Mapping[str, Any]]: ...


class PipelineSink(Protocol):
    """Sink contract separating writes from pipeline policy."""

    def write_raw(self, run_id: str, records: Sequence[Mapping[str, Any]]) -> None: ...

    def write_quarantine(self, run_id: str, records: Sequence[ValidatedRecord]) -> None: ...

    def merge_current(self, run_id: str, records: Sequence[ValidatedRecord]) -> Mapping[str, int]: ...

    def write_audit(self, metrics: PipelineMetrics) -> None: ...


class ContractRegistry:
    """Small contract registry used by validators and source adapters."""

    def __init__(self) -> None:
        self._required_payload_fields: dict[EventType, tuple[str, ...]] = {
            EventType.TOOL_VIEW: ("tool_id", "session_id"),
            EventType.TOOL_START: ("tool_id", "session_id"),
            EventType.TOOL_COMPLETE: ("tool_id", "session_id", "completion_id"),
            EventType.SEARCH: ("query_token", "session_id"),
            EventType.FAVORITE_ADD: ("tool_id",),
            EventType.FAVORITE_REMOVE: ("tool_id",),
            EventType.CONTENT_VIEW: ("content_id", "session_id"),
            EventType.INTEGRATION_LAUNCH: ("integration_id", "tool_id"),
            EventType.SESSION_START: ("session_id",),
            EventType.SESSION_END: ("session_id",),
        }

    def required_fields(self, event_type: EventType) -> tuple[str, ...]:
        return self._required_payload_fields[event_type]

    def validate_payload(self, event_type: EventType, payload: Mapping[str, Any]) -> list[QualityIssue]:
        issues: list[QualityIssue] = []
        for field_name in self.required_fields(event_type):
            if payload.get(field_name) in (None, ""):
                issues.append(
                    QualityIssue(
                        code="MISSING_REQUIRED_FIELD",
                        field=field_name,
                        message=f"{field_name} is required for {event_type.value}",
                    )
                )
        return issues


class Normalizer:
    """Canonical normalization rules shared by batch and event ingestion."""

    def normalize(self, envelope: RecordEnvelope) -> dict[str, Any]:
        payload = dict(envelope.payload)
        normalized: dict[str, Any] = {}
        for key, value in payload.items():
            if isinstance(value, str):
                value = value.strip()
            if key.endswith("_country") and isinstance(value, str):
                value = value.upper()
            if key.endswith("_language") and isinstance(value, str):
                value = value.lower()
            if key.endswith("_amount") and value is not None:
                value = str(Decimal(str(value)).quantize(Decimal("0.01")))
            normalized[key] = value

        normalized["event_id"] = envelope.event_id
        normalized["event_type"] = envelope.event_type.value
        normalized["event_ts"] = envelope.event_ts.isoformat()
        normalized["source_updated_at"] = envelope.source_updated_at.isoformat()
        normalized["source_system"] = envelope.source_system
        normalized["source_version"] = envelope.source_version
        normalized["business_key"] = envelope.business_key
        normalized["schema_version"] = envelope.schema_version
        normalized["account_token"] = envelope.account_token
        normalized["user_token"] = envelope.user_token
        return normalized

    @staticmethod
    def canonical_hash(record: Mapping[str, Any]) -> str:
        stable = {k: v for k, v in record.items() if k not in {"ingested_at", "trace_id"}}
        encoded = orjson.dumps(stable, option=orjson.OPT_SORT_KEYS)
        return hashlib.sha256(encoded).hexdigest()


class RecordValidator:
    """Validation is explicit and produces reason-coded quarantine records."""

    def __init__(self, registry: ContractRegistry | None = None) -> None:
        self.registry = registry or ContractRegistry()

    def validate(self, raw: Mapping[str, Any]) -> tuple[RecordEnvelope | None, list[QualityIssue]]:
        try:
            envelope = RecordEnvelope.model_validate(raw)
        except ValidationError as exc:
            issues = [
                QualityIssue(
                    code="CONTRACT_VALIDATION_FAILED",
                    field=".".join(str(v) for v in item["loc"]),
                    message=item["msg"],
                )
                for item in exc.errors()
            ]
            return None, issues

        issues = self.registry.validate_payload(envelope.event_type, envelope.payload)
        now = dt.datetime.now(UTC)
        if envelope.event_ts > now + dt.timedelta(minutes=5):
            issues.append(
                QualityIssue(
                    code="FUTURE_EVENT_TIME",
                    field="event_ts",
                    message="event_ts is beyond the accepted clock-skew boundary",
                )
            )
        if envelope.source_updated_at < envelope.event_ts - dt.timedelta(days=30):
            issues.append(
                QualityIssue(
                    code="SOURCE_TIME_INCONSISTENT",
                    field="source_updated_at",
                    message="source update time is implausibly older than event time",
                )
            )
        return envelope, issues


@dataclasses.dataclass(frozen=True, slots=True)
class CurrentState:
    business_key: str
    source_version: int
    source_updated_at: dt.datetime
    event_id: str
    canonical_hash: str
    payload: Mapping[str, Any]


class MergePolicy:
    """Deterministic conflict resolution for current-state tables."""

    def decide(self, incoming: ValidatedRecord, current: CurrentState | None) -> RecordStatus:
        env = incoming.envelope
        if current is None:
            return RecordStatus.INSERTED
        if env.source_version < current.source_version:
            return RecordStatus.STALE
        if env.source_version == current.source_version:
            if incoming.canonical_hash == current.canonical_hash:
                return RecordStatus.UNCHANGED
            if env.source_updated_at < current.source_updated_at:
                return RecordStatus.STALE
            if env.source_updated_at == current.source_updated_at and env.event_id <= current.event_id:
                return RecordStatus.STALE
        return RecordStatus.UPDATED


class InMemorySink:
    """Deterministic sink used for local execution and tests.

    External warehouse sinks implement the same protocol and preserve the same merge rules.
    """

    def __init__(self) -> None:
        self.raw: list[dict[str, Any]] = []
        self.quarantine: list[dict[str, Any]] = []
        self.current: dict[str, CurrentState] = {}
        self.audit: list[dict[str, Any]] = []
        self.event_ids: set[str] = set()

    def write_raw(self, run_id: str, records: Sequence[Mapping[str, Any]]) -> None:
        for record in records:
            self.raw.append({"run_id": run_id, **dict(record)})

    def write_quarantine(self, run_id: str, records: Sequence[ValidatedRecord]) -> None:
        for item in records:
            self.quarantine.append(
                {
                    "run_id": run_id,
                    "event_id": item.envelope.event_id,
                    "business_key": item.envelope.business_key,
                    "issues": [issue.model_dump() for issue in item.issues],
                }
            )

    def merge_current(self, run_id: str, records: Sequence[ValidatedRecord]) -> Mapping[str, int]:
        policy = MergePolicy()
        counts: Counter[str] = Counter()
        for item in records:
            env = item.envelope
            if env.event_id in self.event_ids:
                counts[RecordStatus.DUPLICATE.value] += 1
                continue
            current = self.current.get(env.business_key)
            decision = policy.decide(item, current)
            counts[decision.value] += 1
            self.event_ids.add(env.event_id)
            if decision in {RecordStatus.INSERTED, RecordStatus.UPDATED}:
                self.current[env.business_key] = CurrentState(
                    business_key=env.business_key,
                    source_version=env.source_version,
                    source_updated_at=env.source_updated_at,
                    event_id=env.event_id,
                    canonical_hash=item.canonical_hash,
                    payload=item.normalized_payload,
                )
        return counts

    def write_audit(self, metrics: PipelineMetrics) -> None:
        self.audit.append(metrics.as_dict())


class PipelineEngine:
    """Coordinates a bounded source read through audit publication."""

    def __init__(
        self,
        settings: RuntimeSettings | None = None,
        validator: RecordValidator | None = None,
        normalizer: Normalizer | None = None,
    ) -> None:
        self.settings = settings or RuntimeSettings()
        self.validator = validator or RecordValidator()
        self.normalizer = normalizer or Normalizer()

    def _validate_batch(self, raw_records: Sequence[Mapping[str, Any]]) -> tuple[list[ValidatedRecord], list[ValidatedRecord]]:
        accepted: list[ValidatedRecord] = []
        quarantined: list[ValidatedRecord] = []
        for raw in raw_records:
            envelope, issues = self.validator.validate(raw)
            if envelope is None:
                placeholder = RecordEnvelope(
                    event_id=str(raw.get("event_id") or f"invalid-{uuid.uuid4()}"),
                    event_type=EventType.SESSION_START,
                    source_kind=SourceKind.APPLICATION,
                    source_system=str(raw.get("source_system") or "unknown"),
                    schema_version=1,
                    event_ts=dt.datetime.now(UTC),
                    source_updated_at=dt.datetime.now(UTC),
                    ingested_at=dt.datetime.now(UTC),
                    business_key=str(raw.get("business_key") or "invalid"),
                    source_version=1,
                    payload=dict(raw),
                )
                quarantined.append(
                    ValidatedRecord(
                        envelope=placeholder,
                        status=RecordStatus.QUARANTINED,
                        issues=issues,
                    )
                )
                continue

            normalized = self.normalizer.normalize(envelope)
            item = ValidatedRecord(
                envelope=envelope,
                status=RecordStatus.ACCEPTED if not issues else RecordStatus.QUARANTINED,
                issues=issues,
                normalized_payload=normalized,
                canonical_hash=self.normalizer.canonical_hash(normalized),
            )
            if issues:
                quarantined.append(item)
            else:
                accepted.append(item)
        return accepted, quarantined

    def execute(
        self,
        *,
        pipeline_name: str,
        source: SourceAdapter,
        sink: PipelineSink,
        lower_bound: dt.datetime,
    ) -> PipelineMetrics:
        run_id = str(uuid.uuid4())
        high_watermark = source.high_watermark().astimezone(UTC)
        window = BatchWindow(start=lower_bound.astimezone(UTC), end=high_watermark)
        metrics = PipelineMetrics(
            run_id=run_id,
            pipeline_name=pipeline_name,
            started_at=dt.datetime.now(UTC),
        )

        raw_records = [dict(item) for item in source.read(window)]
        metrics.source_count = len(raw_records)
        sink.write_raw(run_id, raw_records)

        accepted, quarantined = self._validate_batch(raw_records)
        metrics.accepted_count = len(accepted)
        metrics.quarantined_count = len(quarantined)
        if quarantined:
            sink.write_quarantine(run_id, quarantined)

        if metrics.bad_record_ratio > self.settings.max_bad_record_ratio:
            metrics.finish()
            sink.write_audit(metrics)
            raise QualityGateError(
                f"bad-record ratio {metrics.bad_record_ratio:.2%} exceeds "
                f"threshold {self.settings.max_bad_record_ratio:.2%}"
            )

        merge_counts = sink.merge_current(run_id, accepted)
        metrics.inserted_count = merge_counts.get(RecordStatus.INSERTED.value, 0)
        metrics.updated_count = merge_counts.get(RecordStatus.UPDATED.value, 0)
        metrics.unchanged_count = merge_counts.get(RecordStatus.UNCHANGED.value, 0)
        metrics.duplicate_count = merge_counts.get(RecordStatus.DUPLICATE.value, 0)
        metrics.stale_count = merge_counts.get(RecordStatus.STALE.value, 0)
        metrics.published_count = metrics.inserted_count + metrics.updated_count

        explained = (
            metrics.inserted_count
            + metrics.updated_count
            + metrics.unchanged_count
            + metrics.duplicate_count
            + metrics.stale_count
            + metrics.quarantined_count
        )
        if explained != metrics.source_count:
            raise PipelineError(
                f"reconciliation failed: source={metrics.source_count} explained={explained}"
            )

        metrics.finish()
        sink.write_audit(metrics)
        return metrics


class IterableSource:
    """Local source adapter for repeatable samples and regression tests."""

    def __init__(self, records: Sequence[Mapping[str, Any]], watermark: dt.datetime | None = None) -> None:
        self.records = list(records)
        self.watermark = watermark or dt.datetime.now(UTC)

    def high_watermark(self) -> dt.datetime:
        return self.watermark

    def read(self, window: BatchWindow) -> Iterator[Mapping[str, Any]]:
        for record in self.records:
            event_ts_raw = record.get("event_ts")
            event_ts = (
                dt.datetime.fromisoformat(str(event_ts_raw).replace("Z", "+00:00"))
                if not isinstance(event_ts_raw, dt.datetime)
                else event_ts_raw
            )
            if window.contains(event_ts.astimezone(UTC)):
                yield record


def build_demo_records(base_time: dt.datetime) -> list[dict[str, Any]]:
    """Create deterministic de-identified records for local execution."""

    rows: list[dict[str, Any]] = []
    for idx, event_type in enumerate(
        [
            EventType.SESSION_START,
            EventType.SEARCH,
            EventType.TOOL_VIEW,
            EventType.TOOL_START,
            EventType.TOOL_COMPLETE,
            EventType.FAVORITE_ADD,
        ]
    ):
        event_ts = base_time + dt.timedelta(seconds=idx * 15)
        payload: dict[str, Any] = {"session_id": "session-demo-001"}
        if event_type in {
            EventType.TOOL_VIEW,
            EventType.TOOL_START,
            EventType.TOOL_COMPLETE,
            EventType.FAVORITE_ADD,
        }:
            payload["tool_id"] = "tool-demo-100"
        if event_type == EventType.TOOL_COMPLETE:
            payload["completion_id"] = "completion-demo-100"
        if event_type == EventType.SEARCH:
            payload["query_token"] = "query-demo-1"
        rows.append(
            {
                "event_id": f"event-demo-{idx:04d}",
                "event_type": event_type.value,
                "source_kind": SourceKind.APPLICATION.value,
                "source_system": "product-api",
                "schema_version": 1,
                "event_ts": event_ts,
                "source_updated_at": event_ts,
                "ingested_at": event_ts + dt.timedelta(seconds=1),
                "business_key": f"{event_type.value}:session-demo-001:{idx}",
                "source_version": 1,
                "payload": payload,
                "trace_id": f"trace-demo-{idx:04d}",
                "account_token": "acct-demo-001",
                "user_token": "user-demo-001",
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a deterministic local pipeline batch")
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))

    base_time = dt.datetime.now(UTC) - dt.timedelta(minutes=2)
    rows = build_demo_records(base_time)
    source = IterableSource(rows, watermark=base_time + dt.timedelta(minutes=1))
    sink = InMemorySink()
    engine = PipelineEngine(RuntimeSettings(max_bad_record_ratio=0.10))
    metrics = engine.execute(
        pipeline_name="product_events",
        source=source,
        sink=sink,
        lower_bound=base_time - dt.timedelta(seconds=1),
    )
    print(json.dumps(metrics.as_dict(), indent=2, default=str))


if __name__ == "__main__":
    main()
