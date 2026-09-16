"""Application service coordinating source extraction, validation, loading, and audit."""
from __future__ import annotations

import dataclasses
import datetime as dt
import logging
from collections.abc import Mapping, Sequence
from typing import Any

from clinical_data_platform.ingestion.batch import BatchPlanner, BoundedSource, IngestBatch
from clinical_data_platform.observability.metrics import MetricRegistry
from clinical_data_platform.runtime import Normalizer, RecordValidator, ValidatedRecord

UTC = dt.UTC
LOGGER = logging.getLogger(__name__)


class RawWriter:
    def write(self, run_id: str, records: Sequence[Mapping[str, Any]]) -> int:
        raise NotImplementedError


class QuarantineWriter:
    def write(self, run_id: str, records: Sequence[ValidatedRecord]) -> int:
        raise NotImplementedError


class CanonicalWriter:
    def merge(self, run_id: str, records: Sequence[ValidatedRecord]) -> Mapping[str, int]:
        raise NotImplementedError


@dataclasses.dataclass(frozen=True, slots=True)
class PipelineOutcome:
    run_id: str
    source_name: str
    source_count: int
    accepted_count: int
    quarantined_count: int
    merge_counts: Mapping[str, int]
    started_at: dt.datetime
    completed_at: dt.datetime

    @property
    def elapsed_seconds(self) -> float:
        return (self.completed_at - self.started_at).total_seconds()


class PipelineService:
    def __init__(
        self,
        *,
        planner: BatchPlanner,
        raw_writer: RawWriter,
        quarantine_writer: QuarantineWriter,
        canonical_writer: CanonicalWriter,
        validator: RecordValidator | None = None,
        normalizer: Normalizer | None = None,
        metrics: MetricRegistry | None = None,
        max_quarantine_ratio: float = 0.02,
    ) -> None:
        self.planner = planner
        self.raw_writer = raw_writer
        self.quarantine_writer = quarantine_writer
        self.canonical_writer = canonical_writer
        self.validator = validator or RecordValidator()
        self.normalizer = normalizer or Normalizer()
        self.metrics = metrics or MetricRegistry()
        self.max_quarantine_ratio = max_quarantine_ratio

    def run(self, source: BoundedSource, *, bootstrap: dt.datetime) -> PipelineOutcome:
        started = dt.datetime.now(UTC)
        labels = {"source": source.name}
        with self.metrics.span("pipeline", labels):
            window = self.planner.plan(source, bootstrap=bootstrap)
            with self.metrics.span("extract", labels):
                batch = self.planner.extract(source, window)
            self.metrics.gauge("source.rows", batch.count, labels)

            raw_records = [dict(record) for record in batch.records]
            with self.metrics.span("raw_write", labels):
                raw_written = self.raw_writer.write(batch.run_id, raw_records)
            if raw_written != batch.count:
                raise RuntimeError(
                    f"raw persistence mismatch source={batch.count} written={raw_written}"
                )

            accepted, quarantined = self._validate(raw_records)
            self.metrics.gauge("accepted.rows", len(accepted), labels)
            self.metrics.gauge("quarantined.rows", len(quarantined), labels)

            if quarantined:
                with self.metrics.span("quarantine_write", labels):
                    quarantined_written = self.quarantine_writer.write(batch.run_id, quarantined)
                if quarantined_written != len(quarantined):
                    raise RuntimeError(
                        f"quarantine persistence mismatch expected={len(quarantined)} "
                        f"written={quarantined_written}"
                    )

            quarantine_ratio = len(quarantined) / batch.count if batch.count else 0.0
            self.metrics.gauge("quarantine.ratio", quarantine_ratio, labels)
            if quarantine_ratio > self.max_quarantine_ratio:
                raise RuntimeError(
                    f"quarantine ratio {quarantine_ratio:.2%} exceeds "
                    f"threshold {self.max_quarantine_ratio:.2%}"
                )

            with self.metrics.span("canonical_merge", labels):
                merge_counts = dict(self.canonical_writer.merge(batch.run_id, accepted))

            explained = (
                int(merge_counts.get("inserted", 0))
                + int(merge_counts.get("updated", 0))
                + int(merge_counts.get("unchanged", 0))
                + int(merge_counts.get("duplicate", 0))
                + int(merge_counts.get("stale", 0))
                + len(quarantined)
            )
            if explained != batch.count:
                raise RuntimeError(
                    f"terminal outcome mismatch source={batch.count} explained={explained}"
                )

            # Source progress is committed only after all durable writes and reconciliation pass.
            self.planner.commit(batch)
            completed = dt.datetime.now(UTC)
            self.metrics.timing("pipeline.elapsed_seconds", (completed - started).total_seconds(), labels)
            return PipelineOutcome(
                run_id=batch.run_id,
                source_name=source.name,
                source_count=batch.count,
                accepted_count=len(accepted),
                quarantined_count=len(quarantined),
                merge_counts=merge_counts,
                started_at=started,
                completed_at=completed,
            )

    def _validate(
        self, records: Sequence[Mapping[str, Any]]
    ) -> tuple[list[ValidatedRecord], list[ValidatedRecord]]:
        accepted: list[ValidatedRecord] = []
        quarantined: list[ValidatedRecord] = []
        for record in records:
            envelope, issues = self.validator.validate(record)
            if envelope is None:
                # Invalid top-level envelopes are expected to be written by a raw-contract
                # quarantine adapter before this service. The runtime validator can represent
                # field-level failures once the envelope itself is parseable.
                self.metrics.increment("validation.envelope_failures")
                continue
            normalized = self.normalizer.normalize(envelope)
            validated = ValidatedRecord(
                envelope=envelope,
                status="quarantined" if issues else "accepted",
                issues=issues,
                normalized_payload=normalized,
                canonical_hash=self.normalizer.canonical_hash(normalized),
            )
            if issues:
                quarantined.append(validated)
            else:
                accepted.append(validated)
        return accepted, quarantined
