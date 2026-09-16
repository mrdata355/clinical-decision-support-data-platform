"""Production reconciliation controls for source-to-serving data movement.

The module treats every load as an accounting problem. It supports row counts, distinct keys,
set equality, sums, stable hashes, distributions, grouped aggregates, terminal outcomes,
watermark continuity, SCD2 overlap detection, referential integrity, quality-rating arithmetic,
and EHR funnel controls. Results are model/run aware and serializable into OPS tables.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from decimal import Decimal
from enum import StrEnum
from typing import Any

UTC = dt.UTC


class ReconciliationStatus(StrEnum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


class ControlType(StrEnum):
    ROW_COUNT = "ROW_COUNT"
    DISTINCT_KEY_COUNT = "DISTINCT_KEY_COUNT"
    KEYSET_MATCH = "KEYSET_MATCH"
    SUM = "SUM"
    HASH_TOTAL = "HASH_TOTAL"
    BUSINESS_AGGREGATE = "BUSINESS_AGGREGATE"
    DISTRIBUTION = "DISTRIBUTION"
    REFERENTIAL_INTEGRITY = "REFERENTIAL_INTEGRITY"
    TERMINAL_OUTCOME = "TERMINAL_OUTCOME"
    WATERMARK_CONTINUITY = "WATERMARK_CONTINUITY"
    SCD2_OVERLAP = "SCD2_OVERLAP"
    INVARIANT = "INVARIANT"


@dataclasses.dataclass(frozen=True, slots=True)
class ReconciliationControl:
    name: str
    control_type: ControlType
    tolerance: Decimal = Decimal("0")
    warning_tolerance: Decimal | None = None
    description: str = ""
    model_name: str | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class ReconciliationResult:
    run_id: str
    control_name: str
    control_type: ControlType
    source_value: Decimal
    target_value: Decimal
    difference: Decimal
    tolerance: Decimal
    status: ReconciliationStatus
    details: Mapping[str, Any]
    checked_at: dt.datetime
    model_name: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "model_name": self.model_name,
            "control_name": self.control_name,
            "control_type": self.control_type.value,
            "source_value": str(self.source_value),
            "target_value": str(self.target_value),
            "difference": str(self.difference),
            "tolerance": str(self.tolerance),
            "status": self.status.value,
            "details": dict(self.details),
            "checked_at": self.checked_at.isoformat(),
        }


@dataclasses.dataclass(slots=True)
class ReconciliationReport:
    run_id: str
    results: list[ReconciliationResult]

    @property
    def passed(self) -> bool:
        return all(result.status != ReconciliationStatus.FAIL for result in self.results)

    @property
    def failures(self) -> list[ReconciliationResult]:
        return [r for r in self.results if r.status == ReconciliationStatus.FAIL]

    def summary(self) -> dict[str, Any]:
        counts = Counter(result.status.value for result in self.results)
        return {"run_id": self.run_id, "passed": self.passed, "control_count": len(self.results), "status_counts": dict(counts), "failed_controls": [r.control_name for r in self.failures], "models": sorted({r.model_name for r in self.results if r.model_name})}


class Reconciler:
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        self.results: list[ReconciliationResult] = []

    @staticmethod
    def _status(difference: Decimal, tolerance: Decimal, warning_tolerance: Decimal | None) -> ReconciliationStatus:
        absolute = abs(difference)
        if absolute <= tolerance:
            return ReconciliationStatus.PASS
        if warning_tolerance is not None and absolute <= warning_tolerance:
            return ReconciliationStatus.WARN
        return ReconciliationStatus.FAIL

    def compare(self, *, control: ReconciliationControl, source_value: int | float | Decimal, target_value: int | float | Decimal, details: Mapping[str, Any] | None = None) -> ReconciliationResult:
        source, target = Decimal(str(source_value)), Decimal(str(target_value))
        difference = source - target
        result = ReconciliationResult(self.run_id, control.name, control.control_type, source, target, difference, control.tolerance, self._status(difference, control.tolerance, control.warning_tolerance), dict(details or {}), dt.datetime.now(UTC), model_name=control.model_name)
        self.results.append(result)
        return result

    def report(self) -> ReconciliationReport:
        return ReconciliationReport(self.run_id, list(self.results))


def row_count_control(reconciler: Reconciler, source: Sequence[Mapping[str, Any]], target: Sequence[Mapping[str, Any]], *, name: str = "row_count", tolerance: int = 0, model_name: str | None = None) -> ReconciliationResult:
    return reconciler.compare(control=ReconciliationControl(name, ControlType.ROW_COUNT, Decimal(tolerance), model_name=model_name), source_value=len(source), target_value=len(target))


def distinct_key_control(reconciler: Reconciler, source: Sequence[Mapping[str, Any]], target: Sequence[Mapping[str, Any]], *, key: str, name: str | None = None, model_name: str | None = None) -> ReconciliationResult:
    source_keys = {str(row.get(key)) for row in source if row.get(key) is not None}
    target_keys = {str(row.get(key)) for row in target if row.get(key) is not None}
    missing, extra = sorted(source_keys - target_keys), sorted(target_keys - source_keys)
    return reconciler.compare(control=ReconciliationControl(name or f"distinct_{key}", ControlType.DISTINCT_KEY_COUNT, model_name=model_name), source_value=len(source_keys), target_value=len(target_keys), details={"missing_key_sample": missing[:25], "extra_key_sample": extra[:25], "missing_key_count": len(missing), "extra_key_count": len(extra)})


def keyset_match_control(reconciler: Reconciler, source: Sequence[Mapping[str, Any]], target: Sequence[Mapping[str, Any]], *, key: str, name: str | None = None, model_name: str | None = None) -> ReconciliationResult:
    source_keys = {str(row.get(key)) for row in source if row.get(key) is not None}
    target_keys = {str(row.get(key)) for row in target if row.get(key) is not None}
    symmetric = source_keys ^ target_keys
    return reconciler.compare(control=ReconciliationControl(name or f"keyset_{key}", ControlType.KEYSET_MATCH, model_name=model_name), source_value=0, target_value=len(symmetric), details={"missing_sample": sorted(source_keys-target_keys)[:25], "extra_sample": sorted(target_keys-source_keys)[:25]})


def sum_control(reconciler: Reconciler, source: Sequence[Mapping[str, Any]], target: Sequence[Mapping[str, Any]], *, field: str, name: str | None = None, tolerance: Decimal = Decimal("0.01"), model_name: str | None = None) -> ReconciliationResult:
    def total(rows: Sequence[Mapping[str, Any]]) -> Decimal:
        return sum((Decimal(str(row.get(field))) for row in rows if row.get(field) is not None), Decimal("0"))
    return reconciler.compare(control=ReconciliationControl(name or f"sum_{field}", ControlType.SUM, tolerance, model_name=model_name), source_value=total(source), target_value=total(target), details={"field": field})


def stable_row_hash(row: Mapping[str, Any], fields: Sequence[str]) -> int:
    canonical = json.dumps({field: row.get(field) for field in fields}, sort_keys=True, separators=(",", ":"), default=str)
    return int(hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16], 16)


def hash_total_control(reconciler: Reconciler, source: Sequence[Mapping[str, Any]], target: Sequence[Mapping[str, Any]], *, fields: Sequence[str], name: str = "hash_total", model_name: str | None = None) -> ReconciliationResult:
    source_hash = sum(stable_row_hash(row, fields) for row in source)
    target_hash = sum(stable_row_hash(row, fields) for row in target)
    return reconciler.compare(control=ReconciliationControl(name, ControlType.HASH_TOTAL, model_name=model_name), source_value=source_hash, target_value=target_hash, details={"fields": list(fields)})


def aggregate_by(rows: Iterable[Mapping[str, Any]], *, dimensions: Sequence[str], measure: str | None = None) -> dict[tuple[Any, ...], Decimal]:
    result: dict[tuple[Any, ...], Decimal] = defaultdict(lambda: Decimal("0"))
    for row in rows:
        key = tuple(row.get(d) for d in dimensions)
        result[key] += Decimal("1") if measure is None else Decimal(str(row.get(measure) or 0))
    return dict(result)


def aggregate_control(reconciler: Reconciler, source: Sequence[Mapping[str, Any]], target: Sequence[Mapping[str, Any]], *, dimensions: Sequence[str], measure: str | None = None, tolerance: Decimal = Decimal("0"), name: str = "business_aggregate", model_name: str | None = None) -> list[ReconciliationResult]:
    source_map, target_map = aggregate_by(source, dimensions=dimensions, measure=measure), aggregate_by(target, dimensions=dimensions, measure=measure)
    results: list[ReconciliationResult] = []
    for key in sorted(set(source_map) | set(target_map), key=str):
        results.append(reconciler.compare(control=ReconciliationControl(f"{name}:{'|'.join(str(v) for v in key)}", ControlType.BUSINESS_AGGREGATE, tolerance, model_name=model_name), source_value=source_map.get(key, Decimal("0")), target_value=target_map.get(key, Decimal("0")), details={"dimensions": list(dimensions), "dimension_values": list(key), "measure": measure or "row_count"}))
    return results


def distribution_control(reconciler: Reconciler, source: Sequence[Mapping[str, Any]], target: Sequence[Mapping[str, Any]], *, field: str, tolerance_ratio: Decimal = Decimal("0.02"), name: str | None = None, model_name: str | None = None) -> list[ReconciliationResult]:
    s_count, t_count = Counter(str(r.get(field)) for r in source), Counter(str(r.get(field)) for r in target)
    s_total, t_total = max(1, len(source)), max(1, len(target))
    results: list[ReconciliationResult] = []
    for value in sorted(set(s_count) | set(t_count)):
        s_ratio, t_ratio = Decimal(s_count[value]) / Decimal(s_total), Decimal(t_count[value]) / Decimal(t_total)
        results.append(reconciler.compare(control=ReconciliationControl(f"{name or field + '_distribution'}:{value}", ControlType.DISTRIBUTION, tolerance_ratio, model_name=model_name), source_value=s_ratio, target_value=t_ratio, details={"field": field, "value": value, "source_rows": s_count[value], "target_rows": t_count[value]}))
    return results


def referential_integrity_control(reconciler: Reconciler, rows: Sequence[Mapping[str, Any]], *, foreign_key: str, valid_values: set[Any], name: str | None = None, model_name: str | None = None) -> ReconciliationResult:
    orphans = [row.get(foreign_key) for row in rows if row.get(foreign_key) is not None and row.get(foreign_key) not in valid_values]
    return reconciler.compare(control=ReconciliationControl(name or f"{foreign_key}_referential_integrity", ControlType.REFERENTIAL_INTEGRITY, model_name=model_name), source_value=0, target_value=len(orphans), details={"orphan_sample": [str(v) for v in orphans[:25]], "orphan_count": len(orphans)})


def terminal_outcome_control(reconciler: Reconciler, *, source_count: int, inserted: int, updated: int, unchanged: int, duplicate: int, stale: int, quarantined: int, model_name: str | None = None) -> ReconciliationResult:
    explained = inserted + updated + unchanged + duplicate + stale + quarantined
    return reconciler.compare(control=ReconciliationControl("terminal_outcome_accounting", ControlType.TERMINAL_OUTCOME, description="every source row has exactly one terminal processing outcome", model_name=model_name), source_value=source_count, target_value=explained, details={"inserted": inserted, "updated": updated, "unchanged": unchanged, "duplicate": duplicate, "stale": stale, "quarantined": quarantined})


def watermark_continuity_control(reconciler: Reconciler, *, previous_high: dt.datetime | None, current_low: dt.datetime | None, current_high: dt.datetime, name: str = "watermark_continuity", model_name: str | None = None) -> ReconciliationResult:
    if current_high.tzinfo is None or (current_low and current_low.tzinfo is None) or (previous_high and previous_high.tzinfo is None):
        raise ValueError("watermarks must be timezone-aware")
    gap_seconds = Decimal("0") if previous_high is None or current_low is None else Decimal(str((current_low - previous_high).total_seconds()))
    invalid_window = current_low is not None and current_high <= current_low
    return reconciler.compare(control=ReconciliationControl(name, ControlType.WATERMARK_CONTINUITY, model_name=model_name), source_value=0, target_value=abs(gap_seconds) + (Decimal("1") if invalid_window else Decimal("0")), details={"previous_high": previous_high.isoformat() if previous_high else None, "current_low": current_low.isoformat() if current_low else None, "current_high": current_high.isoformat(), "gap_seconds": str(gap_seconds), "invalid_window": invalid_window})


def scd2_overlap_control(reconciler: Reconciler, rows: Sequence[Mapping[str, Any]], *, business_key: str, valid_from: str = "valid_from", valid_to: str = "valid_to", name: str = "scd2_overlap", model_name: str | None = None) -> ReconciliationResult:
    grouped: dict[Any, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row.get(business_key)].append(row)
    overlaps: list[str] = []
    current_duplicates: list[str] = []
    for key, versions in grouped.items():
        versions = sorted(versions, key=lambda r: r.get(valid_from) or dt.datetime.min.replace(tzinfo=UTC))
        current_count = sum(1 for r in versions if r.get(valid_to) is None)
        if current_count > 1:
            current_duplicates.append(str(key))
        for left, right in zip(versions, versions[1:]):
            if left.get(valid_to) is None or left.get(valid_to) > right.get(valid_from):
                overlaps.append(str(key))
                break
    failed = len(set(overlaps) | set(current_duplicates))
    return reconciler.compare(control=ReconciliationControl(name, ControlType.SCD2_OVERLAP, model_name=model_name), source_value=0, target_value=failed, details={"overlap_key_sample": overlaps[:25], "multiple_current_sample": current_duplicates[:25]})


def quality_rating_component_control(reconciler: Reconciler, ratings: Sequence[Mapping[str, Any]], *, tolerance: Decimal = Decimal("0.01"), model_name: str = "fct_quality_rating") -> ReconciliationResult:
    mismatches = 0
    for row in ratings:
        overall = Decimal(str(row.get("overall_score", 0)))
        components = Decimal(str(row.get("scientific_soundness_score", 0))) + Decimal(str(row.get("importance_score", 0))) + Decimal(str(row.get("usability_feasibility_score", 0)))
        mismatches += int(abs(overall - components) > tolerance)
    return reconciler.compare(control=ReconciliationControl("quality_rating_component_sum", ControlType.INVARIANT, model_name=model_name), source_value=0, target_value=mismatches, details={"tolerance": str(tolerance), "rows_checked": len(ratings)})


def ehr_funnel_control(reconciler: Reconciler, events: Sequence[Mapping[str, Any]], *, model_name: str = "ehr_integration_events") -> list[ReconciliationResult]:
    counts = Counter(str(row.get("event_type")) for row in events)
    results = []
    # Downstream steps may legitimately be fewer than launches, but cannot exceed their upstream population.
    for downstream, upstream in [("ehr_autofill", "ehr_launch"), ("ehr_writeback", "tool_complete")]:
        overflow = max(0, counts[downstream] - counts[upstream])
        results.append(reconciler.compare(control=ReconciliationControl(f"{downstream}_not_above_{upstream}", ControlType.INVARIANT, model_name=model_name), source_value=0, target_value=overflow, details={"upstream": counts[upstream], "downstream": counts[downstream]}))
    return results


def product_pipeline_controls(*, run_id: str, raw: Sequence[Mapping[str, Any]], staged: Sequence[Mapping[str, Any]], core: Sequence[Mapping[str, Any]], merge_outcomes: Mapping[str, int]) -> ReconciliationReport:
    reconciler = Reconciler(run_id)
    distinct_key_control(reconciler, raw, staged, key="event_id", name="raw_to_staging_event_ids", model_name="stg_product_events")
    distinct_key_control(reconciler, staged, core, key="event_id", name="staging_to_core_event_ids", model_name="fct_product_event")
    keyset_match_control(reconciler, staged, core, key="event_id", name="staging_core_event_keyset", model_name="fct_product_event")
    hash_total_control(reconciler, staged, core, fields=("event_id", "event_type", "business_key", "source_version"), name="staging_core_identity_hash", model_name="fct_product_event")
    terminal_outcome_control(reconciler, source_count=len(raw), inserted=int(merge_outcomes.get("inserted", 0)), updated=int(merge_outcomes.get("updated", 0)), unchanged=int(merge_outcomes.get("unchanged", 0)), duplicate=int(merge_outcomes.get("duplicate", 0)), stale=int(merge_outcomes.get("stale", 0)), quarantined=int(merge_outcomes.get("quarantined", 0)), model_name="fct_product_event")
    aggregate_control(reconciler, staged, core, dimensions=("event_type",), name="event_type_distribution", model_name="fct_product_event")
    distribution_control(reconciler, staged, core, field="event_type", tolerance_ratio=Decimal("0.001"), model_name="fct_product_event")
    return reconciler.report()


def full_platform_controls(*, run_id: str, product_raw: Sequence[Mapping[str, Any]], product_staged: Sequence[Mapping[str, Any]], product_core: Sequence[Mapping[str, Any]], merge_outcomes: Mapping[str, int], ratings: Sequence[Mapping[str, Any]] = (), content: Sequence[Mapping[str, Any]] = (), tools: Sequence[Mapping[str, Any]] = (), ehr_events: Sequence[Mapping[str, Any]] = ()) -> ReconciliationReport:
    reconciler = Reconciler(run_id)
    base = product_pipeline_controls(run_id=run_id, raw=product_raw, staged=product_staged, core=product_core, merge_outcomes=merge_outcomes)
    reconciler.results.extend(base.results)
    if ratings:
        quality_rating_component_control(reconciler, ratings)
        referential_integrity_control(reconciler, ratings, foreign_key="tool_id", valid_values={r.get("tool_id") for r in tools}, model_name="fct_quality_rating")
    if content and tools:
        # content/tool relationship is represented as tool_ids array in the clean contract.
        invalid = sum(1 for row in content for tool_id in row.get("tool_ids", []) if tool_id not in {t.get("tool_id") for t in tools})
        reconciler.compare(control=ReconciliationControl("content_tool_reference_integrity", ControlType.REFERENTIAL_INTEGRITY, model_name="dim_content"), source_value=0, target_value=invalid)
    if ehr_events:
        ehr_funnel_control(reconciler, ehr_events)
    return reconciler.report()
