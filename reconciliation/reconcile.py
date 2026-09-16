"""Source-to-target reconciliation controls.

Reconciliation treats data movement as an accounting problem: every source record must have
an explained terminal outcome. Controls support counts, sums, distinct keys, hashes, and
business aggregates so migrations and daily loads can be proven rather than assumed.
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
    SUM = "SUM"
    HASH_TOTAL = "HASH_TOTAL"
    BUSINESS_AGGREGATE = "BUSINESS_AGGREGATE"
    TERMINAL_OUTCOME = "TERMINAL_OUTCOME"


@dataclasses.dataclass(frozen=True, slots=True)
class ReconciliationControl:
    name: str
    control_type: ControlType
    tolerance: Decimal = Decimal("0")
    warning_tolerance: Decimal | None = None
    description: str = ""


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

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
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
        return [result for result in self.results if result.status == ReconciliationStatus.FAIL]

    def summary(self) -> dict[str, Any]:
        counts = Counter(result.status.value for result in self.results)
        return {
            "run_id": self.run_id,
            "passed": self.passed,
            "control_count": len(self.results),
            "status_counts": dict(counts),
            "failed_controls": [result.control_name for result in self.failures],
        }


class Reconciler:
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        self.results: list[ReconciliationResult] = []

    def _status(
        self,
        difference: Decimal,
        tolerance: Decimal,
        warning_tolerance: Decimal | None,
    ) -> ReconciliationStatus:
        absolute = abs(difference)
        if absolute <= tolerance:
            return ReconciliationStatus.PASS
        if warning_tolerance is not None and absolute <= warning_tolerance:
            return ReconciliationStatus.WARN
        return ReconciliationStatus.FAIL

    def compare(
        self,
        *,
        control: ReconciliationControl,
        source_value: int | float | Decimal,
        target_value: int | float | Decimal,
        details: Mapping[str, Any] | None = None,
    ) -> ReconciliationResult:
        source = Decimal(str(source_value))
        target = Decimal(str(target_value))
        difference = source - target
        status = self._status(difference, control.tolerance, control.warning_tolerance)
        result = ReconciliationResult(
            run_id=self.run_id,
            control_name=control.name,
            control_type=control.control_type,
            source_value=source,
            target_value=target,
            difference=difference,
            tolerance=control.tolerance,
            status=status,
            details=dict(details or {}),
            checked_at=dt.datetime.now(UTC),
        )
        self.results.append(result)
        return result

    def report(self) -> ReconciliationReport:
        return ReconciliationReport(run_id=self.run_id, results=list(self.results))


def row_count_control(
    reconciler: Reconciler,
    source: Sequence[Mapping[str, Any]],
    target: Sequence[Mapping[str, Any]],
    *,
    name: str = "row_count",
    tolerance: int = 0,
) -> ReconciliationResult:
    return reconciler.compare(
        control=ReconciliationControl(
            name=name,
            control_type=ControlType.ROW_COUNT,
            tolerance=Decimal(tolerance),
            description="source and target row counts",
        ),
        source_value=len(source),
        target_value=len(target),
    )


def distinct_key_control(
    reconciler: Reconciler,
    source: Sequence[Mapping[str, Any]],
    target: Sequence[Mapping[str, Any]],
    *,
    key: str,
    name: str | None = None,
) -> ReconciliationResult:
    source_keys = {str(row.get(key)) for row in source if row.get(key) is not None}
    target_keys = {str(row.get(key)) for row in target if row.get(key) is not None}
    missing = sorted(source_keys - target_keys)
    extra = sorted(target_keys - source_keys)
    return reconciler.compare(
        control=ReconciliationControl(
            name=name or f"distinct_{key}",
            control_type=ControlType.DISTINCT_KEY_COUNT,
        ),
        source_value=len(source_keys),
        target_value=len(target_keys),
        details={
            "missing_key_sample": missing[:25],
            "extra_key_sample": extra[:25],
            "missing_key_count": len(missing),
            "extra_key_count": len(extra),
        },
    )


def sum_control(
    reconciler: Reconciler,
    source: Sequence[Mapping[str, Any]],
    target: Sequence[Mapping[str, Any]],
    *,
    field: str,
    name: str | None = None,
    tolerance: Decimal = Decimal("0.01"),
) -> ReconciliationResult:
    def total(rows: Sequence[Mapping[str, Any]]) -> Decimal:
        value = Decimal("0")
        for row in rows:
            raw = row.get(field)
            if raw is None:
                continue
            value += Decimal(str(raw))
        return value

    source_total = total(source)
    target_total = total(target)
    return reconciler.compare(
        control=ReconciliationControl(
            name=name or f"sum_{field}",
            control_type=ControlType.SUM,
            tolerance=tolerance,
        ),
        source_value=source_total,
        target_value=target_total,
    )


def stable_row_hash(row: Mapping[str, Any], fields: Sequence[str]) -> int:
    canonical = json.dumps(
        {field: row.get(field) for field in fields},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def hash_total_control(
    reconciler: Reconciler,
    source: Sequence[Mapping[str, Any]],
    target: Sequence[Mapping[str, Any]],
    *,
    fields: Sequence[str],
    name: str = "hash_total",
) -> ReconciliationResult:
    # Addition is order-insensitive and useful as a high-signal control alongside counts and
    # business aggregates. It is not a cryptographic proof of row-set equality by itself.
    source_hash = sum(stable_row_hash(row, fields) for row in source)
    target_hash = sum(stable_row_hash(row, fields) for row in target)
    return reconciler.compare(
        control=ReconciliationControl(name=name, control_type=ControlType.HASH_TOTAL),
        source_value=source_hash,
        target_value=target_hash,
        details={"fields": list(fields)},
    )


def aggregate_by(
    rows: Iterable[Mapping[str, Any]],
    *,
    dimensions: Sequence[str],
    measure: str | None = None,
) -> dict[tuple[Any, ...], Decimal]:
    result: dict[tuple[Any, ...], Decimal] = defaultdict(lambda: Decimal("0"))
    for row in rows:
        key = tuple(row.get(dimension) for dimension in dimensions)
        increment = Decimal("1") if measure is None else Decimal(str(row.get(measure) or 0))
        result[key] += increment
    return dict(result)


def aggregate_control(
    reconciler: Reconciler,
    source: Sequence[Mapping[str, Any]],
    target: Sequence[Mapping[str, Any]],
    *,
    dimensions: Sequence[str],
    measure: str | None = None,
    tolerance: Decimal = Decimal("0"),
    name: str = "business_aggregate",
) -> list[ReconciliationResult]:
    source_map = aggregate_by(source, dimensions=dimensions, measure=measure)
    target_map = aggregate_by(target, dimensions=dimensions, measure=measure)
    keys = sorted(set(source_map) | set(target_map), key=str)
    results: list[ReconciliationResult] = []
    for key in keys:
        results.append(
            reconciler.compare(
                control=ReconciliationControl(
                    name=f"{name}:{'|'.join(str(v) for v in key)}",
                    control_type=ControlType.BUSINESS_AGGREGATE,
                    tolerance=tolerance,
                ),
                source_value=source_map.get(key, Decimal("0")),
                target_value=target_map.get(key, Decimal("0")),
                details={
                    "dimensions": list(dimensions),
                    "dimension_values": list(key),
                    "measure": measure or "row_count",
                },
            )
        )
    return results


def terminal_outcome_control(
    reconciler: Reconciler,
    *,
    source_count: int,
    inserted: int,
    updated: int,
    unchanged: int,
    duplicate: int,
    stale: int,
    quarantined: int,
) -> ReconciliationResult:
    explained = inserted + updated + unchanged + duplicate + stale + quarantined
    return reconciler.compare(
        control=ReconciliationControl(
            name="terminal_outcome_accounting",
            control_type=ControlType.TERMINAL_OUTCOME,
            tolerance=Decimal("0"),
            description="every source row has exactly one terminal processing outcome",
        ),
        source_value=source_count,
        target_value=explained,
        details={
            "inserted": inserted,
            "updated": updated,
            "unchanged": unchanged,
            "duplicate": duplicate,
            "stale": stale,
            "quarantined": quarantined,
        },
    )


def product_pipeline_controls(
    *,
    run_id: str,
    raw: Sequence[Mapping[str, Any]],
    staged: Sequence[Mapping[str, Any]],
    core: Sequence[Mapping[str, Any]],
    merge_outcomes: Mapping[str, int],
) -> ReconciliationReport:
    reconciler = Reconciler(run_id)
    distinct_key_control(reconciler, raw, staged, key="event_id", name="raw_to_staging_event_ids")
    distinct_key_control(reconciler, staged, core, key="event_id", name="staging_to_core_event_ids")
    hash_total_control(
        reconciler,
        staged,
        core,
        fields=("event_id", "event_type", "business_key", "source_version"),
        name="staging_core_identity_hash",
    )
    terminal_outcome_control(
        reconciler,
        source_count=len(raw),
        inserted=int(merge_outcomes.get("inserted", 0)),
        updated=int(merge_outcomes.get("updated", 0)),
        unchanged=int(merge_outcomes.get("unchanged", 0)),
        duplicate=int(merge_outcomes.get("duplicate", 0)),
        stale=int(merge_outcomes.get("stale", 0)),
        quarantined=int(merge_outcomes.get("quarantined", 0)),
    )
    aggregate_control(
        reconciler,
        staged,
        core,
        dimensions=("event_type",),
        name="event_type_distribution",
    )
    return reconciler.report()
