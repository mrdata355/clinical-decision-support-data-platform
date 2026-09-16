"""Data-quality evaluation for batch and warehouse models.

Rules are intentionally declarative. The engine records every evaluation, distinguishes
warning from blocking severity, and computes failure ratios so operational policy is not
hidden inside individual transformation functions.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import math
import re
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from enum import StrEnum
from typing import Any

UTC = dt.UTC


class Severity(StrEnum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class CheckStatus(StrEnum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


@dataclasses.dataclass(frozen=True, slots=True)
class QualityRule:
    name: str
    field: str | None
    severity: Severity
    predicate: Callable[[Mapping[str, Any]], bool]
    description: str
    max_failure_ratio: float = 0.0

    def __post_init__(self) -> None:
        if not 0 <= self.max_failure_ratio <= 1:
            raise ValueError("max_failure_ratio must be between 0 and 1")


@dataclasses.dataclass(frozen=True, slots=True)
class QualityFailure:
    rule_name: str
    field: str | None
    severity: Severity
    record_key: str
    description: str
    observed_value: Any = None


@dataclasses.dataclass(slots=True)
class RuleResult:
    rule_name: str
    severity: Severity
    total_rows: int
    failed_rows: int
    max_failure_ratio: float
    status: CheckStatus
    failures: list[QualityFailure]

    @property
    def failure_ratio(self) -> float:
        return self.failed_rows / self.total_rows if self.total_rows else 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "rule_name": self.rule_name,
            "severity": self.severity.value,
            "total_rows": self.total_rows,
            "failed_rows": self.failed_rows,
            "failure_ratio": round(self.failure_ratio, 6),
            "max_failure_ratio": self.max_failure_ratio,
            "status": self.status.value,
            "failures": [dataclasses.asdict(item) for item in self.failures],
        }


@dataclasses.dataclass(slots=True)
class QualityReport:
    model_name: str
    run_id: str
    evaluated_at: dt.datetime
    results: list[RuleResult]

    @property
    def blocking_failures(self) -> list[RuleResult]:
        return [
            result
            for result in self.results
            if result.status == CheckStatus.FAIL
            and result.severity in {Severity.ERROR, Severity.CRITICAL}
        ]

    @property
    def passed(self) -> bool:
        return not self.blocking_failures

    def summary(self) -> dict[str, Any]:
        status_counts = Counter(result.status.value for result in self.results)
        return {
            "model_name": self.model_name,
            "run_id": self.run_id,
            "evaluated_at": self.evaluated_at.isoformat(),
            "passed": self.passed,
            "rule_count": len(self.results),
            "status_counts": dict(status_counts),
            "blocking_rules": [result.rule_name for result in self.blocking_failures],
        }


class QualityEngine:
    def __init__(self, *, sample_failure_limit: int = 50) -> None:
        self.sample_failure_limit = sample_failure_limit

    def evaluate(
        self,
        *,
        model_name: str,
        run_id: str,
        records: Sequence[Mapping[str, Any]],
        rules: Sequence[QualityRule],
        record_key: Callable[[Mapping[str, Any]], str] | None = None,
    ) -> QualityReport:
        key_fn = record_key or (lambda row: str(row.get("event_id") or row.get("id") or "unknown"))
        results: list[RuleResult] = []
        for rule in rules:
            failures: list[QualityFailure] = []
            failed_count = 0
            for record in records:
                try:
                    passed = bool(rule.predicate(record))
                except Exception:
                    passed = False
                if passed:
                    continue
                failed_count += 1
                if len(failures) < self.sample_failure_limit:
                    failures.append(
                        QualityFailure(
                            rule_name=rule.name,
                            field=rule.field,
                            severity=rule.severity,
                            record_key=key_fn(record),
                            description=rule.description,
                            observed_value=record.get(rule.field) if rule.field else None,
                        )
                    )
            ratio = failed_count / len(records) if records else 0.0
            if ratio <= rule.max_failure_ratio:
                status = CheckStatus.PASS
            elif rule.severity in {Severity.INFO, Severity.WARNING}:
                status = CheckStatus.WARN
            else:
                status = CheckStatus.FAIL
            results.append(
                RuleResult(
                    rule_name=rule.name,
                    severity=rule.severity,
                    total_rows=len(records),
                    failed_rows=failed_count,
                    max_failure_ratio=rule.max_failure_ratio,
                    status=status,
                    failures=failures,
                )
            )
        return QualityReport(
            model_name=model_name,
            run_id=run_id,
            evaluated_at=dt.datetime.now(UTC),
            results=results,
        )


def not_null(field: str, severity: Severity = Severity.ERROR) -> QualityRule:
    return QualityRule(
        name=f"{field}_not_null",
        field=field,
        severity=severity,
        predicate=lambda row: row.get(field) is not None,
        description=f"{field} must be present",
    )


def not_blank(field: str, severity: Severity = Severity.ERROR) -> QualityRule:
    return QualityRule(
        name=f"{field}_not_blank",
        field=field,
        severity=severity,
        predicate=lambda row: row.get(field) is not None and str(row.get(field)).strip() != "",
        description=f"{field} must contain a non-empty value",
    )


def accepted_values(
    field: str,
    values: Iterable[Any],
    severity: Severity = Severity.ERROR,
    max_failure_ratio: float = 0.0,
) -> QualityRule:
    accepted = frozenset(values)
    return QualityRule(
        name=f"{field}_accepted_values",
        field=field,
        severity=severity,
        predicate=lambda row: row.get(field) in accepted,
        description=f"{field} must be one of {sorted(str(v) for v in accepted)}",
        max_failure_ratio=max_failure_ratio,
    )


def regex_match(
    field: str,
    pattern: str,
    severity: Severity = Severity.ERROR,
    max_failure_ratio: float = 0.0,
) -> QualityRule:
    compiled = re.compile(pattern)
    return QualityRule(
        name=f"{field}_regex",
        field=field,
        severity=severity,
        predicate=lambda row: row.get(field) is not None
        and compiled.fullmatch(str(row.get(field))) is not None,
        description=f"{field} must match {pattern}",
        max_failure_ratio=max_failure_ratio,
    )


def numeric_range(
    field: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
    severity: Severity = Severity.ERROR,
) -> QualityRule:
    def predicate(row: Mapping[str, Any]) -> bool:
        value = row.get(field)
        if value is None:
            return False
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return False
        if math.isnan(numeric) or math.isinf(numeric):
            return False
        if minimum is not None and numeric < minimum:
            return False
        if maximum is not None and numeric > maximum:
            return False
        return True

    return QualityRule(
        name=f"{field}_numeric_range",
        field=field,
        severity=severity,
        predicate=predicate,
        description=f"{field} must be numeric within [{minimum}, {maximum}]",
    )


def timestamp_not_future(
    field: str,
    *,
    tolerance: dt.timedelta = dt.timedelta(minutes=5),
    severity: Severity = Severity.ERROR,
) -> QualityRule:
    def predicate(row: Mapping[str, Any]) -> bool:
        value = row.get(field)
        if value is None:
            return False
        if isinstance(value, str):
            try:
                value = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return False
        if not isinstance(value, dt.datetime) or value.tzinfo is None:
            return False
        return value.astimezone(UTC) <= dt.datetime.now(UTC) + tolerance

    return QualityRule(
        name=f"{field}_not_future",
        field=field,
        severity=severity,
        predicate=predicate,
        description=f"{field} cannot exceed current time by more than {tolerance}",
    )


def product_event_rules() -> list[QualityRule]:
    return [
        not_blank("event_id"),
        not_blank("event_type"),
        not_blank("business_key"),
        not_blank("source_system"),
        not_null("event_ts"),
        timestamp_not_future("event_ts"),
        numeric_range("source_version", minimum=1),
        accepted_values(
            "event_type",
            {
                "session_start",
                "session_end",
                "search",
                "tool_view",
                "tool_start",
                "tool_complete",
                "favorite_add",
                "favorite_remove",
                "content_view",
                "integration_launch",
            },
        ),
        accepted_values(
            "channel",
            {"web", "mobile_web", "ios", "android", "integration", None},
            severity=Severity.WARNING,
            max_failure_ratio=0.005,
        ),
        regex_match(
            "country_code",
            r"^[A-Z]{2}$",
            severity=Severity.WARNING,
            max_failure_ratio=0.02,
        ),
    ]


def clinical_tool_rules() -> list[QualityRule]:
    return [
        not_blank("tool_id"),
        not_blank("tool_name"),
        not_blank("primary_specialty"),
        accepted_values(
            "publication_status",
            {"draft", "review", "published", "retired"},
        ),
        numeric_range("source_version", minimum=1),
        not_null("source_updated_at"),
    ]


def uniqueness_result(
    records: Sequence[Mapping[str, Any]],
    *,
    fields: Sequence[str],
    name: str,
    severity: Severity = Severity.ERROR,
) -> RuleResult:
    counts: Counter[tuple[Any, ...]] = Counter(
        tuple(record.get(field) for field in fields) for record in records
    )
    duplicates = [key for key, count in counts.items() if count > 1]
    failed = sum(counts[key] - 1 for key in duplicates)
    failures = [
        QualityFailure(
            rule_name=name,
            field=",".join(fields),
            severity=severity,
            record_key="|".join(str(value) for value in key),
            description=f"duplicate business key across fields {fields}",
            observed_value=key,
        )
        for key in duplicates[:50]
    ]
    return RuleResult(
        rule_name=name,
        severity=severity,
        total_rows=len(records),
        failed_rows=failed,
        max_failure_ratio=0.0,
        status=CheckStatus.PASS if failed == 0 else CheckStatus.FAIL,
        failures=failures,
    )


def freshness_result(
    *,
    model_name: str,
    maximum_age: dt.timedelta,
    latest_timestamp: dt.datetime | None,
    now: dt.datetime | None = None,
) -> RuleResult:
    now = now or dt.datetime.now(UTC)
    if latest_timestamp is None:
        failed = 1
        message = "no timestamp observed"
    else:
        if latest_timestamp.tzinfo is None:
            raise ValueError("latest_timestamp must be timezone-aware")
        age = now - latest_timestamp.astimezone(UTC)
        failed = int(age > maximum_age)
        message = f"age={age} maximum={maximum_age}"
    failures = []
    if failed:
        failures.append(
            QualityFailure(
                rule_name=f"{model_name}_freshness",
                field=None,
                severity=Severity.CRITICAL,
                record_key=model_name,
                description=message,
            )
        )
    return RuleResult(
        rule_name=f"{model_name}_freshness",
        severity=Severity.CRITICAL,
        total_rows=1,
        failed_rows=failed,
        max_failure_ratio=0,
        status=CheckStatus.FAIL if failed else CheckStatus.PASS,
        failures=failures,
    )
