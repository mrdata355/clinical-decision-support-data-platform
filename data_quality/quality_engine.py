"""Model-aware data-quality engine for the clinical decision-support platform.

The engine evaluates record contracts, uniqueness, freshness, referential integrity,
distribution drift, volume anomalies, quality-rating invariants and model-level gates.
Every result carries the dbt/warehouse model name and run id so failures can be persisted
in OPS.DATA_QUALITY_RESULT without losing provenance.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import math
import re
import statistics
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
    rule_version: str = "1"

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
    model_name: str = "unbound"
    run_id: str = "unbound"
    rule_version: str = "1"
    metric_value: float | None = None
    threshold: Any = None

    @property
    def failure_ratio(self) -> float:
        return self.failed_rows / self.total_rows if self.total_rows else 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "model_name": self.model_name,
            "run_id": self.run_id,
            "rule_name": self.rule_name,
            "rule_version": self.rule_version,
            "severity": self.severity.value,
            "total_rows": self.total_rows,
            "failed_rows": self.failed_rows,
            "failure_ratio": round(self.failure_ratio, 6),
            "max_failure_ratio": self.max_failure_ratio,
            "metric_value": self.metric_value,
            "threshold": self.threshold,
            "status": self.status.value,
            "failures": [dataclasses.asdict(item) for item in self.failures],
        }


@dataclasses.dataclass(slots=True)
class QualityReport:
    model_name: str
    run_id: str
    evaluated_at: dt.datetime
    results: list[RuleResult]
    row_count: int = 0
    contract_version: str | None = None
    transform_version: str | None = None

    @property
    def blocking_failures(self) -> list[RuleResult]:
        return [r for r in self.results if r.status == CheckStatus.FAIL and r.severity in {Severity.ERROR, Severity.CRITICAL}]

    @property
    def passed(self) -> bool:
        return not self.blocking_failures

    def summary(self) -> dict[str, Any]:
        status_counts = Counter(result.status.value for result in self.results)
        return {
            "model_name": self.model_name,
            "run_id": self.run_id,
            "evaluated_at": self.evaluated_at.isoformat(),
            "row_count": self.row_count,
            "contract_version": self.contract_version,
            "transform_version": self.transform_version,
            "passed": self.passed,
            "rule_count": len(self.results),
            "status_counts": dict(status_counts),
            "blocking_rules": [result.rule_name for result in self.blocking_failures],
        }


class QualityEngine:
    def __init__(self, *, sample_failure_limit: int = 50) -> None:
        if sample_failure_limit < 1:
            raise ValueError("sample_failure_limit must be positive")
        self.sample_failure_limit = sample_failure_limit

    def evaluate(
        self,
        *,
        model_name: str,
        run_id: str,
        records: Sequence[Mapping[str, Any]],
        rules: Sequence[QualityRule],
        record_key: Callable[[Mapping[str, Any]], str] | None = None,
        contract_version: str | None = None,
        transform_version: str | None = None,
    ) -> QualityReport:
        key_fn = record_key or (lambda row: str(row.get("event_id") or row.get("tool_id") or row.get("content_id") or row.get("rating_id") or row.get("id") or "unknown"))
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
                    failures.append(QualityFailure(rule.name, rule.field, rule.severity, key_fn(record), rule.description, record.get(rule.field) if rule.field else None))
            ratio = failed_count / len(records) if records else 0.0
            if ratio <= rule.max_failure_ratio:
                status = CheckStatus.PASS
            elif rule.severity in {Severity.INFO, Severity.WARNING}:
                status = CheckStatus.WARN
            else:
                status = CheckStatus.FAIL
            results.append(RuleResult(rule.name, rule.severity, len(records), failed_count, rule.max_failure_ratio, status, failures, model_name=model_name, run_id=run_id, rule_version=rule.rule_version, metric_value=ratio, threshold=rule.max_failure_ratio))
        return QualityReport(model_name, run_id, dt.datetime.now(UTC), results, row_count=len(records), contract_version=contract_version, transform_version=transform_version)


def not_null(field: str, severity: Severity = Severity.ERROR) -> QualityRule:
    return QualityRule(f"{field}_not_null", field, severity, lambda row: row.get(field) is not None, f"{field} must be present")


def not_blank(field: str, severity: Severity = Severity.ERROR) -> QualityRule:
    return QualityRule(f"{field}_not_blank", field, severity, lambda row: row.get(field) is not None and str(row.get(field)).strip() != "", f"{field} must contain a non-empty value")


def accepted_values(field: str, values: Iterable[Any], severity: Severity = Severity.ERROR, max_failure_ratio: float = 0.0) -> QualityRule:
    accepted = frozenset(values)
    return QualityRule(f"{field}_accepted_values", field, severity, lambda row: row.get(field) in accepted, f"{field} must be one of {sorted(str(v) for v in accepted)}", max_failure_ratio)


def regex_match(field: str, pattern: str, severity: Severity = Severity.ERROR, max_failure_ratio: float = 0.0) -> QualityRule:
    compiled = re.compile(pattern)
    return QualityRule(f"{field}_regex", field, severity, lambda row: row.get(field) is not None and compiled.fullmatch(str(row.get(field))) is not None, f"{field} must match {pattern}", max_failure_ratio)


def numeric_range(field: str, *, minimum: float | None = None, maximum: float | None = None, severity: Severity = Severity.ERROR, max_failure_ratio: float = 0.0) -> QualityRule:
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
        return (minimum is None or numeric >= minimum) and (maximum is None or numeric <= maximum)
    return QualityRule(f"{field}_numeric_range", field, severity, predicate, f"{field} must be numeric within [{minimum}, {maximum}]", max_failure_ratio)


def timestamp_not_future(field: str, *, tolerance: dt.timedelta = dt.timedelta(minutes=5), severity: Severity = Severity.ERROR) -> QualityRule:
    def predicate(row: Mapping[str, Any]) -> bool:
        value = row.get(field)
        if isinstance(value, str):
            try:
                value = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return False
        return isinstance(value, dt.datetime) and value.tzinfo is not None and value.astimezone(UTC) <= dt.datetime.now(UTC) + tolerance
    return QualityRule(f"{field}_not_future", field, severity, predicate, f"{field} cannot exceed current time by more than {tolerance}")


def product_event_rules() -> list[QualityRule]:
    return [
        not_blank("event_id"), not_blank("event_type"), not_blank("business_key"), not_blank("source_system"),
        not_null("event_ts"), timestamp_not_future("event_ts"), numeric_range("source_version", minimum=1),
        accepted_values("event_type", {"session_start", "session_end", "search", "search_impression", "search_click", "tool_view", "tool_start", "tool_complete", "result_copy", "result_share", "favorite_add", "favorite_remove", "content_view", "guideline_view", "decision_aid_view", "quality_rating_view", "creator_profile_view", "cme_content_view", "cme_credit_earned", "recommendation_impression", "recommendation_click", "ehr_launch", "ehr_autofill", "ehr_input_confirmed", "ehr_writeback", "integration_launch"}),
        accepted_values("channel", {"web", "mobile_web", "ios", "android", "ehr", "api", "integration", None}, severity=Severity.WARNING, max_failure_ratio=0.005),
        regex_match("country_code", r"^[A-Z]{2}$", severity=Severity.WARNING, max_failure_ratio=0.02),
    ]


def clinical_tool_rules() -> list[QualityRule]:
    return [
        not_blank("tool_id"), not_blank("tool_name"), not_blank("primary_specialty"), not_blank("tool_type"),
        accepted_values("publication_status", {"draft", "review", "published", "retired"}),
        accepted_values("tool_type", {"diagnosis", "rule_out", "prognosis", "formula", "treatment", "algorithm", "risk_assessment", "drug_dosing", "diagnostic_criteria", "decision_aid"}),
        numeric_range("source_version", minimum=1), not_null("source_updated_at"),
    ]


def quality_rating_rules() -> list[QualityRule]:
    return [
        not_blank("rating_id"), not_blank("tool_id"), not_blank("rating_version"),
        numeric_range("overall_score", minimum=0, maximum=10),
        numeric_range("scientific_soundness_score", minimum=0, maximum=6),
        numeric_range("importance_score", minimum=0, maximum=3),
        numeric_range("usability_feasibility_score", minimum=0, maximum=1),
        QualityRule("quality_rating_components_sum", None, Severity.ERROR, lambda r: abs(float(r.get("overall_score", -999)) - (float(r.get("scientific_soundness_score", 0)) + float(r.get("importance_score", 0)) + float(r.get("usability_feasibility_score", 0)))) <= 0.01, "overall score must equal component scores within 0.01"),
        numeric_range("source_version", minimum=1), not_null("source_updated_at"),
    ]


def content_rules() -> list[QualityRule]:
    return [
        not_blank("content_id"), not_blank("content_type"), not_blank("title"),
        accepted_values("publication_status", {"draft", "review", "published", "retired"}),
        accepted_values("content_type", {"when_to_use", "pearls_pitfalls", "why_use", "next_steps", "evidence", "formula", "guideline_summary", "decision_aid", "creator_insight", "cme_module", "editorial"}),
        numeric_range("source_version", minimum=1), not_null("source_updated_at"),
    ]


def uniqueness_result(records: Sequence[Mapping[str, Any]], *, fields: Sequence[str], name: str, severity: Severity = Severity.ERROR, model_name: str = "unbound", run_id: str = "unbound") -> RuleResult:
    counts: Counter[tuple[Any, ...]] = Counter(tuple(record.get(field) for field in fields) for record in records)
    duplicates = [key for key, count in counts.items() if count > 1]
    failed = sum(counts[key] - 1 for key in duplicates)
    failures = [QualityFailure(name, ",".join(fields), severity, "|".join(str(value) for value in key), f"duplicate business key across fields {fields}", key) for key in duplicates[:50]]
    return RuleResult(name, severity, len(records), failed, 0.0, CheckStatus.PASS if failed == 0 else CheckStatus.FAIL, failures, model_name=model_name, run_id=run_id)


def freshness_result(*, model_name: str, maximum_age: dt.timedelta, latest_timestamp: dt.datetime | None, now: dt.datetime | None = None, run_id: str = "unbound") -> RuleResult:
    now = now or dt.datetime.now(UTC)
    if latest_timestamp is None:
        failed, age_seconds, message = 1, None, "no timestamp observed"
    else:
        if latest_timestamp.tzinfo is None:
            raise ValueError("latest_timestamp must be timezone-aware")
        age = now - latest_timestamp.astimezone(UTC)
        age_seconds = age.total_seconds()
        failed, message = int(age > maximum_age), f"age={age} maximum={maximum_age}"
    failures = [QualityFailure(f"{model_name}_freshness", None, Severity.CRITICAL, model_name, message)] if failed else []
    return RuleResult(f"{model_name}_freshness", Severity.CRITICAL, 1, failed, 0, CheckStatus.FAIL if failed else CheckStatus.PASS, failures, model_name=model_name, run_id=run_id, metric_value=age_seconds, threshold=maximum_age.total_seconds())


def referential_integrity_result(*, model_name: str, run_id: str, rows: Sequence[Mapping[str, Any]], field: str, valid_values: set[Any], severity: Severity = Severity.ERROR, max_failure_ratio: float = 0.0) -> RuleResult:
    failures_raw = [row for row in rows if row.get(field) is not None and row.get(field) not in valid_values]
    ratio = len(failures_raw) / len(rows) if rows else 0.0
    status = CheckStatus.PASS if ratio <= max_failure_ratio else CheckStatus.WARN if severity in {Severity.INFO, Severity.WARNING} else CheckStatus.FAIL
    failures = [QualityFailure(f"{field}_referential_integrity", field, severity, str(row.get("event_id") or row.get("id") or "unknown"), f"{field} does not resolve to an approved dimension key", row.get(field)) for row in failures_raw[:50]]
    return RuleResult(f"{field}_referential_integrity", severity, len(rows), len(failures_raw), max_failure_ratio, status, failures, model_name=model_name, run_id=run_id, metric_value=ratio, threshold=max_failure_ratio)


def distribution_drift_result(*, model_name: str, run_id: str, current: Sequence[Mapping[str, Any]], field: str, baseline_distribution: Mapping[str, float], warn_l1: float = 0.15, fail_l1: float = 0.30) -> RuleResult:
    counts = Counter(str(row.get(field)) for row in current)
    total = sum(counts.values()) or 1
    current_dist = {key: value / total for key, value in counts.items()}
    keys = set(current_dist) | set(baseline_distribution)
    l1 = sum(abs(current_dist.get(k, 0.0) - float(baseline_distribution.get(k, 0.0))) for k in keys) / 2
    status = CheckStatus.FAIL if l1 > fail_l1 else CheckStatus.WARN if l1 > warn_l1 else CheckStatus.PASS
    severity = Severity.ERROR if status == CheckStatus.FAIL else Severity.WARNING
    return RuleResult(f"{field}_distribution_drift", severity, len(current), int(status != CheckStatus.PASS), 0.0, status, [], model_name=model_name, run_id=run_id, metric_value=round(l1, 6), threshold={"warn": warn_l1, "fail": fail_l1})


def volume_anomaly_result(*, model_name: str, run_id: str, current_count: int, historical_counts: Sequence[int], warn_ratio: tuple[float, float] = (0.6, 1.8), fail_ratio: tuple[float, float] = (0.3, 3.0)) -> RuleResult:
    baseline = statistics.median(historical_counts) if historical_counts else 0
    ratio = current_count / baseline if baseline else 1.0
    status = CheckStatus.FAIL if ratio < fail_ratio[0] or ratio > fail_ratio[1] else CheckStatus.WARN if ratio < warn_ratio[0] or ratio > warn_ratio[1] else CheckStatus.PASS
    severity = Severity.ERROR if status == CheckStatus.FAIL else Severity.WARNING
    return RuleResult("row_volume_anomaly", severity, 1, int(status != CheckStatus.PASS), 0.0, status, [], model_name=model_name, run_id=run_id, metric_value=round(ratio, 6), threshold={"warn": warn_ratio, "fail": fail_ratio, "baseline_median": baseline})


def quality_suite_for(model_name: str) -> list[QualityRule]:
    suites = {
        "stg_product_events": product_event_rules,
        "dim_clinical_tool": clinical_tool_rules,
        "stg_clinical_tools": clinical_tool_rules,
        "stg_quality_ratings": quality_rating_rules,
        "dim_content": content_rules,
        "stg_content": content_rules,
    }
    factory = suites.get(model_name)
    return factory() if factory else []
