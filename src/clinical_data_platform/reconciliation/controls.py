"""Reusable reconciliation controls for pipeline services."""
from __future__ import annotations

import dataclasses
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any


@dataclasses.dataclass(frozen=True, slots=True)
class ControlOutcome:
    name: str
    source_value: Decimal
    target_value: Decimal
    difference: Decimal
    tolerance: Decimal
    passed: bool
    details: Mapping[str, Any]


class ControlSuite:
    def __init__(self) -> None:
        self.outcomes: list[ControlOutcome] = []

    def compare(
        self,
        name: str,
        source_value: int | float | Decimal,
        target_value: int | float | Decimal,
        *,
        tolerance: Decimal = Decimal("0"),
        details: Mapping[str, Any] | None = None,
    ) -> ControlOutcome:
        source = Decimal(str(source_value))
        target = Decimal(str(target_value))
        difference = source - target
        outcome = ControlOutcome(
            name=name,
            source_value=source,
            target_value=target,
            difference=difference,
            tolerance=tolerance,
            passed=abs(difference) <= tolerance,
            details=dict(details or {}),
        )
        self.outcomes.append(outcome)
        return outcome

    @property
    def passed(self) -> bool:
        return all(outcome.passed for outcome in self.outcomes)

    def raise_for_failure(self) -> None:
        failures = [outcome for outcome in self.outcomes if not outcome.passed]
        if failures:
            names = ", ".join(outcome.name for outcome in failures)
            raise RuntimeError(f"reconciliation controls failed: {names}")


def reconcile_terminal_outcomes(
    suite: ControlSuite,
    *,
    source_count: int,
    inserted: int,
    updated: int,
    unchanged: int,
    duplicate: int,
    stale: int,
    quarantined: int,
) -> ControlOutcome:
    explained = inserted + updated + unchanged + duplicate + stale + quarantined
    return suite.compare(
        "terminal_outcomes",
        source_count,
        explained,
        details={
            "inserted": inserted,
            "updated": updated,
            "unchanged": unchanged,
            "duplicate": duplicate,
            "stale": stale,
            "quarantined": quarantined,
        },
    )


def reconcile_distinct_keys(
    suite: ControlSuite,
    source: Sequence[Mapping[str, Any]],
    target: Sequence[Mapping[str, Any]],
    *,
    key: str,
) -> ControlOutcome:
    source_keys = {row.get(key) for row in source if row.get(key) is not None}
    target_keys = {row.get(key) for row in target if row.get(key) is not None}
    missing = sorted(str(value) for value in source_keys - target_keys)
    extra = sorted(str(value) for value in target_keys - source_keys)
    return suite.compare(
        f"distinct_{key}",
        len(source_keys),
        len(target_keys),
        details={"missing_sample": missing[:20], "extra_sample": extra[:20]},
    )
