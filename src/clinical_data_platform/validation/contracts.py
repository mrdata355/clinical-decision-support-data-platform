"""Data contract registry and compatibility checks."""
from __future__ import annotations

import dataclasses
from collections.abc import Mapping, Sequence
from enum import StrEnum
from typing import Any


class CompatibilityMode(StrEnum):
    BACKWARD = "BACKWARD"
    FORWARD = "FORWARD"
    FULL = "FULL"
    NONE = "NONE"


@dataclasses.dataclass(frozen=True, slots=True)
class FieldSpec:
    name: str
    data_type: str
    nullable: bool = True
    description: str = ""
    classification: str = "internal"


@dataclasses.dataclass(frozen=True, slots=True)
class DataContract:
    name: str
    version: int
    business_key: tuple[str, ...]
    fields: tuple[FieldSpec, ...]
    compatibility: CompatibilityMode = CompatibilityMode.BACKWARD
    owner: str = "data-platform"

    def field_map(self) -> dict[str, FieldSpec]:
        return {field.name: field for field in self.fields}


@dataclasses.dataclass(frozen=True, slots=True)
class CompatibilityIssue:
    code: str
    field: str | None
    message: str
    breaking: bool


class ContractRegistry:
    def __init__(self) -> None:
        self._contracts: dict[tuple[str, int], DataContract] = {}

    def register(self, contract: DataContract) -> None:
        key = (contract.name, contract.version)
        if key in self._contracts:
            raise ValueError(f"contract already registered: {key}")
        self._contracts[key] = contract

    def get(self, name: str, version: int) -> DataContract:
        try:
            return self._contracts[(name, version)]
        except KeyError as exc:
            raise KeyError(f"unknown contract {name} v{version}") from exc

    def latest(self, name: str) -> DataContract:
        versions = [version for contract_name, version in self._contracts if contract_name == name]
        if not versions:
            raise KeyError(f"unknown contract {name}")
        return self._contracts[(name, max(versions))]

    def versions(self, name: str) -> list[int]:
        return sorted(version for contract_name, version in self._contracts if contract_name == name)


def compare_contracts(previous: DataContract, candidate: DataContract) -> list[CompatibilityIssue]:
    issues: list[CompatibilityIssue] = []
    if previous.name != candidate.name:
        return [
            CompatibilityIssue(
                code="CONTRACT_NAME_CHANGED",
                field=None,
                message=f"{previous.name} -> {candidate.name}",
                breaking=True,
            )
        ]
    if candidate.version <= previous.version:
        issues.append(
            CompatibilityIssue(
                code="VERSION_NOT_INCREMENTED",
                field=None,
                message="candidate contract version must be greater than previous version",
                breaking=True,
            )
        )

    old_fields = previous.field_map()
    new_fields = candidate.field_map()

    for name, old in old_fields.items():
        if name not in new_fields:
            issues.append(
                CompatibilityIssue(
                    code="FIELD_REMOVED",
                    field=name,
                    message=f"field {name} was removed",
                    breaking=True,
                )
            )
            continue
        new = new_fields[name]
        if old.data_type != new.data_type:
            issues.append(
                CompatibilityIssue(
                    code="FIELD_TYPE_CHANGED",
                    field=name,
                    message=f"{name}: {old.data_type} -> {new.data_type}",
                    breaking=True,
                )
            )
        if old.nullable and not new.nullable:
            issues.append(
                CompatibilityIssue(
                    code="FIELD_BECAME_REQUIRED",
                    field=name,
                    message=f"{name} changed from nullable to required",
                    breaking=True,
                )
            )

    for name, new in new_fields.items():
        if name in old_fields:
            continue
        issues.append(
            CompatibilityIssue(
                code="FIELD_ADDED",
                field=name,
                message=f"field {name} was added",
                breaking=not new.nullable,
            )
        )

    if previous.business_key != candidate.business_key:
        issues.append(
            CompatibilityIssue(
                code="BUSINESS_KEY_CHANGED",
                field=None,
                message=f"business key changed from {previous.business_key} to {candidate.business_key}",
                breaking=True,
            )
        )
    return issues


def enforce_compatibility(previous: DataContract, candidate: DataContract) -> None:
    if candidate.compatibility == CompatibilityMode.NONE:
        return
    issues = compare_contracts(previous, candidate)
    breaking = [issue for issue in issues if issue.breaking]
    if breaking:
        details = "; ".join(f"{issue.code}:{issue.field or '-'}" for issue in breaking)
        raise ValueError(f"contract compatibility failed: {details}")


def validate_record(contract: DataContract, record: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    fields = contract.field_map()
    for name, spec in fields.items():
        value = record.get(name)
        if value is None and not spec.nullable:
            errors.append(f"{name}: required field is null")
    for key in contract.business_key:
        if record.get(key) in (None, ""):
            errors.append(f"{key}: business key component is missing")
    return errors


def product_event_contract() -> DataContract:
    return DataContract(
        name="product_event",
        version=1,
        business_key=("event_id",),
        owner="product-data",
        fields=(
            FieldSpec("event_id", "string", nullable=False, description="stable event identifier"),
            FieldSpec("event_type", "string", nullable=False),
            FieldSpec("event_ts", "timestamp_tz", nullable=False),
            FieldSpec("source_updated_at", "timestamp_tz", nullable=False),
            FieldSpec("source_system", "string", nullable=False),
            FieldSpec("schema_version", "integer", nullable=False),
            FieldSpec("source_version", "integer", nullable=False),
            FieldSpec("business_key", "string", nullable=False),
            FieldSpec("session_id", "string"),
            FieldSpec("tool_id", "string"),
            FieldSpec("content_id", "string"),
            FieldSpec("account_token", "string", classification="pseudonymous"),
            FieldSpec("user_token", "string", classification="pseudonymous"),
            FieldSpec("channel", "string"),
            FieldSpec("country_code", "string"),
            FieldSpec("language_code", "string"),
            FieldSpec("integration_id", "string"),
            FieldSpec("completion_id", "string"),
            FieldSpec("payload", "variant", nullable=False),
        ),
    )


def clinical_tool_contract() -> DataContract:
    return DataContract(
        name="clinical_tool",
        version=1,
        business_key=("tool_id",),
        owner="content-data",
        fields=(
            FieldSpec("tool_id", "string", nullable=False),
            FieldSpec("tool_slug", "string", nullable=False),
            FieldSpec("tool_name", "string", nullable=False),
            FieldSpec("primary_specialty", "string", nullable=False),
            FieldSpec("condition_group", "string"),
            FieldSpec("evidence_status", "string"),
            FieldSpec("publication_status", "string", nullable=False),
            FieldSpec("first_published_at", "timestamp_tz"),
            FieldSpec("last_reviewed_at", "timestamp_tz"),
            FieldSpec("source_version", "integer", nullable=False),
            FieldSpec("source_updated_at", "timestamp_tz", nullable=False),
        ),
    )


def default_registry() -> ContractRegistry:
    registry = ContractRegistry()
    registry.register(product_event_contract())
    registry.register(clinical_tool_contract())
    return registry
