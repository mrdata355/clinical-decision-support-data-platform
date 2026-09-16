"""Data product ownership, classification, and lineage metadata."""
from __future__ import annotations

import dataclasses
from collections import defaultdict, deque
from collections.abc import Iterable, Mapping
from enum import StrEnum
from typing import Any


class Classification(StrEnum):
    PUBLIC = "public"
    INTERNAL = "internal"
    PSEUDONYMOUS = "pseudonymous"
    RESTRICTED = "restricted"


@dataclasses.dataclass(frozen=True, slots=True)
class DataAsset:
    name: str
    asset_type: str
    domain: str
    owner_team: str
    description: str
    classification: Classification = Classification.INTERNAL
    freshness_slo_minutes: int | None = None
    primary_key: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()


@dataclasses.dataclass(frozen=True, slots=True)
class LineageEdge:
    upstream: str
    downstream: str
    transformation: str


class DataCatalog:
    def __init__(self) -> None:
        self.assets: dict[str, DataAsset] = {}
        self.edges: list[LineageEdge] = []

    def register(self, asset: DataAsset) -> None:
        if asset.name in self.assets:
            raise ValueError(f"duplicate asset {asset.name}")
        self.assets[asset.name] = asset

    def link(self, upstream: str, downstream: str, transformation: str) -> None:
        if upstream not in self.assets:
            raise KeyError(f"unknown upstream asset {upstream}")
        if downstream not in self.assets:
            raise KeyError(f"unknown downstream asset {downstream}")
        self.edges.append(LineageEdge(upstream, downstream, transformation))

    def upstream(self, asset_name: str) -> list[DataAsset]:
        names = [edge.upstream for edge in self.edges if edge.downstream == asset_name]
        return [self.assets[name] for name in names]

    def downstream(self, asset_name: str) -> list[DataAsset]:
        names = [edge.downstream for edge in self.edges if edge.upstream == asset_name]
        return [self.assets[name] for name in names]

    def impact(self, asset_name: str) -> list[DataAsset]:
        if asset_name not in self.assets:
            raise KeyError(asset_name)
        visited: set[str] = {asset_name}
        queue: deque[str] = deque([asset_name])
        impacted: list[DataAsset] = []
        adjacency: dict[str, list[str]] = defaultdict(list)
        for edge in self.edges:
            adjacency[edge.upstream].append(edge.downstream)
        while queue:
            current = queue.popleft()
            for child in adjacency[current]:
                if child in visited:
                    continue
                visited.add(child)
                impacted.append(self.assets[child])
                queue.append(child)
        return impacted

    def by_owner(self, owner_team: str) -> list[DataAsset]:
        return [asset for asset in self.assets.values() if asset.owner_team == owner_team]

    def by_domain(self, domain: str) -> list[DataAsset]:
        return [asset for asset in self.assets.values() if asset.domain == domain]

    def export(self) -> dict[str, Any]:
        return {
            "assets": [dataclasses.asdict(asset) for asset in self.assets.values()],
            "lineage": [dataclasses.asdict(edge) for edge in self.edges],
        }


def default_catalog() -> DataCatalog:
    catalog = DataCatalog()
    assets = [
        DataAsset("raw.product_event", "table", "product", "data-platform", "Immutable product events", Classification.PSEUDONYMOUS, 20, ("raw_id",), ("raw", "events")),
        DataAsset("raw.clinical_tool_snapshot", "table", "content", "content-data", "Clinical tool metadata snapshots", Classification.INTERNAL, 1440, ("ingest_run_id", "source_tool_id"), ("raw", "catalog")),
        DataAsset("staging.product_event", "model", "product", "data-platform", "Normalized product events", Classification.PSEUDONYMOUS, 30, ("event_id",), ("staging",)),
        DataAsset("core.dim_clinical_tool", "dimension", "content", "content-data", "Conformed clinical tool dimension", Classification.INTERNAL, 1440, ("clinical_tool_key",), ("core", "dimension")),
        DataAsset("core.fct_product_event", "fact", "product", "data-platform", "Canonical product event fact", Classification.PSEUDONYMOUS, 30, ("event_id",), ("core", "fact")),
        DataAsset("mart.tool_engagement_daily", "mart", "product", "analytics", "Tool engagement and conversion metrics", Classification.INTERNAL, 60, ("event_date", "clinical_tool_key", "channel", "country_code"), ("mart", "product")),
        DataAsset("mart.specialty_engagement", "mart", "product", "analytics", "Specialty-level usage metrics", Classification.INTERNAL, 60, ("event_date", "primary_specialty"), ("mart", "product")),
        DataAsset("ops.pipeline_run", "control", "platform", "data-platform", "Pipeline run audit", Classification.INTERNAL, 5, ("run_id",), ("ops", "audit")),
        DataAsset("ops.reconciliation_result", "control", "platform", "data-platform", "Source-to-target reconciliation", Classification.INTERNAL, 5, ("run_id", "control_name"), ("ops", "quality")),
    ]
    for asset in assets:
        catalog.register(asset)
    catalog.link("raw.product_event", "staging.product_event", "normalize + deduplicate by event_id/version")
    catalog.link("raw.clinical_tool_snapshot", "core.dim_clinical_tool", "catalog conformance and version resolution")
    catalog.link("staging.product_event", "core.fct_product_event", "dimension resolution + deterministic merge")
    catalog.link("core.dim_clinical_tool", "core.fct_product_event", "surrogate-key resolution")
    catalog.link("core.fct_product_event", "mart.tool_engagement_daily", "daily engagement aggregation")
    catalog.link("core.fct_product_event", "mart.specialty_engagement", "specialty aggregation")
    return catalog
