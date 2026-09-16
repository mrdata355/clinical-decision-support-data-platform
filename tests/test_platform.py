from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from clinical_data_platform.demo_data import build_merge_evidence, generate_events
from clinical_data_platform.ingestion.batch import (
    BatchPlanner,
    InMemoryWatermarkStore,
    SourceWindow,
)
from clinical_data_platform.metadata.catalog import default_catalog
from clinical_data_platform.runtime import (
    EventType,
    IterableSource,
    InMemorySink,
    MergePolicy,
    Normalizer,
    PipelineEngine,
    RecordStatus,
    RuntimeSettings,
    ValidatedRecord,
    build_demo_records,
)
from clinical_data_platform.validation.contracts import (
    DataContract,
    FieldSpec,
    compare_contracts,
    default_registry,
    enforce_compatibility,
)
from data_quality.quality_engine import (
    CheckStatus,
    QualityEngine,
    Severity,
    accepted_values,
    freshness_result,
    numeric_range,
    product_event_rules,
    uniqueness_result,
)
from reconciliation.reconcile import (
    Reconciler,
    distinct_key_control,
    hash_total_control,
    product_pipeline_controls,
    row_count_control,
    terminal_outcome_control,
)

UTC = dt.UTC


def test_batch_window_rejects_naive_timestamps() -> None:
    with pytest.raises(ValueError):
        SourceWindow(
            low=dt.datetime(2026, 1, 1),
            high=dt.datetime(2026, 1, 2),
        )


def test_batch_window_rejects_non_increasing_bounds() -> None:
    value = dt.datetime(2026, 1, 1, tzinfo=UTC)
    with pytest.raises(ValueError):
        SourceWindow(low=value, high=value)


def test_memory_watermark_store_prevents_regression() -> None:
    store = InMemoryWatermarkStore()
    later = dt.datetime(2026, 1, 2, tzinfo=UTC)
    earlier = dt.datetime(2026, 1, 1, tzinfo=UTC)
    store.commit("product", later, "run-1")
    with pytest.raises(ValueError):
        store.commit("product", earlier, "run-2")


def test_normalizer_produces_stable_hash() -> None:
    base = dt.datetime(2026, 1, 1, tzinfo=UTC)
    record = build_demo_records(base)[0]
    source = IterableSource([record], watermark=base + dt.timedelta(minutes=2))
    sink = InMemorySink()
    engine = PipelineEngine(RuntimeSettings(max_bad_record_ratio=1.0))
    engine.execute(
        pipeline_name="demo",
        source=source,
        sink=sink,
        lower_bound=base - dt.timedelta(seconds=1),
    )
    current = next(iter(sink.current.values()))
    assert len(current.canonical_hash) == 64


def test_pipeline_terminal_outcomes_balance() -> None:
    base = dt.datetime(2026, 1, 1, tzinfo=UTC)
    records = build_demo_records(base)
    source = IterableSource(records, watermark=base + dt.timedelta(minutes=5))
    sink = InMemorySink()
    metrics = PipelineEngine(RuntimeSettings(max_bad_record_ratio=0.2)).execute(
        pipeline_name="product_events",
        source=source,
        sink=sink,
        lower_bound=base - dt.timedelta(seconds=1),
    )
    explained = (
        metrics.inserted_count
        + metrics.updated_count
        + metrics.unchanged_count
        + metrics.duplicate_count
        + metrics.stale_count
        + metrics.quarantined_count
    )
    assert metrics.source_count == explained


def test_pipeline_is_idempotent_for_redelivery() -> None:
    base = dt.datetime(2026, 1, 1, tzinfo=UTC)
    records = build_demo_records(base)
    sink = InMemorySink()
    engine = PipelineEngine(RuntimeSettings(max_bad_record_ratio=0.2))
    source = IterableSource(records, watermark=base + dt.timedelta(minutes=5))
    first = engine.execute(
        pipeline_name="product_events",
        source=source,
        sink=sink,
        lower_bound=base - dt.timedelta(seconds=1),
    )
    second_source = IterableSource(records, watermark=base + dt.timedelta(minutes=5))
    second = engine.execute(
        pipeline_name="product_events_replay",
        source=second_source,
        sink=sink,
        lower_bound=base - dt.timedelta(seconds=1),
    )
    assert first.inserted_count > 0
    assert second.duplicate_count == len(records)


def test_product_event_contract_registry() -> None:
    registry = default_registry()
    contract = registry.get("product_event", 1)
    assert contract.business_key == ("event_id",)
    assert "payload" in contract.field_map()


def test_contract_detects_required_field_addition() -> None:
    previous = DataContract(
        name="example",
        version=1,
        business_key=("id",),
        fields=(FieldSpec("id", "string", nullable=False),),
    )
    candidate = DataContract(
        name="example",
        version=2,
        business_key=("id",),
        fields=(
            FieldSpec("id", "string", nullable=False),
            FieldSpec("new_required", "string", nullable=False),
        ),
    )
    issues = compare_contracts(previous, candidate)
    assert any(issue.code == "FIELD_ADDED" and issue.breaking for issue in issues)
    with pytest.raises(ValueError):
        enforce_compatibility(previous, candidate)


def test_quality_engine_accepts_valid_rows() -> None:
    now = dt.datetime.now(UTC) - dt.timedelta(minutes=1)
    rows = [
        {
            "event_id": "event-0001",
            "event_type": "tool_view",
            "business_key": "event-0001",
            "source_system": "product-api",
            "event_ts": now,
            "source_version": 1,
            "channel": "web",
            "country_code": "US",
        }
    ]
    report = QualityEngine().evaluate(
        model_name="product_event",
        run_id="run-1",
        records=rows,
        rules=product_event_rules(),
    )
    assert report.passed


def test_quality_engine_rejects_invalid_version() -> None:
    rows = [{"id": "1", "source_version": 0}]
    report = QualityEngine().evaluate(
        model_name="example",
        run_id="run-1",
        records=rows,
        rules=[numeric_range("source_version", minimum=1)],
    )
    assert not report.passed
    assert report.results[0].status == CheckStatus.FAIL


def test_warning_rule_does_not_block_report() -> None:
    rows = [{"channel": "unknown"}]
    report = QualityEngine().evaluate(
        model_name="example",
        run_id="run-1",
        records=rows,
        rules=[
            accepted_values(
                "channel",
                {"web", "ios"},
                severity=Severity.WARNING,
                max_failure_ratio=0,
            )
        ],
    )
    assert report.passed
    assert report.results[0].status == CheckStatus.WARN


def test_uniqueness_result_detects_duplicate_keys() -> None:
    rows = [{"event_id": "a"}, {"event_id": "a"}, {"event_id": "b"}]
    result = uniqueness_result(rows, fields=["event_id"], name="event_unique")
    assert result.failed_rows == 1
    assert result.status == CheckStatus.FAIL


def test_freshness_result() -> None:
    now = dt.datetime.now(UTC)
    result = freshness_result(
        model_name="product_events",
        maximum_age=dt.timedelta(minutes=30),
        latest_timestamp=now - dt.timedelta(minutes=5),
        now=now,
    )
    assert result.status == CheckStatus.PASS


def test_reconciliation_row_count() -> None:
    reconciler = Reconciler("run-1")
    source = [{"id": 1}, {"id": 2}]
    target = [{"id": 1}, {"id": 2}]
    result = row_count_control(reconciler, source, target)
    assert result.status.value == "PASS"


def test_reconciliation_distinct_keys_reports_gap() -> None:
    reconciler = Reconciler("run-1")
    source = [{"event_id": "a"}, {"event_id": "b"}]
    target = [{"event_id": "a"}]
    result = distinct_key_control(reconciler, source, target, key="event_id")
    assert result.status.value == "FAIL"
    assert result.details["missing_key_count"] == 1


def test_hash_total_is_order_independent() -> None:
    reconciler = Reconciler("run-1")
    left = [{"id": "a", "value": 1}, {"id": "b", "value": 2}]
    right = list(reversed(left))
    result = hash_total_control(reconciler, left, right, fields=["id", "value"])
    assert result.status.value == "PASS"


def test_terminal_outcome_control_balances() -> None:
    reconciler = Reconciler("run-1")
    result = terminal_outcome_control(
        reconciler,
        source_count=100,
        inserted=70,
        updated=10,
        unchanged=5,
        duplicate=5,
        stale=5,
        quarantined=5,
    )
    assert result.status.value == "PASS"


def test_demo_data_merge_evidence_balances() -> None:
    events = generate_events(days=2, events_per_day=100, seed=1)
    evidence = build_merge_evidence(events)
    assert evidence["balanced"] is True
    assert evidence["explained_count"] == evidence["source_count"]
    assert evidence["outcomes"]["inserted"] > 0


def test_catalog_impact_contains_product_mart() -> None:
    catalog = default_catalog()
    impacted = catalog.impact("raw.product_event")
    names = {asset.name for asset in impacted}
    assert "staging.product_event" in names
    assert "core.fct_product_event" in names
    assert "mart.tool_engagement_daily" in names


def test_product_pipeline_control_suite() -> None:
    raw = [
        {"event_id": "a", "event_type": "tool_view", "business_key": "a", "source_version": 1},
        {"event_id": "b", "event_type": "tool_complete", "business_key": "b", "source_version": 1},
    ]
    staged = list(raw)
    core = list(raw)
    report = product_pipeline_controls(
        run_id="run-1",
        raw=raw,
        staged=staged,
        core=core,
        merge_outcomes={"inserted": 2},
    )
    assert report.passed
