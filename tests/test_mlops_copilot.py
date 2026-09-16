from __future__ import annotations

import datetime as dt

import pytest

from copilot.sql_rag import AnalyticsCopilot, DEFAULT_DOCUMENTS, SqlGuard
from mlops.platform import (
    DatasetSnapshot,
    DriftMonitor,
    FeatureSpec,
    GateStatus,
    InMemoryRegistry,
    ModelArtifact,
    ModelStage,
    PromotionController,
    PromotionGate,
    build_training_manifest,
    validate_feature_boundary,
)

UTC = dt.UTC


def test_sql_guard_rejects_raw_access_and_writes() -> None:
    guard = SqlGuard()
    with pytest.raises(ValueError):
        guard.validate("SELECT EVENT_ID FROM RAW.PRODUCT_EVENT")
    with pytest.raises(ValueError):
        guard.validate("DELETE FROM MART.SOME_TABLE")


def test_sql_guard_caps_rows_and_copilot_uses_governed_objects() -> None:
    plan = AnalyticsCopilot(DEFAULT_DOCUMENTS).ask("show ehr autofill performance")
    assert "MART.DT_LIVE_EHR_INTEGRATION" in plan.sql
    assert "LIMIT 500" in plan.sql
    assert "RAW." not in plan.sql


def test_feature_boundary_rejects_phi_payloads() -> None:
    with pytest.raises(ValueError):
        validate_feature_boundary(
            [
                FeatureSpec(
                    name="raw_patient_value",
                    dtype="string",
                    source_model="unsafe",
                    expression="raw_value",
                    contains_phi_payload=True,
                )
            ]
        )


def test_training_manifest_is_reproducible_and_leakage_safe() -> None:
    dataset = DatasetSnapshot(
        dataset_name="recommendation_training",
        snapshot_id="snap-001",
        as_of=dt.datetime(2026, 9, 1, tzinfo=UTC),
        row_count=10_000,
        min_event_ts=dt.datetime(2026, 5, 1, tzinfo=UTC),
        max_event_ts=dt.datetime(2026, 8, 31, tzinfo=UTC),
        feature_schema_hash="a" * 64,
        source_model_versions={"fct_product_event": "v1"},
        query_hash="b" * 64,
    )
    features = [
        FeatureSpec(
            name="tool_28d_completions",
            dtype="number",
            source_model="fct_tool_engagement_daily",
            expression="tool_completions_28d",
            point_in_time_column="event_date",
            ttl_hours=24,
        )
    ]
    manifest = build_training_manifest(
        model_name="tool_recommendation_ranker",
        algorithm="lightgbm_lambdarank",
        dataset=dataset,
        features=features,
        train_window=(
            dt.datetime(2026, 5, 1, tzinfo=UTC),
            dt.datetime(2026, 7, 31, tzinfo=UTC),
        ),
        validation_window=(
            dt.datetime(2026, 8, 1, 2, tzinfo=UTC),
            dt.datetime(2026, 8, 15, tzinfo=UTC),
        ),
        test_window=(
            dt.datetime(2026, 8, 15, 2, tzinfo=UTC),
            dt.datetime(2026, 8, 31, tzinfo=UTC),
        ),
        label_definition="graded_engagement",
        git_sha="c" * 40,
        parameters={"num_leaves": 31},
    )
    assert len(manifest.fingerprint()) == 64


def test_promotion_requires_metric_gate_and_valid_stage_transition() -> None:
    registry = InMemoryRegistry()
    artifact = ModelArtifact(
        model_name="tool_recommendation_ranker",
        version="2026.09.16.1",
        stage=ModelStage.CANDIDATE,
        created_at=dt.datetime.now(UTC),
        git_sha="d" * 40,
        dataset_snapshot_id="snap-001",
        feature_schema_hash="a" * 64,
        algorithm="lightgbm_lambdarank",
        metrics={"ndcg_at_10": 0.75},
        artifact_uri="s3://models/ranker/model.bin",
        preprocessing_uri="s3://models/ranker/preprocess.json",
        training_manifest_uri="s3://models/ranker/manifest.json",
        model_card_uri="s3://models/ranker/model-card.md",
    )
    registry.register(artifact)
    controller = PromotionController(registry)
    report = controller.evaluate(
        artifact,
        [PromotionGate("ndcg_at_10", ">=", 0.72)],
    )
    assert report.passed
    promoted = controller.promote_after_gate(artifact, report, ModelStage.SHADOW)
    assert promoted.stage == ModelStage.SHADOW


def test_drift_monitor_escalates_large_shift() -> None:
    monitor = DriftMonitor(warning=0.10, critical=0.20)
    reference = [float(index % 10) for index in range(1000)]
    current = [float(100 + (index % 10)) for index in range(1000)]
    signal = monitor.numeric("tool_28d_views", reference, current)
    assert signal.status in {GateStatus.WARN, GateStatus.FAIL}
