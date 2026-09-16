# MLOps Control Plane

This folder implements the governed ML lifecycle for product discovery, EHR workflow quality, and platform operations. It is intentionally separated from clinical decision logic: no model here diagnoses disease, recommends treatment, or replaces clinician judgment.

## Model inventory

| Model | Trains on | Predicts / optimizes | Primary metric |
| --- | --- | --- | --- |
| `tool_recommendation_ranker` | de-identified recommendation impressions, clicks, tool starts/completions, favorites, specialty/context tokens, tool metadata and quality bands | ranked clinical-tool relevance for product discovery | NDCG@10 |
| `search_intent_router` | tokenized/hashed search events and downstream clicked/started entity categories | search intent class | macro F1 |
| `ehr_autofill_confidence` | de-identified autofill telemetry, FHIR resource class, field type, recency, terminology match, prior confirmation rates | probability an autofilled field is confirmed by the clinician | Brier score / high-confidence precision |
| `content_engagement_propensity` | content impressions, specialty/context and downstream engagement | probability an impression leads to deeper engagement | average precision |
| `pipeline_anomaly_detector` | pipeline counts, quarantine/duplicate/stale rates, source lag, query scan/queue/cost metrics | operational anomaly score | reviewed-alert precision |

## Code and data touched

`platform.py` references the contracts that connect ML to the rest of the repository:

- `dbt/models/core/fct_product_event.sql` — canonical event and attribution inputs.
- `dbt/models/marts/product/fct_recommendation_performance_daily.sql` — online/offline recommendation evaluation.
- `dbt/models/marts/product/fct_search_funnel_daily.sql` — search routing and discovery metrics.
- `dbt/models/marts/product/fct_integration_health_daily.sql` — autofill/writeback workflow quality.
- `observability/health.py` — model/service SLO integration.
- `reconciliation/reconcile.py` — feature/training-source reconciliation hooks.
- `snowflake/streams_tasks/incremental_processing.sql` — incremental feature refresh and monitoring.
- `site/index.html` and `api/telemetry.js` — operational evidence surface.

## Lifecycle

1. dbt produces point-in-time-correct feature sources in Snowflake.
2. A training job captures a `DatasetSnapshot` and feature-schema hash before reading.
3. `TrainingManifest` records windows, labels, parameters, code SHA, dataset snapshot, and feature definitions.
4. Training and evaluation produce immutable artifacts and a model card.
5. `PromotionController` enforces metric gates before `candidate -> shadow -> canary -> production`.
6. Inference logs contain model/version, latency, outcome metadata and feature-schema version—not raw clinical payloads.
7. Drift monitoring evaluates numeric PSI and categorical unseen-category rates by specialty/channel/integration segments.
8. Production rollback uses the previous immutable registry artifact and its preprocessing manifest.

## Safety and governance boundary

Direct identifiers and raw patient values are prohibited from the feature registry. The EHR autofill-confidence model trains on metadata about the extraction/confirmation process, not patient-level clinical values. Low confidence remains a review signal; it never triggers an automatic clinical action.

`model_registry.yml` is the machine-readable source of truth for labels, features, split strategy, evaluation thresholds, approval requirements, and monitoring cadence.
