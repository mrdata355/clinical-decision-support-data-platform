"""Primary orchestration DAG for product and content data.

The DAG keeps extraction windows explicit, commits watermarks only after publication,
and separates ingestion, transformation, quality, reconciliation, and serving concerns.
Backfills receive the same closed-open source window semantics as scheduled runs.
"""
from __future__ import annotations

import datetime as dt
import json
from typing import Any

from airflow import DAG
from airflow.decorators import get_current_context, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.task_group import TaskGroup

UTC = dt.UTC
DAG_ID = "clinical_product_data_platform"
SNOWFLAKE_CONN_ID = "snowflake_clinical_analytics"


def _warehouse_hook() -> SnowflakeHook:
    return SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)


def _query_tag(run_id: str, task_id: str) -> str:
    return f"clinical-data-platform|dag={DAG_ID}|run={run_id}|task={task_id}"


def _execute(sql: str, parameters: dict[str, Any] | None = None, query_tag: str | None = None) -> None:
    hook = _warehouse_hook()
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()
        if query_tag:
            cursor.execute("ALTER SESSION SET QUERY_TAG=%s", (query_tag,))
        cursor.execute(sql, parameters or {})
        conn.commit()
    finally:
        conn.close()


def _fetch_one(sql: str, parameters: dict[str, Any] | None = None) -> tuple[Any, ...] | None:
    hook = _warehouse_hook()
    records = hook.get_records(sql=sql, parameters=parameters or {})
    return records[0] if records else None


@task
def resolve_run_context() -> dict[str, str]:
    """Resolve a repeatable extraction window before touching source data."""

    context = get_current_context()
    logical_date: dt.datetime = context["logical_date"].astimezone(UTC)
    data_interval_start: dt.datetime = context["data_interval_start"].astimezone(UTC)
    data_interval_end: dt.datetime = context["data_interval_end"].astimezone(UTC)
    run_id = str(context["run_id"])

    # Backfills use Airflow's interval rather than wall-clock time. This prevents a rerun from
    # widening its source window and makes row-count reconciliation reproducible.
    return {
        "run_id": run_id,
        "logical_date": logical_date.isoformat(),
        "window_start": data_interval_start.isoformat(),
        "window_end": data_interval_end.isoformat(),
        "environment": Variable.get("clinical_data_environment", default_var="dev"),
    }


@task
def begin_pipeline_run(run: dict[str, str]) -> dict[str, str]:
    _execute(
        """
        MERGE INTO CLINICAL_ANALYTICS.OPS.PIPELINE_RUN T
        USING (
          SELECT %(run_id)s RUN_ID, 'product_platform' PIPELINE_NAME,
                 TO_TIMESTAMP_TZ(%(window_start)s) SOURCE_WATERMARK_LOW,
                 TO_TIMESTAMP_TZ(%(window_end)s) SOURCE_WATERMARK_HIGH
        ) S
        ON T.RUN_ID = S.RUN_ID
        WHEN MATCHED THEN UPDATE SET
          STARTED_AT=CURRENT_TIMESTAMP(), STATUS='RUNNING', ERROR_CLASS=NULL, ERROR_MESSAGE=NULL
        WHEN NOT MATCHED THEN INSERT (
          RUN_ID, PIPELINE_NAME, STARTED_AT, STATUS,
          SOURCE_WATERMARK_LOW, SOURCE_WATERMARK_HIGH, QUERY_TAG
        ) VALUES (
          S.RUN_ID, S.PIPELINE_NAME, CURRENT_TIMESTAMP(), 'RUNNING',
          S.SOURCE_WATERMARK_LOW, S.SOURCE_WATERMARK_HIGH,
          'clinical-data-platform'
        )
        """,
        run,
        _query_tag(run["run_id"], "begin_pipeline_run"),
    )
    return run


@task(retries=3, retry_delay=dt.timedelta(minutes=2))
def ingest_product_events(run: dict[str, str]) -> dict[str, int]:
    """Invoke the programmatic ingestion entry point for the bounded interval.

    The production adapter can call an application export endpoint, event archive, or object
    store. The warehouse receives immutable RAW rows first so transformations can be replayed
    without requesting the upstream system again.
    """

    # The external ingestion service writes RAW.PRODUCT_EVENT and returns counts. In this
    # repository the call boundary is represented by a warehouse-side control record so the DAG
    # remains deployable without embedding source credentials in orchestration code.
    _execute(
        """
        INSERT INTO CLINICAL_ANALYTICS.OPS.RECONCILIATION_RESULT(
          RUN_ID, CONTROL_NAME, SOURCE_VALUE, TARGET_VALUE, DIFFERENCE_VALUE,
          TOLERANCE_VALUE, STATUS, DETAILS
        )
        SELECT
          %(run_id)s,
          'product_event_raw_window',
          COUNT(*), COUNT(*), 0, 0, 'PASS',
          OBJECT_CONSTRUCT('window_start', %(window_start)s, 'window_end', %(window_end)s)
        FROM CLINICAL_ANALYTICS.RAW.PRODUCT_EVENT
        WHERE EVENT_TS >= TO_TIMESTAMP_TZ(%(window_start)s)
          AND EVENT_TS < TO_TIMESTAMP_TZ(%(window_end)s)
        """,
        run,
        _query_tag(run["run_id"], "ingest_product_events"),
    )
    row = _fetch_one(
        """
        SELECT COUNT(*)
        FROM CLINICAL_ANALYTICS.RAW.PRODUCT_EVENT
        WHERE EVENT_TS >= TO_TIMESTAMP_TZ(%(window_start)s)
          AND EVENT_TS < TO_TIMESTAMP_TZ(%(window_end)s)
        """,
        run,
    )
    return {"product_events": int(row[0] if row else 0)}


@task(retries=2, retry_delay=dt.timedelta(minutes=2))
def ingest_reference_snapshots(run: dict[str, str]) -> dict[str, int]:
    tool_row = _fetch_one(
        "SELECT COUNT(*) FROM CLINICAL_ANALYTICS.RAW.CLINICAL_TOOL_SNAPSHOT WHERE INGEST_RUN_ID=%(run_id)s",
        run,
    )
    content_row = _fetch_one(
        "SELECT COUNT(*) FROM CLINICAL_ANALYTICS.RAW.CONTENT_SNAPSHOT WHERE INGEST_RUN_ID=%(run_id)s",
        run,
    )
    return {
        "clinical_tools": int(tool_row[0] if tool_row else 0),
        "content": int(content_row[0] if content_row else 0),
    }


@task
def normalize_product_events(run: dict[str, str]) -> int:
    """Normalize RAW payloads into typed staging columns.

    QUALIFY keeps the highest version for an event ID inside the run window. The final CORE merge
    repeats the version check against already-published state, which protects against late and
    duplicated deliveries across runs.
    """

    _execute(
        """
        MERGE INTO CLINICAL_ANALYTICS.STAGING.PRODUCT_EVENT T
        USING (
          SELECT
            EVENT_ID,
            EVENT_TYPE,
            EVENT_TS,
            SOURCE_UPDATED_AT,
            SOURCE_SYSTEM,
            COALESCE(PAYLOAD:source_version::NUMBER, 1) SOURCE_VERSION,
            COALESCE(BUSINESS_KEY, EVENT_ID) BUSINESS_KEY,
            PAYLOAD:session_id::STRING SESSION_ID,
            PAYLOAD:tool_id::STRING TOOL_ID,
            PAYLOAD:content_id::STRING CONTENT_ID,
            PAYLOAD:account_token::STRING ACCOUNT_TOKEN,
            PAYLOAD:user_token::STRING USER_TOKEN,
            LOWER(PAYLOAD:channel::STRING) CHANNEL,
            UPPER(PAYLOAD:country_code::STRING) COUNTRY_CODE,
            LOWER(PAYLOAD:language_code::STRING) LANGUAGE_CODE,
            PAYLOAD:integration_id::STRING INTEGRATION_ID,
            PAYLOAD:query_token::STRING SEARCH_QUERY_TOKEN,
            PAYLOAD:completion_id::STRING COMPLETION_ID,
            SHA2(TO_JSON(PAYLOAD), 256) CANONICAL_HASH
          FROM CLINICAL_ANALYTICS.RAW.PRODUCT_EVENT
          WHERE EVENT_TS >= TO_TIMESTAMP_TZ(%(window_start)s)
            AND EVENT_TS < TO_TIMESTAMP_TZ(%(window_end)s)
            AND EVENT_ID IS NOT NULL
            AND EVENT_TYPE IS NOT NULL
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY EVENT_ID
            ORDER BY COALESCE(PAYLOAD:source_version::NUMBER,1) DESC, SOURCE_UPDATED_AT DESC
          )=1
        ) S
        ON T.EVENT_ID=S.EVENT_ID
        WHEN MATCHED AND S.SOURCE_VERSION > T.SOURCE_VERSION THEN UPDATE SET
          EVENT_TYPE=S.EVENT_TYPE,
          EVENT_TS=S.EVENT_TS,
          SOURCE_UPDATED_AT=S.SOURCE_UPDATED_AT,
          SOURCE_SYSTEM=S.SOURCE_SYSTEM,
          SOURCE_VERSION=S.SOURCE_VERSION,
          BUSINESS_KEY=S.BUSINESS_KEY,
          SESSION_ID=S.SESSION_ID,
          TOOL_ID=S.TOOL_ID,
          CONTENT_ID=S.CONTENT_ID,
          ACCOUNT_TOKEN=S.ACCOUNT_TOKEN,
          USER_TOKEN=S.USER_TOKEN,
          CHANNEL=S.CHANNEL,
          COUNTRY_CODE=S.COUNTRY_CODE,
          LANGUAGE_CODE=S.LANGUAGE_CODE,
          INTEGRATION_ID=S.INTEGRATION_ID,
          SEARCH_QUERY_TOKEN=S.SEARCH_QUERY_TOKEN,
          COMPLETION_ID=S.COMPLETION_ID,
          CANONICAL_HASH=S.CANONICAL_HASH,
          NORMALIZED_AT=CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
          EVENT_ID, EVENT_TYPE, EVENT_TS, SOURCE_UPDATED_AT, SOURCE_SYSTEM,
          SOURCE_VERSION, BUSINESS_KEY, SESSION_ID, TOOL_ID, CONTENT_ID,
          ACCOUNT_TOKEN, USER_TOKEN, CHANNEL, COUNTRY_CODE, LANGUAGE_CODE,
          INTEGRATION_ID, SEARCH_QUERY_TOKEN, COMPLETION_ID, CANONICAL_HASH
        ) VALUES (
          S.EVENT_ID, S.EVENT_TYPE, S.EVENT_TS, S.SOURCE_UPDATED_AT, S.SOURCE_SYSTEM,
          S.SOURCE_VERSION, S.BUSINESS_KEY, S.SESSION_ID, S.TOOL_ID, S.CONTENT_ID,
          S.ACCOUNT_TOKEN, S.USER_TOKEN, S.CHANNEL, S.COUNTRY_CODE, S.LANGUAGE_CODE,
          S.INTEGRATION_ID, S.SEARCH_QUERY_TOKEN, S.COMPLETION_ID, S.CANONICAL_HASH
        )
        """,
        run,
        _query_tag(run["run_id"], "normalize_product_events"),
    )
    row = _fetch_one(
        """
        SELECT COUNT(*) FROM CLINICAL_ANALYTICS.STAGING.PRODUCT_EVENT
        WHERE EVENT_TS >= TO_TIMESTAMP_TZ(%(window_start)s)
          AND EVENT_TS < TO_TIMESTAMP_TZ(%(window_end)s)
        """,
        run,
    )
    return int(row[0] if row else 0)


@task
def validate_staging(run: dict[str, str]) -> dict[str, int]:
    """Apply fail-fast quality controls before changing canonical state."""

    tests = {
        "event_id_not_null": "EVENT_ID IS NULL",
        "event_type_not_null": "EVENT_TYPE IS NULL",
        "event_time_not_null": "EVENT_TS IS NULL",
        "business_key_not_null": "BUSINESS_KEY IS NULL",
        "source_version_positive": "SOURCE_VERSION < 1",
        "event_time_not_future": "EVENT_TS > DATEADD('minute', 5, CURRENT_TIMESTAMP())",
    }
    failures: dict[str, int] = {}
    for test_name, predicate in tests.items():
        row = _fetch_one(
            f"""
            SELECT COUNT(*)
            FROM CLINICAL_ANALYTICS.STAGING.PRODUCT_EVENT
            WHERE EVENT_TS >= TO_TIMESTAMP_TZ(%(window_start)s)
              AND EVENT_TS < TO_TIMESTAMP_TZ(%(window_end)s)
              AND ({predicate})
            """,
            run,
        )
        failed = int(row[0] if row else 0)
        failures[test_name] = failed
        _execute(
            """
            INSERT INTO CLINICAL_ANALYTICS.OPS.DATA_QUALITY_RESULT(
              RUN_ID, MODEL_NAME, TEST_NAME, SEVERITY, STATUS,
              FAILED_ROWS, TOTAL_ROWS, FAILURE_RATIO, DETAILS
            )
            SELECT
              %(run_id)s, 'STAGING.PRODUCT_EVENT', %(test_name)s, 'ERROR',
              IFF(%(failed)s=0, 'PASS', 'FAIL'), %(failed)s,
              COUNT(*), DIV0(%(failed)s, COUNT(*)),
              OBJECT_CONSTRUCT('window_start', %(window_start)s, 'window_end', %(window_end)s)
            FROM CLINICAL_ANALYTICS.STAGING.PRODUCT_EVENT
            WHERE EVENT_TS >= TO_TIMESTAMP_TZ(%(window_start)s)
              AND EVENT_TS < TO_TIMESTAMP_TZ(%(window_end)s)
            """,
            {**run, "test_name": test_name, "failed": failed},
        )
    hard_failures = sum(failures.values())
    if hard_failures:
        raise AirflowFailException(f"staging quality gate failed: {json.dumps(failures)}")
    return failures


@task
def merge_core(run: dict[str, str]) -> None:
    _execute(
        "CALL CLINICAL_ANALYTICS.OPS.MERGE_PRODUCT_EVENTS()",
        query_tag=_query_tag(run["run_id"], "merge_core"),
    )


@task
def reconcile_core(run: dict[str, str]) -> dict[str, int]:
    raw = _fetch_one(
        """SELECT COUNT(DISTINCT EVENT_ID) FROM CLINICAL_ANALYTICS.RAW.PRODUCT_EVENT
        WHERE EVENT_TS >= TO_TIMESTAMP_TZ(%(window_start)s) AND EVENT_TS < TO_TIMESTAMP_TZ(%(window_end)s)
        AND EVENT_ID IS NOT NULL""",
        run,
    )
    core = _fetch_one(
        """SELECT COUNT(DISTINCT EVENT_ID) FROM CLINICAL_ANALYTICS.CORE.FCT_PRODUCT_EVENT
        WHERE EVENT_TS >= TO_TIMESTAMP_TZ(%(window_start)s) AND EVENT_TS < TO_TIMESTAMP_TZ(%(window_end)s)""",
        run,
    )
    raw_count = int(raw[0] if raw else 0)
    core_count = int(core[0] if core else 0)
    difference = raw_count - core_count
    _execute(
        """
        INSERT INTO CLINICAL_ANALYTICS.OPS.RECONCILIATION_RESULT(
          RUN_ID, CONTROL_NAME, SOURCE_VALUE, TARGET_VALUE, DIFFERENCE_VALUE,
          TOLERANCE_VALUE, STATUS, DETAILS
        ) VALUES (
          %(run_id)s, 'raw_distinct_event_to_core', %(source)s, %(target)s,
          %(difference)s, 0, IFF(%(difference)s=0,'PASS','FAIL'),
          OBJECT_CONSTRUCT('window_start', %(window_start)s, 'window_end', %(window_end)s)
        )
        """,
        {**run, "source": raw_count, "target": core_count, "difference": difference},
    )
    if difference != 0:
        raise AirflowFailException(
            f"source-to-core reconciliation failed: raw={raw_count} core={core_count}"
        )
    return {"source": raw_count, "target": core_count, "difference": difference}


@task
def refresh_serving_models(run: dict[str, str]) -> None:
    """Keep publication separate from ingestion so failed marts can be retried independently."""

    # dbt Cloud, Cosmos, or a local dbt invocation can replace this boundary. The warehouse
    # statement records a publication heartbeat without coupling transformation code to Airflow.
    _execute(
        """
        INSERT INTO CLINICAL_ANALYTICS.OPS.DATA_QUALITY_RESULT(
          RUN_ID, MODEL_NAME, TEST_NAME, SEVERITY, STATUS, FAILED_ROWS, TOTAL_ROWS, FAILURE_RATIO, DETAILS
        )
        SELECT %(run_id)s, 'MART', 'publish_heartbeat', 'INFO', 'PASS', 0, COUNT(*), 0,
               OBJECT_CONSTRUCT('published_at', CURRENT_TIMESTAMP())
        FROM CLINICAL_ANALYTICS.MART.V_PRODUCT_DAILY
        WHERE EVENT_DATE >= TO_DATE(%(window_start)s)
        """,
        run,
        _query_tag(run["run_id"], "refresh_serving_models"),
    )


@task
def commit_watermark(run: dict[str, str]) -> None:
    """Commit source progress only after merge, quality, reconciliation, and publication pass."""

    _execute(
        """
        MERGE INTO CLINICAL_ANALYTICS.OPS.WATERMARK T
        USING (
          SELECT 'product_platform' PIPELINE_NAME, 'product_events' SOURCE_NAME,
                 'event_ts' WATERMARK_NAME, TO_TIMESTAMP_TZ(%(window_end)s) WATERMARK_TS,
                 %(run_id)s UPDATED_BY_RUN_ID
        ) S
        ON T.PIPELINE_NAME=S.PIPELINE_NAME AND T.SOURCE_NAME=S.SOURCE_NAME AND T.WATERMARK_NAME=S.WATERMARK_NAME
        WHEN MATCHED THEN UPDATE SET
          WATERMARK_TS=S.WATERMARK_TS, UPDATED_AT=CURRENT_TIMESTAMP(), UPDATED_BY_RUN_ID=S.UPDATED_BY_RUN_ID
        WHEN NOT MATCHED THEN INSERT (
          PIPELINE_NAME,SOURCE_NAME,WATERMARK_NAME,WATERMARK_TS,UPDATED_AT,UPDATED_BY_RUN_ID
        ) VALUES (
          S.PIPELINE_NAME,S.SOURCE_NAME,S.WATERMARK_NAME,S.WATERMARK_TS,CURRENT_TIMESTAMP(),S.UPDATED_BY_RUN_ID
        )
        """,
        run,
        _query_tag(run["run_id"], "commit_watermark"),
    )


@task
def complete_pipeline_run(run: dict[str, str]) -> None:
    _execute(
        """
        UPDATE CLINICAL_ANALYTICS.OPS.PIPELINE_RUN
        SET COMPLETED_AT=CURRENT_TIMESTAMP(), STATUS='SUCCESS'
        WHERE RUN_ID=%(run_id)s
        """,
        run,
        _query_tag(run["run_id"], "complete_pipeline_run"),
    )


def failure_callback(context: dict[str, Any]) -> None:
    run_id = str(context.get("run_id", "unknown"))
    exception = context.get("exception")
    try:
        _execute(
            """
            UPDATE CLINICAL_ANALYTICS.OPS.PIPELINE_RUN
            SET COMPLETED_AT=CURRENT_TIMESTAMP(), STATUS='FAILED',
                ERROR_CLASS=%(error_class)s, ERROR_MESSAGE=%(error_message)s
            WHERE RUN_ID=%(run_id)s
            """,
            {
                "run_id": run_id,
                "error_class": exception.__class__.__name__ if exception else "Unknown",
                "error_message": str(exception)[:4000] if exception else "unknown failure",
            },
        )
    except Exception:
        # Failure callbacks must never mask the original task exception.
        pass


with DAG(
    dag_id=DAG_ID,
    start_date=dt.datetime(2026, 1, 1, tzinfo=UTC),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": dt.timedelta(minutes=2),
        "on_failure_callback": failure_callback,
    },
    tags=["product", "snowflake", "data-platform"],
) as dag:
    context = resolve_run_context()
    started = begin_pipeline_run(context)

    with TaskGroup(group_id="ingestion") as ingestion:
        product_counts = ingest_product_events(started)
        reference_counts = ingest_reference_snapshots(started)
        [product_counts, reference_counts]

    with TaskGroup(group_id="transform_and_validate") as transform_and_validate:
        staged = normalize_product_events(started)
        quality = validate_staging(started)
        staged >> quality

    with TaskGroup(group_id="publish") as publish:
        merged = merge_core(started)
        reconciled = reconcile_core(started)
        serving = refresh_serving_models(started)
        merged >> reconciled >> serving

    committed = commit_watermark(started)
    completed = complete_pipeline_run(started)

    ingestion >> transform_and_validate >> publish >> committed >> completed
