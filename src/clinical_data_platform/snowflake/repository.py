"""Snowflake repository methods used by services and tests."""
from __future__ import annotations

import dataclasses
from collections.abc import Mapping, Sequence
from typing import Any

from clinical_data_platform.loaders.snowflake import SnowflakeLoader


@dataclasses.dataclass(frozen=True, slots=True)
class ModelFreshness:
    object_name: str
    minutes_since_success: int | None
    last_status: str | None
    last_source_count: int | None
    last_published_count: int | None


@dataclasses.dataclass(frozen=True, slots=True)
class MergeEvidence:
    run_id: str
    source_count: int
    inserted_count: int
    updated_count: int
    duplicate_count: int
    stale_count: int
    quarantined_count: int
    unchanged_count: int

    @property
    def explained_count(self) -> int:
        return (
            self.inserted_count
            + self.updated_count
            + self.duplicate_count
            + self.stale_count
            + self.quarantined_count
            + self.unchanged_count
        )

    @property
    def balanced(self) -> bool:
        return self.source_count == self.explained_count


class ClinicalWarehouseRepository:
    def __init__(self, loader: SnowflakeLoader) -> None:
        self.loader = loader

    def pipeline_health(self) -> list[ModelFreshness]:
        rows = self.loader.fetch_all(
            """
            SELECT
              PIPELINE_NAME,
              MINUTES_SINCE_SUCCESS,
              LAST_STATUS,
              LAST_SOURCE_COUNT,
              LAST_PUBLISHED_COUNT
            FROM CLINICAL_ANALYTICS.OPS.V_PIPELINE_HEALTH
            ORDER BY PIPELINE_NAME
            """
        )
        return [
            ModelFreshness(
                object_name=str(row[0]),
                minutes_since_success=int(row[1]) if row[1] is not None else None,
                last_status=str(row[2]) if row[2] is not None else None,
                last_source_count=int(row[3]) if row[3] is not None else None,
                last_published_count=int(row[4]) if row[4] is not None else None,
            )
            for row in rows
        ]

    def merge_evidence(self, run_id: str) -> MergeEvidence | None:
        rows = self.loader.fetch_all(
            """
            SELECT
              RUN_ID,
              SOURCE_COUNT,
              INSERTED_COUNT,
              UPDATED_COUNT,
              DUPLICATE_COUNT,
              STALE_COUNT,
              QUARANTINED_COUNT,
              UNCHANGED_COUNT
            FROM CLINICAL_ANALYTICS.OPS.PIPELINE_RUN
            WHERE RUN_ID=%(run_id)s
            """,
            {"run_id": run_id},
        )
        if not rows:
            return None
        row = rows[0]
        return MergeEvidence(
            run_id=str(row[0]),
            source_count=int(row[1] or 0),
            inserted_count=int(row[2] or 0),
            updated_count=int(row[3] or 0),
            duplicate_count=int(row[4] or 0),
            stale_count=int(row[5] or 0),
            quarantined_count=int(row[6] or 0),
            unchanged_count=int(row[7] or 0),
        )

    def reconciliation_results(self, run_id: str) -> list[dict[str, Any]]:
        rows = self.loader.fetch_all(
            """
            SELECT CONTROL_NAME,SOURCE_VALUE,TARGET_VALUE,DIFFERENCE_VALUE,TOLERANCE_VALUE,STATUS,DETAILS
            FROM CLINICAL_ANALYTICS.OPS.RECONCILIATION_RESULT
            WHERE RUN_ID=%(run_id)s
            ORDER BY CONTROL_NAME
            """,
            {"run_id": run_id},
        )
        return [
            {
                "control_name": row[0],
                "source_value": row[1],
                "target_value": row[2],
                "difference_value": row[3],
                "tolerance_value": row[4],
                "status": row[5],
                "details": row[6],
            }
            for row in rows
        ]

    def product_daily(self, start_date: str, end_date: str) -> list[dict[str, Any]]:
        rows = self.loader.fetch_all(
            """
            SELECT EVENT_DATE,CHANNEL,COUNTRY_CODE,SESSIONS_STARTED,SEARCHES,TOOL_VIEWS,
                   TOOL_STARTS,TOOL_COMPLETIONS,CONTENT_VIEWS,UNIQUE_USERS,UNIQUE_ACCOUNTS,COMPLETION_RATE
            FROM CLINICAL_ANALYTICS.MART.V_PRODUCT_DAILY
            WHERE EVENT_DATE >= %(start_date)s AND EVENT_DATE < %(end_date)s
            ORDER BY EVENT_DATE,CHANNEL,COUNTRY_CODE
            """,
            {"start_date": start_date, "end_date": end_date},
        )
        columns = [
            "event_date","channel","country_code","sessions_started","searches","tool_views",
            "tool_starts","tool_completions","content_views","unique_users","unique_accounts","completion_rate",
        ]
        return [dict(zip(columns, row, strict=True)) for row in rows]

    def quarantine_summary(self, days: int = 7) -> list[dict[str, Any]]:
        rows = self.loader.fetch_all(
            """
            SELECT ERROR_CODE,COUNT(*) AS ROWS,COUNT(DISTINCT RUN_ID) AS RUNS,
                   MIN(QUARANTINED_AT) AS FIRST_SEEN,MAX(QUARANTINED_AT) AS LAST_SEEN
            FROM CLINICAL_ANALYTICS.OPS.QUARANTINE_EVENT
            WHERE QUARANTINED_AT >= DATEADD('day', -%(days)s, CURRENT_TIMESTAMP())
            GROUP BY ERROR_CODE
            ORDER BY ROWS DESC
            """,
            {"days": days},
        )
        return [
            {
                "error_code": row[0],
                "rows": int(row[1]),
                "runs": int(row[2]),
                "first_seen": row[3],
                "last_seen": row[4],
            }
            for row in rows
        ]
