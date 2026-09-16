"""Snowflake loading utilities.

The loader centralizes query tags, transaction boundaries, temporary staging, COPY behavior,
parameter binding, deterministic MERGE statements, and audit writes. Application code should
not concatenate untrusted values into SQL identifiers; identifiers are validated before use.
"""
from __future__ import annotations

import contextlib
import dataclasses
import re
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any

import snowflake.connector
from snowflake.connector import SnowflakeConnection

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


class WarehouseError(RuntimeError):
    pass


@dataclasses.dataclass(frozen=True, slots=True)
class SnowflakeConfig:
    account: str
    user: str
    password: str | None = None
    private_key: bytes | None = None
    warehouse: str = "COMPUTE_WH"
    database: str = "CLINICAL_ANALYTICS"
    schema: str = "RAW"
    role: str | None = None
    query_tag: str = "clinical-data-platform"


@dataclasses.dataclass(frozen=True, slots=True)
class MergeColumn:
    name: str
    update: bool = True
    insert: bool = True


@dataclasses.dataclass(frozen=True, slots=True)
class MergeSpec:
    target_table: str
    stage_table: str
    match_columns: tuple[str, ...]
    columns: tuple[MergeColumn, ...]
    version_column: str | None = "SOURCE_VERSION"
    updated_at_column: str | None = "SOURCE_UPDATED_AT"


def _safe_identifier(value: str) -> str:
    parts = value.split(".")
    if not parts or any(not IDENTIFIER.fullmatch(part) for part in parts):
        raise ValueError(f"unsafe SQL identifier: {value!r}")
    return ".".join(parts)


class SnowflakeLoader:
    def __init__(self, config: SnowflakeConfig) -> None:
        self.config = config

    def connect(self) -> SnowflakeConnection:
        kwargs: dict[str, Any] = {
            "account": self.config.account,
            "user": self.config.user,
            "warehouse": self.config.warehouse,
            "database": self.config.database,
            "schema": self.config.schema,
            "session_parameters": {"QUERY_TAG": self.config.query_tag},
        }
        if self.config.role:
            kwargs["role"] = self.config.role
        if self.config.private_key:
            kwargs["private_key"] = self.config.private_key
        elif self.config.password:
            kwargs["password"] = self.config.password
        else:
            raise ValueError("password or private_key is required")
        return snowflake.connector.connect(**kwargs)

    @contextlib.contextmanager
    def transaction(self) -> Iterator[SnowflakeConnection]:
        conn = self.connect()
        try:
            conn.autocommit(False)
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def execute(self, sql: str, params: Mapping[str, Any] | None = None) -> None:
        with self.transaction() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params or {})
            finally:
                cursor.close()

    def fetch_all(self, sql: str, params: Mapping[str, Any] | None = None) -> list[tuple[Any, ...]]:
        conn = self.connect()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, params or {})
                return list(cursor.fetchall())
            finally:
                cursor.close()
        finally:
            conn.close()

    def create_temp_stage_table(
        self,
        conn: SnowflakeConnection,
        *,
        target_table: str,
        stage_name: str,
    ) -> str:
        target = _safe_identifier(target_table)
        stage = _safe_identifier(stage_name)
        cursor = conn.cursor()
        try:
            cursor.execute(f"CREATE OR REPLACE TEMP TABLE {stage} LIKE {target}")
        finally:
            cursor.close()
        return stage

    def insert_rows(
        self,
        conn: SnowflakeConnection,
        *,
        table: str,
        columns: Sequence[str],
        rows: Iterable[Sequence[Any]],
        batch_size: int = 5_000,
    ) -> int:
        table_sql = _safe_identifier(table)
        column_sql = ", ".join(_safe_identifier(column) for column in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO {table_sql} ({column_sql}) VALUES ({placeholders})"
        cursor = conn.cursor()
        count = 0
        batch: list[Sequence[Any]] = []
        try:
            for row in rows:
                if len(row) != len(columns):
                    raise WarehouseError(
                        f"row length {len(row)} does not match columns {len(columns)}"
                    )
                batch.append(row)
                if len(batch) >= batch_size:
                    cursor.executemany(sql, batch)
                    count += len(batch)
                    batch.clear()
            if batch:
                cursor.executemany(sql, batch)
                count += len(batch)
        finally:
            cursor.close()
        return count

    @staticmethod
    def build_merge_sql(spec: MergeSpec) -> str:
        target = _safe_identifier(spec.target_table)
        stage = _safe_identifier(spec.stage_table)
        match = " AND ".join(
            f"T.{_safe_identifier(column)} = S.{_safe_identifier(column)}"
            for column in spec.match_columns
        )
        update_columns = [column.name for column in spec.columns if column.update]
        insert_columns = [column.name for column in spec.columns if column.insert]

        update_guard = "TRUE"
        if spec.version_column:
            version = _safe_identifier(spec.version_column)
            update_guard = f"S.{version} > T.{version}"
            if spec.updated_at_column:
                updated = _safe_identifier(spec.updated_at_column)
                update_guard = (
                    f"(S.{version} > T.{version} OR "
                    f"(S.{version} = T.{version} AND S.{updated} > T.{updated}))"
                )

        update_sql = ",\n        ".join(
            f"T.{_safe_identifier(column)} = S.{_safe_identifier(column)}"
            for column in update_columns
        )
        insert_names = ", ".join(_safe_identifier(column) for column in insert_columns)
        insert_values = ", ".join(f"S.{_safe_identifier(column)}" for column in insert_columns)

        return f"""
MERGE INTO {target} T
USING {stage} S
ON {match}
WHEN MATCHED AND {update_guard} THEN UPDATE SET
        {update_sql}
WHEN NOT MATCHED THEN INSERT ({insert_names})
VALUES ({insert_values})
""".strip()

    def merge(self, conn: SnowflakeConnection, spec: MergeSpec) -> dict[str, int]:
        sql = self.build_merge_sql(spec)
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
            # Snowflake MERGE returns counts in a driver-version-dependent row. Keep the parser
            # defensive; audit logic can fall back to explicit before/after controls if absent.
            if not row:
                return {"inserted": 0, "updated": 0, "deleted": 0}
            numeric = [int(value or 0) for value in row if isinstance(value, (int, float))]
            return {
                "inserted": numeric[0] if len(numeric) > 0 else 0,
                "updated": numeric[1] if len(numeric) > 1 else 0,
                "deleted": numeric[2] if len(numeric) > 2 else 0,
            }
        finally:
            cursor.close()

    def load_and_merge(
        self,
        *,
        target_table: str,
        stage_table: str,
        columns: Sequence[str],
        rows: Iterable[Sequence[Any]],
        match_columns: Sequence[str],
        version_column: str | None = "SOURCE_VERSION",
        updated_at_column: str | None = "SOURCE_UPDATED_AT",
    ) -> dict[str, int]:
        columns_tuple = tuple(MergeColumn(name=column) for column in columns)
        spec = MergeSpec(
            target_table=target_table,
            stage_table=stage_table,
            match_columns=tuple(match_columns),
            columns=columns_tuple,
            version_column=version_column,
            updated_at_column=updated_at_column,
        )
        with self.transaction() as conn:
            self.create_temp_stage_table(conn, target_table=target_table, stage_name=stage_table)
            staged = self.insert_rows(conn, table=stage_table, columns=columns, rows=rows)
            result = self.merge(conn, spec)
            return {"staged": staged, **result}

    def record_pipeline_audit(
        self,
        *,
        run_id: str,
        pipeline_name: str,
        status: str,
        source_count: int,
        accepted_count: int,
        quarantined_count: int,
        inserted_count: int,
        updated_count: int,
        duplicate_count: int,
        stale_count: int,
        unchanged_count: int,
    ) -> None:
        sql = """
        MERGE INTO CLINICAL_ANALYTICS.OPS.PIPELINE_RUN T
        USING (
          SELECT %(run_id)s RUN_ID, %(pipeline_name)s PIPELINE_NAME
        ) S
        ON T.RUN_ID=S.RUN_ID
        WHEN MATCHED THEN UPDATE SET
          STATUS=%(status)s,
          COMPLETED_AT=IFF(%(status)s IN ('SUCCESS','FAILED'), CURRENT_TIMESTAMP(), T.COMPLETED_AT),
          SOURCE_COUNT=%(source_count)s,
          ACCEPTED_COUNT=%(accepted_count)s,
          QUARANTINED_COUNT=%(quarantined_count)s,
          INSERTED_COUNT=%(inserted_count)s,
          UPDATED_COUNT=%(updated_count)s,
          DUPLICATE_COUNT=%(duplicate_count)s,
          STALE_COUNT=%(stale_count)s,
          UNCHANGED_COUNT=%(unchanged_count)s
        WHEN NOT MATCHED THEN INSERT (
          RUN_ID,PIPELINE_NAME,STARTED_AT,STATUS,SOURCE_COUNT,ACCEPTED_COUNT,
          QUARANTINED_COUNT,INSERTED_COUNT,UPDATED_COUNT,DUPLICATE_COUNT,STALE_COUNT,UNCHANGED_COUNT
        ) VALUES (
          %(run_id)s,%(pipeline_name)s,CURRENT_TIMESTAMP(),%(status)s,%(source_count)s,
          %(accepted_count)s,%(quarantined_count)s,%(inserted_count)s,%(updated_count)s,
          %(duplicate_count)s,%(stale_count)s,%(unchanged_count)s
        )
        """
        self.execute(
            sql,
            {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "status": status,
                "source_count": source_count,
                "accepted_count": accepted_count,
                "quarantined_count": quarantined_count,
                "inserted_count": inserted_count,
                "updated_count": updated_count,
                "duplicate_count": duplicate_count,
                "stale_count": stale_count,
                "unchanged_count": unchanged_count,
            },
        )
