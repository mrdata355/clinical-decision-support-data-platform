"""Governed RAG + SQL planning for the analytics/reporting copilot.

The module retrieves a small semantic context from a curated catalog, asks an optional LLM
adapter for a query plan, then validates and rewrites SQL through a strict read-only policy.
The deterministic planner keeps the reference implementation useful without credentials.
"""
from __future__ import annotations

import dataclasses
import json
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any, Protocol


TOKEN = re.compile(r"[a-z0-9_]+")
FORBIDDEN = re.compile(
    r"\b(insert|update|delete|merge|copy|put|get|remove|alter|drop|truncate|create|grant|revoke|call|use)\b",
    re.IGNORECASE,
)
TABLE_REF = re.compile(r"\b(?:from|join)\s+([A-Za-z0-9_.$\"]+)", re.IGNORECASE)
LIMIT_RE = re.compile(r"\blimit\s+(\d+)\b", re.IGNORECASE)


@dataclasses.dataclass(frozen=True, slots=True)
class CatalogDocument:
    doc_id: str
    title: str
    description: str
    objects: tuple[str, ...]
    dimensions: tuple[str, ...]
    measures: tuple[str, ...]
    examples: tuple[str, ...] = ()

    @property
    def text(self) -> str:
        return " ".join(
            [self.title, self.description, *self.objects, *self.dimensions, *self.measures, *self.examples]
        ).lower()


@dataclasses.dataclass(frozen=True, slots=True)
class RetrievedContext:
    document: CatalogDocument
    score: float


@dataclasses.dataclass(frozen=True, slots=True)
class SqlPolicy:
    allowed_schemas: frozenset[str] = frozenset({"MART", "MART_DBT", "OPS"})
    max_rows: int = 500
    deny_select_star: bool = True


@dataclasses.dataclass(frozen=True, slots=True)
class SqlPlan:
    question: str
    sql: str
    rationale: str
    evidence_docs: tuple[str, ...]
    confidence: float


class LLMPlanner(Protocol):
    def plan(self, question: str, context: Sequence[RetrievedContext]) -> SqlPlan: ...


def _tokens(value: str) -> Counter[str]:
    return Counter(TOKEN.findall(value.lower()))


def _cosine_sparse(left: Counter[str], right: Counter[str]) -> float:
    if not left or not right:
        return 0.0
    numerator = sum(value * right.get(key, 0) for key, value in left.items())
    left_norm = sum(value * value for value in left.values()) ** 0.5
    right_norm = sum(value * value for value in right.values()) ** 0.5
    return numerator / (left_norm * right_norm) if left_norm and right_norm else 0.0


class CatalogRetriever:
    def __init__(self, documents: Sequence[CatalogDocument]) -> None:
        self.documents = tuple(documents)
        self._vectors = {doc.doc_id: _tokens(doc.text) for doc in documents}

    def retrieve(self, question: str, *, top_k: int = 4) -> list[RetrievedContext]:
        query = _tokens(question)
        scored = [
            RetrievedContext(document=doc, score=_cosine_sparse(query, self._vectors[doc.doc_id]))
            for doc in self.documents
        ]
        scored.sort(key=lambda item: item.score, reverse=True)
        return scored[:top_k]


class SqlGuard:
    def __init__(self, policy: SqlPolicy | None = None) -> None:
        self.policy = policy or SqlPolicy()

    def validate(self, sql: str) -> str:
        statement = sql.strip().rstrip(";").strip()
        if not statement:
            raise ValueError("empty SQL")
        if ";" in statement:
            raise ValueError("multiple SQL statements are not allowed")
        if not re.match(r"^(select|with)\b", statement, re.IGNORECASE):
            raise ValueError("only SELECT/CTE statements are allowed")
        if FORBIDDEN.search(statement):
            raise ValueError("forbidden SQL operation detected")
        if self.policy.deny_select_star and re.search(r"\bselect\s+\*", statement, re.IGNORECASE):
            raise ValueError("SELECT * is denied; request explicit governed columns")

        tables = [value.replace('"', "") for value in TABLE_REF.findall(statement)]
        if not tables:
            raise ValueError("query must reference a governed analytics object")
        for table in tables:
            parts = table.split(".")
            if len(parts) < 2:
                raise ValueError(f"unqualified object denied: {table}")
            schema = parts[-2].upper()
            if schema not in self.policy.allowed_schemas:
                raise ValueError(f"schema not allowed: {schema}")

        limit_match = LIMIT_RE.search(statement)
        if limit_match:
            requested = int(limit_match.group(1))
            if requested > self.policy.max_rows:
                statement = LIMIT_RE.sub(f"LIMIT {self.policy.max_rows}", statement)
        else:
            statement = f"{statement}\nLIMIT {self.policy.max_rows}"
        return statement


class DeterministicPlanner:
    """Credential-free planner used as a safe fallback and in the live portfolio demo."""

    def plan(self, question: str, context: Sequence[RetrievedContext]) -> SqlPlan:
        q = question.lower()
        docs = tuple(item.document.doc_id for item in context)
        if any(term in q for term in ("ehr", "autofill", "writeback", "integration")):
            sql = """SELECT EVENT_DATE, INTEGRATION_ID, LAUNCHES, CONTEXT_OPENS, AUTOFILL_EVENTS,
WRITEBACKS, CONFIRMATION_RATE
FROM MART.DT_LIVE_EHR_INTEGRATION
WHERE EVENT_DATE >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY EVENT_DATE DESC, CONFIRMATION_RATE ASC"""
            rationale = "Uses the governed EHR integration mart and limits the report to recent de-identified telemetry."
        elif any(term in q for term in ("search", "discover", "query")):
            sql = """SELECT EVENT_DATE, CHANNEL, COUNTRY_CODE, SEARCHES, SEARCH_RESULT_CLICKS,
TOOL_VIEWS, TOOL_COMPLETIONS,
DIV0(SEARCH_RESULT_CLICKS, SEARCHES) AS SEARCH_RESULT_CTR
FROM MART.DT_LIVE_SEARCH_DISCOVERY
WHERE EVENT_DATE >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY EVENT_DATE DESC, SEARCHES DESC"""
            rationale = "Uses the continuous search-discovery aggregate rather than raw query payloads."
        elif any(term in q for term in ("stale", "pipeline", "fresh", "failure", "quarantine")):
            sql = """SELECT PIPELINE_NAME, LAST_RUN_ID, LAST_STATUS, LAST_SOURCE_COUNT,
FRESHNESS_SECONDS, QUARANTINE_RATIO, FAILURES_24H
FROM OPS.DT_CONTINUOUS_PIPELINE_SLO
ORDER BY FAILURES_24H DESC, FRESHNESS_SECONDS DESC"""
            rationale = "Queries the continuous operational SLO surface with no source-level payload access."
        elif any(term in q for term in ("recommend", "model", "ctr", "ranking")):
            sql = """SELECT EVENT_DATE, MODEL_NAME, MODEL_VERSION, IMPRESSIONS, CLICKS,
ATTRIBUTED_TOOL_STARTS, ATTRIBUTED_TOOL_COMPLETIONS, CTR, CLICK_TO_COMPLETION_RATE
FROM MART_DBT.FCT_RECOMMENDATION_PERFORMANCE_DAILY
WHERE EVENT_DATE >= DATEADD('day', -14, CURRENT_DATE())
ORDER BY EVENT_DATE DESC, CTR DESC"""
            rationale = "Compares governed recommendation model performance by version."
        else:
            sql = """SELECT EVENT_DATE, TOOL_ID, TOOL_NAME, PRIMARY_SPECIALTY, CHANNEL,
TOOL_VIEWS, TOOL_STARTS, TOOL_COMPLETIONS, VIEW_TO_START_RATE, START_TO_COMPLETE_RATE
FROM MART.DT_LIVE_TOOL_FUNNEL
WHERE EVENT_DATE >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY EVENT_DATE DESC, TOOL_COMPLETIONS DESC"""
            rationale = "Defaults to the governed live tool-funnel mart for product engagement questions."
        confidence = max((item.score for item in context), default=0.0)
        return SqlPlan(question, sql, rationale, docs, round(min(1.0, 0.55 + confidence), 4))


class AnalyticsCopilot:
    def __init__(
        self,
        documents: Sequence[CatalogDocument],
        *,
        planner: LLMPlanner | None = None,
        guard: SqlGuard | None = None,
    ) -> None:
        self.retriever = CatalogRetriever(documents)
        self.planner = planner or DeterministicPlanner()
        self.guard = guard or SqlGuard()

    def ask(self, question: str) -> SqlPlan:
        question = question.strip()
        if len(question) < 4:
            raise ValueError("question is too short")
        context = self.retriever.retrieve(question)
        candidate = self.planner.plan(question, context)
        safe_sql = self.guard.validate(candidate.sql)
        return dataclasses.replace(candidate, sql=safe_sql)


DEFAULT_DOCUMENTS = (
    CatalogDocument(
        "tool_funnel",
        "Tool funnel",
        "Clinical tool views, starts, completions, favorites and conversion by specialty/channel.",
        ("MART.DT_LIVE_TOOL_FUNNEL",),
        ("EVENT_DATE", "TOOL_ID", "TOOL_NAME", "PRIMARY_SPECIALTY", "CHANNEL", "COUNTRY_CODE"),
        ("TOOL_VIEWS", "TOOL_STARTS", "TOOL_COMPLETIONS", "VIEW_TO_START_RATE", "START_TO_COMPLETE_RATE"),
    ),
    CatalogDocument(
        "search_discovery",
        "Search discovery",
        "Search volume, result clicks and downstream clinical-tool engagement.",
        ("MART.DT_LIVE_SEARCH_DISCOVERY",),
        ("EVENT_DATE", "CHANNEL", "COUNTRY_CODE"),
        ("SEARCHES", "SEARCH_RESULT_CLICKS", "TOOL_VIEWS", "TOOL_COMPLETIONS"),
    ),
    CatalogDocument(
        "ehr_integration",
        "EHR integration",
        "De-identified integration launch, autofill confirmation and result-writeback telemetry.",
        ("MART.DT_LIVE_EHR_INTEGRATION",),
        ("EVENT_DATE", "INTEGRATION_ID"),
        ("LAUNCHES", "AUTOFILL_EVENTS", "WRITEBACKS", "CONFIRMATION_RATE"),
    ),
    CatalogDocument(
        "pipeline_health",
        "Pipeline health",
        "Continuous freshness, failures, source volume and quarantine SLO metrics.",
        ("OPS.DT_CONTINUOUS_PIPELINE_SLO",),
        ("PIPELINE_NAME",),
        ("LAST_SOURCE_COUNT", "FRESHNESS_SECONDS", "QUARANTINE_RATIO", "FAILURES_24H"),
    ),
    CatalogDocument(
        "recommendation_performance",
        "Recommendation model performance",
        "Model-version impressions, CTR and attributed tool conversions.",
        ("MART_DBT.FCT_RECOMMENDATION_PERFORMANCE_DAILY",),
        ("EVENT_DATE", "MODEL_NAME", "MODEL_VERSION"),
        ("IMPRESSIONS", "CLICKS", "CTR", "ATTRIBUTED_TOOL_COMPLETIONS"),
    ),
)


def plan_report(question: str) -> Mapping[str, Any]:
    plan = AnalyticsCopilot(DEFAULT_DOCUMENTS).ask(question)
    return {
        "question": plan.question,
        "sql": plan.sql,
        "rationale": plan.rationale,
        "evidence_docs": list(plan.evidence_docs),
        "confidence": plan.confidence,
        "execution_policy": {
            "read_only": True,
            "schemas": ["MART", "MART_DBT", "OPS"],
            "max_rows": 500,
        },
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("question")
    args = parser.parse_args()
    print(json.dumps(plan_report(args.question), indent=2))
