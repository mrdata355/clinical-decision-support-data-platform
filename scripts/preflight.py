#!/usr/bin/env python3
"""Production-readiness preflight for the clinical decision-support data platform.

The command validates repository architecture, contracts, configuration hygiene, SQL/dbt
anchors, security boundaries, and (in connected mode) external endpoint reachability. It never
prints credentials. Exit 0 means all blocking checks pass; exit 2 means at least one blocking
check failed.
"""
from __future__ import annotations

import argparse
import json
import re
import socket
import ssl
import sys
import tomllib
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import urlparse

from clinical_data_platform.config import get_settings

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True, slots=True)
class Check:
    name: str
    ok: bool
    detail: object
    severity: str = "ERROR"

    @property
    def blocking(self) -> bool:
        return self.severity.upper() in {"ERROR", "CRITICAL"}


REQUIRED_ASSETS = {
    "runtime": "src/clinical_data_platform/runtime.py",
    "control_plane": "src/clinical_data_platform/control_plane.py",
    "quality": "data_quality/quality_engine.py",
    "reconciliation": "reconciliation/reconcile.py",
    "health": "observability/health.py",
    "catalog": "governance/catalog.yml",
    "raw_contract": "lake/raw/raw_contract.yml",
    "clean_contract": "lake/clean/clean_contract.yml",
    "merge_policy": "lake/merge/merge_policy.yml",
    "curated_products": "lake/curated/data_products.yml",
    "dbt_project": "dbt/dbt_project.yml",
    "dbt_schema": "dbt/models/schema.yml",
    "warehouse": "snowflake/production_reference/enterprise_platform.sql",
    "incremental": "snowflake/streams_tasks/incremental_processing.sql",
    "procedures": "snowflake/procedures/merge_and_reconcile.sql",
    "performance": "snowflake/performance/query_optimization.sql",
    "site": "site/index.html",
    "site_app": "site/app.js",
    "vercel": "vercel.json",
    "ml_registry": "mlops/model_registry.yml",
    "ml_training": "mlops/training_pipeline.py",
    "sql_copilot": "ai/sql_rag_copilot.py",
}

CONTRACTS = [
    "contracts/product_event.v1.schema.json",
    "contracts/clinical_tool.v1.schema.json",
    "contracts/content.v1.schema.json",
    "contracts/quality_rating.v1.schema.json",
]

FORBIDDEN_SECRET_PATTERNS = [
    re.compile(r"(?i)(api[_-]?key|password|secret|token)\s*[=:]\s*['\"][^$<{][^'\"]{8,}['\"]"),
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
]

DIRECT_IDENTIFIER_TERMS = {
    "medical_record_number",
    "patient_name",
    "raw_patient_id",
    "ssn",
    "social_security_number",
}


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def check_repository_assets() -> Check:
    missing = [path for path in REQUIRED_ASSETS.values() if not (ROOT / path).exists()]
    return Check("architecture_assets", not missing, {"required": len(REQUIRED_ASSETS), "missing": missing})


def check_json_contracts() -> Check:
    failures: list[dict[str, str]] = []
    for relative in CONTRACTS:
        path = ROOT / relative
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if data.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
                failures.append({"path": relative, "error": "unexpected JSON Schema draft"})
            if data.get("type") != "object" or not data.get("required") or not data.get("properties"):
                failures.append({"path": relative, "error": "missing object/required/properties contract anchors"})
        except (OSError, json.JSONDecodeError) as exc:
            failures.append({"path": relative, "error": str(exc)})
    return Check("json_contracts", not failures, {"validated": len(CONTRACTS) - len(failures), "failures": failures})


def check_pyproject() -> Check:
    try:
        data = tomllib.loads(_read("pyproject.toml"))
        project = data["project"]
        python_version = project.get("requires-python")
        dev = project.get("optional-dependencies", {}).get("dev", [])
        ok = bool(python_version and any("pytest" in item for item in dev) and any("ruff" in item for item in dev))
        return Check("python_project", ok, {"requires_python": python_version, "dev_dependency_count": len(dev)})
    except (OSError, KeyError, tomllib.TOMLDecodeError) as exc:
        return Check("python_project", False, str(exc))


def check_yaml_anchors() -> Check:
    requirements = {
        "governance/catalog.yml": ["no_phi_product_telemetry", "llm_sql_guardrails", "ml_non_clinical_scope", "CORE.FCT_QUALITY_RATING"],
        "lake/clean/clean_contract.yml": ["direct_identifiers_forbidden", "quality_rating", "ehr_integration_event", "quality_gates"],
        "lake/merge/merge_policy.yml": ["continuous_query_contract", "terminal_equation", "scd2", "quality_rating"],
        "lake/curated/data_products.yml": ["search_funnel_daily", "quality_trust_daily", "ehr_integration_daily", "ml_model_health_daily"],
        "sources/ehr_integrations/integration_events.yml": ["smart_on_fhir_event_export", "direct_patient_identifiers_allowed: false", "intelligent_autofill", "result_writeback"],
    }
    missing: dict[str, list[str]] = {}
    for path, tokens in requirements.items():
        text = _read(path) if (ROOT / path).exists() else ""
        absent = [token for token in tokens if token not in text]
        if absent:
            missing[path] = absent
    return Check("domain_config_anchors", not missing, {"files_checked": len(requirements), "missing": missing})


def check_sql_anchors() -> Check:
    requirements = {
        "snowflake/production_reference/enterprise_platform.sql": ["OPS.PIPELINE_RUN", "OPS.RECONCILIATION_RESULT", "CORE.FCT_PRODUCT_EVENT", "CREATE STREAM", "CREATE TASK"],
        "snowflake/procedures/merge_and_reconcile.sql": ["BEGIN_PIPELINE_RUN", "RECORD_QUALITY_RESULT", "COMMIT_WATERMARK", "COMPLETE_PIPELINE_RUN"],
        "snowflake/streams_tasks/incremental_processing.sql": ["SYSTEM$STREAM_HAS_DATA", "TASK_NORMALIZE_PRODUCT_EVENTS", "TASK_MERGE_PRODUCT_EVENTS"],
        "snowflake/performance/query_optimization.sql": ["QUERY_HISTORY", "PARTITIONS_SCANNED", "QUERY_PARAMETERIZED_HASH"],
    }
    missing: dict[str, list[str]] = {}
    for path, tokens in requirements.items():
        text = _read(path).upper() if (ROOT / path).exists() else ""
        absent = [token for token in tokens if token.upper() not in text]
        if absent:
            missing[path] = absent
    return Check("snowflake_architecture", not missing, {"files_checked": len(requirements), "missing": missing})


def check_dbt_models() -> Check:
    model_dir = ROOT / "dbt" / "models"
    sql_files = sorted(model_dir.rglob("*.sql")) if model_dir.exists() else []
    names = {path.name for path in sql_files}
    required = {
        "stg_product_events.sql",
        "stg_clinical_tools.sql",
        "stg_content.sql",
        "stg_quality_ratings.sql",
        "dim_clinical_tool.sql",
        "fct_product_event.sql",
        "fct_quality_rating.sql",
        "fct_tool_engagement_daily.sql",
        "fct_search_funnel_daily.sql",
        "fct_ehr_integration_daily.sql",
    }
    missing = sorted(required - names)
    return Check("dbt_model_inventory", not missing, {"sql_model_count": len(sql_files), "missing": missing})


def check_secret_hygiene() -> Check:
    candidates = [
        *ROOT.rglob("*.py"),
        *ROOT.rglob("*.js"),
        *ROOT.rglob("*.yml"),
        *ROOT.rglob("*.yaml"),
        *ROOT.rglob("*.json"),
        *ROOT.rglob("*.sql"),
    ]
    findings: list[str] = []
    for path in candidates:
        if any(part in {".git", ".venv", "node_modules", "dbt_packages", "target"} for part in path.parts):
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        if any(pattern.search(text) for pattern in FORBIDDEN_SECRET_PATTERNS):
            findings.append(str(path.relative_to(ROOT)))
    return Check("secret_hygiene", not findings, {"possible_secret_files": sorted(set(findings))})


def check_privacy_boundary() -> Check:
    allowed_policy_files = {
        "lake/clean/clean_contract.yml",
        "governance/catalog.yml",
        "sources/ehr_integrations/integration_events.yml",
        "scripts/preflight.py",
    }
    findings: list[str] = []
    scan_roots = [ROOT / "site", ROOT / "dbt", ROOT / "server", ROOT / "api"]
    for root in scan_roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix not in {".py", ".js", ".html", ".sql", ".yml", ".json"}:
                continue
            rel = str(path.relative_to(ROOT))
            if rel in allowed_policy_files:
                continue
            lower = path.read_text(encoding="utf-8", errors="ignore").lower()
            if any(term in lower for term in DIRECT_IDENTIFIER_TERMS):
                findings.append(rel)
    return Check("privacy_boundary", not findings, {"unexpected_direct_identifier_terms": sorted(set(findings))})


def check_host(url: str) -> tuple[bool, str]:
    parsed = urlparse(url)
    host = parsed.hostname
    if not host:
        return False, "URL has no hostname"
    try:
        socket.getaddrinfo(host, parsed.port or 443)
    except socket.gaierror as exc:
        return False, f"DNS lookup failed: {exc}"
    return True, f"DNS resolves for {host}"


def check_https_health(base_url: str, health_path: str) -> Check:
    target = base_url.rstrip("/") + "/" + health_path.lstrip("/")
    parsed = urlparse(target)
    if parsed.scheme != "https":
        return Check("source_api_https", False, "connected source must use HTTPS")
    request = urllib.request.Request(target, method="GET", headers={"User-Agent": "clinical-platform-preflight/2"})
    try:
        with urllib.request.urlopen(request, timeout=5, context=ssl.create_default_context()) as response:
            return Check("source_api_health", 200 <= response.status < 500, {"status": response.status, "host": parsed.hostname})
    except urllib.error.HTTPError as exc:
        # Authentication errors still prove DNS/TLS/routing; credentials are validated by the connector runtime.
        return Check("source_api_health", exc.code in {401, 403, 404, 405, 429}, {"status": exc.code, "host": parsed.hostname}, severity="WARNING" if exc.code in {401, 403} else "ERROR")
    except (urllib.error.URLError, TimeoutError, ssl.SSLError) as exc:
        return Check("source_api_health", False, str(exc))


def run_checks(connected: bool) -> list[Check]:
    checks = [
        check_repository_assets(),
        check_json_contracts(),
        check_pyproject(),
        check_yaml_anchors(),
        check_sql_anchors(),
        check_dbt_models(),
        check_secret_hygiene(),
        check_privacy_boundary(),
    ]
    settings = get_settings()
    if connected:
        placeholders = settings.connected_placeholders()
        checks.append(Check("credentials_and_endpoints", not placeholders, "configured" if not placeholders else {"template_values": placeholders}))
        ok, detail = check_host(settings.source_api_base_url)
        checks.append(Check("source_api_dns", ok, detail))
        checks.append(check_https_health(settings.source_api_base_url, settings.source_api_health_path))
        paths = settings.source_paths()
        checks.append(Check("source_paths", all(value.startswith("/") for value in paths.values()), paths))
    else:
        checks.append(Check("demo_mode", settings.demo_mode, "synthetic/de-identified mode requires no external credentials", severity="WARNING" if not settings.demo_mode else "INFO"))
    return checks


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connected", action="store_true", help="validate connected-mode endpoint and configuration")
    parser.add_argument("--compact", action="store_true", help="emit compact JSON")
    args = parser.parse_args()
    settings = get_settings()
    checks = run_checks(args.connected or not settings.demo_mode)
    blocking_failures = [check.name for check in checks if check.blocking and not check.ok]
    warnings = [check.name for check in checks if check.severity == "WARNING" and not check.ok]
    result = {
        "mode": "connected" if args.connected or not settings.demo_mode else "demo",
        "environment": settings.app_env,
        "passed": not blocking_failures,
        "blocking_failures": blocking_failures,
        "warnings": warnings,
        "checks": [asdict(check) | {"blocking": check.blocking} for check in checks],
    }
    print(json.dumps(result, indent=None if args.compact else 2, default=str))
    return 0 if not blocking_failures else 2


if __name__ == "__main__":
    sys.exit(main())
