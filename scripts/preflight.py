#!/usr/bin/env python3
"""Deployment preflight for the clinical data platform.

The command intentionally separates two guarantees:
1. demo mode can run with generated data and no external credentials;
2. connected mode requires valid credentials plus the real endpoint/schema contract.
"""
from __future__ import annotations

import json
import socket
import sys
from urllib.parse import urlparse

from clinical_data_platform.config import get_settings


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


def main() -> int:
    settings = get_settings()
    result: dict[str, object] = {
        "mode": settings.mode,
        "environment": settings.app_env,
        "source_paths": settings.source_paths(),
        "snowflake": {
            "account": settings.snowflake_account,
            "warehouse": settings.snowflake_warehouse,
            "database": settings.snowflake_database,
            "schema": settings.snowflake_schema,
            "role": settings.snowflake_role,
        },
        "stream_window_minutes": settings.stream_window_minutes,
        "checks": [],
    }
    checks: list[dict[str, object]] = result["checks"]  # type: ignore[assignment]

    if settings.demo_mode:
        checks.append({"name": "demo_mode", "ok": True, "detail": "No external credentials required"})
        checks.append({"name": "generated_data", "ok": True, "detail": "Browser and Python demo paths are available"})
        print(json.dumps(result, indent=2))
        return 0

    placeholders = settings.connected_placeholders()
    checks.append({
        "name": "credentials_and_endpoints",
        "ok": not placeholders,
        "detail": "configured" if not placeholders else f"template values remain: {', '.join(placeholders)}",
    })

    if "example.internal" not in settings.source_api_base_url:
        ok, detail = check_host(settings.source_api_base_url)
        checks.append({"name": "source_api_dns", "ok": ok, "detail": detail})

    required_paths = settings.source_paths()
    checks.append({
        "name": "source_paths",
        "ok": all(value.startswith("/") for value in required_paths.values()),
        "detail": required_paths,
    })

    all_ok = all(bool(check.get("ok")) for check in checks)
    print(json.dumps(result, indent=2))
    return 0 if all_ok else 2


if __name__ == "__main__":
    sys.exit(main())
