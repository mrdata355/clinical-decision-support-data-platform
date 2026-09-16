"""Deterministic de-identified data generator for local platform execution.

Generated records intentionally exercise inserts, updates, duplicates, stale versions,
quarantine, late arrivals, search funnels, tool completions, content views, and integration
launches. The output can be used by tests and the static operations console.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import random
from collections import Counter
from pathlib import Path
from typing import Any

UTC = dt.UTC

SPECIALTIES = [
    "Cardiology",
    "Emergency Medicine",
    "Internal Medicine",
    "Neurology",
    "Pulmonology",
    "Nephrology",
    "Gastroenterology",
    "Hematology",
    "Oncology",
    "Critical Care",
]

CHANNELS = ["web", "mobile_web", "ios", "android", "integration"]
COUNTRIES = ["US", "CA", "GB", "AU", "DE", "FR", "BR", "MX"]
EVENT_TYPES = [
    "session_start",
    "search",
    "tool_view",
    "tool_start",
    "tool_complete",
    "content_view",
    "favorite_add",
    "integration_launch",
    "session_end",
]


def iso(value: dt.datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def generate_tools(count: int = 60) -> list[dict[str, Any]]:
    base = dt.datetime(2026, 1, 1, tzinfo=UTC)
    tools: list[dict[str, Any]] = []
    for idx in range(1, count + 1):
        specialty = SPECIALTIES[(idx - 1) % len(SPECIALTIES)]
        tools.append(
            {
                "tool_id": f"tool-{idx:04d}",
                "tool_slug": f"clinical-tool-{idx:04d}",
                "tool_name": f"Clinical Decision Tool {idx:04d}",
                "primary_specialty": specialty,
                "condition_group": f"condition-group-{((idx - 1) % 20) + 1:02d}",
                "evidence_status": "reviewed",
                "publication_status": "published",
                "first_published_at": iso(base + dt.timedelta(days=idx)),
                "last_reviewed_at": iso(base + dt.timedelta(days=180 + idx)),
                "source_version": 1,
                "source_updated_at": iso(base + dt.timedelta(days=180 + idx)),
            }
        )
    return tools


def generate_content(count: int = 80) -> list[dict[str, Any]]:
    base = dt.datetime(2026, 2, 1, tzinfo=UTC)
    rows: list[dict[str, Any]] = []
    for idx in range(1, count + 1):
        specialty = SPECIALTIES[(idx + 2) % len(SPECIALTIES)]
        rows.append(
            {
                "content_id": f"content-{idx:04d}",
                "content_type": "guideline" if idx % 3 == 0 else "reference",
                "title": f"Clinical Reference {idx:04d}",
                "specialty": specialty,
                "condition_group": f"condition-group-{((idx - 1) % 20) + 1:02d}",
                "publication_status": "published",
                "source_version": 1,
                "source_updated_at": iso(base + dt.timedelta(days=idx)),
            }
        )
    return rows


def event_payload(
    event_type: str,
    *,
    tool_id: str,
    content_id: str,
    session_id: str,
    channel: str,
    country: str,
    completion_id: str,
    integration_id: str,
    query_token: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "session_id": session_id,
        "channel": channel,
        "country_code": country,
        "language_code": "en",
    }
    if event_type in {"tool_view", "tool_start", "tool_complete", "favorite_add", "integration_launch"}:
        payload["tool_id"] = tool_id
    if event_type == "tool_complete":
        payload["completion_id"] = completion_id
    if event_type == "content_view":
        payload["content_id"] = content_id
    if event_type == "search":
        payload["query_token"] = query_token
    if event_type == "integration_launch":
        payload["integration_id"] = integration_id
    return payload


def generate_events(
    *,
    days: int = 14,
    events_per_day: int = 500,
    seed: int = 355,
) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    start = dt.datetime(2026, 9, 1, tzinfo=UTC)
    events: list[dict[str, Any]] = []
    event_number = 0

    for day in range(days):
        day_start = start + dt.timedelta(days=day)
        for _ in range(events_per_day):
            event_number += 1
            event_type = rng.choices(
                EVENT_TYPES,
                weights=[4, 10, 25, 15, 12, 12, 4, 3, 5],
                k=1,
            )[0]
            second = rng.randint(0, 86_399)
            event_ts = day_start + dt.timedelta(seconds=second)
            session_num = rng.randint(1, max(20, events_per_day // 4))
            session_id = f"session-{day:02d}-{session_num:05d}"
            tool_id = f"tool-{rng.randint(1,60):04d}"
            content_id = f"content-{rng.randint(1,80):04d}"
            channel = rng.choice(CHANNELS)
            country = rng.choice(COUNTRIES)
            account_token = f"acct-{rng.randint(1,2200):06d}"
            user_token = f"user-{rng.randint(1,4200):06d}"
            event_id = f"evt-{day:02d}-{event_number:09d}"
            source_version = 1
            events.append(
                {
                    "event_id": event_id,
                    "event_type": event_type,
                    "source_kind": "integration" if channel == "integration" else "application",
                    "source_system": "integration-gateway" if channel == "integration" else "product-api",
                    "schema_version": 1,
                    "event_ts": iso(event_ts),
                    "source_updated_at": iso(event_ts + dt.timedelta(seconds=rng.randint(0, 120))),
                    "ingested_at": iso(event_ts + dt.timedelta(seconds=rng.randint(2, 300))),
                    "business_key": event_id,
                    "source_version": source_version,
                    "trace_id": f"trace-{event_number:09d}",
                    "account_token": account_token,
                    "user_token": user_token,
                    "payload": event_payload(
                        event_type,
                        tool_id=tool_id,
                        content_id=content_id,
                        session_id=session_id,
                        channel=channel,
                        country=country,
                        completion_id=f"completion-{event_number:09d}",
                        integration_id=f"integration-{rng.randint(1,20):03d}",
                        query_token=f"query-{rng.randint(1,500):05d}",
                    ),
                }
            )

    # Add later versions for a deterministic subset. These demonstrate version-aware updates.
    update_candidates = events[::211]
    for original in update_candidates:
        revised = json.loads(json.dumps(original))
        revised["event_id"] = original["event_id"]
        revised["source_version"] = 2
        revised["source_updated_at"] = iso(
            dt.datetime.fromisoformat(original["source_updated_at"].replace("Z", "+00:00"))
            + dt.timedelta(hours=1)
        )
        revised["ingested_at"] = iso(
            dt.datetime.fromisoformat(original["ingested_at"].replace("Z", "+00:00"))
            + dt.timedelta(hours=1, minutes=5)
        )
        revised["payload"]["correction_code"] = "source_revision"
        events.append(revised)

    # Exact redeliveries demonstrate idempotency without changing business state.
    for original in events[50::389]:
        events.append(json.loads(json.dumps(original)))

    # Older versions arriving late demonstrate stale-write protection.
    for original in update_candidates[::3]:
        stale = json.loads(json.dumps(original))
        stale["source_version"] = 1
        stale["ingested_at"] = iso(
            dt.datetime.fromisoformat(original["ingested_at"].replace("Z", "+00:00"))
            + dt.timedelta(days=2)
        )
        stale["payload"]["late_delivery"] = True
        events.append(stale)

    # A small invalid sample demonstrates quarantine reason codes.
    for idx in range(5):
        events.append(
            {
                "event_id": f"invalid-{idx:04d}",
                "event_type": "tool_complete",
                "source_kind": "application",
                "source_system": "product-api",
                "schema_version": 1,
                "event_ts": iso(start + dt.timedelta(days=days - 1, hours=20, minutes=idx)),
                "source_updated_at": iso(start + dt.timedelta(days=days - 1, hours=20, minutes=idx)),
                "ingested_at": iso(start + dt.timedelta(days=days - 1, hours=20, minutes=idx, seconds=10)),
                "business_key": f"invalid-{idx:04d}",
                "source_version": 1,
                "payload": {"session_id": f"invalid-session-{idx}", "channel": "web", "country_code": "US"},
            }
        )

    return sorted(events, key=lambda row: (row["ingested_at"], row["event_id"], row["source_version"]))


def build_merge_evidence(events: list[dict[str, Any]]) -> dict[str, Any]:
    current: dict[str, dict[str, Any]] = {}
    seen_delivery: set[tuple[str, int, str]] = set()
    outcomes: Counter[str] = Counter()
    audit: list[dict[str, Any]] = []

    for row in events:
        event_id = row["event_id"]
        version = int(row.get("source_version", 1))
        updated_at = row["source_updated_at"]
        delivery_key = (event_id, version, updated_at)

        if event_id.startswith("invalid-"):
            outcome = "quarantined"
        elif delivery_key in seen_delivery:
            outcome = "duplicate"
        else:
            prior = current.get(event_id)
            if prior is None:
                outcome = "inserted"
                current[event_id] = row
            else:
                prior_version = int(prior.get("source_version", 1))
                prior_updated = prior["source_updated_at"]
                if version < prior_version or (version == prior_version and updated_at < prior_updated):
                    outcome = "stale"
                elif version == prior_version and updated_at == prior_updated:
                    outcome = "unchanged"
                else:
                    outcome = "updated"
                    current[event_id] = row
            seen_delivery.add(delivery_key)

        outcomes[outcome] += 1
        if len(audit) < 180:
            audit.append(
                {
                    "event_id": event_id,
                    "source_version": version,
                    "outcome": outcome,
                    "event_type": row.get("event_type"),
                    "source_updated_at": updated_at,
                    "ingested_at": row.get("ingested_at"),
                    "business_key": row.get("business_key"),
                }
            )

    source_count = len(events)
    explained = sum(outcomes.values())
    return {
        "source_count": source_count,
        "current_state_count": len(current),
        "outcomes": dict(outcomes),
        "explained_count": explained,
        "balanced": source_count == explained,
        "audit_sample": audit,
    }


def build_pipeline_runs(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_day: dict[str, list[dict[str, Any]]] = {}
    for row in events:
        day = row["event_ts"][:10]
        by_day.setdefault(day, []).append(row)
    runs: list[dict[str, Any]] = []
    for idx, (day, rows) in enumerate(sorted(by_day.items()), start=1):
        evidence = build_merge_evidence(rows)
        outcomes = evidence["outcomes"]
        started = dt.datetime.fromisoformat(day + "T00:15:00+00:00")
        duration_seconds = 75 + (idx * 17) % 140
        runs.append(
            {
                "run_id": f"run-{day.replace('-', '')}-{idx:03d}",
                "pipeline_name": "product_events",
                "status": "SUCCESS",
                "started_at": iso(started),
                "completed_at": iso(started + dt.timedelta(seconds=duration_seconds)),
                "duration_seconds": duration_seconds,
                "source_count": len(rows),
                "inserted_count": outcomes.get("inserted", 0),
                "updated_count": outcomes.get("updated", 0),
                "duplicate_count": outcomes.get("duplicate", 0),
                "stale_count": outcomes.get("stale", 0),
                "unchanged_count": outcomes.get("unchanged", 0),
                "quarantined_count": outcomes.get("quarantined", 0),
                "balanced": evidence["balanced"],
            }
        )
    return runs


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="data/demo")
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--events-per-day", type=int, default=500)
    args = parser.parse_args()

    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    tools = generate_tools()
    content = generate_content()
    events = generate_events(days=args.days, events_per_day=args.events_per_day)
    merge_evidence = build_merge_evidence(events)
    runs = build_pipeline_runs(events)

    (output / "clinical_tools.json").write_text(json.dumps(tools, indent=2))
    (output / "content.json").write_text(json.dumps(content, indent=2))
    with (output / "product_events.jsonl").open("w", encoding="utf-8") as handle:
        for row in events:
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")
    (output / "merge_evidence.json").write_text(json.dumps(merge_evidence, indent=2))
    (output / "pipeline_runs.json").write_text(json.dumps(runs, indent=2))
    print(json.dumps({"tools": len(tools), "content": len(content), "events": len(events), "runs": len(runs)}, indent=2))


if __name__ == "__main__":
    main()
