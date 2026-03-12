"""Source commands: list, show, schema, create, delete, sync, stats, quality, entities.

Sources are datasets - tables, files, or systems that feed records into
the identity resolution pipeline.
"""
from __future__ import annotations

import json
import sys
from typing import Any

from kanoniv.cli.output import (
    print_json, print_table, print_detail, print_error, print_success,
)


def cmd_source(args: Any, client: Any) -> None:
    """Route to the appropriate source sub-command."""
    action = args.action
    dispatch = {
        "list": _list,
        "show": _show,
        "schema": _schema,
        "create": _create,
        "delete": _delete,
        "sync": _sync,
        "stats": _stats,
        "quality": _quality,
        "entities": _entities,
    }
    fn = dispatch.get(action)
    if fn:
        fn(args, client)
    else:
        sys.stderr.write(
            "usage: kanoniv source "
            "{list,show,schema,create,delete,sync,stats,quality,entities}\n"
        )
        sys.exit(1)


def _list(args: Any, client: Any) -> None:
    """List all sources."""
    resp = client.get("/v1/sources")

    if args.format == "json":
        print_json(resp)
        return

    sources = resp if isinstance(resp, list) else []
    rows = []
    for s in sources:
        sid = s.get("id", "-")
        if len(sid) > 12:
            sid = sid[:8] + "..."
        rows.append([
            sid,
            s.get("name", "-"),
            s.get("source_type", "-"),
            str(s.get("record_count", "-")),
            s.get("created_at", "-")[:19] if s.get("created_at") else "-",
        ])
    print_table(["ID", "NAME", "TYPE", "RECORDS", "CREATED"], rows)


def _show(args: Any, client: Any) -> None:
    """Show source details."""
    resp = client.get(f"/v1/sources/{args.source_id}")

    if args.format == "json":
        print_json(resp)
        return

    fields = [
        ("Source", resp.get("id", args.source_id)),
        ("Name", resp.get("name", "-")),
        ("Type", resp.get("source_type", "-")),
        ("Records", str(resp.get("record_count", "-"))),
        ("Entity Type", resp.get("entity_type", "-")),
        ("Created", resp.get("created_at", "-")),
        ("Updated", resp.get("updated_at", "-")),
    ]
    print_detail("Source", fields)

    config = resp.get("config")
    if isinstance(config, dict) and config:
        sys.stdout.write("\n  Config\n")
        for k, v in config.items():
            sys.stdout.write(f"    {k}: {v}\n")


def _schema(args: Any, client: Any) -> None:
    """Show source schema (columns, types, roles)."""
    resp = client.get(f"/v1/ingest/sources/{args.source_id}/preview")

    if args.format == "json":
        print_json(resp)
        return

    columns = resp.get("columns", resp.get("schema", []))
    if not columns:
        sys.stdout.write("No schema available.\n")
        return

    if isinstance(columns, list) and columns and isinstance(columns[0], dict):
        rows = []
        for col in columns:
            rows.append([
                col.get("name", "-"),
                col.get("type", col.get("data_type", "-")),
                col.get("role", col.get("signal", "-")),
                str(col.get("null_rate", "-")),
                str(col.get("cardinality", "-")),
            ])
        print_table(["COLUMN", "TYPE", "ROLE", "NULL_RATE", "CARDINALITY"], rows)
    else:
        print_json(resp)

    # Preview rows
    preview = resp.get("rows", resp.get("preview", []))
    if preview:
        sys.stdout.write(f"\n  {len(preview)} sample rows available (use --format json to see)\n")


def _create(args: Any, client: Any) -> None:
    """Create a new source."""
    body: dict[str, Any] = {
        "name": args.name,
        "source_type": getattr(args, "source_type", "csv"),
    }
    config_str = getattr(args, "config", None)
    if config_str:
        try:
            body["config"] = json.loads(config_str)
        except json.JSONDecodeError as e:
            print_error(f"Invalid JSON config: {e}")

    resp = client.post("/v1/sources", body)

    if args.format == "json":
        print_json(resp)
    else:
        sid = resp.get("id", "-") if resp else "-"
        print_success(f"Created source {args.name} ({sid})")


def _delete(args: Any, client: Any) -> None:
    """Delete a source and all its records."""
    resp = client.delete(f"/v1/sources/{args.source_id}")

    if args.format == "json":
        print_json(resp)
    else:
        print_success(f"Deleted source {args.source_id}")


def _sync(args: Any, client: Any) -> None:
    """Trigger a source sync."""
    resp = client.post(f"/v1/sources/{args.source_id}/sync")

    if args.format == "json":
        print_json(resp)
    else:
        status = resp.get("status", "started") if resp else "started"
        print_success(f"Sync {status} for source {args.source_id}")


def _stats(args: Any, client: Any) -> None:
    """Show statistics for a source: record counts, field coverage, etc."""
    resp = client.get(f"/v1/sources/{args.source_id}/stats")

    if args.format == "json":
        print_json(resp)
        return

    fields = [
        ("Source", resp.get("name", args.source_id)),
        ("Records", str(resp.get("record_count", "-"))),
        ("Unique IDs", str(resp.get("unique_ids", "-"))),
        ("Duplicates", str(resp.get("duplicate_count", "-"))),
        ("Null Rate", str(resp.get("null_rate", "-"))),
        ("Last Ingested", resp.get("last_ingested_at", resp.get("updated_at", "-"))),
    ]
    fields = [(k, v) for k, v in fields if v != "-"]
    print_detail("Source Stats", fields)

    field_stats = resp.get("field_stats", resp.get("columns", []))
    if field_stats and isinstance(field_stats, list):
        sys.stdout.write("\n")
        rows = []
        for f in field_stats:
            if isinstance(f, dict):
                rows.append([
                    f.get("name", f.get("field", "-")),
                    str(f.get("fill_rate", f.get("non_null_pct", "-"))),
                    str(f.get("cardinality", "-")),
                    str(f.get("avg_length", "-")),
                ])
        print_table(["FIELD", "FILL_RATE", "CARDINALITY", "AVG_LEN"], rows)


def _quality(args: Any, client: Any) -> None:
    """Show data quality report for a source."""
    resp = client.get(f"/v1/sources/{args.source_id}/quality")

    if args.format == "json":
        print_json(resp)
        return

    score = resp.get("quality_score", resp.get("score", "-"))
    fields = [
        ("Source", resp.get("name", args.source_id)),
        ("Quality Score", str(score)),
        ("Grade", resp.get("grade", "-")),
        ("Records Analyzed", str(resp.get("records_analyzed", "-"))),
    ]
    fields = [(k, v) for k, v in fields if v != "-"]
    print_detail("Data Quality", fields)

    issues = resp.get("issues", resp.get("warnings", []))
    if issues:
        sys.stdout.write("\n  Issues\n")
        for issue in issues:
            if isinstance(issue, str):
                sys.stdout.write(f"    - {issue}\n")
            elif isinstance(issue, dict):
                severity = issue.get("severity", "info")
                msg = issue.get("message", issue.get("description", "-"))
                sys.stdout.write(f"    [{severity}] {msg}\n")


def _entities(args: Any, client: Any) -> None:
    """List entities resolved from a specific source."""
    params: dict[str, str] = {
        "source": args.source_id,
        "limit": str(getattr(args, "limit", 20)),
    }

    resp = client.get("/v1/entities", params=params)

    if args.format == "json":
        print_json(resp)
        return

    entities = resp if isinstance(resp, list) else resp.get("entities", [])
    if not entities:
        sys.stdout.write("No entities from this source.\n")
        return

    rows = []
    for e in entities:
        eid = e.get("entity_id", "-")
        if len(eid) > 12:
            eid = eid[:8] + "..."
        rows.append([
            eid,
            e.get("entity_type", "-"),
            str(e.get("confidence_score", "-")),
            str(e.get("member_count", "-")),
        ])
    print_table(["ENTITY_ID", "TYPE", "CONFIDENCE", "MEMBERS"], rows)
