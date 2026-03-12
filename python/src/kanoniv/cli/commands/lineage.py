"""Lineage command: cross-source entity merge tracing.

Shows entities that span multiple data sources, revealing how the engine
connected records across different ingestion batches.

Usage:
    kanoniv lineage --source event_registrations --entity-type person
    kanoniv lineage --source event_registrations crm_contacts --entity-type person
    kanoniv lineage --entity-type person
"""
from __future__ import annotations

import sys
from typing import Any

from kanoniv.cli.output import print_json, print_table


def _extract_name(cd: dict[str, Any]) -> str:
    """Extract a display name from canonical_data fields."""
    for key in ("full_name", "name", "display_name"):
        if cd.get(key):
            return str(cd[key])
    parts = []
    if cd.get("first_name"):
        parts.append(str(cd["first_name"]))
    if cd.get("last_name"):
        parts.append(str(cd["last_name"]))
    return " ".join(parts) if parts else "-"


def cmd_lineage(args: Any, client: Any) -> None:
    """Show cross-source entity lineage."""
    params: dict[str, Any] = {
        "limit": str(args.limit),
        "offset": str(args.offset),
    }

    source_list = getattr(args, "source", None)
    if source_list:
        params["sources"] = ",".join(source_list)

    if getattr(args, "entity_type", None):
        params["entity_type"] = args.entity_type

    if getattr(args, "reveal_pii", False):
        params["reveal_pii"] = "true"

    resp = client.get("/v1/lineage", params=params)

    if args.format == "json":
        print_json(resp)
        return

    entities = resp.get("entities", []) if isinstance(resp, dict) else []
    total = resp.get("total", 0) if isinstance(resp, dict) else 0

    if not entities:
        sys.stdout.write("No cross-source entities found.\n")
        return

    for ent in entities:
        eid = str(ent.get("entity_id", "-"))
        if len(eid) > 12:
            eid = eid[:12] + "..."
        cd = ent.get("canonical_data") or {}
        name = _extract_name(cd)
        sc = ent.get("source_count", 0)
        rc = ent.get("record_count", 0)

        sys.stdout.write(
            f"\nEntity {eid} - {name} ({sc} sources, {rc} records)\n"
        )

        records = ent.get("records", [])
        rows = []
        for r in records:
            conf = r.get("confidence")
            conf_str = f"{float(conf):.2f}" if conf is not None else "-"
            ingested = r.get("ingested_at") or "-"
            if ingested != "-":
                ingested = ingested[:10]
            rows.append([
                r.get("source_name", "-"),
                r.get("external_id", "-"),
                conf_str,
                ingested,
            ])
        print_table(["SOURCE", "EXTERNAL_ID", "CONFIDENCE", "INGESTED"], rows)

    showing = len(entities)
    sys.stdout.write(
        f"\n{total} cross-source entities (showing {showing})\n"
    )
