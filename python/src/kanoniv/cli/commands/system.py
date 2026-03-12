"""System commands: version, health, stats, purge, reindex, recompute.

Operational visibility and maintenance for the Kanoniv platform.
"""
from __future__ import annotations

import sys
from typing import Any

import kanoniv
from kanoniv.cli.output import print_json, print_table, print_detail, print_success


def cmd_system(args: Any, client: Any | None) -> None:
    """Route to the appropriate system sub-command."""
    action = args.action

    # version doesn't need a client
    if action == "version":
        _version(args)
        return

    dispatch = {
        "health": _health,
        "stats": _stats,
        "purge": _purge,
        "reindex": _reindex,
        "recompute": _recompute,
    }
    fn = dispatch.get(action)
    if fn:
        if client is None:
            sys.stderr.write("This command requires an API key.\n")
            sys.exit(1)
        fn(args, client)
    else:
        sys.stderr.write(
            "usage: kanoniv system "
            "{version,health,stats,purge,reindex,recompute}\n"
        )
        sys.exit(1)


def _version(args: Any) -> None:
    """Show version info."""
    if args.format == "json":
        print_json({"version": kanoniv.__version__})
    else:
        sys.stdout.write(f"kanoniv {kanoniv.__version__}\n")


def _health(args: Any, client: Any) -> None:
    """Check API health."""
    resp = client.get("/health")

    if args.format == "json":
        print_json(resp)
        return

    if resp:
        status = resp.get("status", "unknown")
        sys.stdout.write(f"API: {status}\n")
        for k, v in resp.items():
            if k != "status":
                sys.stdout.write(f"  {k}: {v}\n")
    else:
        sys.stdout.write("API: ok\n")


def _stats(args: Any, client: Any) -> None:
    """Show dashboard statistics."""
    params: dict = {}
    entity_type = getattr(args, "entity_type", None)
    if entity_type:
        params["entity_type"] = entity_type

    resp = client.get("/v1/stats", params=params if params else None)

    if args.format == "json":
        print_json(resp)
        return

    header = "Dashboard"
    if entity_type:
        header += f" ({entity_type})"
    sys.stdout.write(f"{header}\n\n")

    stat_fields = [
        ("total_canonical_entities", "Entities"),
        ("total_external_entities", "Records"),
        ("pending_reviews", "Pending reviews"),
        ("merge_rate", "Merge rate"),
    ]

    for key, label in stat_fields:
        val = resp.get(key)
        if val is not None:
            if key == "merge_rate":
                try:
                    display = f"{float(val) * 100.0:.1f}%"
                except (TypeError, ValueError):
                    display = "-"
            else:
                display = str(val)
            sys.stdout.write(f"  {label + ':':<16} {display}\n")

    # Per-entity-type breakdown
    by_type = resp.get("by_entity_type")
    if by_type and isinstance(by_type, dict):
        sys.stdout.write("\n  By Entity Type\n")
        rows = []
        for et, stats in sorted(by_type.items()):
            records = stats.get("records", 0)
            entities = stats.get("entities", 0)
            mr = stats.get("merge_rate", 0.0)
            rows.append([et, str(records), str(entities), f"{mr * 100.0:.1f}%"])
        print_table(["TYPE", "RECORDS", "ENTITIES", "MERGE RATE"], rows)


def _purge(args: Any, client: Any) -> None:
    """Delete all tenant data, or only data for a specific entity type."""
    confirm = getattr(args, "confirm", False)
    entity_type = getattr(args, "entity_type", None)

    if entity_type:
        # Entity-type scoped purge - only delete data for one entity type
        if not confirm:
            sys.stdout.write(
                f"This will DELETE all '{entity_type}' data for your tenant:\n"
            )
            sys.stdout.write(f"  - All '{entity_type}' external entities\n")
            sys.stdout.write(f"  - All '{entity_type}' canonical entities\n")
            sys.stdout.write(f"  - All related identity links, match audit, and merge history\n")
            sys.stdout.write(f"\n  Other entity types will NOT be affected.\n\n")
            answer = input("Type 'yes' to confirm: ").strip().lower()
            if answer != "yes":
                sys.stdout.write("Aborted.\n")
                return

        sys.stdout.write("\n")

        try:
            result = client.post(
                f"/v1/admin/purge-tenant-data?entity_type={entity_type}", {}
            )
            if result and isinstance(result, dict):
                tables = result.get("tables", result)
                if isinstance(tables, dict):
                    for table, count in sorted(tables.items()):
                        if isinstance(count, int) and count > 0:
                            sys.stdout.write(f"  purged {table}: {count} rows\n")
                msg = result.get("message", "")
                if msg:
                    sys.stdout.write(f"\n  {msg}\n")
        except SystemExit:
            pass

        return

    # Full tenant purge
    if not confirm:
        sys.stdout.write("This will DELETE all data for your tenant:\n")
        sys.stdout.write("  - All sources and their records\n")
        sys.stdout.write("  - All canonical entities and identity links\n")
        sys.stdout.write("  - All identity plans\n")
        sys.stdout.write("  - All match audit history\n")
        sys.stdout.write("  - All reconciliation jobs\n")
        sys.stdout.write("  - All agent runs and memory entries\n")
        sys.stdout.write("  - All conversations\n")
        sys.stdout.write("\n")
        answer = input("Type 'yes' to confirm: ").strip().lower()
        if answer != "yes":
            sys.stdout.write("Aborted.\n")
            return

    sys.stdout.write("\n")

    # Delete all sources
    sources = client.get("/v1/sources")
    if sources:
        for s in sources:
            name = s.get("name", "?")
            sid = s.get("id")
            try:
                client.delete(f"/v1/sources/{sid}")
                sys.stdout.write(f"  deleted source: {name}\n")
            except SystemExit:
                sys.stderr.write(f"  failed to delete source {name}\n")
    else:
        sys.stdout.write("  no sources to delete\n")

    # Delete identity plans
    try:
        plans = client.get("/v1/identity/specs")
    except SystemExit:
        plans = None

    if plans and isinstance(plans, list):
        for plan in plans:
            vid = plan.get("identity_version") or plan.get("id")
            if vid:
                try:
                    client.delete(f"/v1/identity/specs/{vid}")
                    sys.stdout.write(f"  deleted plan: {vid}\n")
                except SystemExit:
                    sys.stderr.write(f"  failed to delete plan {vid}\n")
    else:
        sys.stdout.write("  no identity plans to delete\n")

    # Purge remaining data
    try:
        result = client.post("/v1/admin/purge-tenant-data", {})
        if result and isinstance(result, dict):
            tables = result.get("tables", result)
            if isinstance(tables, dict):
                for table, count in sorted(tables.items()):
                    if isinstance(count, int) and count > 0:
                        sys.stdout.write(f"  purged {table}: {count} rows\n")
    except SystemExit:
        pass

    sys.stdout.write("\n  Purge complete. Tenant is clean.\n")


def _reindex(args: Any, client: Any) -> None:
    """Trigger reindexing of the identity graph and search indexes."""
    target = getattr(args, "target", None)

    body: dict = {}
    if target:
        body["target"] = target

    resp = client.post("/v1/admin/reindex", body if body else None)

    if args.format == "json":
        print_json(resp)
    else:
        status = resp.get("status", "started") if resp else "started"
        label = f" ({target})" if target else ""
        print_success(f"Reindex{label}: {status}")


def _recompute(args: Any, client: Any) -> None:
    """Trigger recomputation of derived data (canonical records, graph metrics)."""
    target = getattr(args, "target", None)

    body: dict = {}
    if target:
        body["target"] = target

    resp = client.post("/v1/admin/recompute", body if body else None)

    if args.format == "json":
        print_json(resp)
    else:
        status = resp.get("status", "started") if resp else "started"
        label = f" ({target})" if target else ""
        print_success(f"Recompute{label}: {status}")
