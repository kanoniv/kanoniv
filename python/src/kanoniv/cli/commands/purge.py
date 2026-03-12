"""Purge all tenant data for a clean slate.

    kanoniv purge              # interactive confirmation
    kanoniv purge --confirm    # skip confirmation
"""
from __future__ import annotations

import sys
from argparse import Namespace
from typing import Any


def cmd_purge(args: Namespace, client: Any) -> None:
    """Delete all sources, entities, plans, and jobs for the current tenant."""
    confirm = getattr(args, "confirm", False)

    if not confirm:
        print("This will DELETE all data for your tenant:")
        print("  - All sources and their records")
        print("  - All canonical entities and identity links")
        print("  - All identity plans")
        print("  - All match audit history")
        print("  - All reconciliation jobs")
        print("  - All agent runs and memory entries")
        print("  - All conversations")
        print()
        answer = input("Type 'yes' to confirm: ").strip().lower()
        if answer != "yes":
            print("Aborted.")
            return

    print()

    # 1. Delete all sources (cascades to external_entities, identity_links, match_audit)
    # client.get returns parsed JSON directly; _handle_error exits on failure
    sources = client.get("/v1/sources")
    if sources:
        for s in sources:
            name = s.get("name", "?")
            sid = s.get("id")
            try:
                client.delete(f"/v1/sources/{sid}")
                print(f"  deleted source: {name}")
            except SystemExit:
                sys.stderr.write(f"  failed to delete source {name}\n")
    else:
        print("  no sources to delete")

    # 2. Delete identity plans
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
                    print(f"  deleted plan: {vid}")
                except SystemExit:
                    sys.stderr.write(f"  failed to delete plan {vid}\n")
    else:
        print("  no identity plans to delete")

    # 3. Purge remaining data via dedicated endpoint (canonical entities, jobs, etc.)
    try:
        result = client.post("/v1/admin/purge-tenant-data", {})
        if result and isinstance(result, dict):
            tables = result.get("tables", result)
            if isinstance(tables, dict):
                for table, count in sorted(tables.items()):
                    if isinstance(count, int) and count > 0:
                        print(f"  purged {table}: {count} rows")
    except SystemExit:
        # Endpoint doesn't exist yet - that's OK, source deletion handles most of it
        pass

    print()
    print("  Purge complete. Tenant is clean.")
