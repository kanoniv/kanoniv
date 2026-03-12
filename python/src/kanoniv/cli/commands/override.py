"""Override commands: list, create, delete.

Overrides are manual corrections - force-merge or force-split directives
that take precedence over the engine's automatic matching decisions.
"""
from __future__ import annotations

import sys
from typing import Any

from kanoniv.cli.output import print_json, print_table, print_success


def cmd_override(args: Any, client: Any) -> None:
    """Route to the appropriate override sub-command."""
    action = args.action
    dispatch = {
        "list": _list,
        "create": _create,
        "delete": _delete,
    }
    fn = dispatch.get(action)
    if fn:
        fn(args, client)
    else:
        sys.stderr.write("usage: kanoniv override {list,create,delete}\n")
        sys.exit(1)


def _list(args: Any, client: Any) -> None:
    """List all overrides."""
    resp = client.get("/v1/overrides")

    if args.format == "json":
        print_json(resp)
        return

    # Vec<ManualOverride>: { id, override_type, override_data, reason, canonical_entity_id, created_at, ... }
    overrides = resp if isinstance(resp, list) else []
    if not overrides:
        sys.stdout.write("No overrides.\n")
        return

    rows = []
    for o in overrides:
        oid = str(o.get("id", "-"))
        if len(oid) > 12:
            oid = oid[:8] + "..."
        # Entity IDs live inside override_data
        data = o.get("override_data", {}) or {}
        ea = str(data.get("entity_a_id", "-"))
        eb = str(data.get("entity_b_id", "-"))
        if len(ea) > 12:
            ea = ea[:8] + "..."
        if len(eb) > 12:
            eb = eb[:8] + "..."
        rows.append([
            oid,
            o.get("override_type", "-"),
            ea,
            eb,
            o.get("reason", "-") or "-",
            o.get("created_at", "-")[:19] if o.get("created_at") else "-",
        ])
    print_table(["ID", "TYPE", "ENTITY_A", "ENTITY_B", "REASON", "CREATED"], rows)


def _create(args: Any, client: Any) -> None:
    """Create a manual override."""
    # ManualOverride expects override_data as JSON with entity IDs inside
    body: dict[str, Any] = {
        "override_type": f"force_{args.override_type}",
        "override_data": {
            "entity_a_id": args.entity_a,
            "entity_b_id": args.entity_b,
        },
    }

    resp = client.post("/v1/overrides", body)

    if args.format == "json":
        print_json(resp)
    else:
        oid = resp.get("id", "-") if resp else "-"
        print_success(
            f"Created {args.override_type} override: "
            f"{args.entity_a} <-> {args.entity_b} ({oid})"
        )


def _delete(args: Any, client: Any) -> None:
    """Delete an override."""
    resp = client.delete(f"/v1/overrides/{args.override_id}")

    if args.format == "json":
        print_json(resp)
    else:
        print_success(f"Deleted override {args.override_id}")
