"""Entity commands: show, search, list, linked, history, neighbors, merge, split, reassign, delete, link, unlink, explain, lock, revert, attrs, candidates, diff.

Entities are resolved identities - all records that represent the same real-world
thing, clustered into a single canonical node.

Graph mutation commands (merge, split, reassign, delete, link, unlink) operate
directly on the persistent identity graph with immediate effect. Override commands
(kanoniv override) are still available for deferred reconciliation-time hints.
"""
from __future__ import annotations

import sys
from typing import Any

from kanoniv.cli.output import (
    print_json, print_table, print_detail, print_kv, print_error,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_name(cd: dict[str, Any]) -> str:
    """Extract a display name from canonical_data fields."""
    # Prefer composite fields first
    for key in ("full_name", "name", "display_name"):
        if cd.get(key):
            return str(cd[key])
    # Fall back to first_name + last_name
    parts = []
    if cd.get("first_name"):
        parts.append(str(cd["first_name"]))
    if cd.get("last_name"):
        parts.append(str(cd["last_name"]))
    return " ".join(parts) if parts else "-"


def _extract_email(cd: dict[str, Any]) -> str:
    """Extract email from canonical_data."""
    val = cd.get("email")
    return str(val) if val else "-"


def _extract_phone(cd: dict[str, Any]) -> str:
    """Extract phone from canonical_data."""
    val = cd.get("phone")
    return str(val) if val else "-"


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

def cmd_entity(args: Any, client: Any) -> None:
    """Route to the appropriate entity sub-command."""
    action = args.action
    dispatch = {
        "show": _show,
        "search": _search,
        "list": _list,
        "linked": _linked,
        "history": _history,
        "neighbors": _neighbors,
        "merge": _merge,
        "split": _split,
        "reassign": _reassign,
        "delete": _delete,
        "link": _link,
        "unlink": _unlink,
        "explain": _explain,
        "lock": _lock,
        "revert": _revert,
        "attrs": _attrs,
        "candidates": _candidates,
        "diff": _diff,
    }
    fn = dispatch.get(action)
    if fn:
        fn(args, client)
    else:
        sys.stderr.write(
            "usage: kanoniv entity "
            "{show,search,list,linked,history,neighbors,merge,split,reassign,"
            "delete,link,unlink,explain,lock,revert,attrs,candidates,diff}\n"
        )
        sys.exit(1)


def _show(args: Any, client: Any) -> None:
    """Show entity details: canonical attributes, confidence, member count."""
    json_out = getattr(args, "json_output", False) or args.format == "json"
    show_attrs = getattr(args, "show_attrs", False)
    show_records = getattr(args, "show_records", False)
    show_sources = getattr(args, "show_sources", False)

    # If --relationships requested, show shared identity signals
    if getattr(args, "show_relationships", False):
        params: dict[str, Any] = {}
        if getattr(args, "rel_field", None):
            params["field"] = args.rel_field
        resp = client.get(
            f"/v1/entities/{args.entity_id}/relationships",
            params=params or None,
        )
        if json_out:
            print_json(resp)
            return
        signals = resp.get("shared_signals", []) if resp else []
        total = resp.get("total_records", 0) if resp else 0
        if not signals:
            sys.stdout.write("No shared signals found.\n")
            return
        for sig in signals:
            sys.stdout.write(f"\n  Shared {sig['field']}: {sig['value']}\n")
            for rec in sig.get("records", []):
                sys.stdout.write(
                    f"    {rec['source_name']}:{rec['external_id']}\n"
                )
        sys.stdout.write(
            f"\n  {total} records, {len(signals)} shared signals\n"
        )
        return

    # If --records requested, fetch the linked endpoint (includes canonical + links)
    if show_records:
        resp = client.get(f"/v1/canonical/{args.entity_id}/linked")
        if json_out:
            print_json(resp)
            return
        canonical = resp.get("canonical", {}) if isinstance(resp, dict) else {}
        cd = canonical.get("canonical_data") or {}
        # Show entity summary
        name = _extract_name(cd)
        email = _extract_email(cd)
        etype = canonical.get("entity_type", "-")
        sys.stdout.write(f"  {name} ({email}) - {etype}\n\n")
        # Show linked records
        records = resp.get("linked_entities", []) if isinstance(resp, dict) else []
        links = resp.get("links", []) if isinstance(resp, dict) else []
        confidence_map: dict[str, str] = {}
        for lnk in links:
            ext_id = lnk.get("external_entity_id", "")
            conf = lnk.get("confidence")
            if ext_id and conf is not None:
                confidence_map[ext_id] = f"{float(conf):.2f}"
        rows = []
        for r in records:
            rid = str(r.get("id", "-"))
            rows.append([
                r.get("source_name", "-"),
                r.get("external_id", "-"),
                confidence_map.get(rid, "-"),
                r.get("ingested_at", "-")[:10] if r.get("ingested_at") else "-",
            ])
        print_table(["SOURCE", "EXTERNAL_ID", "CONFIDENCE", "INGESTED"], rows)
        return

    resp = client.get(f"/v1/canonical/{args.entity_id}")

    if json_out:
        print_json(resp)
        return

    cd = resp.get("canonical_data") or {}

    # --attrs: show only canonical attributes
    if show_attrs:
        if not cd:
            sys.stdout.write("No canonical attributes.\n")
        else:
            print_kv("Canonical Attributes", cd)
        return

    # --sources: show field provenance
    if show_sources:
        prov = resp.get("field_provenance") or {}
        if not prov:
            sys.stdout.write("No field provenance data.\n")
        else:
            print_kv("Field Provenance (field -> source)", prov)
        return

    # Default: full entity detail view
    eid = resp.get("id", resp.get("entity_id", args.entity_id))

    fields = [
        ("Entity", str(eid)),
        ("Name", _extract_name(cd)),
        ("Email", _extract_email(cd)),
        ("Phone", _extract_phone(cd)),
        ("Type", resp.get("entity_type", "-")),
        ("Confidence", str(resp.get("confidence_score", "-"))),
        ("Locked", str(resp.get("is_locked", False))),
        ("Updated", resp.get("updated_at", "-")),
    ]
    print_detail("Entity", fields)

    if cd:
        print_kv("Canonical Attributes", cd)


def _search(args: Any, client: Any) -> None:
    """Search entities by attributes."""
    params: dict[str, Any] = {
        "limit": str(args.limit),
        "offset": str(args.offset),
    }
    if args.query:
        params["q"] = args.query
    if getattr(args, "name", None):
        params["q"] = args.name
    if getattr(args, "email", None):
        params["q"] = args.email
    if getattr(args, "phone", None):
        params["q"] = args.phone
    if getattr(args, "source", None):
        params["source"] = args.source
    if getattr(args, "entity_type", None):
        params["entity_type"] = args.entity_type
    if getattr(args, "min_records", None) is not None:
        params["min_records"] = str(args.min_records)
    if getattr(args, "max_records", None) is not None:
        params["max_records"] = str(args.max_records)
    if getattr(args, "min_confidence", None) is not None:
        params["min_confidence"] = str(args.min_confidence)
    if getattr(args, "has_fields", None):
        params["has"] = args.has_fields
    if getattr(args, "missing_fields", None):
        params["missing"] = args.missing_fields

    resp = client.get("/v1/entities", params=params)

    if args.format == "json":
        print_json(resp)
        return

    # EntitySearchResponse: { data: Vec<SearchEntity>, total }
    # Each SearchEntity is tagged: { kind: "Canonical"|"External", entity: {...} }
    raw_data = resp if isinstance(resp, list) else resp.get("data", [])
    rows = []
    for item in raw_data:
        # Unwrap tagged enum if present
        e = item.get("entity", item) if isinstance(item, dict) else item
        eid = str(e.get("id", e.get("entity_id", "-")))
        cd = e.get("canonical_data") or {}

        etype = e.get("entity_type", "-")
        updated = (
            e.get("updated_at", e.get("ingested_at", "-"))[:10]
            if e.get("updated_at", e.get("ingested_at")) else "-"
        )

        rows.append([eid, _extract_name(cd), _extract_email(cd), _extract_phone(cd), etype, updated])
    print_table(["ID", "NAME", "EMAIL", "PHONE", "TYPE", "UPDATED"], rows)

    total = None
    if isinstance(resp, dict):
        total = resp.get("total")
    if total is not None:
        sys.stdout.write(f"\n  {total} total entities\n")


def _list(args: Any, client: Any) -> None:
    """List entities (convenience alias for search with no filters)."""
    # Ensure _search's expected attributes exist with safe defaults
    for attr in ("query", "name", "email", "phone", "source",
                 "has_fields", "missing_fields"):
        if not hasattr(args, attr):
            setattr(args, attr, None)
    _search(args, client)


def _linked(args: Any, client: Any) -> None:
    """Show all source records linked to an entity."""
    resp = client.get(f"/v1/canonical/{args.entity_id}/linked")

    if args.format == "json":
        print_json(resp)
        return

    # CanonicalDetailResponse: { canonical, linked_entities, links }
    canonical = resp.get("canonical", {}) if isinstance(resp, dict) else {}
    cd = canonical.get("canonical_data") or {}

    # Show entity summary at top
    if cd:
        name = _extract_name(cd)
        email = _extract_email(cd)
        etype = canonical.get("entity_type", "-")
        sys.stdout.write(f"  {name} ({email}) - {etype}\n\n")

    # LinkedEntityRef: { id, data_source_id, source_name, external_id, entity_type, ingested_at }
    records = resp if isinstance(resp, list) else resp.get("linked_entities", [])
    links = resp.get("links", []) if isinstance(resp, dict) else []

    if not records:
        sys.stdout.write("No linked records.\n")
        return

    # Build confidence lookup: external_entity_id -> confidence
    confidence_map: dict[str, str] = {}
    for lnk in links:
        ext_id = lnk.get("external_entity_id", "")
        conf = lnk.get("confidence")
        if ext_id and conf is not None:
            confidence_map[ext_id] = f"{float(conf):.2f}"

    rows = []
    for r in records:
        rid = str(r.get("id", "-"))
        conf = confidence_map.get(rid, "-")
        rows.append([
            r.get("source_name", "-"),
            r.get("external_id", "-"),
            conf,
            r.get("entity_type", "-"),
            r.get("ingested_at", "-")[:10] if r.get("ingested_at") else "-",
        ])
    print_table(["SOURCE", "EXTERNAL_ID", "CONFIDENCE", "TYPE", "INGESTED"], rows)


def _history(args: Any, client: Any) -> None:
    """Show entity merge history and lifecycle events."""
    resp = client.get(f"/v1/entities/{args.entity_id}/history")

    if args.format == "json":
        print_json(resp)
        return

    # Response is Vec<AuditEvent>: { id, actor_type, action, resource_type, resource_id, reason, timestamp }
    events = resp if isinstance(resp, list) else resp.get("events", [])
    if not events:
        sys.stdout.write("No history.\n")
        return

    rows = []
    for ev in events:
        eid = str(ev.get("id", "-"))
        if len(eid) > 12:
            eid = eid[:8] + "..."
        rows.append([
            eid,
            ev.get("action", "-"),
            ev.get("reason", "-") or "-",
            ev.get("timestamp", "-")[:19] if ev.get("timestamp") else "-",
        ])
    print_table(["ID", "ACTION", "REASON", "TIMESTAMP"], rows)


def _neighbors(args: Any, client: Any) -> None:
    """Show graph neighbors of an entity."""
    params: dict[str, Any] = {}
    depth = getattr(args, "depth", 1)
    if depth and depth > 1:
        params["depth"] = str(depth)

    resp = client.get(f"/v1/prism/graph/{args.entity_id}", params=params or None)

    if args.format == "json":
        print_json(resp)
        return

    center = resp.get("center", {})
    neighbors = resp.get("neighbors", [])

    if center:
        etype = center.get("entity_type", "-")
        label = center.get("label", center.get("name", "-"))
        sys.stdout.write(f"  Center: {args.entity_id} ({etype}: {label})\n\n")

    if not neighbors:
        sys.stdout.write("  No neighbors.\n")
        return

    rows = []
    for n in neighbors:
        nid = n.get("entity_id", "-")
        if len(nid) > 12:
            nid = nid[:8] + "..."
        rows.append([
            nid,
            n.get("entity_type", "-"),
            n.get("label", n.get("name", "-")),
            n.get("relationship", n.get("edge_type", "-")),
            str(n.get("strength", "-")),
        ])
    print_table(["ID", "TYPE", "LABEL", "RELATIONSHIP", "STRENGTH"], rows)


def _merge(args: Any, client: Any) -> None:
    """Immediately merge two entities in the identity graph."""
    body: dict[str, Any] = {
        "entity_a_id": args.entity_a,
        "entity_b_id": args.entity_b,
    }
    reason = getattr(args, "reason", None)
    if reason:
        body["reason"] = reason

    resp = client.post("/v1/entities/merge", body)

    if args.format == "json":
        print_json(resp)
    else:
        winner = resp.get("winner_id", "-") if resp else "-"
        moved = resp.get("members_moved", 0) if resp else 0
        total = resp.get("total_members", 0) if resp else 0
        sys.stdout.write(
            f"Merged {args.entity_a} + {args.entity_b}\n"
            f"  Winner: {winner}\n"
            f"  Members moved: {moved}, Total: {total}\n"
        )


def _split(args: Any, client: Any) -> None:
    """Split members out of a canonical entity into new entities."""
    body: dict[str, Any] = {
        "canonical_id": args.canonical_id,
        "member_ids": args.member_ids,
    }
    reason = getattr(args, "reason", None)
    if reason:
        body["reason"] = reason

    resp = client.post("/v1/entities/split", body)

    if args.format == "json":
        print_json(resp)
    else:
        original = resp.get("original_id", "-") if resp else "-"
        remaining = resp.get("remaining_members", 0) if resp else 0
        new_entities = resp.get("new_entities", []) if resp else []
        sys.stdout.write(
            f"Split from {original} (remaining members: {remaining})\n"
        )
        for ne in new_entities:
            sys.stdout.write(
                f"  New entity: {ne.get('canonical_id', '-')} "
                f"(member: {ne.get('member_id', '-')})\n"
            )


def _reassign(args: Any, client: Any) -> None:
    """Move a source record from its current entity to a different one."""
    body: dict[str, Any] = {
        "external_entity_id": args.ext_id,
        "to_canonical_id": args.to_entity,
    }
    reason = getattr(args, "reason", None)
    if reason:
        body["reason"] = reason

    resp = client.post("/v1/entities/reassign", body)

    if args.format == "json":
        print_json(resp)
    else:
        from_id = resp.get("from_canonical_id", "-") if resp else "-"
        to_id = resp.get("to_canonical_id", "-") if resp else "-"
        sys.stdout.write(
            f"Reassigned {args.ext_id}\n"
            f"  From: {from_id}\n"
            f"  To:   {to_id}\n"
        )


def _delete(args: Any, client: Any) -> None:
    """Delete an empty canonical entity (no members)."""
    resp = client.delete(f"/v1/entities/delete/{args.entity_id}")

    if args.format == "json":
        print_json(resp)
    else:
        sys.stdout.write(f"Deleted entity {args.entity_id}\n")


def _link(args: Any, client: Any) -> None:
    """Create a soft relationship edge between two entities."""
    body: dict[str, Any] = {
        "entity_a_id": args.entity_a,
        "entity_b_id": args.entity_b,
    }
    weight = getattr(args, "weight", None)
    if weight is not None:
        body["weight"] = weight

    resp = client.post("/v1/entities/link", body)

    if args.format == "json":
        print_json(resp)
    else:
        edge_id = resp.get("edge_id", "-") if resp else "-"
        sys.stdout.write(
            f"Linked {args.entity_a} <-> {args.entity_b} (edge: {edge_id})\n"
        )


def _unlink(args: Any, client: Any) -> None:
    """Remove a soft relationship edge between two entities."""
    body: dict[str, Any] = {
        "entity_a_id": args.entity_a,
        "entity_b_id": args.entity_b,
    }

    resp = client.post("/v1/entities/unlink", body)

    if args.format == "json":
        print_json(resp)
    else:
        sys.stdout.write(
            f"Unlinked {args.entity_a} <-> {args.entity_b}\n"
        )


def _explain(args: Any, client: Any) -> None:
    """Explain why an entity exists in its current state."""
    resp = client.get(f"/v1/entities/{args.entity_id}/explain")

    if args.format == "json":
        print_json(resp)
        return

    member_count = resp.get("member_count", 0) if resp else 0
    merges = resp.get("merge_history", []) if resp else []
    events = resp.get("events", []) if resp else []
    evidence = resp.get("match_evidence", []) if resp else []

    sys.stdout.write(f"Entity {args.entity_id} ({member_count} members)\n\n")

    if merges:
        sys.stdout.write("  Merge History:\n")
        for m in merges:
            sys.stdout.write(
                f"    {m.get('merged_at', '-')[:19]} "
                f"{m.get('merge_type', '-')}: "
                f"winner={m.get('winner_entity_id', '-')[:8]}... "
                f"loser={m.get('loser_entity_id', '-')[:8]}...\n"
            )
        sys.stdout.write("\n")

    if events:
        sys.stdout.write("  Events:\n")
        for ev in events:
            sys.stdout.write(
                f"    {ev.get('created_at', '-')[:19]} "
                f"{ev.get('event_type', '-')}\n"
            )
        sys.stdout.write("\n")

    if evidence:
        sys.stdout.write("  Match Evidence:\n")
        for e in evidence:
            sys.stdout.write(
                f"    {e.get('entity_a_id', '-')[:8]}... <-> "
                f"{e.get('entity_b_id', '-')[:8]}... "
                f"score={e.get('confidence', '-')} "
                f"decision={e.get('decision', '-')}\n"
            )
    elif not merges and not events:
        sys.stdout.write("  No history or evidence found.\n")


def _lock(args: Any, client: Any) -> None:
    """Lock an entity from further merges."""
    body: dict[str, Any] = {"locked": True}
    resp = client.post(f"/v1/entities/{args.entity_id}/lock", body)

    if args.format == "json":
        print_json(resp or {"status": "locked", "entity_id": args.entity_id})
    else:
        sys.stdout.write(f"Locked entity {args.entity_id}\n")


def _revert(args: Any, client: Any) -> None:
    """Revert an entity to a previous state."""
    resp = client.post(f"/v1/entities/{args.entity_id}/revert/{args.event_id}")

    if args.format == "json":
        print_json(resp)
    else:
        sys.stdout.write(
            f"Reverted entity {args.entity_id} to event {args.event_id}\n"
        )


def _attrs(args: Any, client: Any) -> None:
    """Show canonical attributes for an entity (gold record fields only)."""
    resp = client.get(f"/v1/canonical/{args.entity_id}")

    if args.format == "json":
        canonical = resp.get("canonical_data", resp)
        print_json(canonical)
        return

    canonical = resp.get("canonical_data")
    if not isinstance(canonical, dict) or not canonical:
        sys.stdout.write("No canonical attributes.\n")
        return

    print_kv("Canonical Attributes", canonical)


def _candidates(args: Any, client: Any) -> None:
    """Show merge candidates for an entity - nearby entities that might match."""
    params: dict[str, str] = {}
    limit = getattr(args, "limit", 10)
    if limit:
        params["limit"] = str(limit)

    resp = client.get(
        f"/v1/entities/{args.entity_id}/candidates", params=params or None,
    )

    if args.format == "json":
        print_json(resp)
        return

    candidates = resp if isinstance(resp, list) else resp.get("candidates", [])
    if not candidates:
        sys.stdout.write("No merge candidates.\n")
        return

    rows = []
    for c in candidates:
        cid = c.get("entity_id", "-")
        if len(cid) > 12:
            cid = cid[:8] + "..."
        rows.append([
            cid,
            c.get("entity_type", "-"),
            str(c.get("score", "-")),
            str(c.get("member_count", "-")),
        ])
    print_table(["ID", "TYPE", "SCORE", "MEMBERS"], rows)


def _diff(args: Any, client: Any) -> None:
    """Diff two entities' canonical attributes side by side."""
    resp_a = client.get(f"/v1/canonical/{args.entity_a}")
    resp_b = client.get(f"/v1/canonical/{args.entity_b}")

    if args.format == "json":
        print_json({
            "entity_a": resp_a.get("canonical_data", {}),
            "entity_b": resp_b.get("canonical_data", {}),
        })
        return

    data_a = resp_a.get("canonical_data", {}) or {}
    data_b = resp_b.get("canonical_data", {}) or {}

    all_keys = sorted(set(list(data_a.keys()) + list(data_b.keys())))
    if not all_keys:
        sys.stdout.write("Both entities have no canonical attributes.\n")
        return

    id_a = args.entity_a[:8] + "..." if len(args.entity_a) > 12 else args.entity_a
    id_b = args.entity_b[:8] + "..." if len(args.entity_b) > 12 else args.entity_b

    rows = []
    for key in all_keys:
        val_a = str(data_a.get(key, "")) if key in data_a else ""
        val_b = str(data_b.get(key, "")) if key in data_b else ""
        match = "=" if val_a == val_b else "!="
        rows.append([key, val_a, match, val_b])

    print_table(["FIELD", id_a, "", id_b], rows)
