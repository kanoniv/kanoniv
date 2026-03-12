"""Row commands: show, resolve, lookup, memberships, trace.

Rows are individual source records - the raw data fed into the identity
resolution pipeline before clustering into entities.
"""
from __future__ import annotations

import sys
from typing import Any

from kanoniv.cli.output import print_json, print_table, print_detail, print_kv


def cmd_row(args: Any, client: Any) -> None:
    """Route to the appropriate row sub-command."""
    action = args.action
    dispatch = {
        "show": _show,
        "resolve": _resolve,
        "lookup": _lookup,
        "memberships": _memberships,
        "trace": _trace,
    }
    fn = dispatch.get(action)
    if fn:
        fn(args, client)
    else:
        sys.stderr.write(
            "usage: kanoniv row {show,resolve,lookup,memberships,trace}\n"
        )
        sys.exit(1)


def _show(args: Any, client: Any) -> None:
    """Show a source record by source name and external_id."""
    source = args.source
    external_id = args.external_id

    resp = client.get(
        "/v1/resolve",
        params={"source_name": source, "external_id": external_id},
    )

    if args.format == "json":
        print_json(resp)
        return

    if not resp:
        sys.stdout.write("Record not found.\n")
        return

    entity_id = resp.get("entity_id", "-")
    fields = [
        ("Source", source),
        ("External ID", external_id),
        ("Entity", entity_id),
        ("Confidence", str(resp.get("confidence", "-"))),
        ("Link Type", resp.get("link_type", "-")),
        ("Created", resp.get("created_at", "-")),
    ]
    print_detail("Record", fields)

    data = resp.get("data", resp.get("raw_data", resp.get("canonical_data")))
    if isinstance(data, dict) and data:
        print_kv("Fields", data)


def _resolve(args: Any, client: Any) -> None:
    """Real-time resolve a record: ingest + match + link in one call."""
    import json as _json

    source = args.source
    external_id = args.external_id

    body: dict[str, Any] = {
        "source_name": source,
        "external_id": external_id,
    }

    data_str = getattr(args, "data", None)
    if data_str:
        try:
            body["data"] = _json.loads(data_str)
        except _json.JSONDecodeError as e:
            sys.stderr.write(f"error: Invalid JSON data: {e}\n")
            sys.exit(1)

    resp = client.post("/v1/resolve/realtime", body)

    if args.format == "json":
        print_json(resp)
        return

    if not resp:
        sys.stdout.write("No resolution result.\n")
        return

    entity_id = resp.get("entity_id", "-")
    action_taken = resp.get("action", resp.get("resolution", "-"))
    fields = [
        ("Entity", entity_id),
        ("Action", str(action_taken)),
        ("Confidence", str(resp.get("confidence", "-"))),
        ("Match Count", str(resp.get("match_count", resp.get("candidates", "-")))),
    ]
    print_detail("Resolve", fields)


def _lookup(args: Any, client: Any) -> None:
    """Look up what entity a source record belongs to."""
    source = args.source
    external_id = args.external_id

    resp = client.get(
        "/v1/resolve",
        params={"source_name": source, "external_id": external_id},
    )

    if args.format == "json":
        print_json(resp)
        return

    if not resp:
        sys.stdout.write("No entity found for this record.\n")
        return

    entity_id = resp.get("entity_id", "-")
    sys.stdout.write(f"{entity_id}\n")


def _memberships(args: Any, client: Any) -> None:
    """List source-to-entity memberships."""
    params: dict[str, str] = {"limit": str(getattr(args, "limit", 50))}

    source = getattr(args, "source", None)
    if source:
        params["source_name"] = source
    entity_id = getattr(args, "entity_id", None)
    if entity_id:
        params["entity_id"] = entity_id

    resp = client.get("/v1/memberships", params=params)

    if args.format == "json":
        print_json(resp)
        return

    items = resp if isinstance(resp, list) else resp.get("memberships", [])
    if not items:
        sys.stdout.write("No memberships.\n")
        return

    rows = []
    for m in items:
        eid = m.get("entity_id", "-")
        if len(eid) > 12:
            eid = eid[:8] + "..."
        rows.append([
            m.get("source_system", m.get("source_name", "-")),
            m.get("external_id", m.get("source_id", "-")),
            eid,
            str(m.get("confidence", "-")),
            m.get("link_type", "-"),
        ])
    print_table(["SOURCE", "EXTERNAL_ID", "ENTITY", "CONFIDENCE", "LINK_TYPE"], rows)

    total = None
    if isinstance(resp, dict):
        total = resp.get("total")
    if total is not None:
        sys.stdout.write(f"\n  {total} total memberships\n")


def _trace(args: Any, client: Any) -> None:
    """Show match audit trail for a source record."""
    source = args.source
    external_id = args.external_id

    params: dict[str, str] = {
        "source_name": source,
        "external_id": external_id,
    }

    resp = client.get("/v1/match/trace", params=params)

    if args.format == "json":
        print_json(resp)
        return

    # TraceResponse: { traces: Vec<TraceEntry> }
    # TraceEntry: { entity_a_id, entity_b_id, score, decision, rule_trace, created_at }
    traces = resp if isinstance(resp, list) else resp.get("traces", [])
    if not traces:
        sys.stdout.write("No match trace for this record.\n")
        return

    rows = []
    for t in traces:
        ea = str(t.get("entity_a_id", "-"))
        eb = str(t.get("entity_b_id", "-"))
        if len(ea) > 12:
            ea = ea[:8] + "..."
        if len(eb) > 12:
            eb = eb[:8] + "..."
        rows.append([
            ea,
            eb,
            str(t.get("score", "-")),
            t.get("decision", "-"),
            t.get("created_at", "-")[:19] if t.get("created_at") else "-",
        ])
    print_table(["ENTITY_A", "ENTITY_B", "SCORE", "DECISION", "CREATED"], rows)
