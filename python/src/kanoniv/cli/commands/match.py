"""Match commands: explain, rules, pending, decide, candidates, cluster, test.

The matching layer handles similarity scoring and resolution decisions -
the core science of identity resolution.
"""
from __future__ import annotations

import sys
from typing import Any

from kanoniv.cli.output import (
    print_json, print_table, print_detail, print_error, print_success,
)


def cmd_match(args: Any, client: Any) -> None:
    """Route to the appropriate match sub-command."""
    action = args.action
    dispatch = {
        "explain": _explain,
        "rules": _rules,
        "pending": _pending,
        "decide": _decide,
        "candidates": _candidates,
        "cluster": _cluster,
        "test": _test,
    }
    fn = dispatch.get(action)
    if fn:
        fn(args, client)
    else:
        sys.stderr.write(
            "usage: kanoniv match "
            "{explain,rules,pending,decide,candidates,cluster,test}\n"
        )
        sys.exit(1)


def _explain(args: Any, client: Any) -> None:
    """Explain match between two records with field-level similarity."""
    # Parse source:external_id format
    record_a = args.record_a
    record_b = args.record_b

    body: dict[str, Any] = {
        "record_a": record_a,
        "record_b": record_b,
    }

    resp = client.post("/v1/match/explain", body)

    if args.format == "json":
        print_json(resp)
        return

    decision = resp.get("decision", "-")
    total_score = resp.get("total_score", resp.get("score", "-"))

    sys.stdout.write(f"Match Explanation\n\n")
    sys.stdout.write(f"  Decision:    {decision}\n")
    sys.stdout.write(f"  Total Score: {total_score}\n")

    # Field-level breakdown
    fields = resp.get("field_scores", resp.get("fields", []))
    if fields:
        sys.stdout.write("\n")
        rows = []
        for f in fields:
            if isinstance(f, dict):
                rows.append([
                    f.get("field", "-"),
                    str(f.get("similarity", f.get("score", "-"))),
                    str(f.get("weight", "-")),
                    f.get("comparator", "-"),
                ])
        print_table(["FIELD", "SIMILARITY", "WEIGHT", "COMPARATOR"], rows)

    rules = resp.get("rules_triggered", [])
    if rules:
        sys.stdout.write("\n  Rules Triggered\n")
        for r in rules:
            if isinstance(r, str):
                sys.stdout.write(f"    - {r}\n")
            elif isinstance(r, dict):
                sys.stdout.write(f"    - {r.get('name', r)}\n")


def _rules(args: Any, client: Any) -> None:
    """List active matching rules."""
    resp = client.get("/v1/rules")

    if args.format == "json":
        print_json(resp)
        return

    rules = resp if isinstance(resp, list) else []
    if not rules:
        sys.stdout.write("No active rules.\n")
        return

    rows = []
    for r in rules:
        rows.append([
            r.get("name", "-"),
            r.get("rule_type", "-"),
            str(r.get("weight", "-")),
            r.get("created_at", "-")[:19] if r.get("created_at") else "-",
        ])
    print_table(["NAME", "TYPE", "WEIGHT", "CREATED"], rows)


def _pending(args: Any, client: Any) -> None:
    """List pending match decisions awaiting review."""
    params = {"limit": str(getattr(args, "limit", 20))}
    resp = client.get("/v1/resolve/pending", params=params)

    if args.format == "json":
        print_json(resp)
        return

    items = resp if isinstance(resp, list) else resp.get("pending", [])
    if not items:
        sys.stdout.write("No pending decisions.\n")
        return

    rows = []
    for p in items:
        entity_a = p.get("entity_a_id", "-")
        entity_b = p.get("entity_b_id", "-")
        if len(entity_a) > 12:
            entity_a = entity_a[:8] + "..."
        if len(entity_b) > 12:
            entity_b = entity_b[:8] + "..."
        rows.append([
            entity_a,
            entity_b,
            str(p.get("score", "-")),
            p.get("source_a", "-"),
            p.get("source_b", "-"),
        ])
    print_table(["ENTITY_A", "ENTITY_B", "SCORE", "SOURCE_A", "SOURCE_B"], rows)


def _decide(args: Any, client: Any) -> None:
    """Accept or reject a pending match decision."""
    accept = getattr(args, "accept", False)
    reject = getattr(args, "reject", False)

    if not accept and not reject:
        print_error("Specify --accept or --reject")
    if accept and reject:
        print_error("Cannot specify both --accept and --reject")

    decision = "accept" if accept else "reject"
    body: dict[str, Any] = {
        "entity_a_id": args.entity_a,
        "entity_b_id": args.entity_b,
        "decision": decision,
    }
    reason = getattr(args, "reason", None)
    if reason:
        body["reason"] = reason

    resp = client.post("/v1/resolve/quick", body)

    if args.format == "json":
        print_json(resp)
    else:
        verb = "Accepted" if accept else "Rejected"
        print_success(f"{verb} match: {args.entity_a} <-> {args.entity_b}")


def _candidates(args: Any, client: Any) -> None:
    """Show match candidates for an entity - records that could potentially match."""
    params: dict[str, str] = {}
    limit = getattr(args, "limit", 20)
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
        sys.stdout.write("No match candidates.\n")
        return

    rows = []
    for c in candidates:
        cid = c.get("entity_id", c.get("candidate_id", "-"))
        if len(cid) > 12:
            cid = cid[:8] + "..."
        rows.append([
            cid,
            c.get("entity_type", "-"),
            str(c.get("score", "-")),
            str(c.get("member_count", "-")),
        ])
    print_table(["CANDIDATE", "TYPE", "SCORE", "MEMBERS"], rows)


def _cluster(args: Any, client: Any) -> None:
    """Show the match cluster an entity belongs to."""
    resp = client.get(f"/v1/match/cluster/{args.entity_id}")

    if args.format == "json":
        print_json(resp)
        return

    cluster_id = resp.get("cluster_id", "-")
    fields = [
        ("Entity", args.entity_id),
        ("Cluster", str(cluster_id)),
        ("Cluster Size", str(resp.get("size", resp.get("member_count", "-")))),
        ("Coherence", str(resp.get("coherence_score", "-"))),
    ]
    fields = [(k, v) for k, v in fields if v != "-"]
    print_detail("Match Cluster", fields)

    members = resp.get("members", [])
    if members:
        sys.stdout.write("\n")
        rows = []
        for m in members:
            mid = m.get("entity_id", "-")
            if len(mid) > 12:
                mid = mid[:8] + "..."
            rows.append([
                mid,
                m.get("source", m.get("source_name", "-")),
                m.get("external_id", "-"),
                str(m.get("confidence", "-")),
            ])
        print_table(["ENTITY", "SOURCE", "EXTERNAL_ID", "CONFIDENCE"], rows)


def _test(args: Any, client: Any) -> None:
    """Test match two records (dry-run) - score without persisting."""
    record_a = args.record_a
    record_b = args.record_b

    body: dict[str, Any] = {
        "record_a": record_a,
        "record_b": record_b,
        "dry_run": True,
    }

    resp = client.post("/v1/match/explain", body)

    if args.format == "json":
        print_json(resp)
        return

    decision = resp.get("decision", "-")
    total_score = resp.get("total_score", resp.get("score", "-"))

    sys.stdout.write("Match Test (dry-run)\n\n")
    sys.stdout.write(f"  Record A:  {record_a}\n")
    sys.stdout.write(f"  Record B:  {record_b}\n")
    sys.stdout.write(f"  Decision:  {decision}\n")
    sys.stdout.write(f"  Score:     {total_score}\n")

    fields = resp.get("field_scores", resp.get("fields", []))
    if fields:
        sys.stdout.write("\n")
        rows = []
        for f in fields:
            if isinstance(f, dict):
                rows.append([
                    f.get("field", "-"),
                    str(f.get("similarity", f.get("score", "-"))),
                    str(f.get("weight", "-")),
                    f.get("comparator", "-"),
                ])
        print_table(["FIELD", "SIMILARITY", "WEIGHT", "COMPARATOR"], rows)
