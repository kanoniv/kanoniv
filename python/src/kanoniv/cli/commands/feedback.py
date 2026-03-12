"""Feedback commands: list, create, delete.

Feedback labels drive active learning - manual match/no_match annotations
that train the Fellegi-Sunter probabilistic model.
"""
from __future__ import annotations

import sys
from typing import Any

from kanoniv.cli.output import print_json, print_table, print_success


def cmd_feedback(args: Any, client: Any) -> None:
    """Route to the appropriate feedback sub-command."""
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
        sys.stderr.write("usage: kanoniv feedback {list,create,delete}\n")
        sys.exit(1)


def _list(args: Any, client: Any) -> None:
    """List feedback labels."""
    params = {"limit": str(getattr(args, "limit", 20))}
    resp = client.get("/v1/feedback", params=params)

    if args.format == "json":
        print_json(resp)
        return

    # Vec<FeedbackLabelResponse>: { id, entity_a_id, entity_b_id, source_a, source_b, label, reason, created_at }
    items = resp if isinstance(resp, list) else resp.get("labels", [])
    if not items:
        sys.stdout.write("No feedback labels.\n")
        return

    rows = []
    for f in items:
        fid = str(f.get("id", "-"))
        if len(fid) > 12:
            fid = fid[:8] + "..."
        rows.append([
            fid,
            f.get("label", "-"),
            f.get("entity_a_id", "-"),
            f.get("entity_b_id", "-"),
            f.get("source_a", "-"),
            f.get("source_b", "-"),
            f.get("created_at", "-")[:19] if f.get("created_at") else "-",
        ])
    print_table(["ID", "LABEL", "ENTITY_A", "ENTITY_B", "SOURCE_A", "SOURCE_B", "CREATED"], rows)


def _create(args: Any, client: Any) -> None:
    """Create a feedback label."""
    # FeedbackLabelInput requires: entity_a_id, entity_b_id, source_a, source_b, label
    body: dict[str, Any] = {
        "labels": [{
            "entity_a_id": args.entity_a,
            "entity_b_id": args.entity_b,
            "source_a": args.source_a,
            "source_b": args.source_b,
            "label": args.label,
        }],
    }
    reason = getattr(args, "reason", None)
    if reason:
        body["labels"][0]["reason"] = reason

    resp = client.post("/v1/feedback", body)

    if args.format == "json":
        print_json(resp)
    else:
        print_success(
            f"Created {args.label} label: {args.entity_a} <-> {args.entity_b}"
        )


def _delete(args: Any, client: Any) -> None:
    """Delete a feedback label."""
    resp = client.delete(f"/v1/feedback/{args.feedback_id}")

    if args.format == "json":
        print_json(resp)
    else:
        print_success(f"Deleted feedback {args.feedback_id}")
