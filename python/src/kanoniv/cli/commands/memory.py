"""Memory commands: create, recall, search, intent, expertise, list, delete."""
from __future__ import annotations

import json
import sys
from typing import Any

from kanoniv.cli.output import print_json, print_table, print_success


def cmd_memory(args: Any, client: Any) -> None:
    """Route to the appropriate memory sub-command."""
    dispatch = {
        "create": _create,
        "recall": _recall,
        "search": _search,
        "intent": _intent,
        "expertise": _expertise,
        "list": _list,
        "delete": _delete,
    }
    fn = dispatch.get(args.action)
    if fn:
        fn(args, client)
    else:
        sys.stderr.write(
            "usage: kanoniv memory "
            "{create,recall,search,intent,expertise,list,delete}\n"
        )
        sys.exit(1)


def _create(args: Any, client: Any) -> None:
    """Create a memory entry (decision, investigation, pattern, knowledge)."""
    body: dict[str, Any] = {
        "entry_type": args.entry_type,
        "title": args.title,
        "content": args.content or "",
    }
    if args.slug:
        body["slug"] = args.slug
    if args.entity_ids:
        body["linked_entities"] = [e.strip() for e in args.entity_ids.split(",")]
    if args.author:
        body["author"] = args.author
    metadata: dict[str, Any] = {}
    if args.tags:
        metadata["tags"] = [t.strip() for t in args.tags.split(",")]
    if metadata:
        body["metadata"] = metadata

    resp = client.post("/v1/memory", body)

    if args.format == "json":
        print_json(resp)
    else:
        entry_id = resp.get("id", "?") if resp else "?"
        print_success(f"Memory entry created: {entry_id}")


def _recall(args: Any, client: Any) -> None:
    """Get all memory linked to an entity."""
    params: dict[str, Any] = {"entity_id": args.entity_id}
    if args.limit:
        params["limit"] = args.limit

    resp = client.get("/v1/memory", params=params)
    entries = resp if isinstance(resp, list) else []

    if args.format == "json":
        print_json(entries)
        return

    if not entries:
        sys.stdout.write("No memory entries found for this entity.\n")
        return

    rows = []
    for e in entries:
        rows.append([
            str(e.get("id", ""))[:8],
            e.get("entry_type", ""),
            e.get("title", e.get("slug", ""))[:50],
            e.get("author", "-"),
            str(e.get("created_at", ""))[:19],
        ])
    print_table(["ID", "TYPE", "TITLE", "AUTHOR", "CREATED"], rows)


def _search(args: Any, client: Any) -> None:
    """Full-text search across all memory entries."""
    params: dict[str, Any] = {}
    if args.query:
        params["q"] = args.query
    if args.entry_type:
        params["entry_type"] = args.entry_type
    if args.author:
        params["author"] = args.author
    if args.limit:
        params["limit"] = args.limit

    resp = client.get("/v1/memory", params=params if params else None)
    entries = resp if isinstance(resp, list) else []

    if args.format == "json":
        print_json(entries)
        return

    if not entries:
        sys.stdout.write("No memory entries found.\n")
        return

    rows = []
    for e in entries:
        rows.append([
            str(e.get("id", ""))[:8],
            e.get("entry_type", ""),
            e.get("title", e.get("slug", ""))[:50],
            e.get("author", "-"),
            str(e.get("created_at", ""))[:19],
        ])
    print_table(["ID", "TYPE", "TITLE", "AUTHOR", "CREATED"], rows)


def _intent(args: Any, client: Any) -> None:
    """Declare intent - tell other agents what you're about to do."""
    body: dict[str, Any] = {
        "entry_type": "intent",
        "title": args.message,
        "content": args.message,
    }
    if args.entity_ids:
        body["linked_entities"] = [e.strip() for e in args.entity_ids.split(",")]
    if args.author:
        body["author"] = args.author
    body["metadata"] = {"expires_in": args.ttl}

    resp = client.post("/v1/memory", body)

    if args.format == "json":
        print_json(resp)
    else:
        print_success(f"Intent declared: {args.message}")


def _expertise(args: Any, client: Any) -> None:
    """Query which agents have expertise with an entity or domain."""
    params: dict[str, Any] = {"entry_type": "expertise"}
    if args.query:
        params["q"] = args.query
    if args.entity_id:
        params["entity_id"] = args.entity_id
    params["limit"] = args.limit or 20

    resp = client.get("/v1/memory", params=params)
    entries = resp if isinstance(resp, list) else []

    if args.format == "json":
        print_json(entries)
        return

    if not entries:
        sys.stdout.write("No expertise entries found.\n")
        return

    rows = []
    for e in entries:
        rows.append([
            e.get("author", "-"),
            e.get("title", e.get("slug", ""))[:50],
            str(e.get("created_at", ""))[:19],
        ])
    print_table(["AGENT", "EXPERTISE", "SINCE"], rows)


def _list(args: Any, client: Any) -> None:
    """List all memory entries."""
    params: dict[str, Any] = {"limit": args.limit or 20}
    if args.entry_type:
        params["entry_type"] = args.entry_type

    resp = client.get("/v1/memory", params=params)
    entries = resp if isinstance(resp, list) else []

    if args.format == "json":
        print_json(entries)
        return

    if not entries:
        sys.stdout.write("No memory entries.\n")
        return

    rows = []
    for e in entries:
        rows.append([
            str(e.get("id", ""))[:8],
            e.get("entry_type", ""),
            e.get("title", e.get("slug", ""))[:50],
            e.get("author", "-"),
            str(e.get("created_at", ""))[:19],
        ])
    print_table(["ID", "TYPE", "TITLE", "AUTHOR", "CREATED"], rows)


def _delete(args: Any, client: Any) -> None:
    """Delete a memory entry."""
    client.delete(f"/v1/memory/{args.entry_id}")

    if args.format == "json":
        print_json({"status": "deleted", "id": args.entry_id})
    else:
        print_success(f"Memory entry deleted: {args.entry_id}")
