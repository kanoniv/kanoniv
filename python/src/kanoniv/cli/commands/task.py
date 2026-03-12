"""Task commands: create, list, update, show.

Cross-agent task assignment - like GitHub Issues for AI agents.
"""
from __future__ import annotations

import sys
from typing import Any

from kanoniv.cli.output import print_json, print_table, print_success, print_detail


def cmd_task(args: Any, client: Any) -> None:
    """Route to the appropriate task sub-command."""
    dispatch = {
        "create": _create,
        "list": _list,
        "update": _update,
        "show": _show,
    }
    fn = dispatch.get(args.action)
    if fn:
        fn(args, client)
    else:
        sys.stderr.write(
            "usage: kanoniv task {create,list,update,show}\n"
        )
        sys.exit(1)


def _create(args: Any, client: Any) -> None:
    """Create a task and optionally assign it to another agent."""
    body: dict[str, Any] = {
        "entry_type": "task",
        "title": args.title,
        "content": args.content or args.title,
    }
    if args.slug:
        body["slug"] = args.slug
    if args.entity_ids:
        body["linked_entities"] = [e.strip() for e in args.entity_ids.split(",")]
    if args.author:
        body["author"] = args.author

    metadata: dict[str, Any] = {
        "status": "open",
        "priority": args.priority or "medium",
    }
    if args.assigned_to:
        metadata["assigned_to"] = args.assigned_to
    body["metadata"] = metadata

    resp = client.post("/v1/memory", body)

    if args.format == "json":
        print_json(resp)
    else:
        entry_id = resp.get("id", "?") if resp else "?"
        assigned = f" -> {args.assigned_to}" if args.assigned_to else ""
        print_success(f"Task created: {entry_id} \"{args.title}\"{assigned}")


def _list(args: Any, client: Any) -> None:
    """List tasks, optionally filtered by status or assignee."""
    params: dict[str, Any] = {
        "entry_type": "task",
        "limit": args.limit or 20,
    }
    if args.assigned_to:
        params["q"] = args.assigned_to
    if args.status:
        # Map CLI status names to memory entry status
        status_map = {"open": "active", "in_progress": "active", "done": "resolved"}
        params["status"] = status_map.get(args.status, args.status)

    resp = client.get("/v1/memory", params=params)
    entries = resp if isinstance(resp, list) else []

    if args.format == "json":
        print_json(entries)
        return

    if not entries:
        sys.stdout.write("No tasks found.\n")
        return

    rows = []
    for e in entries:
        meta = e.get("metadata") or {}
        status = meta.get("status", "open")
        assigned = meta.get("assigned_to", "-")
        priority = meta.get("priority", "-")
        rows.append([
            str(e.get("id", ""))[:8],
            _status_icon(status) + " " + status,
            priority,
            e.get("title", "")[:40],
            e.get("author", "-"),
            assigned,
            str(e.get("created_at", ""))[:10],
        ])
    print_table(
        ["ID", "STATUS", "PRI", "TITLE", "CREATED BY", "ASSIGNED TO", "DATE"],
        rows,
    )


def _update(args: Any, client: Any) -> None:
    """Update a task's status."""
    # Map CLI status to memory entry status
    status_map = {"open": "active", "in_progress": "active", "done": "resolved"}
    memory_status = status_map.get(args.status, args.status)

    body: dict[str, Any] = {
        "status": memory_status,
        "metadata": {
            "status": args.status,
        },
    }
    if args.note:
        body["metadata"]["note"] = args.note
    if args.author:
        body["metadata"]["updated_by"] = args.author

    resp = client.put(f"/v1/memory/{args.task_id}", body)

    if args.format == "json":
        print_json(resp)
    else:
        print_success(f"Task {args.task_id[:8]}... updated to: {args.status}")


def _show(args: Any, client: Any) -> None:
    """Show task details."""
    # Fetch by searching for the ID
    resp = client.get(f"/v1/memory/{args.task_id}")

    if args.format == "json":
        print_json(resp)
        return

    if not resp:
        sys.stderr.write(f"Task not found: {args.task_id}\n")
        sys.exit(1)

    meta = resp.get("metadata") or {}
    fields = [
        ("ID", str(resp.get("id", ""))),
        ("Title", resp.get("title", "")),
        ("Status", meta.get("status", "open")),
        ("Priority", meta.get("priority", "-")),
        ("Created by", resp.get("author", "-")),
        ("Assigned to", meta.get("assigned_to", "-")),
        ("Created", str(resp.get("created_at", ""))[:19]),
        ("Updated", str(resp.get("updated_at", ""))[:19]),
    ]
    print_detail("Task", fields)

    content = resp.get("content", "")
    if content and content != resp.get("title", ""):
        sys.stdout.write(f"\n  {content}\n")

    linked = resp.get("linked_entities") or []
    if linked:
        sys.stdout.write(f"\n  Linked entities: {', '.join(str(e) for e in linked)}\n")


def _status_icon(status: str) -> str:
    """Return a simple text icon for task status."""
    icons = {
        "open": "[ ]",
        "in_progress": "[~]",
        "done": "[x]",
        "resolved": "[x]",
    }
    return icons.get(status, "[ ]")
