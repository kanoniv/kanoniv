"""Agent management commands.

Subcommands: list, enable, disable, trigger, runs, actions, approve, reject, register, who.
"""
from __future__ import annotations

import json
import sys
from typing import Any

from kanoniv.cli.output import print_json, print_table, print_error, print_success


def cmd_agent(args: Any, http_client: Any) -> None:
    """Route to the appropriate agent sub-command."""
    sub = args.agent_action
    if sub == "list":
        _list(args, http_client)
    elif sub == "enable":
        _enable(args, http_client)
    elif sub == "disable":
        _disable(args, http_client)
    elif sub == "trigger":
        _trigger(args, http_client)
    elif sub == "runs":
        _runs(args, http_client)
    elif sub == "actions":
        _actions(args, http_client)
    elif sub == "approve":
        _approve(args, http_client)
    elif sub == "reject":
        _reject(args, http_client)
    elif sub == "register":
        _register(args, http_client)
    elif sub == "who":
        _who(args, http_client)
    elif sub == "rename":
        _rename(args, http_client)
    else:
        print_error(f"Unknown agent action: {sub}")


def _list(args: Any, client: Any) -> None:
    resp = client.get("/v1/agents/configs")
    if args.format == "json":
        print_json(resp)
        return

    items = resp if isinstance(resp, list) else []
    rows = []
    for c in items:
        enabled = c.get("enabled", False)
        status = "enabled" if enabled else "disabled"
        rows.append([
            c.get("agent_type", "-"),
            status,
            c.get("schedule", "-"),
            c.get("updated_at", "-"),
        ])
    print_table(["AGENT", "STATUS", "SCHEDULE", "UPDATED"], rows)


def _enable(args: Any, client: Any) -> None:
    body: dict[str, Any] = {"enabled": True}
    if args.settings:
        try:
            body["settings"] = json.loads(args.settings)
        except json.JSONDecodeError as e:
            print_error(f"Invalid JSON settings: {e}")
            return

    resp = client.put(f"/v1/agents/configs/{args.agent_type}", body)
    if args.format == "json":
        print_json(resp)
    else:
        sys.stdout.write(f"Enabled agent {args.agent_type}\n")


def _disable(args: Any, client: Any) -> None:
    resp = client.put(f"/v1/agents/configs/{args.agent_type}", {"enabled": False})
    if args.format == "json":
        print_json(resp)
    else:
        sys.stdout.write(f"Disabled agent {args.agent_type}\n")


def _trigger(args: Any, client: Any) -> None:
    resp = client.post(f"/v1/agents/configs/{args.agent_type}/trigger")
    if args.format == "json":
        print_json(resp)
    else:
        run_id = resp.get("run_id", "-") if resp else "-"
        sys.stdout.write(f"Triggered agent {args.agent_type} - run {run_id}\n")


def _runs(args: Any, client: Any) -> None:
    params: dict[str, Any] = {"limit": str(getattr(args, "limit", 20))}
    if getattr(args, "type", None):
        params["agent_type"] = args.type

    resp = client.get("/v1/agents/runs", params)
    if args.format == "json":
        print_json(resp)
        return

    items = resp if isinstance(resp, list) else []
    rows = []
    for r in items:
        duration = r.get("duration_ms")
        dur_str = f"{duration}ms" if duration is not None else "-"
        rows.append([
            r.get("id", "-"),
            r.get("agent_type", "-"),
            r.get("status", "-"),
            r.get("started_at", "-"),
            dur_str,
        ])
    print_table(["ID", "AGENT", "STATUS", "STARTED", "DURATION"], rows)


def _actions(args: Any, client: Any) -> None:
    params: dict[str, Any] = {"limit": str(getattr(args, "limit", 20))}
    if getattr(args, "status_filter", None):
        params["status"] = args.status_filter

    resp = client.get("/v1/agents/actions", params)
    if args.format == "json":
        print_json(resp)
        return

    items = resp if isinstance(resp, list) else []
    rows = []
    for a in items:
        rows.append([
            a.get("id", "-"),
            a.get("agent_type", "-"),
            a.get("action_type", "-"),
            a.get("status", "-"),
            a.get("created_at", "-"),
        ])
    print_table(["ID", "AGENT", "ACTION", "STATUS", "CREATED"], rows)


def _approve(args: Any, client: Any) -> None:
    resp = client.post(f"/v1/agents/actions/{args.action_id}/approve")
    if args.format == "json":
        print_json(resp)
    else:
        sys.stdout.write(f"Approved action {args.action_id}\n")


def _reject(args: Any, client: Any) -> None:
    resp = client.post(f"/v1/agents/actions/{args.action_id}/reject")
    if args.format == "json":
        print_json(resp)
    else:
        sys.stdout.write(f"Rejected action {args.action_id}\n")


def _register(args: Any, client: Any) -> None:
    """Register an agent in the agent directory."""
    body: dict[str, Any] = {"name": args.name}
    if args.capabilities:
        body["capabilities"] = [c.strip() for c in args.capabilities.split(",")]
    if args.description:
        body["description"] = args.description
    if getattr(args, "instance_id", None):
        body["instance_id"] = args.instance_id

    resp = client.post("/v1/agent-registry/register", body)

    if args.format == "json":
        print_json(resp)
    else:
        name = resp.get("name", args.name) if resp else args.name
        instance = resp.get("instance_id", "default") if resp else "default"
        status = resp.get("status", "online") if resp else "online"
        print_success(f"Registered: {name} (instance: {instance}, status: {status})")


def _who(args: Any, client: Any) -> None:
    """List all registered agents (the agent directory)."""
    resp = client.get("/v1/agent-registry")

    if args.format == "json":
        print_json(resp)
        return

    items = resp if isinstance(resp, list) else []
    if not items:
        sys.stdout.write("No agents registered yet.\n")
        sys.stdout.write("  Register with: kanoniv agent register <name>\n")
        return

    rows = []
    for a in items:
        status = a.get("status", "?")
        icon = {"online": "[+]", "idle": "[~]", "offline": "[-]"}.get(status, "[ ]")
        caps = ", ".join(a.get("capabilities", []))
        last_seen = str(a.get("last_seen_at", ""))[:19]
        instance = a.get("instance_id", "default")
        name = a.get("name", "?")
        if instance != "default":
            name = f"{name} ({instance})"
        rows.append([
            f"{icon} {name}",
            status,
            caps or "-",
            a.get("description", "") or "-",
            last_seen,
        ])
    print_table(["AGENT", "STATUS", "CAPABILITIES", "DESCRIPTION", "LAST SEEN"], rows)


def _rename(args: Any, client: Any) -> None:
    """Rename an agent."""
    resp = client.post(f"/v1/agent-registry/{args.current_name}/rename", {"new_name": args.new_name})
    if args.format == "json":
        print_json(resp)
    else:
        print_success(f"Renamed '{args.current_name}' -> '{args.new_name}'")
