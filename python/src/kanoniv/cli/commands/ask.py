"""LLM chat commands: ask, plan, explain, generate-spec.

All commands hit the Cloud API via SSE streaming.
"""
from __future__ import annotations

import json
import sys
from typing import Any

from kanoniv.cli.output import print_json, print_error
from kanoniv.cli.config import save_last_conversation, get_last_conversation


def cmd_ask(args: Any, http_client: Any) -> None:
    """Route to the appropriate ask sub-command."""
    if args.plan:
        _run_plan(args, http_client)
    elif args.generate_spec:
        _run_generate_spec(args, http_client)
    elif args.entity_ids:
        _run_explain(args, http_client)
    else:
        _run_chat(args, http_client)


def _run_chat(args: Any, client: Any) -> None:
    """Stream a chat response via SSE."""
    body: dict[str, Any] = {"message": args.message}
    if not getattr(args, "new_conversation", False):
        conv = args.conversation or get_last_conversation()
        if conv:
            body["conversation_id"] = conv

    if args.format == "json":
        resp = client.post("/v1/llm/chat", body)
        print_json(resp)
        return

    conversation_id = None
    for event_type, data in client.stream_sse("/v1/llm/chat", body):
        if event_type == "conversation_id" and data:
            conversation_id = data.strip()
            save_last_conversation(conversation_id)
        elif event_type == "text" and data:
            sys.stdout.write(data)
            sys.stdout.flush()
        elif event_type == "tool_use" and data:
            try:
                parsed = json.loads(data)
                name = parsed.get("name", "")
                sys.stderr.write(f"[querying {name}...]\n")
            except (json.JSONDecodeError, TypeError):
                pass
        elif event_type == "usage" and data:
            pass  # silently consume token usage events
        elif event_type == "done":
            break
        elif event_type == "error" and data:
            sys.stderr.write(f"\nerror: {data}\n")

    sys.stdout.write("\n")
    if conversation_id:
        sys.stderr.write(f"conversation: {conversation_id}\n")


def _run_plan(args: Any, client: Any) -> None:
    """Request an execution plan from the LLM."""
    body: dict[str, Any] = {"intent": args.message}
    if args.conversation:
        body["conversation_id"] = args.conversation

    resp = client.post("/v1/llm/plan", body)

    if args.format == "json":
        print_json(resp)
        return

    goal = resp.get("goal", "")
    steps = resp.get("steps", [])
    risks = resp.get("risks", "")

    if goal:
        sys.stdout.write(f"Goal: {goal}\n")
    if steps:
        sys.stdout.write("Steps:\n")
        for i, step in enumerate(steps, 1):
            agent = step.get("agent_type", "-")
            desc = step.get("description", "-")
            sys.stdout.write(f"  {i}. {agent:<20} {desc}\n")
    if risks:
        sys.stdout.write(f"Risks: {risks}\n")
    conv_id = resp.get("conversation_id")
    if conv_id:
        sys.stderr.write(f"\nconversation: {conv_id}\n")


def _run_generate_spec(args: Any, client: Any) -> None:
    """Generate an identity spec via the LLM."""
    source_names = []
    if args.sources:
        source_names = [s.strip() for s in args.sources.split(",")]

    body: dict[str, Any] = {
        "description": args.message,
        "source_names": source_names,
    }
    if args.conversation:
        body["conversation_id"] = args.conversation

    resp = client.post("/v1/llm/generate-spec", body)

    if args.format == "json":
        print_json(resp)
        return

    yaml_str = resp.get("spec_yaml")
    if yaml_str:
        sys.stdout.write(yaml_str)
        sys.stdout.write("\n")
    else:
        spec = resp.get("spec")
        if spec:
            print_json(spec)

    conv_id = resp.get("conversation_id")
    if conv_id:
        sys.stderr.write(f"\nconversation: {conv_id}\n")


def _run_explain(args: Any, client: Any) -> None:
    """Explain entities via the LLM."""
    entity_ids = [eid.strip() for eid in args.entity_ids.split(",")]

    body: dict[str, Any] = {
        "question": args.message,
        "entity_ids": entity_ids,
    }
    if args.conversation:
        body["conversation_id"] = args.conversation

    resp = client.post("/v1/llm/explain", body)

    if args.format == "json":
        print_json(resp)
        return

    explanation = resp.get("explanation") or resp.get("response", "")
    if explanation:
        sys.stdout.write(f"{explanation}\n")

    conv_id = resp.get("conversation_id")
    if conv_id:
        sys.stderr.write(f"\nconversation: {conv_id}\n")
