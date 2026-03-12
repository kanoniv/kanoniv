"""Knowledge commands: sync, pull, log.

Cross-machine knowledge sharing and unified activity stream.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from kanoniv.cli.output import print_json, print_table, print_success


# -------------------------------------------------------------------------
# kanoniv sync - push local knowledge to cloud
# -------------------------------------------------------------------------

def cmd_sync(args: Any, client: Any) -> None:
    """Detect local knowledge files and push them to the cloud."""
    entries = _detect_knowledge()

    if not entries:
        sys.stdout.write("No knowledge files found to sync.\n")
        sys.stdout.write("  Looked for: CLAUDE.md, .claude/commands/*.md, memory/*.md\n")
        return

    if args.format == "json":
        # Just show what would be synced
        if getattr(args, "dry_run", False):
            print_json({"entries": [{"slug": e["slug"], "title": e["title"]} for e in entries]})
            return

    if getattr(args, "dry_run", False):
        sys.stdout.write(f"Would sync {len(entries)} entries:\n\n")
        for e in entries:
            size = len(e.get("content", ""))
            sys.stdout.write(f"  {e['slug']:<40} ({_format_bytes(size)})\n")
        return

    sys.stdout.write(f"Syncing {len(entries)} knowledge entries...\n\n")

    resp = client.post("/v1/memory/sync", {"entries": entries})

    if args.format == "json":
        print_json(resp)
        return

    created = resp.get("created", 0) if resp else 0
    updated = resp.get("updated", 0) if resp else 0
    print_success(f"Synced: {created} created, {updated} updated.")


# -------------------------------------------------------------------------
# kanoniv pull - download cloud knowledge to local files
# -------------------------------------------------------------------------

def cmd_pull(args: Any, client: Any) -> None:
    """Download knowledge entries from the cloud to local files."""
    resp = client.get("/v1/memory", params={"entry_type": "knowledge", "limit": "200"})
    entries = resp if isinstance(resp, list) else []

    if not entries:
        sys.stdout.write("No knowledge entries found. Run 'kanoniv sync' first.\n")
        return

    sys.stdout.write(f"Found {len(entries)} knowledge entries.\n\n")

    cwd = Path.cwd()
    home = Path.home()
    dry_run = getattr(args, "dry_run", False)
    created = 0
    updated = 0
    unchanged = 0

    for entry in entries:
        slug = entry.get("slug", "")
        content = entry.get("content", "")

        target = _route_slug(slug, cwd, home)
        if not target:
            sys.stdout.write(f"  ? {slug} - skipped (unknown prefix)\n")
            continue

        rel_path = _rel_path(target, cwd)

        # Check existing
        existing = None
        if target.exists():
            try:
                existing = target.read_text()
            except OSError:
                pass

        if existing == content:
            unchanged += 1
            continue

        if dry_run:
            if existing is None:
                sys.stdout.write(f"  + {rel_path} ({_format_bytes(len(content))}) [new]\n")
                created += 1
            else:
                diff = _simple_diff(existing, content)
                sys.stdout.write(f"  ~ {rel_path} ({_format_bytes(len(content))}) [{diff}]\n")
                updated += 1
            continue

        # Write
        target.parent.mkdir(parents=True, exist_ok=True)

        if existing is None:
            target.write_text(content)
            sys.stdout.write(f"  + {rel_path} ({_format_bytes(len(content))})\n")
            created += 1
        else:
            diff = _simple_diff(existing, content)
            target.write_text(content)
            sys.stdout.write(f"  ~ {rel_path} ({_format_bytes(len(content))}) [{diff}]\n")
            updated += 1

    prefix = "Would write" if dry_run else "Written"
    sys.stdout.write(f"\n{prefix}: {created} new, {updated} updated, {unchanged} unchanged.\n")


# -------------------------------------------------------------------------
# kanoniv log - unified activity stream
# -------------------------------------------------------------------------

def cmd_log(args: Any, client: Any) -> None:
    """Show unified activity stream across agents."""
    limit = args.limit or 50

    # Parse --since
    since = None
    if args.since:
        since = _parse_since(args.since)

    # Fetch all 3 streams in parallel (sequential here, but fast)
    events_params: dict[str, Any] = {"limit": limit}
    memory_params: dict[str, Any] = {"limit": limit}
    tasks_params: dict[str, Any] = {"entry_type": "task", "limit": limit}

    if since:
        events_params["since"] = since
        memory_params["since"] = since
        tasks_params["since"] = since

    events = _safe_get(client, "/v1/events", events_params)
    memories = _safe_get(client, "/v1/memory", memory_params)
    tasks = _safe_get(client, "/v1/memory", tasks_params)

    # Normalize into unified entries
    unified: list[dict[str, str]] = []

    ev_list = events if isinstance(events, list) else (events.get("events", []) if isinstance(events, dict) else [])
    for ev in ev_list:
        unified.append({
            "time": ev.get("created_at") or ev.get("timestamp", ""),
            "agent": (ev.get("metadata") or {}).get("agent") or ev.get("source_name", "-"),
            "action": ev.get("event_type") or ev.get("type", "event"),
            "detail": f"ENT_{str(ev.get('entity_id', ''))[:8]}" if ev.get("entity_id") else "",
            "extra": "",
            "type": "event",
        })

    mem_list = memories if isinstance(memories, list) else []
    for m in mem_list:
        if m.get("entry_type") == "task":
            continue
        unified.append({
            "time": m.get("created_at", ""),
            "agent": m.get("author") or (m.get("metadata") or {}).get("author", "-"),
            "action": m.get("entry_type", "memory"),
            "detail": (m.get("title") or m.get("slug", ""))[:50],
            "extra": "",
            "type": "memory",
        })

    task_list = tasks if isinstance(tasks, list) else []
    for t in task_list:
        meta = t.get("metadata") or {}
        status = meta.get("status", "open")
        assigned = meta.get("assigned_to", "")
        unified.append({
            "time": t.get("created_at", ""),
            "agent": t.get("author") or meta.get("author", "-"),
            "action": f"task:{status}",
            "detail": (t.get("title") or t.get("slug", ""))[:40],
            "extra": f"-> {assigned}" if assigned else "",
            "type": "task",
        })

    # Filter by agent
    agent_filter = getattr(args, "agent", None)
    if agent_filter:
        unified = [e for e in unified if e["agent"] == agent_filter or agent_filter in e["extra"]]

    # Sort by time descending
    unified.sort(key=lambda e: e.get("time", ""), reverse=True)

    if args.format == "json":
        print_json(unified[:limit])
        return

    if not unified:
        msg = "No activity found."
        if since:
            msg += f" (since {since})"
        sys.stdout.write(f"{msg}\n")
        return

    # Print header
    sys.stdout.write("\n")

    # Color codes
    TYPE_COLORS = {
        "event": "\033[36m",   # cyan
        "memory": "\033[33m",  # yellow
        "task": "\033[35m",    # magenta
    }
    RESET = "\033[0m"
    DIM = "\033[2m"

    for e in unified[:limit]:
        ts = _format_timestamp(e["time"])
        color = TYPE_COLORS.get(e["type"], "")
        agent = e["agent"][:16].ljust(16)
        action = e["action"][:16].ljust(16)
        extra = f" {DIM}{e['extra']}{RESET}" if e["extra"] else ""
        sys.stdout.write(
            f"  {DIM}{ts}{RESET}  {agent}  {color}{action}{RESET}  {e['detail']}{extra}\n"
        )

    sys.stdout.write(f"\n  {len(unified[:limit])} entries.")
    if since:
        sys.stdout.write(f" Since {since}.")
    sys.stdout.write("\n\n")


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def _detect_knowledge() -> list[dict[str, Any]]:
    """Detect local CLAUDE.md, skills, and memory files."""
    cwd = Path.cwd()
    home = Path.home()
    entries: list[dict[str, Any]] = []

    # 1. CLAUDE.md
    claude_md = cwd / "CLAUDE.md"
    if claude_md.exists():
        entries.append({
            "slug": "project/claude-md",
            "title": "CLAUDE.md - Project Instructions",
            "content": claude_md.read_text(),
            "entry_type": "knowledge",
            "source_name": "claude-md",
        })

    # 2. Skills in .claude/commands/
    skills_dir = cwd / ".claude" / "commands"
    if skills_dir.is_dir():
        for f in sorted(skills_dir.glob("*.md")):
            stem = f.stem
            entries.append({
                "slug": f"skill/{stem}",
                "title": f"Skill: {stem.replace('-', ' ').title()}",
                "content": f.read_text(),
                "entry_type": "knowledge",
                "source_name": "claude-skills",
            })

    # 3. Memory files in ~/.claude/projects/.../memory/
    memory_dir = _find_memory_dir(cwd, home)
    if memory_dir and memory_dir.is_dir():
        for f in sorted(memory_dir.glob("*.md")):
            stem = f.stem
            entries.append({
                "slug": f"memory/{stem}",
                "title": stem.replace("-", " ").title(),
                "content": f.read_text(),
                "entry_type": "knowledge",
                "source_name": "claude-memory",
            })

    return entries


def _find_memory_dir(cwd: Path, home: Path) -> Path | None:
    """Find the Claude Code memory directory for the current project."""
    projects_dir = home / ".claude" / "projects"
    if not projects_dir.exists():
        return None

    # Build expected mangled name: /home/user/my-project -> -home-user-my-project
    mangled = str(cwd).replace("/", "-")
    memory_path = projects_dir / mangled / "memory"
    if memory_path.exists():
        return memory_path

    # Fallback: scan
    try:
        for d in projects_dir.iterdir():
            if d.name == mangled:
                mp = d / "memory"
                if mp.exists():
                    return mp
    except OSError:
        pass

    return None


def _route_slug(slug: str, cwd: Path, home: Path) -> Path | None:
    """Map a slug to a local file path (with path traversal protection)."""
    if slug == "project/claude-md":
        return cwd / "CLAUDE.md"
    elif slug.startswith("skill/"):
        name = _sanitize_name(slug[len("skill/"):])
        if name:
            return cwd / ".claude" / "commands" / f"{name}.md"
    elif slug.startswith("memory/"):
        name = _sanitize_name(slug[len("memory/"):])
        memory_dir = _find_or_create_memory_dir(cwd, home)
        if name and memory_dir:
            return memory_dir / f"{name}.md"
    return None


def _find_or_create_memory_dir(cwd: Path, home: Path) -> Path | None:
    """Find or create the Claude Code memory directory."""
    projects_dir = home / ".claude" / "projects"
    mangled = str(cwd).replace("/", "-")
    memory_path = projects_dir / mangled / "memory"

    if memory_path.exists():
        return memory_path

    try:
        memory_path.mkdir(parents=True, exist_ok=True)
        return memory_path
    except OSError:
        return None


def _sanitize_name(name: str) -> str | None:
    """Strip path separators and traversal sequences."""
    clean = name.replace("..", "").replace("/", "").replace("\\", "")
    # Get just the basename
    clean = Path(clean).name
    return clean if clean else None


def _safe_get(client: Any, path: str, params: dict) -> Any:
    """GET with error swallowing (for parallel stream fetches)."""
    try:
        return client.get(path, params=params)
    except (SystemExit, Exception):
        return []


def _parse_since(val: str) -> str:
    """Parse relative time (1h, 24h, 7d) or pass through ISO timestamp."""
    import re
    m = re.match(r"^(\d+)([hmd])$", val)
    if m:
        amount = int(m.group(1))
        unit = m.group(2)
        if unit == "h":
            delta = timedelta(hours=amount)
        elif unit == "d":
            delta = timedelta(days=amount)
        else:  # m
            delta = timedelta(minutes=amount)
        return (datetime.now(timezone.utc) - delta).isoformat()
    return val


def _format_timestamp(ts: str) -> str:
    """Format ISO timestamp to compact display."""
    if not ts:
        return "              "
    try:
        # Parse and format
        return ts[5:19].replace("T", " ")
    except Exception:
        return ts[:14]


def _simple_diff(old_text: str, new_text: str) -> str:
    """Show line count change between old and new text."""
    old_lines = old_text.count("\n") + 1
    new_lines = new_text.count("\n") + 1
    diff = new_lines - old_lines
    if diff > 0:
        return f"+{diff} lines"
    if diff < 0:
        return f"{diff} lines"
    return "modified"


def _format_bytes(n: int) -> str:
    """Format byte count for display."""
    if n < 1024:
        return f"{n} bytes"
    return f"{n / 1024:.1f} KB"


def _rel_path(target: Path, cwd: Path) -> str:
    """Get relative path for display."""
    try:
        return str(target.relative_to(cwd))
    except ValueError:
        return str(target)
