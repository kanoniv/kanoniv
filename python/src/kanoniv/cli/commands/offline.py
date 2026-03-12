"""Offline commands that run via the local Rust engine (_native).

No network required. Commands: validate, compile, hash, diff, plan.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from kanoniv.cli.output import print_json, print_error, print_table


def _read_spec(path: str) -> str:
    """Read a YAML spec file and return its contents."""
    p = Path(path)
    if not p.exists():
        print_error(f"File not found: {path}")
    return p.read_text()


def _load_native():
    """Lazy-load the Rust native module."""
    try:
        from kanoniv import _native
        return _native
    except ImportError:
        print_error(
            "Native engine not available. "
            "Reinstall with: pip install kanoniv"
        )


def cmd_validate(args: Any) -> None:
    """Validate a YAML spec file."""
    native = _load_native()
    yaml_str = _read_spec(args.path)
    errors = native.validate_strict(yaml_str)

    if args.format == "json":
        print_json({"valid": len(errors) == 0, "errors": errors})
    elif errors:
        sys.stderr.write(f"Found {len(errors)} error(s):\n")
        for err in errors:
            sys.stderr.write(f"  {err}\n")
        sys.exit(1)
    else:
        sys.stdout.write("Valid.\n")


def cmd_compile(args: Any) -> None:
    """Compile a YAML spec to intermediate representation."""
    native = _load_native()
    yaml_str = _read_spec(args.path)
    ir = native.compile_ir(yaml_str)

    if args.output:
        Path(args.output).write_text(json.dumps(ir, indent=2))
        sys.stdout.write(f"Wrote IR to {args.output}\n")
    else:
        print_json(ir)


def cmd_hash(args: Any) -> None:
    """Compute SHA-256 hash of a spec."""
    native = _load_native()
    yaml_str = _read_spec(args.path)
    h = native.hash(yaml_str)

    if args.format == "json":
        print_json({"hash": h})
    else:
        sys.stdout.write(f"{h}\n")


def cmd_diff(args: Any) -> None:
    """Diff two YAML spec files."""
    native = _load_native()
    yaml_a = _read_spec(args.v1)
    yaml_b = _read_spec(args.v2)
    result = native.diff(yaml_a, yaml_b)

    if args.format == "json":
        print_json(result)
    else:
        # Table output
        added = result.get("rules_added", [])
        removed = result.get("rules_removed", [])
        modified = result.get("rules_modified", [])
        threshold_changed = result.get("thresholds_changed", False)
        threshold_changes = result.get("threshold_changes", [])

        if added:
            sys.stdout.write("Rules added:\n")
            for r in added:
                sys.stdout.write(f"  + {r}\n")
        if removed:
            sys.stdout.write("Rules removed:\n")
            for r in removed:
                sys.stdout.write(f"  - {r}\n")
        if modified:
            sys.stdout.write("Rules modified:\n")
            for r in modified:
                sys.stdout.write(f"  ~ {r}\n")
        if threshold_changed:
            sys.stdout.write("Thresholds changed:\n")
            for c in threshold_changes:
                if isinstance(c, dict):
                    path = c.get("path", "")
                    old = c.get("old_value", "")
                    new = c.get("new_value", "")
                    sys.stdout.write(f"  {path}: {old} -> {new}\n")
                else:
                    sys.stdout.write(f"  {c}\n")
        if not (added or removed or modified or threshold_changed):
            sys.stdout.write("No differences.\n")


def cmd_plan(args: Any) -> None:
    """Generate an execution plan for a spec."""
    native = _load_native()
    yaml_str = _read_spec(args.path)
    result = native.plan(yaml_str)

    if args.format == "json":
        print_json(result)
    else:
        summary = result.get("summary", "")
        stages = result.get("execution_stages", [])
        risks = result.get("risk_flags", [])

        if summary:
            sys.stdout.write(f"{summary}\n\n")

        if stages:
            sys.stdout.write("Stages:\n")
            rows = []
            for s in stages:
                rows.append([
                    str(s.get("stage", "")),
                    s.get("name", ""),
                    s.get("description", ""),
                ])
            print_table(["#", "STAGE", "DESCRIPTION"], rows)

        if risks:
            sys.stdout.write("\nRisk flags:\n")
            for r in risks:
                severity = r.get("severity", "")
                msg = r.get("message", str(r))
                sys.stdout.write(f"  [{severity}] {msg}\n")
