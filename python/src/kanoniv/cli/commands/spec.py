"""Spec commands: validate, compile, hash, diff, plan, list, show, upload, delete.

Specs define the identity model - entity types, matching rules, blocking keys,
thresholds, and survivorship policies. Offline commands (validate, compile,
hash, diff, plan) use the local Rust engine. Cloud commands (list, show,
upload, delete) use the API.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from kanoniv.cli.output import print_json, print_table, print_detail, print_error


def cmd_spec(args: Any, client: Any | None) -> None:
    """Route to the appropriate spec sub-command."""
    action = args.action

    # Offline commands (no client needed)
    offline_dispatch = {
        "validate": _validate,
        "compile": _compile,
        "hash": _hash,
        "diff": _diff,
        "plan": _plan,
    }
    if action in offline_dispatch:
        offline_dispatch[action](args)
        return

    # Cloud commands (client required)
    cloud_dispatch = {
        "list": _list,
        "show": _show,
        "upload": _upload,
        "delete": _delete,
    }
    fn = cloud_dispatch.get(action)
    if fn:
        if client is None:
            print_error("This command requires an API key")
        fn(args, client)
    else:
        sys.stderr.write(
            "usage: kanoniv spec "
            "{validate,compile,hash,diff,plan,list,show,upload,delete}\n"
        )
        sys.exit(1)


# -- Offline commands (reuse logic from offline.py) -------------------------

def _read_spec(path: str) -> str:
    p = Path(path)
    if not p.exists():
        print_error(f"File not found: {path}")
    return p.read_text()


def _load_native():
    try:
        from kanoniv import _native
        return _native
    except ImportError:
        print_error(
            "Native engine not available. "
            "Reinstall with: pip install kanoniv"
        )


def _validate(args: Any) -> None:
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


def _compile(args: Any) -> None:
    """Compile a YAML spec to intermediate representation."""
    import json as json_mod

    native = _load_native()
    yaml_str = _read_spec(args.path)
    ir = native.compile_ir(yaml_str)

    output = getattr(args, "output", None)
    if output:
        Path(output).write_text(json_mod.dumps(ir, indent=2))
        sys.stdout.write(f"Wrote IR to {output}\n")
    else:
        print_json(ir)


def _hash(args: Any) -> None:
    """Compute SHA-256 hash of a spec."""
    native = _load_native()
    yaml_str = _read_spec(args.path)
    h = native.hash(yaml_str)

    if args.format == "json":
        print_json({"hash": h})
    else:
        sys.stdout.write(f"{h}\n")


def _diff(args: Any) -> None:
    """Diff two YAML spec files."""
    native = _load_native()
    yaml_a = _read_spec(args.v1)
    yaml_b = _read_spec(args.v2)
    result = native.diff(yaml_a, yaml_b)

    if args.format == "json":
        print_json(result)
        return

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


def _plan(args: Any) -> None:
    """Generate an execution plan for a spec."""
    native = _load_native()
    yaml_str = _read_spec(args.path)
    result = native.plan(yaml_str)

    if args.format == "json":
        print_json(result)
        return

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


# -- Cloud commands --------------------------------------------------------

def _list(args: Any, client: Any) -> None:
    """List identity specs in the Cloud."""
    resp = client.get("/v1/identity/specs")

    if args.format == "json":
        print_json(resp)
        return

    specs = resp if isinstance(resp, list) else []
    if not specs:
        sys.stdout.write("No specs.\n")
        return

    rows = []
    for s in specs:
        rows.append([
            s.get("identity_version", s.get("version", "-")),
            s.get("entity_type", "-"),
            str(s.get("rule_count", "-")),
            s.get("created_at", "-")[:19] if s.get("created_at") else "-",
        ])
    print_table(["VERSION", "ENTITY_TYPE", "RULES", "CREATED"], rows)


def _show(args: Any, client: Any) -> None:
    """Show spec details."""
    resp = client.get(f"/v1/identity/specs/{args.version}")

    if args.format == "json":
        print_json(resp)
        return

    fields = [
        ("Version", resp.get("identity_version", resp.get("version", "-"))),
        ("Entity Type", resp.get("entity_type", "-")),
        ("Status", resp.get("status", "-")),
        ("Created", resp.get("created_at", "-")),
    ]
    print_detail("Spec", fields)

    # Print YAML content if available
    yaml_content = resp.get("yaml_content", resp.get("spec_yaml"))
    if yaml_content:
        sys.stdout.write("\n  --- YAML ---\n")
        for line in yaml_content.split("\n"):
            sys.stdout.write(f"  {line}\n")


def _upload(args: Any, client: Any) -> None:
    """Upload a YAML spec to the Cloud."""
    yaml_str = _read_spec(args.path)
    compile_spec = getattr(args, "compile_spec", False)

    body: dict[str, Any] = {
        "yaml_content": yaml_str,
        "compile": compile_spec,
    }
    resp = client.post("/v1/identity/specs", body)

    if args.format == "json":
        print_json(resp)
    else:
        version = "-"
        if resp:
            version = resp.get("identity_version", resp.get("version", "-"))
        sys.stdout.write(f"Uploaded spec: {version}\n")


def _delete(args: Any, client: Any) -> None:
    """Delete a spec from the Cloud."""
    resp = client.delete(f"/v1/identity/specs/{args.version}")

    if args.format == "json":
        print_json(resp)
    else:
        sys.stdout.write(f"Deleted spec: {args.version}\n")
