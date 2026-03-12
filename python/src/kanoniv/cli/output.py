"""Output formatting for the Kanoniv CLI."""
from __future__ import annotations

import json
import sys
from typing import Any, Sequence


def print_json(data: Any) -> None:
    """Pretty-print JSON to stdout."""
    json.dump(data, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")


def print_table(headers: list[str], rows: list[list[str]]) -> None:
    """Print a simple ASCII table to stdout."""
    if not rows:
        for h in headers:
            sys.stdout.write(h + "  ")
        sys.stdout.write("\n(no results)\n")
        return

    # Compute column widths
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if i < len(widths):
                widths[i] = max(widths[i], len(cell))

    # Header
    header_line = "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    sys.stdout.write(header_line + "\n")
    sys.stdout.write("  ".join("-" * w for w in widths) + "\n")

    # Rows
    for row in rows:
        line = "  ".join(
            (row[i] if i < len(row) else "").ljust(widths[i])
            for i in range(len(headers))
        )
        sys.stdout.write(line + "\n")


def print_error(msg: str) -> None:
    """Print an error message to stderr and exit."""
    sys.stderr.write(f"error: {msg}\n")
    sys.exit(1)


def print_success(msg: str) -> None:
    """Print a success message to stdout."""
    sys.stdout.write(f"{msg}\n")


def print_detail(title: str, fields: Sequence[tuple[str, str]]) -> None:
    """Print a titled detail view with labeled fields."""
    sys.stdout.write(f"{title}\n\n")
    if not fields:
        return
    max_label = max(len(f[0]) for f in fields)
    for label, value in fields:
        sys.stdout.write(f"  {label + ':':<{max_label + 2}} {value}\n")


def print_kv(title: str, data: dict[str, Any]) -> None:
    """Print a section of key-value pairs."""
    if title:
        sys.stdout.write(f"\n  {title}\n")
    if not data:
        sys.stdout.write("    (none)\n")
        return
    max_key = max(len(str(k)) for k in data)
    for k, v in data.items():
        display = str(v) if not isinstance(v, (dict, list)) else json.dumps(v)
        sys.stdout.write(f"    {str(k):<{max_key + 2}} {display}\n")
