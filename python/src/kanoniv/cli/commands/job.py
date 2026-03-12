"""Job commands: list, show, cancel, run.

Jobs are background tasks - reconciliation, autotune, exports, and recomputes.
"""
from __future__ import annotations

import json
import sys
import time
from typing import Any

from kanoniv.cli.output import print_json, print_table, print_detail, print_error


def cmd_job(args: Any, client: Any) -> None:
    """Route to the appropriate job sub-command."""
    action = args.action
    dispatch = {
        "list": _list,
        "show": _show,
        "cancel": _cancel,
        "run": _run,
    }
    fn = dispatch.get(action)
    if fn:
        fn(args, client)
    else:
        sys.stderr.write("usage: kanoniv job {list,show,cancel,run}\n")
        sys.exit(1)


def _list(args: Any, client: Any) -> None:
    """List recent jobs."""
    params: dict[str, Any] = {"limit": str(getattr(args, "limit", 20))}
    job_type = getattr(args, "job_type", None)
    if job_type:
        params["job_type"] = job_type

    resp = client.get("/v1/jobs", params=params)

    if args.format == "json":
        print_json(resp)
        return

    items = resp if isinstance(resp, list) else []
    rows = []
    for j in items:
        jid = j.get("id", "-")
        if len(jid) > 12:
            jid = jid[:8] + "..."
        rows.append([
            jid,
            j.get("job_type", "-"),
            j.get("status", "-"),
            j.get("created_at", "-")[:19] if j.get("created_at") else "-",
        ])
    print_table(["ID", "TYPE", "STATUS", "CREATED"], rows)


def _show(args: Any, client: Any) -> None:
    """Show job details and results."""
    resp = client.get(f"/v1/jobs/{args.job_id}")

    if args.format == "json":
        print_json(resp)
        return

    fields = [
        ("Job", resp.get("id", args.job_id)),
        ("Type", resp.get("job_type", "-")),
        ("Status", resp.get("status", "-")),
        ("Created", resp.get("created_at", "-")),
        ("Completed", resp.get("completed_at", "-")),
    ]

    # Duration
    started = resp.get("started_at")
    completed = resp.get("completed_at")
    duration = resp.get("duration_ms")
    if duration:
        fields.append(("Duration", f"{duration}ms"))

    print_detail("Job", fields)

    # Result summary
    result = resp.get("result")
    if isinstance(result, dict) and result:
        sys.stdout.write("\n  Result\n")
        for k, v in result.items():
            sys.stdout.write(f"    {k}: {v}\n")

    # Error
    error = resp.get("error")
    if error:
        sys.stdout.write(f"\n  Error: {error}\n")


def _cancel(args: Any, client: Any) -> None:
    """Cancel a running job."""
    resp = client.post(f"/v1/jobs/{args.job_id}/cancel")

    if args.format == "json":
        print_json(resp)
    else:
        sys.stdout.write(f"Cancelled job {args.job_id}\n")


def _run(args: Any, client: Any) -> None:
    """Run a new job."""
    body: dict[str, Any] = {"job_type": args.job_type}

    payload_str = getattr(args, "payload", None)
    if payload_str:
        try:
            body["payload"] = json.loads(payload_str)
        except json.JSONDecodeError as e:
            print_error(f"Invalid JSON payload: {e}")

    resp = client.post("/v1/jobs/run", body)
    job_id = resp.get("job_id") or resp.get("id", "") if resp else ""

    wait = getattr(args, "wait", False)
    if not wait:
        if args.format == "json":
            print_json(resp)
        else:
            sys.stdout.write(f"Job submitted: {job_id}\n")
            sys.stdout.write(f"  Run 'kanoniv job show {job_id}' to check status\n")
        return

    # Poll until complete
    sys.stderr.write(f"  Running {args.job_type}...")
    sys.stderr.flush()
    start = time.monotonic()
    max_wait = 30 * 60

    while True:
        time.sleep(2)
        elapsed = time.monotonic() - start

        if elapsed > max_wait:
            sys.stderr.write("\n")
            sys.stderr.write(
                f"Timed out after 30 minutes. Job {job_id} may still be running.\n"
            )
            sys.exit(1)

        status_resp = client.get(f"/v1/jobs/{job_id}")
        status = status_resp.get("status", "unknown")
        secs = int(elapsed)

        sys.stderr.write(f"\r  Running {args.job_type}... {status} ({secs}s)")
        sys.stderr.flush()

        if status in ("completed", "succeeded"):
            sys.stderr.write("\n")
            if args.format == "json":
                print_json(status_resp)
            else:
                sys.stdout.write(f"Job complete ({secs}s)\n")
                result = status_resp.get("result", {})
                if isinstance(result, dict):
                    for k, v in result.items():
                        sys.stdout.write(f"  {k}: {v}\n")
            return

        if status in ("failed", "error"):
            sys.stderr.write("\n")
            msg = status_resp.get("error", "Unknown error")
            sys.stderr.write(f"Job failed: {msg}\n")
            sys.exit(1)
