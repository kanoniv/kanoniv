"""Cloud engine commands: autodetect, reconcile, stats.

These call the Kanoniv API and mirror the Rust CLI output format.
"""
from __future__ import annotations

import sys
import time
from argparse import Namespace

from kanoniv.cli.output import print_json, print_table


def cmd_autodetect(args: Namespace, client) -> None:
    """Profile ingested data, detect entity types and identity signals.

    When multiple entity types are given, runs autodetect for each type
    sequentially and prints results for each.
    """
    entity_types = getattr(args, "entity_type", None) or [None]
    # Normalize: single None means run once without entity_type filter
    if isinstance(entity_types, str):
        entity_types = [entity_types]

    for et in entity_types:
        _autodetect_one(args, client, et)
        if len(entity_types) > 1 and et != entity_types[-1]:
            print()
            print("-" * 50)
            print()


def _autodetect_one(args: Namespace, client, entity_type: str | None) -> None:
    """Run autodetect for a single entity type."""
    body: dict = {"bootstrap": args.bootstrap}
    if entity_type:
        body["entity_type"] = entity_type
    if args.version:
        body["identity_version"] = args.version
    if args.sample_size:
        body["sample_size"] = args.sample_size

    # Parse --map flags: "canonical=col1,col2" -> {"canonical": ["col1", "col2"]}
    raw_mappings = getattr(args, "field_mappings", None)
    if raw_mappings:
        field_mappings: dict[str, list[str]] = {}
        for mapping in raw_mappings:
            if "=" not in mapping:
                sys.stderr.write(
                    f"  warn: ignoring invalid --map '{mapping}' (expected CANONICAL=col1,col2)\n"
                )
                continue
            canonical, cols_str = mapping.split("=", 1)
            canonical = canonical.strip()
            cols = [c.strip() for c in cols_str.split(",") if c.strip()]
            if canonical and cols:
                field_mappings[canonical] = cols
        if field_mappings:
            body["field_mappings"] = field_mappings

    resp = client.post("/v1/autodetect", body)

    if args.format == "json":
        print_json(resp)
        return

    total = resp.get("total_records", 0)
    sampled = resp.get("rows_sampled", 0)

    if sampled == 0:
        if entity_type:
            sys.stderr.write(f"warn: No data found for entity type '{entity_type}'.\n")
        else:
            sys.stderr.write("warn: No data found. Ingest records first.\n")
        return

    header = "Autodetect Results"
    if entity_type:
        header += f" ({entity_type})"
    print(header)
    print()

    inferred = resp.get("inferred_entity_type")
    if inferred:
        print(f"  Entity type:    {inferred}")
    print(f"  Total records:  {total}")
    print(f"  Rows sampled:   {sampled}")

    sources = resp.get("sources", [])
    if sources:
        print(f"  Sources:        {', '.join(sources)}")

    signals = resp.get("identity_signals", [])
    print(f"  Signals:        {', '.join(signals) if signals else '(none)'}")

    # Blocking keys
    blocking_keys = resp.get("blocking_keys", [])
    if blocking_keys:
        print()
        print("  Blocking Keys")
        for key in blocking_keys:
            if isinstance(key, list):
                print(f"    [{', '.join(key)}]")

    # Columns with detected signals
    columns = resp.get("columns", [])
    signal_cols = [c for c in columns if c.get("signal")]
    if signal_cols:
        print()
        rows = []
        for c in signal_cols:
            uniqueness = c.get("uniqueness")
            null_rate = c.get("null_rate")
            cardinality = c.get("cardinality")
            rows.append([
                c.get("name", "-"),
                c.get("signal", "-"),
                f"{uniqueness:.2f}" if uniqueness is not None else "-",
                f"{null_rate:.2f}" if null_rate is not None else "-",
                str(cardinality) if cardinality is not None else "-",
            ])
        print_table(
            ["COLUMN", "SIGNAL", "UNIQUENESS", "NULL_RATE", "CARDINALITY"],
            rows,
        )

    bootstrapped = resp.get("bootstrapped_version")
    if bootstrapped:
        print()
        print(f"  ok: Bootstrapped identity plan: {bootstrapped}")


def cmd_reconcile(args: Namespace, client) -> None:
    """Trigger reconciliation job(s), optionally wait for completion.

    When multiple entity types are given, submits a job for each type
    and waits for all to complete (if --wait).
    """
    if args.dry_run:
        resp = client.post("/v1/reconcile/dry-run", {})
        if args.format == "json":
            print_json(resp)
        else:
            print("Dry Run Results")
            print()
            clusters = resp.get("cluster_count")
            merges = resp.get("merge_count")
            rate = resp.get("merge_rate")
            if clusters is not None:
                print(f"  Canonical entities: {clusters}")
            if merges is not None:
                print(f"  Merges:             {merges}")
            if rate is not None:
                print(f"  Merge rate:         {rate * 100.0:.1f}%")
        return

    entity_types = getattr(args, "entity_type", None) or [None]
    if isinstance(entity_types, str):
        entity_types = [entity_types]

    # Submit all jobs first
    jobs: list[tuple[str | None, str]] = []  # (entity_type, job_id)
    for et in entity_types:
        body: dict = {"job_type": "reconciliation"}
        if et:
            body["payload"] = {"identity_version": f"{et}_v1"}
        resp = client.post("/v1/jobs/run", body)
        job_id = resp.get("job_id") or resp.get("id", "")
        jobs.append((et, job_id))
        label = f" ({et})" if et else ""
        print(f"  Submitted reconciliation{label}: {job_id}")

    if not args.wait:
        if not entity_types or entity_types == [None]:
            print("  Run 'kanoniv reconcile --wait' to follow progress")
        return

    # Wait for each job sequentially
    print()
    for et, job_id in jobs:
        label = f" ({et})" if et else ""
        _wait_for_job(args, client, job_id, label)
        print()


def _wait_for_job(args: Namespace, client, job_id: str, label: str) -> None:
    """Poll a single reconciliation job until complete."""
    sys.stderr.write(f"  Reconciling{label}...")
    sys.stderr.flush()
    start = time.monotonic()
    max_wait = 30 * 60

    while True:
        time.sleep(2)
        elapsed = time.monotonic() - start

        if elapsed > max_wait:
            sys.stderr.write("\n")
            sys.stderr.write(
                f"error: Timed out after 30 minutes. Job {job_id} may still be running"
                f" - check with: kanoniv jobs\n"
            )
            sys.exit(1)

        status_resp = client.get(f"/v1/jobs/{job_id}")
        status = status_resp.get("status", "unknown")
        secs = int(elapsed)

        sys.stderr.write(f"\r  Reconciling{label}... {status} ({secs}s)")
        sys.stderr.flush()

        if status in ("completed", "succeeded"):
            sys.stderr.write("\n")

            if args.format == "json":
                print_json(status_resp)
            else:
                print(f"  ok: Reconciliation{label} complete ({secs}s)")
                result = status_resp.get("result", {})
                if isinstance(result, dict):
                    merges = result.get("merge_count")
                    entities = result.get("entity_count")
                    if merges is not None:
                        print(f"     Merges: {merges}")
                    if entities is not None:
                        print(f"     Entities: {entities}")
            return

        if status in ("failed", "error"):
            sys.stderr.write("\n")
            msg = status_resp.get("error", "Unknown error")
            sys.stderr.write(f"error: Reconciliation{label} failed: {msg}\n")
            sys.exit(1)


def cmd_stats(args: Namespace, client) -> None:
    """Show dashboard statistics."""
    params: dict = {}
    entity_type = getattr(args, "entity_type", None)
    if entity_type:
        params["entity_type"] = entity_type

    resp = client.get("/v1/stats", params=params if params else None)

    if args.format == "json":
        print_json(resp)
        return

    header = "Dashboard"
    if entity_type:
        header += f" ({entity_type})"
    print(header)
    print()

    fields = [
        ("total_canonical_entities", "Entities"),
        ("total_external_entities", "Records"),
        ("pending_reviews", "Pending reviews"),
        ("merge_rate", "Merge rate"),
    ]

    for key, label in fields:
        val = resp.get(key)
        if val is not None:
            if key == "merge_rate":
                try:
                    display = f"{float(val) * 100.0:.1f}%"
                except (TypeError, ValueError):
                    display = "-"
            else:
                display = str(val)
            print(f"  {label + ':':<16} {display}")

    # Show per-entity-type breakdown if available (aggregate mode only)
    by_type = resp.get("by_entity_type")
    if by_type and isinstance(by_type, dict):
        print()
        print("  By Entity Type")
        rows = []
        for et, stats in sorted(by_type.items()):
            records = stats.get("records", 0)
            entities = stats.get("entities", 0)
            mr = stats.get("merge_rate", 0.0)
            rows.append([et, str(records), str(entities), f"{mr * 100.0:.1f}%"])
        print_table(["TYPE", "RECORDS", "ENTITIES", "MERGE RATE"], rows)


def cmd_autotune(args: Namespace, client) -> None:
    """Optimize matching thresholds via autotune."""
    body: dict = {
        "max_iterations": getattr(args, "max_iterations", 50),
    }
    if args.version:
        body["identity_version"] = args.version

    sys.stderr.write("  Optimizing...")
    sys.stderr.flush()

    resp = client.post("/v1/autotune", body, timeout=300)

    sys.stderr.write("\r")
    sys.stderr.flush()

    if args.format == "json":
        print_json(resp)
        return

    status = resp.get("status", "unknown")
    mutations = resp.get("mutations_accepted", 0)
    iterations = resp.get("iterations_used", 0)
    total_entities = resp.get("total_entities", 0)
    sampled = resp.get("sampled_entities", 0)

    if status == "no_data":
        print("  No data found. Ingest and reconcile first.")
        return

    if status == "no_improvement":
        print(f"  Already well-calibrated ({iterations} iterations, {sampled}/{total_entities} records sampled)")
        return

    print(f"  {mutations} improvements applied ({iterations} iterations, {sampled}/{total_entities} records sampled)")

    before = resp.get("metrics_before", {})
    after = resp.get("metrics_after", {})
    if before and after:
        print()
        for key in ["merge_rate", "conflict_rate", "entropy"]:
            b = before.get(key)
            a = after.get(key)
            if b is not None and a is not None:
                arrow = ">" if a > b else "<" if a < b else "="
                print(f"    {key:<16} {b:.3f} {arrow} {a:.3f}")

    explanation = resp.get("explanation", "")
    if explanation:
        print()
        print(f"  {explanation}")


def cmd_jobs(args: Namespace, client) -> None:
    """List recent jobs."""
    resp = client.get("/v1/jobs", params={"limit": str(args.limit)})

    if args.format == "json":
        print_json(resp)
        return

    items = resp if isinstance(resp, list) else []
    rows = []
    for j in items:
        rows.append([
            j.get("id", "-"),
            j.get("job_type", "-"),
            j.get("status", "-"),
            j.get("created_at", "-"),
        ])
    print_table(["ID", "TYPE", "STATUS", "CREATED"], rows)
