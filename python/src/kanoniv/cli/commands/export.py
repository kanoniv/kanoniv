"""Export commands: entities, memberships, graph, matches to CSV/JSON."""
from __future__ import annotations

import csv
import json
import sys
from argparse import Namespace


def cmd_export_entities(args: Namespace, client) -> None:
    """Export resolved canonical entities to a file."""
    output_path = args.output
    fmt = getattr(args, "export_format", "csv")
    entity_type = getattr(args, "entity_type", None)
    reveal_pii = getattr(args, "reveal_pii", False)

    all_entities: list[dict] = []
    offset = 0
    limit = 1000

    while True:
        params: dict = {"limit": str(limit), "offset": str(offset)}
        if entity_type:
            params["entity_type"] = entity_type
        if reveal_pii:
            params["reveal_pii"] = "true"

        resp = client.get("/v1/export/entities", params=params)
        entities = resp.get("entities", [])
        all_entities.extend(entities)

        count = len(all_entities)
        sys.stderr.write(f"\r  Fetching... {count:,} entities")
        sys.stderr.flush()

        if not resp.get("has_more", False):
            break
        offset += limit

    sys.stderr.write("\n")

    if not all_entities:
        sys.stderr.write("  No entities found.\n")
        return

    if fmt == "json":
        with open(output_path, "w") as f:
            json.dump(all_entities, f, indent=2, default=str)
    else:
        _write_entities_csv(all_entities, output_path)

    sys.stderr.write(f"  ok: Wrote {len(all_entities):,} entities to {output_path}\n")


def _write_entities_csv(entities: list[dict], path: str) -> None:
    """Flatten entities with canonical_data into a CSV file."""
    # Collect all canonical_data keys across all entities
    all_keys: set[str] = set()
    for e in entities:
        cd = e.get("canonical_data")
        if isinstance(cd, dict):
            all_keys.update(cd.keys())

    base_cols = [
        "entity_id", "entity_type", "confidence_score",
        "member_count", "updated_at",
    ]
    extra_cols = sorted(all_keys)
    fieldnames = base_cols + extra_cols

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for e in entities:
            row: dict = {
                "entity_id": e.get("entity_id", ""),
                "entity_type": e.get("entity_type", ""),
                "confidence_score": e.get("confidence_score", ""),
                "member_count": e.get("member_count", ""),
                "updated_at": e.get("updated_at", ""),
            }
            cd = e.get("canonical_data")
            if isinstance(cd, dict):
                for k, v in cd.items():
                    row[k] = v if not isinstance(v, (dict, list)) else json.dumps(v)
            writer.writerow(row)


def cmd_export_memberships(args: Namespace, client) -> None:
    """Export entity memberships to a file."""
    output_path = args.output
    fmt = getattr(args, "export_format", "csv")

    all_memberships: list[dict] = []
    offset = 0
    limit = 5000

    while True:
        params: dict = {"limit": str(limit), "offset": str(offset)}
        resp = client.get("/v1/export/memberships", params=params)
        memberships = resp.get("memberships", [])
        all_memberships.extend(memberships)

        count = len(all_memberships)
        sys.stderr.write(f"\r  Fetching... {count:,} memberships")
        sys.stderr.flush()

        if len(memberships) < limit:
            break
        offset += limit

    sys.stderr.write("\n")

    if not all_memberships:
        sys.stderr.write("  No memberships found.\n")
        return

    if fmt == "json":
        with open(output_path, "w") as f:
            json.dump(all_memberships, f, indent=2, default=str)
    else:
        fieldnames = [
            "source_system", "source_id", "entity_id",
            "confidence", "link_type", "created_at",
        ]
        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=fieldnames, extrasaction="ignore",
            )
            writer.writeheader()
            for m in all_memberships:
                writer.writerow({k: m.get(k, "") for k in fieldnames})

    sys.stderr.write(
        f"  ok: Wrote {len(all_memberships):,} memberships to {output_path}\n"
    )


def cmd_export_graph(args: Namespace, client) -> None:
    """Export the identity graph (nodes and edges) to a file."""
    output_path = args.output
    fmt = getattr(args, "export_format", "json")
    entity_type = getattr(args, "entity_type", None)

    params: dict = {}
    if entity_type:
        params["entity_type"] = entity_type

    resp = client.get("/v1/graph/export", params=params if params else None)

    nodes = resp.get("nodes", []) if isinstance(resp, dict) else []
    edges = resp.get("edges", []) if isinstance(resp, dict) else []

    if not nodes and not edges:
        sys.stderr.write("  No graph data to export.\n")
        return

    if fmt == "json":
        with open(output_path, "w") as f:
            json.dump(resp, f, indent=2, default=str)
    else:
        # CSV: export nodes and edges as separate sections
        with open(output_path, "w", newline="") as f:
            writer = csv.writer(f)
            # Nodes
            writer.writerow(["# NODES"])
            node_fields = ["entity_id", "entity_type", "label", "member_count"]
            writer.writerow(node_fields)
            for n in nodes:
                writer.writerow([n.get(k, "") for k in node_fields])
            # Edges
            writer.writerow([])
            writer.writerow(["# EDGES"])
            edge_fields = ["source_id", "target_id", "edge_type", "weight"]
            writer.writerow(edge_fields)
            for e in edges:
                writer.writerow([e.get(k, "") for k in edge_fields])

    sys.stderr.write(
        f"  ok: Wrote {len(nodes):,} nodes + {len(edges):,} edges to {output_path}\n"
    )


def cmd_export_matches(args: Namespace, client) -> None:
    """Export match audit results to a file."""
    output_path = args.output
    fmt = getattr(args, "export_format", "csv")
    entity_type = getattr(args, "entity_type", None)

    all_matches: list[dict] = []
    offset = 0
    limit = 1000

    while True:
        params: dict = {"limit": str(limit), "offset": str(offset)}
        if entity_type:
            params["entity_type"] = entity_type

        resp = client.get("/v1/match/audit", params=params)
        matches = resp.get("matches", resp.get("audit", []))
        all_matches.extend(matches)

        count = len(all_matches)
        sys.stderr.write(f"\r  Fetching... {count:,} match records")
        sys.stderr.flush()

        if len(matches) < limit:
            break
        offset += limit

    sys.stderr.write("\n")

    if not all_matches:
        sys.stderr.write("  No match audit records found.\n")
        return

    if fmt == "json":
        with open(output_path, "w") as f:
            json.dump(all_matches, f, indent=2, default=str)
    else:
        fieldnames = [
            "entity_a_id", "entity_b_id", "score", "decision",
            "match_type", "created_at",
        ]
        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=fieldnames, extrasaction="ignore",
            )
            writer.writeheader()
            for m in all_matches:
                writer.writerow({k: m.get(k, "") for k in fieldnames})

    sys.stderr.write(
        f"  ok: Wrote {len(all_matches):,} match records to {output_path}\n"
    )
