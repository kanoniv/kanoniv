"""Graph commands: stats, cluster, clusters, bridges, refresh, influence, risk, orphans, conflicts, density.

The persistent identity graph holds entities, relationships, and merge edges.
Graph analytics reveal influence, risk, and structural patterns.
"""
from __future__ import annotations

import sys
from typing import Any

from kanoniv.cli.output import print_json, print_table, print_detail, print_success


def cmd_graph(args: Any, client: Any) -> None:
    """Route to the appropriate graph sub-command."""
    action = args.action
    dispatch = {
        "stats": _stats,
        "cluster": _cluster,
        "clusters": _clusters,
        "bridges": _bridges,
        "refresh": _refresh,
        "influence": _influence,
        "risk": _risk,
        "orphans": _orphans,
        "conflicts": _conflicts,
        "density": _density,
    }
    fn = dispatch.get(action)
    if fn:
        fn(args, client)
    else:
        sys.stderr.write(
            "usage: kanoniv graph "
            "{stats,cluster,clusters,bridges,refresh,influence,risk,"
            "orphans,conflicts,density}\n"
        )
        sys.exit(1)


def _stats(args: Any, client: Any) -> None:
    """Show graph statistics: entities, edges, merges, density."""
    resp = client.get("/v1/graph/stats")

    if args.format == "json":
        print_json(resp)
        return

    # GraphStats: { nodes, edges, components, density, avg_degree }
    fields = [
        ("Nodes", str(resp.get("nodes", "-"))),
        ("Edges", str(resp.get("edges", "-"))),
        ("Components", str(resp.get("components", "-"))),
        ("Density", str(resp.get("density", "-"))),
        ("Avg Degree", str(resp.get("avg_degree", "-"))),
    ]
    fields = [(k, v) for k, v in fields if v != "-"]
    print_detail("Graph", fields)


def _cluster(args: Any, client: Any) -> None:
    """Show all members of an entity's cluster."""
    resp = client.get(f"/v1/graph/clusters/{args.entity_id}")

    if args.format == "json":
        print_json(resp)
        return

    # ClusterDetail: { component_index, members: Vec<Uuid>, internal_edges, density }
    members = resp.get("members", []) if isinstance(resp, dict) else resp
    if not members:
        sys.stdout.write("No cluster members.\n")
        return

    comp_idx = resp.get("component_index", "-") if isinstance(resp, dict) else "-"
    internal = resp.get("internal_edges", "-") if isinstance(resp, dict) else "-"
    density = resp.get("density", "-") if isinstance(resp, dict) else "-"

    sys.stdout.write(f"  Component: {comp_idx}  Edges: {internal}  Density: {density}\n\n")

    rows = []
    for m in members:
        mid = str(m) if not isinstance(m, dict) else str(m.get("entity_id", m))
        if len(mid) > 12:
            mid = mid[:8] + "..."
        rows.append([mid])
    print_table(["MEMBER_ID"], rows)


def _clusters(args: Any, client: Any) -> None:
    """List all clusters in the identity graph."""
    params = {"limit": str(getattr(args, "limit", 20))}
    resp = client.get("/v1/graph/clusters", params=params)

    if args.format == "json":
        print_json(resp)
        return

    clusters = resp if isinstance(resp, list) else resp.get("clusters", [])

    min_size = getattr(args, "min_size", None)
    if min_size is not None:
        clusters = [c for c in clusters if c.get("size", c.get("member_count", 0)) >= min_size]

    if not clusters:
        sys.stdout.write("No clusters.\n")
        return

    rows = []
    for c in clusters:
        cid = c.get("cluster_id", c.get("entity_id", "-"))
        if len(cid) > 12:
            cid = cid[:8] + "..."
        rows.append([
            cid,
            str(c.get("size", c.get("member_count", "-"))),
            c.get("entity_type", "-"),
            str(c.get("avg_confidence", "-")),
        ])
    print_table(["CLUSTER_ID", "SIZE", "TYPE", "AVG_CONFIDENCE"], rows)


def _bridges(args: Any, client: Any) -> None:
    """Show bridge entities that connect different graph components."""
    resp = client.get("/v1/graph/signals/bridges")

    if args.format == "json":
        print_json(resp)
        return

    # Vec<BridgeEntity>: { entity_id, bridge_score, degree, components_connected }
    bridges = resp if isinstance(resp, list) else resp.get("bridges", [])
    if not bridges:
        sys.stdout.write("No bridge entities.\n")
        return

    rows = []
    for b in bridges:
        eid = str(b.get("entity_id", "-"))
        if len(eid) > 12:
            eid = eid[:8] + "..."
        rows.append([
            eid,
            str(b.get("bridge_score", "-")),
            str(b.get("degree", "-")),
            str(b.get("components_connected", "-")),
        ])
    print_table(["ENTITY", "BRIDGE_SCORE", "DEGREE", "COMPONENTS"], rows)


def _refresh(args: Any, client: Any) -> None:
    """Refresh graph analytics (recompute influence, risk, clusters)."""
    resp = client.post("/v1/graph/refresh")

    if args.format == "json":
        print_json(resp)
    else:
        status = resp.get("status", "started") if resp else "started"
        print_success(f"Graph refresh {status}")


def _influence(args: Any, client: Any) -> None:
    """Show influence score for an entity."""
    resp = client.get(f"/v1/graph/influence/{args.entity_id}")

    if args.format == "json":
        print_json(resp)
        return

    # InfluenceScore: { entity_id, pagerank, betweenness, degree, reach_2hop }
    fields = [
        ("Entity", args.entity_id),
        ("PageRank", str(resp.get("pagerank", "-"))),
        ("Betweenness", str(resp.get("betweenness", "-"))),
        ("Degree", str(resp.get("degree", "-"))),
        ("2-Hop Reach", str(resp.get("reach_2hop", "-"))),
    ]
    fields = [(k, v) for k, v in fields if v != "-"]
    print_detail("Influence", fields)


def _risk(args: Any, client: Any) -> None:
    """Show risk score for an entity."""
    resp = client.get(f"/v1/graph/risk/{args.entity_id}")

    if args.format == "json":
        print_json(resp)
        return

    # RiskResponse: { entity_id, churn_risk, contagion_risk, merge_risk, composite_risk }
    fields = [
        ("Entity", args.entity_id),
        ("Composite Risk", str(resp.get("composite_risk", "-"))),
        ("Churn Risk", str(resp.get("churn_risk", "-"))),
        ("Contagion Risk", str(resp.get("contagion_risk", "-"))),
        ("Merge Risk", str(resp.get("merge_risk", "-"))),
    ]
    fields = [(k, v) for k, v in fields if v != "-"]
    print_detail("Risk", fields)


def _orphans(args: Any, client: Any) -> None:
    """List orphan entities - entities with no connections to other entities."""
    params: dict[str, str] = {"limit": str(getattr(args, "limit", 20))}
    entity_type = getattr(args, "entity_type", None)
    if entity_type:
        params["entity_type"] = entity_type

    resp = client.get("/v1/graph/orphans", params=params)

    if args.format == "json":
        print_json(resp)
        return

    # OrphansResponse: { orphans: Vec<OrphanEntity>, total }
    # OrphanEntity: { entity_id, entity_type, member_count }
    orphans = resp if isinstance(resp, list) else resp.get("orphans", [])
    if not orphans:
        sys.stdout.write("No orphan entities.\n")
        return

    rows = []
    for o in orphans:
        oid = str(o.get("entity_id", "-"))
        if len(oid) > 12:
            oid = oid[:8] + "..."
        rows.append([
            oid,
            o.get("entity_type", "-") or "-",
            str(o.get("member_count", "-")),
        ])
    print_table(["ENTITY_ID", "TYPE", "MEMBERS"], rows)

    total = None
    if isinstance(resp, dict):
        total = resp.get("total")
    if total is not None:
        sys.stdout.write(f"\n  {total} orphan entities\n")


def _conflicts(args: Any, client: Any) -> None:
    """List conflicting or incoherent clusters in the graph."""
    params: dict[str, str] = {"limit": str(getattr(args, "limit", 20))}

    resp = client.get("/v1/graph/conflicts", params=params)

    if args.format == "json":
        print_json(resp)
        return

    conflicts = resp if isinstance(resp, list) else resp.get("conflicts", [])
    if not conflicts:
        sys.stdout.write("No conflicts detected.\n")
        return

    # ClusterConflict: { cluster_id, conflict_type, severity, entities_involved: Vec<Uuid>, description }
    rows = []
    for c in conflicts:
        cid = str(c.get("cluster_id", "-"))
        if len(cid) > 12:
            cid = cid[:8] + "..."
        involved = c.get("entities_involved", [])
        entity_count = len(involved) if isinstance(involved, list) else str(involved)
        rows.append([
            cid,
            c.get("conflict_type", "-"),
            c.get("severity", "-"),
            str(entity_count),
            c.get("description", "-"),
        ])
    print_table(["CLUSTER", "TYPE", "SEVERITY", "ENTITIES", "DESCRIPTION"], rows)


def _density(args: Any, client: Any) -> None:
    """Show graph density metrics - connectivity and clustering statistics."""
    resp = client.get("/v1/graph/density")

    if args.format == "json":
        print_json(resp)
        return

    # DensityResponse: { edge_density, avg_degree, max_degree, median_degree,
    #                     connected_components, largest_component_size }
    fields = [
        ("Edge Density", str(resp.get("edge_density", "-"))),
        ("Avg Degree", str(resp.get("avg_degree", "-"))),
        ("Max Degree", str(resp.get("max_degree", "-"))),
        ("Median Degree", str(resp.get("median_degree", "-"))),
        ("Connected Components", str(resp.get("connected_components", "-"))),
        ("Largest Component", str(resp.get("largest_component_size", "-"))),
    ]
    fields = [(k, v) for k, v in fields if v != "-"]
    print_detail("Graph Density", fields)
