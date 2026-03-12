"""Kanoniv CLI - Identity resolution infrastructure.

Command structure: ``kanoniv <domain> <action>``

Domains: entity, source, match, graph, row, spec, job, export, override, feedback, system, memory, task
Workflows: ingest, reconcile, autotune, autodetect, sync, pull, log, ask, agent
Auth: login, logout, context

Offline commands (spec validate/compile/hash/diff/plan) use the local Rust
engine via ``_native``. Cloud commands use httpx to call the Kanoniv API.

Entry point: ``kanoniv`` (installed via pip).
"""
from __future__ import annotations

import argparse
import sys

import kanoniv


# ---------------------------------------------------------------------------
# Parser construction
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="kanoniv",
        description="Kanoniv - Identity resolution infrastructure",
    )
    p.add_argument(
        "--version", action="version", version=f"kanoniv {kanoniv.__version__}"
    )
    p.add_argument(
        "--api-key", dest="api_key_flag", default=None,
        help="API key (overrides env/stored creds)",
    )
    p.add_argument(
        "--api-url", dest="api_url_flag", default=None,
        help="API base URL (default: https://api.kanoniv.com)",
    )
    p.add_argument(
        "--format", dest="format", choices=["json", "table"], default="table",
        help="Output format (default: table)",
    )

    sub = p.add_subparsers(dest="command")

    # ── ENTITY domain ─────────────────────────────────────────────
    _add_entity_parser(sub)

    # ── SOURCE domain ─────────────────────────────────────────────
    _add_source_parser(sub)

    # ── MATCH domain ──────────────────────────────────────────────
    _add_match_parser(sub)

    # ── GRAPH domain ──────────────────────────────────────────────
    _add_graph_parser(sub)

    # ── ROW domain ────────────────────────────────────────────────
    _add_row_parser(sub)

    # ── SPEC domain ───────────────────────────────────────────────
    _add_spec_parser(sub)

    # ── JOB domain ────────────────────────────────────────────────
    _add_job_parser(sub)

    # ── EXPORT domain ─────────────────────────────────────────────
    _add_export_parser(sub)

    # ── OVERRIDE domain ───────────────────────────────────────────
    _add_override_parser(sub)

    # ── FEEDBACK domain ───────────────────────────────────────────
    _add_feedback_parser(sub)

    # ── SYSTEM domain ─────────────────────────────────────────────
    _add_system_parser(sub)

    # ── MEMORY domain ────────────────────────────────────────────
    _add_memory_parser(sub)

    # ── TASK domain ─────────────────────────────────────────────
    _add_task_parser(sub)

    # ── Workflow commands (top-level convenience) ──────────────────
    _add_ingest_parser(sub)
    _add_reconcile_parser(sub)
    _add_autotune_parser(sub)
    _add_autodetect_parser(sub)

    # ── Knowledge sharing (top-level) ─────────────────────────────
    _add_sync_parser(sub)
    _add_pull_parser(sub)
    _add_log_parser(sub)

    # ── LINEAGE (top-level workflow) ──────────────────────────────
    _add_lineage_parser(sub)

    # ── LLM / Agent ───────────────────────────────────────────────
    _add_ask_parser(sub)
    _add_agent_parser(sub)

    # ── Auth ──────────────────────────────────────────────────────
    sub.add_parser("login", help="Store API credentials")
    sub.add_parser("logout", help="Remove stored credentials")
    sub.add_parser("context", help="Show current auth context")

    return p


# ---------------------------------------------------------------------------
# Domain parsers
# ---------------------------------------------------------------------------

def _add_entity_parser(sub) -> None:
    sp = sub.add_parser("entity", help="Resolved identity entities")
    s = sp.add_subparsers(dest="action")

    p = s.add_parser("show", help="Show entity details")
    p.add_argument("entity_id", help="Entity UUID")
    p.add_argument(
        "--records", dest="show_records", action="store_true",
        help="Include linked source records",
    )
    p.add_argument(
        "--attrs", dest="show_attrs", action="store_true",
        help="Show only canonical attributes",
    )
    p.add_argument(
        "--sources", dest="show_sources", action="store_true",
        help="Show field provenance (which source won each field)",
    )
    p.add_argument(
        "--json", dest="json_output", action="store_true",
        help="Raw JSON output",
    )
    p.add_argument(
        "--relationships", dest="show_relationships", action="store_true",
        help="Show shared identity signals between linked records",
    )
    p.add_argument(
        "--field", dest="rel_field", default=None,
        help="Filter relationships by field name (e.g. phone, email)",
    )

    p = s.add_parser("list", help="List all entities")
    p.add_argument("--limit", type=int, default=20, help="Max results (default: 20)")
    p.add_argument("--offset", type=int, default=0, help="Offset for pagination")
    p.add_argument(
        "--entity-type", dest="entity_type", default=None,
        help="Filter by entity type (person, company, product)",
    )
    p.add_argument(
        "--min-records", dest="min_records", type=int, default=None,
        help="Minimum linked source records (e.g. 2 = duplicates)",
    )
    p.add_argument(
        "--max-records", dest="max_records", type=int, default=None,
        help="Maximum linked source records (e.g. 1 = singletons)",
    )
    p.add_argument(
        "--min-confidence", dest="min_confidence", type=float, default=None,
        help="Minimum confidence score (0.0 - 1.0)",
    )
    p.add_argument(
        "--has", dest="has_fields", default=None,
        help="Only entities with these fields (comma-separated: email,phone)",
    )
    p.add_argument(
        "--missing", dest="missing_fields", default=None,
        help="Only entities missing these fields (comma-separated: email,phone)",
    )

    p = s.add_parser("search", help="Search entities")
    p.add_argument("-q", "--query", "--search", default=None, help="Free-text search")
    p.add_argument("--name", default=None, help="Filter by name")
    p.add_argument("--email", default=None, help="Filter by email")
    p.add_argument("--phone", default=None, help="Filter by phone")
    p.add_argument("--source", default=None, help="Filter by source name")
    p.add_argument(
        "--entity-type", dest="entity_type", default=None,
        help="Filter by entity type (person, company, product)",
    )
    p.add_argument(
        "--min-records", dest="min_records", type=int, default=None,
        help="Minimum linked source records (e.g. 2 = duplicates)",
    )
    p.add_argument(
        "--max-records", dest="max_records", type=int, default=None,
        help="Maximum linked source records (e.g. 1 = singletons)",
    )
    p.add_argument(
        "--min-confidence", dest="min_confidence", type=float, default=None,
        help="Minimum confidence score (0.0 - 1.0)",
    )
    p.add_argument(
        "--has", dest="has_fields", default=None,
        help="Only entities with these fields (comma-separated: email,phone)",
    )
    p.add_argument(
        "--missing", dest="missing_fields", default=None,
        help="Only entities missing these fields (comma-separated: email,phone)",
    )
    p.add_argument("--limit", type=int, default=20, help="Max results (default: 20)")
    p.add_argument("--offset", type=int, default=0, help="Offset for pagination")

    p = s.add_parser("linked", help="Show linked source records")
    p.add_argument("entity_id", help="Entity UUID")

    p = s.add_parser("history", help="Show merge history and lifecycle events")
    p.add_argument("entity_id", help="Entity UUID")

    p = s.add_parser("neighbors", help="Show graph neighbors")
    p.add_argument("entity_id", help="Entity UUID")
    p.add_argument("--depth", type=int, default=1, help="Traversal depth (default: 1)")

    p = s.add_parser("merge", help="Immediately merge two entities")
    p.add_argument("entity_a", help="First entity UUID")
    p.add_argument("entity_b", help="Second entity UUID")
    p.add_argument("--reason", default=None, help="Reason for merge")

    p = s.add_parser("split", help="Split members out of an entity")
    p.add_argument("canonical_id", help="Canonical entity UUID to split from")
    p.add_argument("member_ids", nargs="+", help="External entity UUIDs to eject")
    p.add_argument("--reason", default=None, help="Reason for split")

    p = s.add_parser("reassign", help="Move a source record to another entity")
    p.add_argument("ext_id", help="External entity UUID to move")
    p.add_argument("to_entity", help="Target canonical entity UUID")
    p.add_argument("--reason", default=None, help="Reason for reassignment")

    p = s.add_parser("delete", help="Delete an empty entity (no members)")
    p.add_argument("entity_id", help="Entity UUID")

    p = s.add_parser("link", help="Create soft edge between entities")
    p.add_argument("entity_a", help="First entity UUID")
    p.add_argument("entity_b", help="Second entity UUID")
    p.add_argument("--weight", type=float, default=None, help="Edge weight (default: 1.0)")

    p = s.add_parser("unlink", help="Remove soft edge between entities")
    p.add_argument("entity_a", help="First entity UUID")
    p.add_argument("entity_b", help="Second entity UUID")

    p = s.add_parser("explain", help="Explain why an entity exists")
    p.add_argument("entity_id", help="Entity UUID")

    p = s.add_parser("lock", help="Lock entity from further merges")
    p.add_argument("entity_id", help="Entity UUID")

    p = s.add_parser("revert", help="Revert entity to a previous state")
    p.add_argument("entity_id", help="Entity UUID")
    p.add_argument("event_id", help="Event ID to revert to")

    p = s.add_parser("attrs", help="Show canonical attributes (gold record)")
    p.add_argument("entity_id", help="Entity UUID")

    p = s.add_parser("candidates", help="Show merge candidates")
    p.add_argument("entity_id", help="Entity UUID")
    p.add_argument("--limit", type=int, default=10, help="Max results (default: 10)")

    p = s.add_parser("diff", help="Diff two entities' attributes")
    p.add_argument("entity_a", help="First entity UUID")
    p.add_argument("entity_b", help="Second entity UUID")


def _add_source_parser(sub) -> None:
    sp = sub.add_parser("source", help="Data sources")
    s = sp.add_subparsers(dest="action")

    s.add_parser("list", help="List all sources")

    p = s.add_parser("show", help="Show source details")
    p.add_argument("source_id", help="Source UUID")

    p = s.add_parser("schema", help="Show source schema and column types")
    p.add_argument("source_id", help="Source UUID")

    p = s.add_parser("create", help="Create a new source")
    p.add_argument("name", help="Source name")
    p.add_argument("--type", dest="source_type", default="csv", help="Source type")
    p.add_argument("--config", default=None, help="Source config as JSON")

    p = s.add_parser("delete", help="Delete a source")
    p.add_argument("source_id", help="Source UUID")

    p = s.add_parser("sync", help="Trigger a source sync")
    p.add_argument("source_id", help="Source UUID")

    p = s.add_parser("stats", help="Show source statistics")
    p.add_argument("source_id", help="Source UUID")

    p = s.add_parser("quality", help="Data quality report for a source")
    p.add_argument("source_id", help="Source UUID")

    p = s.add_parser("entities", help="List entities from a source")
    p.add_argument("source_id", help="Source UUID")
    p.add_argument("--limit", type=int, default=20, help="Max results (default: 20)")


def _add_match_parser(sub) -> None:
    sp = sub.add_parser("match", help="Matching and similarity")
    s = sp.add_subparsers(dest="action")

    p = s.add_parser("explain", help="Explain match between two records")
    p.add_argument("record_a", help="First record (source:external_id)")
    p.add_argument("record_b", help="Second record (source:external_id)")

    s.add_parser("rules", help="Show active matching rules")

    p = s.add_parser("pending", help="List pending match decisions")
    p.add_argument("--limit", type=int, default=20, help="Max results (default: 20)")

    p = s.add_parser("decide", help="Accept or reject a pending match")
    p.add_argument("entity_a", help="First entity UUID")
    p.add_argument("entity_b", help="Second entity UUID")
    p.add_argument("--accept", action="store_true", help="Accept the match")
    p.add_argument("--reject", action="store_true", help="Reject the match")
    p.add_argument("--reason", default=None, help="Reason for decision")

    p = s.add_parser("candidates", help="Match candidates for an entity")
    p.add_argument("entity_id", help="Entity UUID")
    p.add_argument("--limit", type=int, default=20, help="Max results (default: 20)")

    p = s.add_parser("cluster", help="Show match cluster for an entity")
    p.add_argument("entity_id", help="Entity UUID")

    p = s.add_parser("test", help="Test match two records (dry-run)")
    p.add_argument("record_a", help="First record (source:external_id)")
    p.add_argument("record_b", help="Second record (source:external_id)")


def _add_graph_parser(sub) -> None:
    sp = sub.add_parser("graph", help="Identity graph analytics")
    s = sp.add_subparsers(dest="action")

    s.add_parser("stats", help="Graph statistics")

    p = s.add_parser("cluster", help="Show cluster members for an entity")
    p.add_argument("entity_id", help="Entity UUID")

    p = s.add_parser("clusters", help="List all clusters")
    p.add_argument("--limit", type=int, default=20, help="Max results (default: 20)")
    p.add_argument("--min-size", dest="min_size", type=int, default=None, help="Min cluster size")

    s.add_parser("bridges", help="Bridge signals between entity types")
    s.add_parser("refresh", help="Refresh graph analytics")

    p = s.add_parser("influence", help="Entity influence score")
    p.add_argument("entity_id", help="Entity UUID")

    p = s.add_parser("risk", help="Entity risk score")
    p.add_argument("entity_id", help="Entity UUID")

    p = s.add_parser("orphans", help="List orphan entities (no connections)")
    p.add_argument("--limit", type=int, default=20, help="Max results (default: 20)")
    p.add_argument(
        "--entity-type", dest="entity_type", default=None,
        help="Filter by entity type",
    )

    p = s.add_parser("conflicts", help="List conflicting/incoherent clusters")
    p.add_argument("--limit", type=int, default=20, help="Max results (default: 20)")

    p = s.add_parser("density", help="Graph density and connectivity metrics")


def _add_row_parser(sub) -> None:
    sp = sub.add_parser("row", help="Source records (rows)")
    s = sp.add_subparsers(dest="action")

    p = s.add_parser("show", help="Show a source record")
    p.add_argument("source", help="Source name")
    p.add_argument("external_id", help="External ID")

    p = s.add_parser("resolve", help="Real-time resolve a record")
    p.add_argument("source", help="Source name")
    p.add_argument("external_id", help="External ID")
    p.add_argument("--data", default=None, help="Record data as JSON")

    p = s.add_parser("lookup", help="Look up entity for a record")
    p.add_argument("source", help="Source name")
    p.add_argument("external_id", help="External ID")

    p = s.add_parser("memberships", help="List source-to-entity memberships")
    p.add_argument("--source", default=None, help="Filter by source name")
    p.add_argument("--entity-id", dest="entity_id", default=None, help="Filter by entity")
    p.add_argument("--limit", type=int, default=50, help="Max results (default: 50)")

    p = s.add_parser("trace", help="Show match audit trail for a record")
    p.add_argument("source", help="Source name")
    p.add_argument("external_id", help="External ID")


def _add_spec_parser(sub) -> None:
    sp = sub.add_parser("spec", help="Identity spec management")
    s = sp.add_subparsers(dest="action")

    # Offline commands
    p = s.add_parser("validate", help="Validate a YAML spec")
    p.add_argument("path", help="Path to spec YAML file")

    p = s.add_parser("compile", help="Compile spec to IR")
    p.add_argument("path", help="Path to spec YAML file")
    p.add_argument("-o", "--output", default=None, help="Output file path")

    p = s.add_parser("hash", help="Compute spec hash")
    p.add_argument("path", help="Path to spec YAML file")

    p = s.add_parser("diff", help="Diff two spec files")
    p.add_argument("v1", help="Path to first spec YAML")
    p.add_argument("v2", help="Path to second spec YAML")

    p = s.add_parser("plan", help="Generate execution plan")
    p.add_argument("path", help="Path to spec YAML file")

    # Cloud commands
    s.add_parser("list", help="List identity specs (Cloud)")

    p = s.add_parser("show", help="Show spec details (Cloud)")
    p.add_argument("version", help="Spec version name")

    p = s.add_parser("upload", help="Upload spec to Cloud")
    p.add_argument("path", help="Path to spec YAML file")
    p.add_argument(
        "--compile", dest="compile_spec", action="store_true",
        help="Compile spec after upload",
    )

    p = s.add_parser("delete", help="Delete a spec (Cloud)")
    p.add_argument("version", help="Spec version name")


def _add_job_parser(sub) -> None:
    sp = sub.add_parser("job", help="Background jobs")
    s = sp.add_subparsers(dest="action")

    p = s.add_parser("list", help="List recent jobs")
    p.add_argument("--limit", type=int, default=20, help="Max results (default: 20)")
    p.add_argument("--type", dest="job_type", default=None, help="Filter by job type")

    p = s.add_parser("show", help="Show job details")
    p.add_argument("job_id", help="Job UUID")

    p = s.add_parser("cancel", help="Cancel a running job")
    p.add_argument("job_id", help="Job UUID")

    p = s.add_parser("run", help="Run a new job")
    p.add_argument("job_type", help="Job type (reconciliation, autotune, export)")
    p.add_argument("--wait", action="store_true", help="Wait for completion")
    p.add_argument("--payload", default=None, help="Job payload as JSON")


def _add_export_parser(sub) -> None:
    sp = sub.add_parser("export", help="Export data")
    s = sp.add_subparsers(dest="export_target")

    p = s.add_parser("entities", help="Export resolved canonical entities")
    p.add_argument("-o", "--output", required=True, help="Output file path")
    p.add_argument(
        "--entity-type", dest="entity_type", default=None,
        help="Filter by entity type",
    )
    p.add_argument(
        "--reveal-pii", dest="reveal_pii", action="store_true",
        help="Include unmasked PII fields (admin only)",
    )
    p.add_argument(
        "--format", dest="export_format", choices=["csv", "json"],
        default="csv", help="Output format (default: csv)",
    )

    p = s.add_parser("memberships", help="Export source-to-entity memberships")
    p.add_argument("-o", "--output", required=True, help="Output file path")
    p.add_argument(
        "--format", dest="export_format", choices=["csv", "json"],
        default="csv", help="Output format (default: csv)",
    )

    p = s.add_parser("graph", help="Export identity graph (nodes + edges)")
    p.add_argument("-o", "--output", required=True, help="Output file path")
    p.add_argument(
        "--entity-type", dest="entity_type", default=None,
        help="Filter by entity type",
    )
    p.add_argument(
        "--format", dest="export_format", choices=["csv", "json"],
        default="json", help="Output format (default: json)",
    )

    p = s.add_parser("matches", help="Export match audit results")
    p.add_argument("-o", "--output", required=True, help="Output file path")
    p.add_argument(
        "--entity-type", dest="entity_type", default=None,
        help="Filter by entity type",
    )
    p.add_argument(
        "--format", dest="export_format", choices=["csv", "json"],
        default="csv", help="Output format (default: csv)",
    )


def _add_override_parser(sub) -> None:
    sp = sub.add_parser("override", help="Manual merge/split overrides")
    s = sp.add_subparsers(dest="action")

    s.add_parser("list", help="List all overrides")

    p = s.add_parser("create", help="Create a manual override")
    p.add_argument("entity_a", help="First entity UUID")
    p.add_argument("entity_b", help="Second entity UUID")
    p.add_argument(
        "--type", dest="override_type", choices=["merge", "split"],
        required=True, help="Override type: merge or split",
    )

    p = s.add_parser("delete", help="Delete an override")
    p.add_argument("override_id", help="Override UUID")


def _add_feedback_parser(sub) -> None:
    sp = sub.add_parser("feedback", help="Active learning labels")
    s = sp.add_subparsers(dest="action")

    p = s.add_parser("list", help="List feedback labels")
    p.add_argument("--limit", type=int, default=20, help="Max results (default: 20)")

    p = s.add_parser("create", help="Create a feedback label")
    p.add_argument("entity_a", help="First entity/record ID")
    p.add_argument("entity_b", help="Second entity/record ID")
    p.add_argument("--source-a", dest="source_a", required=True, help="Source name for entity A")
    p.add_argument("--source-b", dest="source_b", required=True, help="Source name for entity B")
    p.add_argument(
        "--label", choices=["match", "no_match"], required=True,
        help="Label: match or no_match",
    )
    p.add_argument("--reason", default=None, help="Reason for the label")

    p = s.add_parser("delete", help="Delete a feedback label")
    p.add_argument("feedback_id", help="Feedback label UUID")


def _add_system_parser(sub) -> None:
    sp = sub.add_parser("system", help="System operations")
    s = sp.add_subparsers(dest="action")

    s.add_parser("version", help="Show version info")
    s.add_parser("health", help="Check API health")

    p = s.add_parser("stats", help="Dashboard statistics")
    p.add_argument(
        "--entity-type", dest="entity_type", default=None,
        help="Filter by entity type",
    )

    p = s.add_parser("purge", help="Delete all tenant data")
    p.add_argument("--confirm", action="store_true", help="Skip confirmation")
    p.add_argument(
        "--entity-type", dest="entity_type", default=None,
        help="Only purge data for this entity type (e.g. product, person)",
    )

    p = s.add_parser("reindex", help="Rebuild search indexes")
    p.add_argument(
        "--target", default=None,
        help="Specific target to reindex (e.g. entities, graph)",
    )

    p = s.add_parser("recompute", help="Recompute derived data")
    p.add_argument(
        "--target", default=None,
        help="Specific target to recompute (e.g. canonical, metrics)",
    )


# ---------------------------------------------------------------------------
# Memory and Task domain parsers
# ---------------------------------------------------------------------------

def _add_memory_parser(sub) -> None:
    sp = sub.add_parser("memory", help="Agent memory (decisions, patterns, knowledge)")
    s = sp.add_subparsers(dest="action")

    p = s.add_parser("create", help="Create a memory entry")
    p.add_argument("title", help="Title of the memory entry")
    p.add_argument("--content", default=None, help="Detailed content")
    p.add_argument(
        "--type", dest="entry_type", default="knowledge",
        choices=["decision", "investigation", "pattern", "knowledge", "expertise"],
        help="Entry type (default: knowledge)",
    )
    p.add_argument("--slug", default=None, help="Unique slug identifier")
    p.add_argument("--entity-ids", dest="entity_ids", default=None, help="Linked entity UUIDs (comma-separated)")
    p.add_argument("--author", default=None, help="Author name (agent or human)")
    p.add_argument("--tags", default=None, help="Tags (comma-separated)")

    p = s.add_parser("recall", help="Get all memory linked to an entity")
    p.add_argument("entity_id", help="Entity UUID")
    p.add_argument("--limit", type=int, default=20, help="Max results (default: 20)")

    p = s.add_parser("search", help="Full-text search across memory")
    p.add_argument("-q", "--query", default=None, help="Search query")
    p.add_argument(
        "--type", dest="entry_type", default=None,
        choices=["decision", "investigation", "pattern", "knowledge", "expertise", "intent"],
        help="Filter by entry type",
    )
    p.add_argument("--author", default=None, help="Filter by author")
    p.add_argument("--limit", type=int, default=20, help="Max results (default: 20)")

    p = s.add_parser("intent", help="Declare what you're about to do")
    p.add_argument("message", help="Intent description")
    p.add_argument("--entity-ids", dest="entity_ids", default=None, help="Linked entity UUIDs (comma-separated)")
    p.add_argument("--author", default=None, help="Author name")
    p.add_argument("--ttl", type=int, default=3600, help="Time-to-live in seconds (default: 3600)")

    p = s.add_parser("expertise", help="Find agents with expertise in a domain")
    p.add_argument("-q", "--query", default=None, help="Domain or topic to search")
    p.add_argument("--entity-id", dest="entity_id", default=None, help="Entity UUID")
    p.add_argument("--limit", type=int, default=20, help="Max results (default: 20)")

    p = s.add_parser("list", help="List all memory entries")
    p.add_argument(
        "--type", dest="entry_type", default=None,
        help="Filter by entry type",
    )
    p.add_argument("--limit", type=int, default=20, help="Max results (default: 20)")

    p = s.add_parser("delete", help="Delete a memory entry")
    p.add_argument("entry_id", help="Memory entry UUID")


def _add_task_parser(sub) -> None:
    sp = sub.add_parser("task", help="Cross-agent task assignment")
    s = sp.add_subparsers(dest="action")

    p = s.add_parser("create", help="Create a task")
    p.add_argument("title", help="Task title")
    p.add_argument("--content", default=None, help="Detailed description")
    p.add_argument("--slug", default=None, help="Unique slug identifier")
    p.add_argument("--assigned-to", dest="assigned_to", default=None, help="Agent name to assign to")
    p.add_argument(
        "--priority", default="medium",
        choices=["low", "medium", "high", "critical"],
        help="Priority (default: medium)",
    )
    p.add_argument("--entity-ids", dest="entity_ids", default=None, help="Linked entity UUIDs (comma-separated)")
    p.add_argument("--author", default=None, help="Creator name")

    p = s.add_parser("list", help="List tasks")
    p.add_argument("--assigned-to", dest="assigned_to", default=None, help="Filter by assignee")
    p.add_argument(
        "--status", default=None,
        choices=["open", "in_progress", "done"],
        help="Filter by status",
    )
    p.add_argument("--limit", type=int, default=20, help="Max results (default: 20)")

    p = s.add_parser("update", help="Update a task's status")
    p.add_argument("task_id", help="Task UUID")
    p.add_argument(
        "status", choices=["open", "in_progress", "done"],
        help="New status",
    )
    p.add_argument("--note", default=None, help="Note about the status change")
    p.add_argument("--author", default=None, help="Who is updating")

    p = s.add_parser("show", help="Show task details")
    p.add_argument("task_id", help="Task UUID")


# ---------------------------------------------------------------------------
# Knowledge sharing parsers (top-level)
# ---------------------------------------------------------------------------

def _add_sync_parser(sub) -> None:
    p = sub.add_parser("sync", help="Push local knowledge (CLAUDE.md, skills, memory) to cloud")
    p.add_argument(
        "--dry-run", dest="dry_run", action="store_true",
        help="Show what would be synced without uploading",
    )


def _add_pull_parser(sub) -> None:
    p = sub.add_parser("pull", help="Download cloud knowledge to local files")
    p.add_argument(
        "--dry-run", dest="dry_run", action="store_true",
        help="Show what would be written without downloading",
    )


def _add_log_parser(sub) -> None:
    p = sub.add_parser("log", help="Unified activity stream across agents")
    p.add_argument("--limit", type=int, default=50, help="Max entries (default: 50)")
    p.add_argument(
        "--since", default=None,
        help="Time filter: ISO timestamp or relative (1h, 24h, 7d)",
    )
    p.add_argument("--agent", default=None, help="Filter by agent name")


# ---------------------------------------------------------------------------
# Workflow command parsers (top-level convenience)
# ---------------------------------------------------------------------------

def _add_ingest_parser(sub) -> None:
    p = sub.add_parser("ingest", help="Ingest CSV/JSON files into the Cloud")
    p.add_argument("files", nargs="+", help="CSV/JSON files, globs, or directories")
    p.add_argument(
        "--id-column", dest="id_column", default=None,
        help="Column to use as external_id (auto-detected if not set)",
    )
    p.add_argument(
        "--entity-type", dest="entity_type", nargs="*", default=None,
        help="Entity type(s) to filter for (e.g. person product company)",
    )


def _add_reconcile_parser(sub) -> None:
    p = sub.add_parser("reconcile", help="Run reconciliation")
    p.add_argument("--wait", action="store_true", help="Wait for completion")
    p.add_argument(
        "--dry-run", dest="dry_run", action="store_true",
        help="Dry run (evaluate without persisting)",
    )
    p.add_argument(
        "--entity-type", dest="entity_type", nargs="*", default=None,
        help="Entity type(s) to reconcile (e.g. person product company)",
    )


def _add_autotune_parser(sub) -> None:
    p = sub.add_parser("autotune", help="Optimize matching thresholds")
    p.add_argument("--wait", action="store_true", help="Wait for completion")
    p.add_argument(
        "--max-iterations", dest="max_iterations", type=int, default=50,
        help="Maximum optimization iterations (default: 50)",
    )
    p.add_argument("--version", default=None, help="Identity plan version to optimize")


def _add_autodetect_parser(sub) -> None:
    p = sub.add_parser(
        "autodetect", help="Profile data, detect entity types and signals"
    )
    p.add_argument(
        "--bootstrap", action="store_true",
        help="Bootstrap an identity plan from detected signals",
    )
    p.add_argument(
        "--entity-type", dest="entity_type", nargs="*", default=None,
        help="Entity type(s) to profile (e.g. person product company)",
    )
    p.add_argument(
        "--version", default=None, help="Identity plan version name for bootstrap",
    )
    p.add_argument(
        "--sample-size", dest="sample_size", type=int, default=None,
        help="Number of rows to sample (default: 500)",
    )
    p.add_argument(
        "--map", dest="field_mappings", action="append", default=None,
        metavar="CANONICAL=col1,col2",
        help=(
            "Map source columns to canonical fields. Repeatable. "
            "Example: --map 'product_name=title,description' --map 'brand=manufacturer'"
        ),
    )


def _add_lineage_parser(sub) -> None:
    p = sub.add_parser("lineage", help="Cross-source entity merge lineage")
    p.add_argument(
        "--source", nargs="+", default=None,
        help="Source name(s) to trace",
    )
    p.add_argument(
        "--entity-type", dest="entity_type", default=None,
        help="Entity type filter",
    )
    p.add_argument("--limit", type=int, default=20, help="Max results (default: 20)")
    p.add_argument("--offset", type=int, default=0, help="Offset for pagination")
    p.add_argument(
        "--reveal-pii", dest="reveal_pii", action="store_true", default=False,
        help="Reveal unmasked PII fields (admin only)",
    )
    p.set_defaults(action="show")


def _add_ask_parser(sub) -> None:
    p = sub.add_parser("ask", help="Chat with the Kanoniv LLM")
    p.add_argument("message", help="Message to send")
    p.add_argument(
        "--conversation", default=None, help="Continue existing conversation",
    )
    p.add_argument(
        "--new", dest="new_conversation", action="store_true",
        help="Start a new conversation",
    )
    p.add_argument(
        "--plan", dest="plan", action="store_true", help="Force plan generation mode",
    )
    p.add_argument(
        "--generate-spec", dest="generate_spec", action="store_true",
        help="Generate an identity spec",
    )
    p.add_argument(
        "--sources", default=None,
        help="Source names for spec generation (comma-separated)",
    )
    p.add_argument(
        "--entity-ids", dest="entity_ids", default=None,
        help="Entity IDs for explain (comma-separated UUIDs)",
    )


def _add_agent_parser(sub) -> None:
    sp = sub.add_parser("agent", help="Manage AI agents")
    s = sp.add_subparsers(dest="agent_action")

    s.add_parser("list", help="List agent configurations")

    p = s.add_parser("enable", help="Enable an agent")
    p.add_argument("agent_type", help="Agent type (e.g. auto_merge)")
    p.add_argument("--settings", default=None, help="Settings as JSON")

    p = s.add_parser("disable", help="Disable an agent")
    p.add_argument("agent_type", help="Agent type")

    p = s.add_parser("trigger", help="Trigger an agent run")
    p.add_argument("agent_type", help="Agent type")

    p = s.add_parser("runs", help="List agent runs")
    p.add_argument("--type", default=None, help="Filter by agent type")
    p.add_argument("--limit", type=int, default=20, help="Max results")

    p = s.add_parser("actions", help="List agent actions")
    p.add_argument(
        "--status", dest="status_filter", default=None,
        help="Filter by status (pending, approved, rejected)",
    )
    p.add_argument("--limit", type=int, default=20, help="Max results")

    p = s.add_parser("approve", help="Approve a pending action")
    p.add_argument("action_id", help="Action ID")

    p = s.add_parser("reject", help="Reject a pending action")
    p.add_argument("action_id", help="Action ID")

    # Agent Registry (discovery)
    p = s.add_parser("register", help="Register an agent in the directory")
    p.add_argument("name", help="Agent name (e.g. sdr-agent, code-agent)")
    p.add_argument("--capabilities", default=None, help="Comma-separated capabilities (e.g. crm,email,billing)")
    p.add_argument("--description", default=None, help="What this agent does")
    p.add_argument("--instance-id", dest="instance_id", default=None, help="Instance ID for horizontal scaling")

    s.add_parser("who", help="List all registered agents (agent directory)")

    p = s.add_parser("rename", help="Rename a registered agent")
    p.add_argument("current_name", help="Current agent name")
    p.add_argument("new_name", help="New agent name")


# ---------------------------------------------------------------------------
# Legacy alias rewriting (backward compatibility)
# ---------------------------------------------------------------------------

# Old flat commands -> new domain structure
_LEGACY_ALIASES: dict[str, list[str]] = {
    "validate": ["spec", "validate"],
    "compile": ["spec", "compile"],
    "hash": ["spec", "hash"],
    "diff": ["spec", "diff"],
    "plan": ["spec", "plan"],
    "jobs": ["job", "list"],
    "stats": ["system", "stats"],
    "purge": ["system", "purge"],
    "entities": ["entity", "search"],
    "sources": ["source"],
    "matches": ["match"],
    "rows": ["row"],
    "overrides": ["override"],
    "specs": ["spec"],
    "clusters": ["graph", "clusters"],
}


def _looks_like_uuid(s: str) -> bool:
    """Check if a string looks like a UUID (8-4-4-4-12 hex pattern)."""
    import re
    return bool(re.match(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-', s))


def _rewrite_legacy_args(argv: list[str]) -> list[str]:
    """Rewrite legacy flat commands to domain structure before parsing.

    e.g. ``kanoniv validate foo.yaml`` -> ``kanoniv spec validate foo.yaml``
    e.g. ``kanoniv entity <uuid>`` -> ``kanoniv entity show <uuid>``
    """
    if len(argv) < 2:
        return argv
    cmd = argv[1]
    if cmd in _LEGACY_ALIASES:
        return [argv[0]] + _LEGACY_ALIASES[cmd] + argv[2:]
    # kanoniv entity <uuid> [flags] -> kanoniv entity show <uuid> [flags]
    if len(argv) >= 3 and cmd == "entity" and _looks_like_uuid(argv[2]):
        return [argv[0], "entity", "show"] + argv[2:]
    return argv


# ---------------------------------------------------------------------------
# Client helper
# ---------------------------------------------------------------------------

def _require_cloud_client(args: argparse.Namespace):
    """Build an HTTP client for cloud commands, or exit with instructions."""
    from kanoniv.cli.config import resolve_api_key, resolve_api_url

    api_key = resolve_api_key(args.api_key_flag)
    if not api_key:
        sys.stderr.write(
            "No API key found. Set one with:\n"
            "  kanoniv login\n"
            "  --api-key <key>\n"
            "  KANONIV_API_KEY=<key>\n"
        )
        sys.exit(1)

    api_url = resolve_api_url(args.api_url_flag)

    from kanoniv.cli.http import CliHttpClient
    return CliHttpClient(api_url, api_key)


def _try_cloud_client(args: argparse.Namespace):
    """Build an HTTP client if credentials are available, else return None."""
    from kanoniv.cli.config import resolve_api_key, resolve_api_url

    api_key = resolve_api_key(args.api_key_flag)
    if not api_key:
        return None

    api_url = resolve_api_url(args.api_url_flag)

    from kanoniv.cli.http import CliHttpClient
    return CliHttpClient(api_url, api_key)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

# Domains that always need a cloud client
_CLOUD_DOMAINS = {
    "entity", "source", "match", "graph", "row", "job",
    "override", "feedback", "export", "lineage",
    "memory", "task",
}

# Domains that may or may not need a cloud client
_MIXED_DOMAINS = {"spec", "system"}

def _dispatch(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    cmd = args.command

    # ── Domain commands ───────────────────────────────────────────

    if cmd in _CLOUD_DOMAINS:
        action = getattr(args, "action", None) or getattr(args, "export_target", None)
        if not action:
            parser.parse_args([cmd, "--help"])
            return
        client = _require_cloud_client(args)
        try:
            _dispatch_cloud_domain(cmd, args, client)
        finally:
            client.close()
        return

    if cmd in _MIXED_DOMAINS:
        action = getattr(args, "action", None)
        if not action:
            parser.parse_args([cmd, "--help"])
            return
        client = _try_cloud_client(args)
        try:
            _dispatch_mixed_domain(cmd, args, client)
        finally:
            if client:
                client.close()
        return

    # ── Workflow commands ─────────────────────────────────────────

    if cmd == "ingest":
        client = _require_cloud_client(args)
        try:
            from kanoniv.cli.commands.ingest import cmd_ingest
            cmd_ingest(args, client)
        finally:
            client.close()
        return

    if cmd in ("autodetect", "reconcile", "autotune"):
        client = _require_cloud_client(args)
        try:
            from kanoniv.cli.commands.engine import (
                cmd_autodetect, cmd_reconcile, cmd_autotune,
            )
            dispatch = {
                "autodetect": cmd_autodetect,
                "reconcile": cmd_reconcile,
                "autotune": cmd_autotune,
            }
            dispatch[cmd](args, client)
        finally:
            client.close()
        return

    # ── Knowledge sharing ─────────────────────────────────────────

    if cmd in ("sync", "pull", "log"):
        client = _require_cloud_client(args)
        try:
            from kanoniv.cli.commands.knowledge import cmd_sync, cmd_pull, cmd_log
            dispatch = {
                "sync": cmd_sync,
                "pull": cmd_pull,
                "log": cmd_log,
            }
            dispatch[cmd](args, client)
        finally:
            client.close()
        return

    # ── Auth commands ─────────────────────────────────────────────

    if cmd in ("login", "logout", "context"):
        from kanoniv.cli.commands.auth import cmd_login, cmd_logout, cmd_context
        dispatch = {
            "login": cmd_login,
            "logout": cmd_logout,
            "context": cmd_context,
        }
        dispatch[cmd](args)
        return

    # ── LLM / Agent ───────────────────────────────────────────────

    if cmd == "ask":
        client = _require_cloud_client(args)
        try:
            from kanoniv.cli.commands.ask import cmd_ask
            cmd_ask(args, client)
        finally:
            client.close()
        return

    if cmd == "agent":
        if not args.agent_action:
            sys.stderr.write(
                "usage: kanoniv agent "
                "{list,enable,disable,trigger,runs,actions,approve,reject}\n"
            )
            sys.exit(1)
        client = _require_cloud_client(args)
        try:
            from kanoniv.cli.commands.agent import cmd_agent
            cmd_agent(args, client)
        finally:
            client.close()
        return

    # ── Unknown ───────────────────────────────────────────────────

    parser.print_help()
    sys.exit(1)


def _dispatch_cloud_domain(
    domain: str, args: argparse.Namespace, client: object,
) -> None:
    """Dispatch to a cloud-only domain command module."""
    if domain == "entity":
        from kanoniv.cli.commands.entity import cmd_entity
        cmd_entity(args, client)
    elif domain == "source":
        from kanoniv.cli.commands.source import cmd_source
        cmd_source(args, client)
    elif domain == "match":
        from kanoniv.cli.commands.match import cmd_match
        cmd_match(args, client)
    elif domain == "graph":
        from kanoniv.cli.commands.graph import cmd_graph
        cmd_graph(args, client)
    elif domain == "row":
        from kanoniv.cli.commands.row import cmd_row
        cmd_row(args, client)
    elif domain == "job":
        from kanoniv.cli.commands.job import cmd_job
        cmd_job(args, client)
    elif domain == "override":
        from kanoniv.cli.commands.override import cmd_override
        cmd_override(args, client)
    elif domain == "feedback":
        from kanoniv.cli.commands.feedback import cmd_feedback
        cmd_feedback(args, client)
    elif domain == "lineage":
        from kanoniv.cli.commands.lineage import cmd_lineage
        cmd_lineage(args, client)
    elif domain == "memory":
        from kanoniv.cli.commands.memory import cmd_memory
        cmd_memory(args, client)
    elif domain == "task":
        from kanoniv.cli.commands.task import cmd_task
        cmd_task(args, client)
    elif domain == "export":
        target = getattr(args, "export_target", None)
        if not target:
            sys.stderr.write(
                "usage: kanoniv export "
                "{entities,memberships,graph,matches}\n"
            )
            sys.exit(1)
        from kanoniv.cli.commands.export import (
            cmd_export_entities, cmd_export_memberships,
            cmd_export_graph, cmd_export_matches,
        )
        export_dispatch = {
            "entities": cmd_export_entities,
            "memberships": cmd_export_memberships,
            "graph": cmd_export_graph,
            "matches": cmd_export_matches,
        }
        fn = export_dispatch.get(target)
        if fn:
            fn(args, client)
        else:
            sys.stderr.write(
                "usage: kanoniv export "
                "{entities,memberships,graph,matches}\n"
            )
            sys.exit(1)


def _dispatch_mixed_domain(
    domain: str, args: argparse.Namespace, client: object | None,
) -> None:
    """Dispatch to a domain that may use offline or cloud commands."""
    if domain == "spec":
        from kanoniv.cli.commands.spec import cmd_spec
        cmd_spec(args, client)
    elif domain == "system":
        from kanoniv.cli.commands.system import cmd_system
        cmd_system(args, client)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = _build_parser()
    args = parser.parse_args(_rewrite_legacy_args(sys.argv)[1:])

    if not args.command:
        parser.print_help()
        sys.exit(0)

    try:
        _dispatch(args, parser)
    except KeyboardInterrupt:
        sys.stderr.write("\n")
        sys.exit(130)
    except Exception as exc:
        sys.stderr.write(f"error: {exc}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
