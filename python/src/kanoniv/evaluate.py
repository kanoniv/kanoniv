"""Evaluation functions for measuring identity resolution quality."""
from __future__ import annotations

from dataclasses import dataclass, field
from itertools import combinations
from typing import Any

from .reconcile import ReconcileResult


@dataclass
class EvaluateResult:
    """Three-layer evaluation metrics for an identity resolution run.

    Layer 1 (structural) and Layer 2 (stability) are always populated.
    Layer 3 (ground truth) is populated only when ``ground_truth`` is provided.
    """

    # Layer 1: Structural (always populated)
    total_records: int
    total_clusters: int
    merge_rate: float
    singletons: int
    singletons_pct: float
    largest_cluster: int
    cluster_distribution: dict[int, int]   # size -> count
    pairs_evaluated: int
    decisions: dict[str, int]              # merge/nomerge/review counts

    # Layer 2: Stability (from telemetry)
    field_stats: list[dict] = field(default_factory=list)
    blocking_groups: int = 0

    # Layer 3: Ground truth (None when no labels)
    precision: float | None = None
    recall: float | None = None
    f1: float | None = None
    true_positives: int | None = None
    false_positives: int | None = None
    false_negatives: int | None = None
    predicted_pairs: int | None = None
    ground_truth_pairs: int | None = None
    ground_truth_clusters: int | None = None

    def summary(self) -> str:
        """Return a human-readable summary of the evaluation."""
        lines = [
            "Evaluation Results",
            "=" * 50,
            "",
            "  Structural",
            "  ----------",
            f"  Total Records:      {self.total_records:,}",
            f"  Total Clusters:     {self.total_clusters:,}",
            f"  Merge Rate:         {self.merge_rate:.1%}",
            f"  Singletons:         {self.singletons:,} ({self.singletons_pct:.1%})",
            f"  Largest Cluster:    {self.largest_cluster}",
            f"  Pairs Evaluated:    {self.pairs_evaluated:,}",
        ]
        if self.decisions:
            for k, v in sorted(self.decisions.items()):
                lines.append(f"    {k}: {v:,}")

        lines += [
            "",
            "  Stability",
            "  ---------",
            f"  Blocking Groups:    {self.blocking_groups:,}",
        ]
        if self.field_stats:
            lines.append(f"  Fields:             {len(self.field_stats)}")
            for fs in self.field_stats:
                name = fs.get("field", "?")
                avg = fs.get("avg_score", 0.0)
                matched = fs.get("matched", 0)
                evaluated = fs.get("evaluated", 0)
                match_rate = matched / evaluated if evaluated else 0.0
                lines.append(
                    f"    {name}: avg_score={avg:.3f}, "
                    f"matched={matched:,}/{evaluated:,} ({match_rate:.1%})"
                )

        if self.precision is not None:
            lines += [
                "",
                "  Ground Truth",
                "  ------------",
                f"  Precision:          {self.precision:.4f}",
                f"  Recall:             {self.recall:.4f}",
                f"  F1 Score:           {self.f1:.4f}",
                "",
                f"  True Positives:     {self.true_positives:,}",
                f"  False Positives:    {self.false_positives:,}",
                f"  False Negatives:    {self.false_negatives:,}",
                "",
                f"  Predicted Pairs:    {self.predicted_pairs:,}",
                f"  Ground Truth Pairs: {self.ground_truth_pairs:,}",
                f"  GT Clusters:        {self.ground_truth_clusters:,}",
            ]

        return "\n".join(lines)

    def __repr__(self) -> str:
        if self.precision is not None:
            return (
                f"EvaluateResult(precision={self.precision:.4f}, "
                f"recall={self.recall:.4f}, f1={self.f1:.4f})"
            )
        return (
            f"EvaluateResult(clusters={self.total_clusters}, "
            f"merge_rate={self.merge_rate:.1%}, "
            f"singletons={self.singletons})"
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _pairs_from_clusters(
    clusters: list[set[tuple[str, str]]],
) -> set[tuple[tuple[str, str], tuple[str, str]]]:
    """Convert list of member sets to a canonical pairwise set.

    Each pair is sorted so (A, B) and (B, A) produce the same tuple.
    """
    pairs: set[tuple[tuple[str, str], tuple[str, str]]] = set()
    for cluster in clusters:
        for a, b in combinations(sorted(cluster), 2):
            pairs.add((a, b))
    return pairs


def _clusters_from_result(
    result: ReconcileResult,
    entity_map: dict[str, tuple[str, str]],
) -> list[set[tuple[str, str]]]:
    """Convert UUID-based clusters to (source_name, external_id) clusters."""
    clusters: list[set[tuple[str, str]]] = []
    for uuid_cluster in result.clusters:
        member_set: set[tuple[str, str]] = set()
        for uid in uuid_cluster:
            if uid in entity_map:
                member_set.add(entity_map[uid])
        if len(member_set) >= 2:
            clusters.append(member_set)
    return clusters


def _parse_ground_truth(
    ground_truth: Any,
) -> list[set[tuple[str, str]]]:
    """Normalize ground truth from dict or DataFrame to list of member sets.

    Accepts:
        - dict: {true_entity_id: [(source_name, external_id), ...]}
        - pandas DataFrame with columns [record_id, source_name, true_entity_id]
    """
    # DataFrame
    try:
        import pandas as pd

        if isinstance(ground_truth, pd.DataFrame):
            required = {"record_id", "source_name", "true_entity_id"}
            missing = required - set(ground_truth.columns)
            if missing:
                raise ValueError(
                    f"Ground truth DataFrame missing columns: {sorted(missing)}"
                )
            clusters: dict[str, set[tuple[str, str]]] = {}
            for _, row in ground_truth.iterrows():
                eid = str(row["true_entity_id"])
                clusters.setdefault(eid, set()).add(
                    (str(row["source_name"]), str(row["record_id"]))
                )
            return [c for c in clusters.values() if len(c) >= 2]
    except ImportError:
        pass

    # Dict format
    if isinstance(ground_truth, dict):
        result: list[set[tuple[str, str]]] = []
        for _eid, members in ground_truth.items():
            member_set = {(str(s), str(r)) for s, r in members}
            if len(member_set) >= 2:
                result.append(member_set)
        return result

    raise TypeError(
        f"ground_truth must be a dict or pandas DataFrame, got {type(ground_truth).__name__}"
    )


def _cluster_size_distribution(clusters: list[list[str]]) -> dict[int, int]:
    """Build {size: count} distribution from raw UUID clusters."""
    dist: dict[int, int] = {}
    for c in clusters:
        size = len(c)
        dist[size] = dist.get(size, 0) + 1
    return dist


# ---------------------------------------------------------------------------
# Core evaluation logic (called by ReconcileResult.evaluate())
# ---------------------------------------------------------------------------

def _evaluate(
    result: ReconcileResult,
    ground_truth: Any = None,
) -> EvaluateResult:
    """Build a 3-layer EvaluateResult from a ReconcileResult.

    Layers 1+2 are always populated. Layer 3 requires ground_truth.
    """
    # --- Layer 1: Structural ---
    total_records = sum(len(c) for c in result.clusters)
    total_clusters = len(result.clusters)
    merge_rate = 1.0 - (total_clusters / total_records) if total_records > 0 else 0.0
    singletons = sum(1 for c in result.clusters if len(c) == 1)
    singletons_pct = singletons / total_clusters if total_clusters > 0 else 0.0
    largest_cluster = max((len(c) for c in result.clusters), default=0)
    cluster_distribution = _cluster_size_distribution(result.clusters)

    # --- Layer 2: Stability (from telemetry) ---
    telemetry = result.telemetry
    pairs_evaluated = telemetry.get("pairs_evaluated", 0)
    blocking_groups = telemetry.get("blocking_groups", 0)
    decisions = {
        k: int(v) for k, v in telemetry.get("decisions_by_type", {}).items()
    }

    field_stats: list[dict] = []
    for rt in telemetry.get("rule_telemetry", []):
        field_stats.append({
            "field": rt.get("rule_name", ""),
            "evaluated": rt.get("evaluated", 0),
            "matched": rt.get("matched", 0),
            "skipped": rt.get("skipped", 0),
            "avg_score": rt.get("avg_score", 0.0),
        })

    er = EvaluateResult(
        total_records=total_records,
        total_clusters=total_clusters,
        merge_rate=merge_rate,
        singletons=singletons,
        singletons_pct=singletons_pct,
        largest_cluster=largest_cluster,
        cluster_distribution=cluster_distribution,
        pairs_evaluated=pairs_evaluated,
        decisions=decisions,
        field_stats=field_stats,
        blocking_groups=blocking_groups,
    )

    # --- Layer 3: Ground truth (optional) ---
    if ground_truth is not None:
        entity_map = getattr(result, "_entity_map", None)
        if entity_map is None:
            raise ValueError(
                "ReconcileResult has no _entity_map. Cannot compute ground truth metrics."
            )

        predicted_clusters = _clusters_from_result(result, entity_map)
        gt_clusters = _parse_ground_truth(ground_truth)

        # Collect all records present in ground truth
        gt_records: set[tuple[str, str]] = set()
        for cluster in gt_clusters:
            gt_records.update(cluster)

        # Filter predicted clusters to only include records in ground truth
        filtered_predicted: list[set[tuple[str, str]]] = []
        for cluster in predicted_clusters:
            filtered = cluster & gt_records
            if len(filtered) >= 2:
                filtered_predicted.append(filtered)

        predicted_pairs = _pairs_from_clusters(filtered_predicted)
        gt_pairs = _pairs_from_clusters(gt_clusters)

        tp = len(predicted_pairs & gt_pairs)
        fp = len(predicted_pairs - gt_pairs)
        fn = len(gt_pairs - predicted_pairs)

        precision = tp / (tp + fp) if (tp + fp) > 0 else 1.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 1.0
        f1_score = (
            (2 * precision * recall / (precision + recall))
            if (precision + recall) > 0
            else 0.0
        )

        er.precision = precision
        er.recall = recall
        er.f1 = f1_score
        er.true_positives = tp
        er.false_positives = fp
        er.false_negatives = fn
        er.predicted_pairs = len(predicted_pairs)
        er.ground_truth_pairs = len(gt_pairs)
        er.ground_truth_clusters = len(gt_clusters)

    return er
