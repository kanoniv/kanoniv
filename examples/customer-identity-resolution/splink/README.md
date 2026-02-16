# Approach 2: Splink + DuckDB

Identity resolution using [Splink](https://github.com/moj-analytical-services/splink), the most popular open-source record linkage library (5k+ GitHub stars), with DuckDB as the in-process SQL backend.

## How It Works

Everything runs in a single Python script (`resolve.py`):

1. **Normalization** (100 lines) - Clean names (nickname resolution: Bob=Robert), emails (plus-addressing, Gmail dots), phones (E.164), companies (strip suffixes)
2. **Model definition** (30 lines) - Declare Splink comparisons and blocking rules
3. **Training** (15 lines) - Two-phase Fellegi-Sunter: random sampling for u-probabilities, then EM for m-probabilities
4. **Predict + Cluster** (5 lines) - Score all candidate pairs, cluster via connected components
5. **Golden records** (30 lines) - Source-priority survivorship per field

```bash
pip install -r requirements.txt
python resolve.py
```

Output goes to `output/`:
- `resolved_entities.csv` - golden records (one row per resolved entity)
- `entity_graph.csv` - source record to entity mapping

## Results

```
Input records:        6,539
Resolved entities:    2,233
Compression ratio:      2.9x
Candidate pairs:      7,549
Elapsed time:          2.6s
```

## What It Gets Right

- **Statistical matching** - Fellegi-Sunter with EM learns m/u probabilities from your data instead of hand-tuning weights. The model adapts to your data distribution, not the other way around.
- **Jaro-Winkler with multiple thresholds** - `JaroWinklerAtThresholds("first_name", [0.92, 0.80])` gives 3 comparison levels per field (high similarity, partial, no match). This is significantly more expressive than binary exact/non-match, and it's why Splink achieves the highest merge rate of the three approaches.
- **Graph clustering** - Proper connected components via graph algorithm. No fragile iterative SQL - if A matches B and B matches C, Splink always finds the A-B-C cluster.
- **Zero infrastructure** - DuckDB runs in-process. No database server, no warehouse, no Docker. `pip install` and go.
- **Well-maintained** - Active development by the UK Ministry of Justice analytical services team. Good documentation, active community, regular releases.
- **Interactive profiling** - Built-in Splink charts for m/u distributions, waterfall match breakdowns, and cluster visualization. Useful for debugging and tuning.

## Challenges

- **Manual normalization** - You write all cleanup code yourself. The 100-line normalization section in `resolve.py` handles nicknames (70+ mappings), email plus-addressing, Gmail dot trick, domain aliases, E.164 phone formatting, company suffix stripping, and name parsing ("Last, First" format). This is tedious, error-prone, and must be maintained as new patterns appear.
- **Manual survivorship** - You write golden record assembly yourself (30 lines of Python groupby logic). Changing field-level strategies means editing code, not config.
- **Training is fragile** - EM parameter estimation requires careful blocking rule selection for each training pass. We tried 4 different training strategies before finding one that produces good results. The wrong approach gives silently bad m/u estimates and poor match quality with no warning.
- **No governance** - No built-in freshness checks, schema validation, PII tagging, audit logging, or shadow-mode deploys. You build observability yourself or go without.
- **Python-only** - Doesn't integrate into dbt DAGs natively. If your pipeline is dbt-based, Splink requires a separate orchestration step (Airflow, Dagster, etc.) to bridge the gap.
- **Configuration is code** - The model definition lives in Python, not a reviewable YAML document. Code review for matching rule changes requires reading Python API calls, not a declarative spec.
- **No incremental mode** - Every run reprocesses the full dataset. For large datasets, this means recomputing all pairwise scores even when only a few records changed.
