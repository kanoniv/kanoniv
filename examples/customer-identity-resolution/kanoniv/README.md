# Approach 3: Kanoniv

Identity resolution as code. A single YAML spec (170 lines of config) replaces hand-written SQL models or 440 lines of Python.

## How It Works

1. **Define a spec** (`kanoniv.yml`) - Sources, blocking, Fellegi-Sunter scoring, normalizers, survivorship, governance
2. **Run the notebook** (`customer_entity.ipynb`) - Or call the Python SDK directly:

```python
from kanoniv import Spec, Source, reconcile, validate

spec = Spec.from_file("kanoniv.yml")
validate(spec).raise_on_error()

sources = [
    Source.from_csv("crm_contacts", "../data/crm_contacts.csv", primary_key="crm_contact_id"),
    Source.from_csv("billing_accounts", "../data/billing_accounts.csv", primary_key="billing_account_id"),
    Source.from_csv("support_users", "../data/support_users.csv", primary_key="support_user_id"),
    Source.from_csv("app_signups", "../data/app_signups.csv", primary_key="app_user_id"),
    Source.from_csv("partner_leads", "../data/partner_leads.csv", primary_key="partner_lead_id"),
]

result = reconcile(sources, spec)
df = result.to_pandas()
print(f"{result.cluster_count} entities, {result.merge_rate:.0%} merge rate")
```

## Running

```bash
pip install kanoniv pandas matplotlib jupyter
cd kanoniv/
jupyter notebook customer_entity.ipynb
```

## Results

```
Input records:        6,539
Golden records:       2,443
Merge rate:           62.6%
Compression:          2.7x
Runtime:              <0.5s
```

## What the Notebook Covers

| Part | Topic |
|------|-------|
| 1 | **Exploring the raw data**  - record counts, field completeness heatmap, format inconsistencies, cross-source email overlap |
| 2 | **The YAML spec**  - sources, blocking, Fellegi-Sunter scoring, normalizers, survivorship, governance |
| 3 | **Validation and execution plan** |
| 4 | **Reconciliation**  - running the Rust engine locally via PyO3 |
| 5 | **Golden records**  - cluster sizes, source coverage, field completeness before vs after |
| 6 | **Quality analysis**  - over-merge detection, survivorship issues, spec tuning recommendations |
| 7 | **Lineage**  - kanoniv IDs, entity tracing, cross-source match patterns |
| 8 | **Business enrichment**  - joining billing/support data for Customer 360, at-risk identification |

## What It Gets Right

- **Declarative** - The entire identity resolution pipeline is a YAML file. Matching rules, normalizers, survivorship, governance - all reviewable in a pull request. No Python to maintain, no SQL to debug.
- **Built-in normalizers** - Email (plus-addressing, Gmail dots), phone (E.164), name (Unicode NFC), nickname (Bob=Robert, 70+ mappings), domain, generic. Declared per field in the spec, not hand-coded.
- **Fellegi-Sunter with EM** - Same statistical matching as Splink. u-probabilities estimated from random sampling (unbiased), m-probabilities learned via EM. Initial probabilities in the spec are starting hints, not final values.
- **Fast** - Rust engine via PyO3. 6,500 records in <0.5s. The same workload takes 2.6s in Splink (6.5x slower).
- **Governance built in** - Freshness checks, schema validation, PII field tagging, audit logging, shadow-mode threshold protection. These aren't add-ons; they're spec fields.
- **Validation before execution** - `validate(spec)` catches errors before you run anything. `plan(spec)` shows the full execution plan with stages, strategies, and risk flags.
- **Spec diffing** - `diff(spec_v1, spec_v2)` shows exactly what changed between versions: rules, thresholds, sources, survivorship. Useful for CI gates and review workflows.
- **Source adapters** - `Source.from_csv()`, `Source.from_pandas()`, `Source.from_warehouse()`, `Source.from_dbt()`. Load data from anywhere, reconcile locally.

## Challenges

- **Younger project** - Splink has years of production use and a large community. Kanoniv is newer, with a smaller ecosystem and fewer battle-tested edge cases.
- **Single-threshold comparisons** - Kanoniv uses one threshold per Jaro-Winkler comparison (continuous score), while Splink supports multi-threshold levels (0.92/0.80) giving 3 comparison levels per field. This is the main reason Splink achieves a slightly higher merge rate (65.9% vs 62.6%).
- **Rust dependency** - The engine is a compiled Rust binary distributed as a Python wheel. This is transparent for `pip install`, but if you need to build from source or run on an uncommon platform, it's more complex than pure Python.
- **YAML learning curve** - The spec format is expressive but has its own schema to learn. The [spec reference](https://kanoniv.com/docs/spec-reference/) documents all fields, but there's an initial ramp-up compared to writing familiar Python or SQL.
