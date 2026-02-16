# Identity Resolution in Pure SQL (dbt)

Hand-rolled identity resolution using dbt and DuckDB. No external libraries - just SQL.

## Pipeline

```
crm_contacts ─┐
billing_accounts ─┤
support_users ─────┤─> spine ─> blocking ─> scoring ─> clusters ─> dim_customers
app_signups ──────┤
partner_leads ────┘
```

| Model | Purpose |
|-------|---------|
| `int_identity__spine` | Union 5 sources, normalize emails/phones/names/nicknames |
| `int_identity__blocking` | Generate candidate pairs via email, phone, and name blocking keys |
| `int_identity__scoring` | Jaro-Winkler similarity scoring with hand-tuned weights |
| `int_identity__clusters` | Connected components via iterative label propagation (6 passes) |
| `dim_customers` | Survivorship - pick best value per field by source priority |

## Quick Start

```bash
# Option 1: dbt (requires dbt-duckdb)
pip install dbt-duckdb
dbt seed --profiles-dir .
dbt run --profiles-dir .

# Option 2: Standalone (just DuckDB, no dbt needed)
pip install duckdb
python calculate_metrics.py
```

## Results

```
Input Records:    6,539
Golden Records:   3,583
Merge Rate:       45%
Compression:      1.8x
```

Note: hand-tuned SQL gets a lower merge rate than probabilistic approaches (Splink: 66%, Kanoniv: 63%). This is the fundamental limitation - fixed weights cannot adapt to field-value frequency the way Fellegi-Sunter does.

## How It Works

**Normalization** (4 macros in `identity_macros.sql`):
- Email: lowercase, strip plus-addressing, Gmail dot trick, googlemail alias
- Phone: strip non-digits
- Name: lowercase + trim
- Nickname: 48 mappings (bob->robert, bill->william, etc.)

**Blocking**: Five keys per record - email, phone, first+last name, email username+last name, last name+company. Only pairs sharing a key become candidates.

**Scoring**: Weighted sum of field comparisons. Email exact match = 5.0, email username match = 3.0, phone = 4.0, name/company = Jaro-Winkler similarity scaled by weight. Threshold = 4.0.

**Clustering**: Iterative label propagation (6 passes) finds connected components by propagating minimum cluster IDs across match edges.

**Survivorship**: `first_value()` window functions with source priority ordering (CRM > Billing > App > Support > Partners, with field-level overrides for phone and company).
