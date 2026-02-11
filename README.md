<p align="center">
  <h1 align="center">Kanoniv</h1>
  <p align="center"><strong>Identity resolution as code</strong></p>
  <p align="center">Match, merge, and master entity data across systems — defined in YAML, powered by Rust.</p>
</p>

<p align="center">
  <a href="https://pypi.org/project/kanoniv/"><img src="https://img.shields.io/pypi/v/kanoniv" alt="PyPI"></a>
  <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License"></a>
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.9%2B-blue" alt="Python 3.9+"></a>
  <a href="https://kanoniv.com/docs/getting-started/"><img src="https://img.shields.io/badge/docs-kanoniv.com-gold" alt="Docs"></a>
</p>

---

Kanoniv matches records across data sources — CRM, billing, support, marketing — to identify which records refer to the same real-world entity. It produces **golden records** with the best values from each source.

```python
from kanoniv import Spec, Source, reconcile, validate

spec = Spec.from_file("customer-spec.yaml")
validate(spec).raise_on_error()

sources = [
    Source.from_csv("crm", "contacts.csv"),
    Source.from_csv("billing", "stripe.csv"),
]
result = reconcile(sources, spec)

print(f"Golden records: {len(result.golden_records)}")
print(f"Merge rate: {result.merge_rate:.1%}")
```

## Why Kanoniv

- **Golden records out of the box.** Most ER tools stop at matching. Kanoniv includes survivorship — the logic that decides which values survive into the canonical record.
- **Declarative YAML spec.** Your matching logic lives in a version-controlled file, not scattered across code. Review it in a PR, test it in CI, deploy it to production.
- **Offline-first.** The Python SDK runs entirely on your machine. No API keys, no accounts, no data leaves your environment.
- **Fast.** The reconciliation engine is written in Rust and compiled to native Python extensions via PyO3. 100K records in seconds on a laptop.

## Install

```bash
pip install kanoniv
```

Pre-built wheels available for Linux x86_64, macOS Intel, and macOS Apple Silicon.

## Quick Start

### 1. Define a spec

```yaml
# customer-spec.yaml
api_version: kanoniv/v2
identity_version: "1.0"
entity:
  name: customer
sources:
  - name: crm
    adapter: csv
    location: contacts.csv
    primary_key: id
    attributes:
      email: email
      name: full_name
      phone: phone
  - name: billing
    adapter: csv
    location: stripe.csv
    primary_key: id
    attributes:
      email: email
      name: name
      phone: phone
blocking:
  keys:
    - [email]
    - [phone]
rules:
  - name: email_exact
    type: exact
    field: email
    weight: 1.0
  - name: name_fuzzy
    type: similarity
    field: name
    algorithm: jaro_winkler
    threshold: 0.85
    weight: 0.8
  - name: phone_match
    type: similarity
    field: phone
    algorithm: levenshtein
    threshold: 0.8
    weight: 0.6
decision:
  thresholds:
    match: 0.7
    review: 0.4
survivorship:
  strategy: source_priority
  source_order: [crm, billing]
```

### 2. Validate and reconcile

```python
from kanoniv import Spec, Source, reconcile, validate, plan

spec = Spec.from_file("customer-spec.yaml")

# Validate — catches errors before you run
result = validate(spec)
result.raise_on_error()

# Plan — see what the engine will do
p = plan(spec)
print(p.summary())

# Reconcile — runs locally via Rust engine
sources = [
    Source.from_csv("crm", "contacts.csv"),
    Source.from_csv("billing", "stripe.csv"),
]
result = reconcile(sources, spec)

# Results
print(f"Clusters: {result.cluster_count}")
print(f"Golden records: {len(result.golden_records)}")
print(f"Merge rate: {result.merge_rate:.1%}")

# Export to Pandas
golden_df = result.to_pandas()
golden_df.to_csv("golden_customers.csv", index=False)
```

### 3. Compare spec versions

```python
from kanoniv import Spec, diff

v1 = Spec.from_file("spec-v1.yaml")
v2 = Spec.from_file("spec-v2.yaml")

d = diff(v1, v2)
print(d.summary)
# version: 1.0 -> 2.0; sources: +1 -0 ~0; rules: +1 -0 ~0; thresholds changed
```

## Features

### Matching Rules

| Rule Type | Algorithm | Best For |
|-----------|-----------|----------|
| `exact` | String equality | Identifiers (email, SSN) |
| `similarity/jaro_winkler` | Character similarity + prefix bonus | Names |
| `similarity/levenshtein` | Edit distance | Addresses, short strings |
| `similarity/soundex` | Phonetic encoding | Name spelling variations |
| `similarity/metaphone` | Double metaphone | Name pronunciation |
| `similarity/cosine` | Token overlap | Long text, descriptions |
| `similarity/haversine` | Geographic distance | Coordinates |
| `range` | Numeric tolerance | Dates, amounts |
| `composite` | AND/OR rule groups | Multi-field matching |
| `ml` | ML model scoring | Custom models |

### Survivorship Strategies

| Strategy | Logic |
|----------|-------|
| `source_priority` | Prefer values from higher-priority sources |
| `most_recent` | Use the most recently updated value |
| `most_complete` | Use the longest / non-null value |
| `aggregate` | Combine values across sources |
| `custom` | User-defined logic per field |

### Source Adapters

| Adapter | Method |
|---------|--------|
| CSV | `Source.from_csv(name, path)` |
| Pandas | `Source.from_pandas(name, dataframe)` |
| SQL / Warehouse | `Source.from_warehouse(name, connection, query)` |
| dbt | `Source.from_dbt(name, project_dir, model)` |

### Spec Validation

The SDK validates specs before execution, catching configuration errors early:

```python
result = validate(spec)
# Checks: required fields, valid rule types, threshold ranges,
#          source references, blocking key consistency, and more
```

### Execution Plans

Inspect what the engine will do before running:

```python
p = plan(spec)
print(p.summary())
print(f"Risk flags: {len(p.risk_flags)}")
for flag in p.risk_flags:
    print(f"  [{flag['severity']}] {flag['message']}")
```

### Spec Diffing

Track changes between spec versions for safe deployments:

```python
d = diff(spec_v1, spec_v2)
print(d.sources_added)      # ['hubspot']
print(d.rules_added)        # ['name_metaphone']
print(d.thresholds_changed) # True
```

## Architecture

```
Python SDK (kanoniv)
  |  <-- pip install kanoniv
  v
PyO3 FFI bridge
  |  <-- Python -> Rust (zero-copy where possible)
  v
Rust reconciliation engine
  |  <-- Normalize -> Block -> Compare -> Score -> Cluster -> Survive
  v
Golden records (Pandas DataFrame)
```

The Rust engine handles all CPU-intensive work: string normalization, blocking, pairwise comparison, scoring, clustering, and survivorship. The Python SDK is a thin wrapper for I/O and data conversion.

## How It Compares

| Feature | Kanoniv | Splink | Zingg | Dedupe | AWS ER |
|---------|---------|--------|-------|--------|--------|
| Golden records | Yes | No | Enterprise | No | No |
| Survivorship | Yes | No | Enterprise | No | No |
| Declarative config | YAML | Python | JSON | Python | CloudFormation |
| Local-first | Yes | Yes | Yes (Spark) | Yes | No |
| Real-time API | Yes (Cloud) | No | No | No | Near real-time |
| Language | Rust + Python | Python + Spark | Spark | Python | Proprietary |
| License | Apache-2.0 | MIT | AGPL | MIT | Proprietary |

## Kanoniv Cloud

Self-serve cloud platform with additional capabilities:

- **Real-time resolution API** — sub-millisecond entity lookup
- **Persistent identity graph** — queryable via REST API
- **Dashboard** — match quality metrics, reconciliation history
- **Webhook notifications** — trigger workflows on identity events
- **Enterprise** — SSO, SCIM, HIPAA compliance, audit logs, BYOK encryption

```python
pip install kanoniv[cloud]
```

```python
from kanoniv import Client

client = Client(base_url="https://api.kanoniv.com", api_key="kn_...")
```

[Get started](https://kanoniv.com/docs/getting-started/) | [API reference](https://kanoniv.com/docs/api-reference/) | [Pricing](https://kanoniv.com/docs/pricing/)

## Documentation

- [Getting Started](https://kanoniv.com/docs/getting-started/)
- [Spec Reference](https://kanoniv.com/docs/spec-reference/)
- [Python SDK Guide](https://kanoniv.com/docs/sdks/python-sdk)
- [API Reference](https://kanoniv.com/docs/api-reference/)
- [Tutorials](https://kanoniv.com/docs/tutorials/)
- [Blog](https://kanoniv.com/docs/blog/)

## Use Cases

- **[Customer 360](https://kanoniv.com/docs/use-cases/customer-360)** — Unify customer records across CRM, billing, and support
- **[Payment Deduplication](https://kanoniv.com/docs/use-cases/payment-dedup)** — Detect duplicate invoices and payments
- **[Lead Matching](https://kanoniv.com/docs/use-cases/lead-matching)** — Match inbound leads to existing accounts
- **[Patient Matching](https://kanoniv.com/docs/use-cases/patient-matching)** — HIPAA-compliant patient record linking

## What's in This Repo

| Directory | Description |
|---|---|
| [`crates/validator`](crates/validator/) | Rust CLI + library for validating, compiling, planning, and diffing identity specs |
| [`python/`](python/) | Python SDK source — native extension built with [PyO3](https://pyo3.rs) + [maturin](https://www.maturin.rs) |

The reconciliation engine (matching, blocking, scoring, clustering, survivorship) is compiled into the published PyPI wheels. Install with `pip install kanoniv` — no Rust toolchain required.

## License

Apache-2.0 — see [LICENSE](LICENSE) for details.

---

<p align="center">
  <a href="https://kanoniv.com">Website</a> &middot;
  <a href="https://kanoniv.com/docs/getting-started/">Docs</a> &middot;
  <a href="https://kanoniv.com/docs/blog/">Blog</a> &middot;
  <a href="https://pypi.org/project/kanoniv/">PyPI</a> &middot;
  <a href="https://kanoniv.com/docs/pricing/">Pricing</a>
</p>
