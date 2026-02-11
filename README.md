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

## Install

```bash
pip install kanoniv
```

Pre-built wheels for Linux x86_64, macOS Intel, and macOS Apple Silicon. No Rust toolchain required.

## Core Concepts

Kanoniv has five core concepts. They form a pipeline: **Spec** &rarr; **Validate** &rarr; **Plan** &rarr; **Reconcile** &rarr; **Diff**.

### 1. Spec — Define your identity logic

A **Spec** is a YAML file that declares everything about how records should be matched. It replaces hundreds of lines of ad-hoc matching code with a single, version-controlled configuration.

```yaml
# customer-spec.yaml
api_version: kanoniv/v2
identity_version: "1.0"
entity:
  name: customer

sources:                              # Where your data lives
  - name: salesforce
    adapter: csv
    location: sf_contacts.csv
    primary_key: sf_id
    attributes:
      email: email
      first_name: first_name
      last_name: last_name
      phone: phone
  - name: stripe
    adapter: csv
    location: stripe_customers.csv
    primary_key: cus_id
    attributes:
      email: email
      name: name
      phone: phone

blocking:                             # Reduce comparison space
  keys:
    - [email]
    - [phone]

rules:                                # How to compare records
  - name: email_exact
    type: exact
    field: email
    weight: 1.0
  - name: name_fuzzy
    type: similarity
    field: first_name
    algorithm: jaro_winkler
    threshold: 0.85
    weight: 0.8
  - name: phone_match
    type: similarity
    field: phone
    algorithm: levenshtein
    threshold: 0.8
    weight: 0.6

decision:                             # When to merge vs. review
  thresholds:
    match: 0.7
    review: 0.4

survivorship:                         # Which values win
  strategy: source_priority
  source_order: [salesforce, stripe]
```

A spec has six sections:

| Section | Purpose |
|---------|---------|
| `sources` | Declare data sources and map their columns to common attributes |
| `blocking` | Reduce the comparison space — only records sharing a blocking key are compared |
| `rules` | Define how to compare records (exact, fuzzy, phonetic, composite, ML) |
| `decision` | Set thresholds — above `match` = auto-merge, above `review` = human review |
| `survivorship` | When records merge, decide which source's values win for the golden record |

```python
from kanoniv import Spec

spec = Spec.from_file("customer-spec.yaml")

print(spec.entity)                    # {'name': 'customer'}
print([s['name'] for s in spec.sources])  # ['salesforce', 'stripe']
print([r['name'] for r in spec.rules])    # ['email_exact', 'name_fuzzy', 'phone_match']
```

### 2. Validate — Catch errors before you run

**Validate** checks your spec for structural errors, invalid references, and logical mistakes — before any data is touched.

```python
from kanoniv import validate

result = validate(spec)
print(result)
# <ValidationResult: Valid>

result.raise_on_error()  # Raises if invalid — safe to use in CI
```

Validation catches:
- Missing required fields (`entity`, `sources`, `rules`)
- Invalid rule types or algorithms
- Threshold values out of range
- Source references that don't exist
- Blocking keys referencing unmapped attributes
- Survivorship strategies referencing unknown sources

```python
# Strict mode — also catches serde/schema issues
from kanoniv import validate_strict

errors = validate_strict(spec)
# ['KNV-E201: Invalid survivorship strategy ...']
```

### 3. Plan — Preview the execution

**Plan** compiles your spec into an execution plan and surfaces risk flags — without touching data. Use it to understand what the engine will do and catch potential quality issues.

```python
from kanoniv import plan

p = plan(spec)
print(p.summary())
```

```
  Identity:     customer (1.0)
  Sources:      2 (salesforce, stripe)
  Signals:      email (exact, w=1), first_name (similarity/jaro_winkler, w=0.8),
                phone (similarity/levenshtein, w=0.6)
  Blocking:     email, phone
  Thresholds:   merge >= 0.7, review >= 0.4
  Stages:       8 execution stages
  Survivorship: source_priority [salesforce, stripe]
  Risk flags:   0 critical, 1 high, 1 medium
  Plan hash:    sha256:a3f8c91d...
```

```python
# Inspect risk flags
for flag in p.risk_flags:
    print(f"  [{flag['severity']}] {flag['message']}")
    print(f"    -> {flag['recommendation']}")

# [high] Rule 'phone_match' has threshold 0.80 — risk of over-merging
#    -> Consider raising threshold to 0.85+ or adding verification rules
# [medium] No temporal configuration — identity resolution is not time-aware
#    -> Add temporal config if entities have time-dependent attributes
```

The plan hash is deterministic — same spec always produces the same hash. Use it to detect config drift in CI/CD.

### 4. Reconcile — Run identity resolution

**Reconcile** runs the full pipeline locally: load data, normalize, block, compare, score, cluster, and build golden records. The Rust engine handles all CPU-intensive work.

```python
from kanoniv import Source, reconcile

sources = [
    Source.from_csv("salesforce", "sf_contacts.csv", primary_key="sf_id"),
    Source.from_csv("stripe", "stripe_customers.csv", primary_key="cus_id"),
]

result = reconcile(sources, spec)
```

**What you get back:**

```python
# High-level metrics
print(f"Input records:  {200}")
print(f"Clusters:       {result.cluster_count}")      # 172 unique identities
print(f"Golden records: {len(result.golden_records)}") # 172
print(f"Merge rate:     {result.merge_rate:.1%}")      # 14.0%
print(f"Decisions:      {len(result.decisions)}")      # 174

# Clusters — which records belong together
for cluster in result.clusters[:3]:
    print(cluster)
# ['sf_003rSO42uKiv7Ry', 'cus_pQvhxKkZssTKu3']  <- matched pair
# ['sf_0037XsrSvBpDmH5']                           <- singleton
# ['cus_ZQDA2kiT80g4DJ', 'sf_003CLFuMk9TsuZH']   <- matched pair

# Decisions — why each pair was matched
for d in result.decisions[:2]:
    print(f"  {d['decision']} (confidence={d['confidence']:.3f})")
    print(f"  matched on: {d['matched_on']}")
# merge (confidence=0.783)
# matched on: ['email_exact', 'phone_match']

# Golden records — export to Pandas
golden_df = result.to_pandas()
golden_df.to_csv("golden_customers.csv", index=False)
```

Source adapters:

| Adapter | Usage |
|---------|-------|
| CSV | `Source.from_csv("name", "path.csv", primary_key="id")` |
| Pandas | `Source.from_pandas("name", dataframe, primary_key="id")` |
| SQL / Warehouse | `Source.from_warehouse("name", connection, query)` |
| dbt | `Source.from_dbt("name", project_dir, model)` |

### 5. Diff — Compare spec versions

**Diff** compares two spec versions to show exactly what changed — sources added/removed, rules modified, thresholds shifted. Use it to review changes before deploying a new spec version.

```python
from kanoniv import Spec, diff

v1 = Spec.from_file("spec-v1.yaml")
v2 = Spec.from_file("spec-v2.yaml")

d = diff(v1, v2)
print(d.summary)
# version: 1.0 -> 2.0; sources: +1 -0 ~0; rules: +1 -0 ~0; thresholds changed

print(d.sources_added)       # ['hubspot']
print(d.rules_added)         # ['name_metaphone']
print(d.thresholds_changed)  # True
print(d.has_changes)         # True
```

## End-to-End Example

Putting it all together — a complete script from spec to golden records:

```python
from kanoniv import Spec, Source, validate, plan, reconcile

# 1. Spec — load your identity logic
spec = Spec.from_file("customer-spec.yaml")

# 2. Validate — fail fast on bad config
validate(spec).raise_on_error()

# 3. Plan — check for risk flags
p = plan(spec)
print(p.summary())
assert len([f for f in p.risk_flags if f['severity'] == 'critical']) == 0

# 4. Reconcile — run the engine
sources = [
    Source.from_csv("salesforce", "sf_contacts.csv", primary_key="sf_id"),
    Source.from_csv("stripe", "stripe_customers.csv", primary_key="cus_id"),
]
result = reconcile(sources, spec)

print(f"Matched {result.cluster_count} identities from {200} records")
print(f"Merge rate: {result.merge_rate:.1%}")

# 5. Export golden records
result.to_pandas().to_csv("golden_customers.csv", index=False)
```

## Why Kanoniv

- **Golden records out of the box.** Most ER tools stop at matching. Kanoniv includes survivorship — the logic that decides which values survive into the canonical record.
- **Declarative YAML spec.** Your matching logic lives in a version-controlled file, not scattered across code. Review it in a PR, test it in CI, deploy it to production.
- **Offline-first.** The Python SDK runs entirely on your machine. No API keys, no accounts, no data leaves your environment.
- **Fast.** The reconciliation engine is written in Rust and compiled to native Python extensions via PyO3. 100K records in seconds on a laptop.
- **Full pipeline.** Validate &rarr; Plan &rarr; Reconcile &rarr; Diff. Each step catches problems earlier, before they reach production.

## Matching Rules

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

## Survivorship Strategies

| Strategy | Logic |
|----------|-------|
| `source_priority` | Prefer values from higher-priority sources |
| `most_recent` | Use the most recently updated value |
| `most_complete` | Use the longest / non-null value |
| `aggregate` | Combine values across sources |
| `custom` | User-defined logic per field |

## Architecture

```
                    customer-spec.yaml
                          |
                     Spec.from_file()
                          |
        +---------+-------+-------+---------+
        |         |               |         |
   validate()   plan()      reconcile()   diff()
        |         |               |         |
   errors/ok   risk flags    +---------+  changes
                             |  Rust   |
                             | Engine  |
                             +---------+
                             | Normalize
                             | Block
                             | Compare
                             | Score
                             | Cluster
                             | Survive
                             +---------+
                                  |
                          Golden Records
                        (Pandas DataFrame)
```

The Rust engine handles all CPU-intensive work. The Python SDK is a thin wrapper for I/O and data conversion. No data leaves your machine.

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
