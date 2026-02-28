<p align="center">
  <h1 align="center">Kanoniv</h1>
  <p align="center"><strong>Identity resolution as code</strong></p>
  <p align="center">Match, merge, and master entity data across systems - defined in YAML, powered by Rust.</p>
</p>

<p align="center">
  <a href="https://pypi.org/project/kanoniv/"><img src="https://img.shields.io/pypi/v/kanoniv" alt="PyPI"></a>
  <a href="https://www.npmjs.com/package/@kanoniv/mcp"><img src="https://img.shields.io/npm/v/@kanoniv/mcp" alt="npm"></a>
  <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License"></a>
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.9%2B-blue" alt="Python 3.9+"></a>
  <a href="https://kanoniv.com/docs/getting-started/"><img src="https://img.shields.io/badge/docs-kanoniv.com-gold" alt="Docs"></a>
</p>

---

Kanoniv matches records across data sources - CRM, billing, support, marketing - to identify which records refer to the same real-world entity. It produces **golden records** with the best values from each source.

**Three ways to use Kanoniv:**

| | Python SDK | MCP Server | REST API |
|---|---|---|---|
| **Install** | `pip install kanoniv` | `npx @kanoniv/mcp` | `api.kanoniv.com` |
| **Best for** | Batch reconciliation, CI/CD | AI assistants (Claude, Cursor) | Production pipelines |
| **Runs** | Locally (Rust engine via PyO3) | Locally (native binary) | Cloud |

## Python SDK

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

### Install

```bash
pip install kanoniv
```

Pre-built wheels for Linux x86_64, macOS (Intel + Apple Silicon), and Windows. No Rust toolchain required.

### CLI

The SDK includes a full CLI for both offline and cloud workflows:

```bash
# Offline (no account needed)
kanoniv validate identity.yaml     # Validate a spec
kanoniv plan identity.yaml         # Preview execution plan
kanoniv diff v1.yaml v2.yaml       # Compare spec versions
kanoniv hash identity.yaml         # Deterministic plan hash

# Cloud (requires API key)
kanoniv login                      # Authenticate
kanoniv ingest ./data/             # Upload data
kanoniv reconcile --wait           # Run reconciliation
kanoniv stats                      # View entity counts
kanoniv ask "who shares a company?"  # Natural language queries
```

## MCP Server

Connect your AI assistant to Kanoniv's identity resolution platform via the [Model Context Protocol](https://modelcontextprotocol.io). Works with Claude Desktop, Cursor, Windsurf, and any MCP-compatible client.

```json
{
  "mcpServers": {
    "kanoniv": {
      "command": "npx",
      "args": ["-y", "@kanoniv/mcp"],
      "env": {
        "KANONIV_API_KEY": "kn_..."
      }
    }
  }
}
```

Get your API key from your [Kanoniv dashboard](https://app.kanoniv.com) under **Settings > API Keys**.

**13 category tools** with action-based dispatch:

| Tool | Actions |
|------|---------|
| `manage_specs` | create |
| `manage_sources` | create, update, delete, sync |
| `manage_ingest` | batch, create_mapping |
| `manage_jobs` | run, cancel, dry_run, simulate |
| `manage_entities` | lock, revert, resolve_realtime, resolve_bulk, bulk_linked |
| `manage_matching` | quick_resolve, create/delete feedback & overrides |
| `manage_rules` | create |
| `manage_crm` | trigger_sync, merge, dismiss, split, autotune, update_settings |
| `manage_graph` | refresh |
| `manage_memory` | create, update, append, archive, link_entities |
| `manage_detect` | profile, bootstrap |
| `manage_agents` | list/get/update/delete configs, list/get/rollback runs, list/approve/reject actions |
| `analyze_entities` | group_by, filter, field_stats |

**70 resources** (lazy-loaded, zero context cost) for browsing specs, sources, jobs, entities, match explanations, CRM duplicates, graph intelligence, memory entries, and more.

See the [MCP Server docs](https://kanoniv.com/docs/ai/mcp-server) for configuration options and tool profiles.

## Core Concepts

Kanoniv has five core concepts. They form a pipeline: **Spec** -> **Validate** -> **Plan** -> **Reconcile** -> **Diff**.

### 1. Spec - Define your identity logic

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
| `blocking` | Reduce the comparison space - only records sharing a blocking key are compared |
| `rules` | Define how to compare records (exact, fuzzy, phonetic, composite, ML) |
| `decision` | Set thresholds - above `match` = auto-merge, above `review` = human review |
| `survivorship` | When records merge, decide which source's values win for the golden record |

### 2. Validate - Catch errors before you run

```python
from kanoniv import validate

result = validate(spec)
result.raise_on_error()  # Raises if invalid - safe to use in CI
```

Validation catches missing fields, invalid rule types, threshold values out of range, dangling source references, and more.

### 3. Plan - Preview the execution

```python
from kanoniv import plan

p = plan(spec)
print(p.summary())
```

Compiles the spec into an execution plan and surfaces risk flags - without touching data.

### 4. Reconcile - Run identity resolution

```python
from kanoniv import Source, reconcile

sources = [
    Source.from_csv("salesforce", "sf_contacts.csv", primary_key="sf_id"),
    Source.from_csv("stripe", "stripe_customers.csv", primary_key="cus_id"),
]

result = reconcile(sources, spec)

print(f"Golden records: {result.cluster_count}")
print(f"Merge rate: {result.merge_rate:.1%}")

# Export golden records
result.to_pandas().to_csv("golden_customers.csv", index=False)
```

Source adapters:

| Adapter | Usage |
|---------|-------|
| CSV | `Source.from_csv("name", "path.csv", primary_key="id")` |
| Pandas | `Source.from_pandas("name", dataframe, primary_key="id")` |
| Polars | `Source.from_polars("name", dataframe, primary_key="id")` |
| PyArrow | `Source.from_arrow("name", table, primary_key="id")` |
| DuckDB | `Source.from_duckdb("name", connection, "SELECT * FROM t", primary_key="id")` |
| SQL / Warehouse | `Source.from_warehouse("name", "table", connection_string="...")` |
| dbt | `Source.from_dbt("name", "model_name", manifest_path="target/manifest.json")` |

### 5. Diff - Compare spec versions

```python
from kanoniv import Spec, diff

v1 = Spec.from_file("spec-v1.yaml")
v2 = Spec.from_file("spec-v2.yaml")

d = diff(v1, v2)
print(d.summary)
# version: 1.0 -> 2.0; sources: +1 -0 ~0; rules: +1 -0 ~0; thresholds changed
```

## Why Kanoniv

- **Golden records out of the box.** Most ER tools stop at matching. Kanoniv includes survivorship - the logic that decides which values survive into the canonical record.
- **Declarative YAML spec.** Your matching logic lives in a version-controlled file, not scattered across code. Review it in a PR, test it in CI, deploy it to production.
- **Offline-first.** The Python SDK runs entirely on your machine. No API keys, no accounts, no data leaves your environment.
- **Fast.** The reconciliation engine is written in Rust and compiled to native Python extensions via PyO3. 100K records in seconds on a laptop.
- **AI-native.** The MCP server gives AI assistants full access to your identity graph - specs, jobs, entities, duplicates, analytics - through natural language.

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

All rules support both deterministic (rules-based) and probabilistic (Fellegi-Sunter with EM training) matching.

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
| Probabilistic matching | Yes (Fellegi-Sunter + EM) | Yes | Yes (Spark ML) | Yes | Yes |
| Declarative config | YAML | Python | JSON | Python | CloudFormation |
| MCP server | Yes | No | No | No | No |
| CLI | Yes | No | No | No | No |
| Local-first | Yes | Yes | Yes (Spark) | Yes | No |
| Real-time API | Yes (Cloud) | No | No | No | Near real-time |
| Language | Rust + Python | Python + Spark | Spark | Python | Proprietary |
| License | Apache-2.0 | MIT | AGPL | MIT | Proprietary |

## Kanoniv Cloud

The local SDK is great for batch reconciliation. When you need real-time resolution on every API call, a persistent identity graph that grows over time, or team-based review workflows - that's what Cloud adds.

- **Real-time resolution API** - resolve an identity in under 1ms, on every ingest or API call
- **Persistent identity graph** - golden records that evolve as new data arrives, queryable via REST
- **Incremental updates** - new records match against existing entities without re-running the full pipeline
- **MCP server** - give AI assistants full access to your identity graph
- **Dashboard** - match quality metrics, reconciliation history, review queues
- **Enterprise** - SSO, SCIM, HIPAA compliance, audit logs, BYOK encryption

```bash
pip install kanoniv[cloud]
```

```python
from kanoniv import Client

client = Client(base_url="https://api.kanoniv.com", api_key="kn_...")
```

[Get started](https://kanoniv.com/docs/getting-started/) | [API reference](https://kanoniv.com/docs/api-reference/) | [Pricing](https://kanoniv.com/pricing/)

## Documentation

- [Getting Started](https://kanoniv.com/docs/getting-started/)
- [Spec Reference](https://kanoniv.com/docs/spec-reference/)
- [Python SDK Guide](https://kanoniv.com/docs/sdks/python-sdk)
- [MCP Server](https://kanoniv.com/docs/ai/mcp-server)
- [CLI Reference](https://kanoniv.com/docs/sdks/cli)
- [API Reference](https://kanoniv.com/docs/api-reference/)
- [Tutorials](https://kanoniv.com/docs/tutorials/)

## Use Cases

- **[Customer 360](https://kanoniv.com/docs/use-cases/customer-360)** - Unify customer records across CRM, billing, and support
- **[Payment Deduplication](https://kanoniv.com/docs/use-cases/payment-dedup)** - Detect duplicate invoices and payments
- **[Lead Matching](https://kanoniv.com/docs/use-cases/lead-matching)** - Match inbound leads to existing accounts
- **[Patient Matching](https://kanoniv.com/docs/use-cases/patient-matching)** - HIPAA-compliant patient record linking

## Try It

Run the bundled example - 6,500 records across 5 source systems, resolved in under a second:

```bash
pip install kanoniv pandas
cd examples/customer-identity-resolution/kanoniv/
python -c "
from kanoniv import Spec, Source, reconcile, validate

spec = Spec.from_file('kanoniv.yml')
validate(spec).raise_on_error()

sources = [
    Source.from_csv('crm_contacts', '../data/crm_contacts.csv', primary_key='crm_contact_id'),
    Source.from_csv('billing_accounts', '../data/billing_accounts.csv', primary_key='billing_account_id'),
    Source.from_csv('support_users', '../data/support_users.csv', primary_key='support_user_id'),
    Source.from_csv('app_signups', '../data/app_signups.csv', primary_key='app_user_id'),
    Source.from_csv('partner_leads', '../data/partner_leads.csv', primary_key='partner_lead_id'),
]
result = reconcile(sources, spec)
print(f'{result.cluster_count} golden records from 6,539 input records')
print(f'Merge rate: {result.merge_rate:.1%}')
result.to_pandas().to_csv('golden_customers.csv', index=False)
print('Exported to golden_customers.csv')
"
```

The [`examples/`](examples/) directory also includes the same problem solved with [dbt/SQL](examples/customer-identity-resolution/dbt-sql/) and [Splink](examples/customer-identity-resolution/splink/) for comparison. See the [full walkthrough notebook](examples/customer-identity-resolution/kanoniv/customer_entity.ipynb) for data exploration, quality analysis, and business enrichment.

For **product entity resolution** across heterogeneous retail feeds, see the [product reconciliation example](examples/product-reconciliation/) - covers iterative spec refinement (rules-based -> Fellegi-Sunter), three-layer evaluation with P/R/F1, and entity-level diffing with ChangeLog.

## What's in This Repo

| Directory | Description |
|---|---|
| [`python/`](python/) | Python SDK source - native extension built with [PyO3](https://pyo3.rs) + [maturin](https://www.maturin.rs) |
| [`crates/validator`](crates/validator/) | Rust CLI + library for validating, compiling, planning, and diffing identity specs |
| [`examples/`](examples/) | End-to-end examples: [customer dedup](examples/customer-identity-resolution/) (6,500 records, 5 sources) and [product reconciliation](examples/product-reconciliation/) (1,000 records, 4 sources) |

The reconciliation engine (matching, blocking, scoring, clustering, survivorship) is compiled into the published PyPI wheels. Install with `pip install kanoniv` - no Rust toolchain required.

## License

Apache-2.0 - see [LICENSE](LICENSE) for details.

---

<p align="center">
  <a href="https://kanoniv.com">Website</a> &middot;
  <a href="https://kanoniv.com/docs/getting-started/">Docs</a> &middot;
  <a href="https://www.npmjs.com/package/@kanoniv/mcp">MCP Server</a> &middot;
  <a href="https://pypi.org/project/kanoniv/">PyPI</a> &middot;
  <a href="https://kanoniv.com/pricing/">Pricing</a>
</p>
