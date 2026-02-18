# Customer Identity Resolution: Three Approaches Compared

Resolve 6,500 customer records across 5 source systems (CRM, Billing, Support, App, Partner) into unified golden records.

Same data. Same problem. Three different approaches.

## The Problem

Your customers exist in multiple systems, each with different IDs, name formats, and data quality. "Bob Smith" in CRM is "Robert Smith" in Billing is "bob.smith+work@gmail.com" in App signups. You need a single customer view.

## The Approaches

| | [dbt-sql/](./dbt-sql/) | [splink/](./splink/) | [kanoniv/](./kanoniv/) |
|---|---|---|---|
| **What** | Pure SQL in dbt | Splink + DuckDB | Kanoniv engine |
| **Files** | 5 SQL models + 1 macro file | 1 Python script | 1 YAML spec + notebook |
| **Lines of code** | 350 | 440 | 170 (spec only) |
| **Matching** | Hand-tuned weights | Fellegi-Sunter + EM | Fellegi-Sunter + EM |
| **String matching** | Jaro-Winkler | Jaro-Winkler (multi-threshold) | Jaro-Winkler |
| **Normalization** | 4 macros (email, phone, name, nickname) | Python functions (100 lines) | Declarative (email, phone, nickname, name) |
| **Transitive closure** | Iterative label propagation (6 passes) | Graph algorithm | Graph algorithm |
| **Survivorship** | Window functions (80 lines) | Python groupby (30 lines) | Declarative YAML (10 lines) |
| **Infrastructure** | dbt + DuckDB | Python + DuckDB | Python (Rust engine via PyO3) |
| **Adding a source** | Edit spine union + survivorship | Edit load function | Add `sources:` entry |

## Results

All three approaches solve the same problem on the same data:

| Metric | dbt-sql | Splink | Kanoniv |
|--------|---------|--------|---------|
| Input records | 6,539 | 6,539 | 6,539 |
| Golden records | 3,583 | 2,233 | 2,168 |
| Merge rate | 45% | 65.9% | 66.8% |
| Compression | 1.8x | 2.9x | 3.0x |
| Runtime | <1s (DuckDB) | 2.6s | 0.4s |

## Shared Data

All approaches use the same 10 CSV files in [`data/`](./data/):

| File | Records | Description |
|------|---------|-------------|
| `crm_contacts.csv` | 1,839 | CRM contacts with name, email, phone, company |
| `crm_companies.csv` | 107 | Company records |
| `billing_accounts.csv` | 1,200 | Billing accounts ("Last, First" name format) |
| `billing_invoices.csv` | 2,974 | Invoice records |
| `support_users.csv` | 1,400 | Support users (display_name format) |
| `support_tickets.csv` | 2,861 | Support tickets |
| `app_signups.csv` | 1,300 | App signups (email + optional name) |
| `app_events.csv` | 6,397 | App events |
| `partner_leads.csv` | 800 | Partner leads |
| `partner_referrals.csv` | 484 | Partner referrals |

5 source systems contribute to entity resolution: CRM, Billing, Support, App, and Partners. The remaining files (companies, invoices, tickets, events, referrals) are used for enrichment after resolution.

## Getting Started

Pick any approach and follow its README:

- **[dbt-sql/](./dbt-sql/)** - See what hand-rolled identity resolution in SQL looks like
- **[splink/](./splink/)** - Try the most popular open-source record linkage library
- **[kanoniv/](./kanoniv/)** - See what declarative identity resolution looks like

## Links

- [Kanoniv Documentation](https://kanoniv.com/docs)
- [Python SDK on PyPI](https://pypi.org/project/kanoniv/)
- [Splink Documentation](https://moj-analytical-services.github.io/splink/)
