# Product Entity Resolution

Reconcile 1,000 product records across 4 heterogeneous retail feeds into a unified product catalog.

## The Problem

The same product appears in your ecommerce store, wholesale supplier feed, marketplace listings, and retail inventory - each with different identifiers, naming conventions, and prices. You need one canonical product catalog.

| Source | Records | Identifiers | What's Missing |
|--------|---------|-------------|---------------|
| `ecommerce_catalog.csv` | 300 | UPC (12-digit barcode), SKU | No MPN |
| `wholesale_feed.csv` | 250 | GTIN-13 ("0" + UPC), MPN | Different barcode format |
| `marketplace_listings.csv` | 200 | ASIN | No barcode, no MPN |
| `retail_inventory.csv` | 250 | manufacturer_code (= MPN) | No barcode |

### Key challenges

- **UPC vs GTIN**: `030000000007` (12 digits) vs `0030000000007` (13 digits) - same product, different format
- **No shared identifier** between marketplace and retail - must match on product name + brand only
- **Name variations**: `"Samsung Galaxy Buds FE - New"` vs `"Samsung Galaxy Buds FE"`
- **Price divergence**: wholesale cost < retail price < marketplace list price

## The Notebook

[`product_reconciliation.ipynb`](./product_reconciliation.ipynb) walks through the full Kanoniv local SDK, start to finish:

| Section | What It Covers |
|---------|---------------|
| 1. Data exploration | Load CSVs, show schemas, demonstrate the UPC/GTIN mismatch |
| 2. Spec v1 | Rules-based `weighted_sum` scoring with 4 rules |
| 3. First reconciliation | Full `ReconcileResult` API - clusters, golden records, decisions, telemetry, entity lookup |
| 4. Evaluation (L1+L2) | Structural + stability metrics (no labels needed) |
| 5. Ground truth (L3) | Build ground truth from known linkages, compute pairwise P/R/F1 |
| 6. Spec v2 | Fellegi-Sunter probabilistic matching with `diff()` |
| 7. ChangeLog | Entity-level diff between v1 and v2 results |
| 8. Comparison | Side-by-side P/R/F1 table, `save()` / `load()` |

## Results

| Metric | V1 (weighted_sum) | V2 (fellegi_sunter) |
|--------|-------------------|---------------------|
| Clusters | 709 | 703 |
| Merge rate | 29.1% | 29.7% |
| **Precision** | 0.7004 | **0.7652** |
| **Recall** | 0.8386 | **0.9058** |
| **F1** | 0.7633 | **0.8296** |
| Runtime | <0.5s | <0.5s |

Fellegi-Sunter improves both precision and recall. Its null-aware log-likelihood scoring handles partial evidence (missing barcode, missing MPN) without penalty, while the weighted sum approach implicitly penalizes missing fields.

## Running

```bash
pip install kanoniv pandas jupyter
cd examples/product-reconciliation
jupyter notebook product_reconciliation.ipynb
```

Or use the SDK directly:

```python
from kanoniv import Spec, Source, reconcile

spec = Spec.from_string(open("spec.yml").read())
sources = [
    Source.from_csv("ecommerce", "data/ecommerce_catalog.csv", primary_key="product_id"),
    Source.from_csv("wholesale", "data/wholesale_feed.csv", primary_key="item_id"),
    Source.from_csv("marketplace", "data/marketplace_listings.csv", primary_key="listing_id"),
    Source.from_csv("retail", "data/retail_inventory.csv", primary_key="inventory_id"),
]
result = reconcile(sources, spec)
print(f"{result.cluster_count} products, {result.merge_rate:.0%} merge rate")
```

## API Coverage

Every `ReconcileResult` method and property is exercised in the notebook:

```
Source.from_csv       Spec.from_string      validate        plan          diff
reconcile             result.clusters       result.to_pandas()            result.decisions
result.telemetry      result.entity_lookup  result.cluster_count          result.merge_rate
result.evaluate()     result.evaluate(ground_truth=)                      result.changes_since()
result.save()         ReconcileResult.load()
EvaluateResult        ChangeLog             DiffResult
```

## Links

- [Kanoniv Documentation](https://kanoniv.com/docs)
- [Python SDK on PyPI](https://pypi.org/project/kanoniv/)
- [Spec Reference](https://kanoniv.com/docs/spec-reference/)
