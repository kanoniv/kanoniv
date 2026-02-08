# kanoniv

Python client for the [Kanoniv](https://kanoniv.com) identity resolution API.

## Installation

```bash
pip install kanoniv
```

## Quick Start

```python
import kanoniv

client = kanoniv.Client(
    api_key="kn_abc123",
    base_url="https://api.kanoniv.com",
)

# Resolve an identity
result = client.resolve(system="salesforce", external_id="003xxx")
print(result["canonical_data"])

# Search entities
results = client.entities.search(q="john@acme.com")

# Ingest records
client.ingest("source-uuid", records=[
    {"id": "ext_1", "type": "contact", "name": "John", "email": "john@acme.com"},
])

# Dashboard stats
stats = client.stats()
print(f"{stats['total_canonical_entities']} canonical entities")
```

## Async Usage

```python
async with kanoniv.AsyncClient(api_key="kn_...") as client:
    result = await client.resolve(system="crm", external_id="sf_123")
    entities = await client.entities.search(q="jane")
```

## Authentication

```python
# API key (recommended for programmatic use)
client = kanoniv.Client(api_key="kn_abc123")

# JWT bearer token
client = kanoniv.Client(access_token="eyJ...")
```

## Resources

| Resource | Methods |
|----------|---------|
| `client.entities` | `search()`, `get()`, `get_linked()`, `history()` |
| `client.sources` | `list()`, `get()`, `create()`, `update()`, `delete()`, `sync()`, `preview()` |
| `client.rules` | `list()`, `create()`, `history()` |
| `client.jobs` | `list()`, `get()`, `run()`, `cancel()` |
| `client.reviews` | `list()`, `decide()` |
| `client.overrides` | `list()`, `create()`, `delete()` |
| `client.audit` | `list()`, `entity_trail()` |
| `client.specs` | `list()`, `get()`, `ingest()` |

## Error Handling

```python
from kanoniv import NotFoundError, RateLimitError

try:
    entity = client.entities.get("nonexistent")
except NotFoundError:
    print("Entity not found")
except RateLimitError as e:
    print(f"Rate limited, retry after {e.retry_after}s")
```

## License

Apache-2.0
