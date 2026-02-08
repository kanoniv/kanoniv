# Kanoniv

Identity resolution as code — open-source engine for matching, merging, and mastering entity data across systems.

## What's in this repo

| Directory | Description |
|---|---|
| [`crates/validator`](crates/validator/) | Rust CLI + library for validating, compiling, planning, and diffing Kanoniv identity specs |
| [`python`](python/) | Python SDK (`pip install kanoniv`) — local-first spec validation and planning with Rust-powered native extension |

## Quick start

### CLI

```bash
cargo install --path crates/validator

kanoniv validate identity.yaml
kanoniv plan identity.yaml
kanoniv compile identity.yaml
kanoniv diff v1.yaml v2.yaml
kanoniv hash identity.yaml
```

### Python

```bash
pip install kanoniv
```

```python
from kanoniv import Spec, validate, plan

spec = Spec.from_file("identity.yaml")
result = validate(spec)
if result:
    print(plan(spec).summary())
```

For the API client (optional):

```bash
pip install kanoniv[cloud]
```

```python
from kanoniv import Client

client = Client(base_url="https://api.kanoniv.com", api_key="kn_...")
```

## Related repos

- [kanoniv/spec](https://github.com/kanoniv/spec) — The open standard for declarative identity resolution
- [kanoniv/dbt-kanoniv](https://github.com/kanoniv/dbt-kanoniv) — dbt package for Kanoniv identity data modeling

## License

Apache-2.0
