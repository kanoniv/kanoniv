# Kanoniv Validator

> Rust CLI + library for validating, compiling, planning, and diffing Kanoniv identity specifications.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](../../LICENSE)
[![Crates.io](https://img.shields.io/crates/v/kanoniv.svg)](https://crates.io/crates/kanoniv)

Part of the [Kanoniv](https://github.com/kanoniv/kanoniv) monorepo.

---

## Installation

### From Cargo

```bash
cargo install kanoniv
```

### From Source

```bash
git clone https://github.com/kanoniv/kanoniv.git
cd kanoniv/crates/validator
cargo build --release
```

---

## Usage

### Validate a Spec

```bash
kanoniv validate identity.yaml
```

Output:
```
 Schema valid
 Semantic checks passed
 identity.yaml is valid
```

Errors:
```
 Rule 'email_exact' references unknown field 'email_address'
  -> Did you mean 'email'?

  at identity.yaml:42:7
```

### Compile to IR

```bash
kanoniv compile identity.yaml -o plan.json
```

Produces a JSON intermediate representation (IR) with:
- Resolved sources
- Computed plan hash
- Normalized rule graph

### Compute Plan Hash

```bash
kanoniv hash identity.yaml
```

Output:
```
sha256:a1b2c3d4e5f6...
```

### Diff Two Versions

```bash
kanoniv diff v1.yaml v2.yaml
```

Output:
```diff
 rules:
-  - name: email_exact
-    weight: 0.8
+  - name: email_exact
+    weight: 0.9

Warning: Threshold change may affect match rates
```

---

## CI Integration

### GitHub Actions

```yaml
- name: Validate Identity Specs
  run: |
    kanoniv validate specs/*.yaml
```

### Pre-commit Hook

```bash
#!/bin/bash
for file in $(git diff --cached --name-only | grep '\.yaml$'); do
  kanoniv validate "$file" || exit 1
done
```

---

## Related Projects

| Repo | Description |
|------|-------------|
| [kanoniv/kanoniv](https://github.com/kanoniv/kanoniv) | Monorepo: SDK, validator, examples |
| [kanoniv/dbt-kanoniv](https://github.com/kanoniv/dbt-kanoniv) | dbt package |

---

## License

Apache License 2.0 - see [LICENSE](../../LICENSE)
