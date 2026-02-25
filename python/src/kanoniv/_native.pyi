"""Type stubs for the Rust native extension (kanoniv._native)."""

def validate(yaml_str: str) -> list[str]:
    """Validate a YAML spec - returns list of errors (empty = valid)."""
    ...

def validate_schema(yaml_str: str) -> list[str]:
    """Validate schema structure only."""
    ...

def validate_semantics(yaml_str: str) -> list[str]:
    """Validate semantic/business rules only."""
    ...

def parse(yaml_str: str) -> dict:
    """Parse a YAML spec string into a dict."""
    ...

def compile_ir(yaml_str: str) -> dict:
    """Compile a YAML spec to intermediate representation with plan_hash."""
    ...

def diff(yaml_a: str, yaml_b: str) -> dict:
    """Diff two YAML specs - returns rules added/removed/modified, thresholds changed."""
    ...

def hash(yaml_str: str) -> str:
    """Compute SHA-256 hash of a spec (sha256:...)."""
    ...

def plan(yaml_str: str) -> dict:
    """Generate a full execution plan with stages, strategies, risk flags, and summary."""
    ...
