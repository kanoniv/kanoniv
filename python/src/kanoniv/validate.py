"""Spec validation — thin wrapper over Rust validator."""
from kanoniv._native import validate as _validate
from kanoniv.spec import Spec

class ValidationResult:
    def __init__(self, errors: list[str]):
        self.errors = errors
        self.valid = len(errors) == 0

    def raise_on_error(self) -> None:
        if not self.valid:
            raise ValueError(f"Spec validation failed:\n" + "\n".join(self.errors))

    def __bool__(self) -> bool:
        return self.valid

    def __repr__(self) -> str:
        if self.valid:
            return "<ValidationResult: Valid>"
        return f"<ValidationResult: {len(self.errors)} errors>"

def validate(spec: Spec) -> ValidationResult:
    errors = _validate(spec.raw)
    return ValidationResult(errors)
