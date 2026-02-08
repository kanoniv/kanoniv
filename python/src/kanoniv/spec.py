"""Identity spec loading and representation."""
from pathlib import Path
from kanoniv._native import parse

class Spec:
    """A Kanoniv identity specification."""

    def __init__(self, raw_yaml: str, *, path: str | None = None):
        self._raw = raw_yaml
        self._path = path
        self._parsed = parse(raw_yaml)  # calls Rust

    @classmethod
    def from_file(cls, path: str | Path) -> "Spec":
        p = Path(path)
        return cls(p.read_text(), path=str(p))

    @classmethod
    def from_string(cls, yaml_str: str) -> "Spec":
        return cls(yaml_str)

    @property
    def entity(self) -> str:
        return self._parsed.get("entity", "")

    @property
    def version(self) -> str:
        return self._parsed.get("version", "0.0.0")

    @property
    def sources(self) -> list[dict]:
        return self._parsed.get("sources", [])

    @property
    def rules(self) -> list[dict]:
        return self._parsed.get("rules", [])

    @property
    def raw(self) -> str:
        return self._raw

    @property
    def parsed(self) -> dict:
        return self._parsed
