"""Local embedding provider using sentence-transformers.

Lazy-loaded: only imports when first embedding is requested.
Falls back gracefully if sentence-transformers is not installed.
"""

from __future__ import annotations

import struct
from typing import Any

_MODEL_NAME = "all-MiniLM-L6-v2"
_DIMS = 384


class LocalEmbedder:
    """Generate embeddings locally using sentence-transformers (all-MiniLM-L6-v2).

    The model is lazy-loaded on first use. If sentence-transformers is not
    installed, all operations return None gracefully.
    """

    def __init__(self, model_name: str = _MODEL_NAME) -> None:
        self._model_name = model_name
        self._model: Any = None
        self._available: bool | None = None

    @property
    def available(self) -> bool:
        if self._available is None:
            try:
                import sentence_transformers  # noqa: F401

                self._available = True
            except ImportError:
                self._available = False
        return self._available

    @property
    def dims(self) -> int:
        return _DIMS

    @property
    def model_name(self) -> str:
        return self._model_name

    def _load_model(self) -> Any:
        if self._model is None:
            from sentence_transformers import SentenceTransformer

            self._model = SentenceTransformer(self._model_name)
        return self._model

    def embed(self, text: str) -> list[float] | None:
        """Embed a single text string. Returns None if not available."""
        if not self.available:
            return None
        model = self._load_model()
        vec = model.encode(text, normalize_embeddings=True)
        return vec.tolist()

    def embed_batch(self, texts: list[str]) -> list[list[float]] | None:
        """Embed multiple texts. Returns None if not available."""
        if not self.available or not texts:
            return None
        model = self._load_model()
        vecs = model.encode(texts, normalize_embeddings=True, batch_size=32)
        return [v.tolist() for v in vecs]


def vector_to_bytes(vec: list[float]) -> bytes:
    """Pack a float vector into bytes for SQLite storage."""
    return struct.pack(f"{len(vec)}f", *vec)


def bytes_to_vector(data: bytes) -> list[float]:
    """Unpack bytes back into a float vector."""
    n = len(data) // 4
    return list(struct.unpack(f"{n}f", data))


def cosine_similarity(a: list[float], b: list[float]) -> float:
    """Compute cosine similarity between two vectors.

    Uses pure Python to avoid numpy dependency. Vectors are assumed
    to be normalized (unit length) if produced by LocalEmbedder.
    """
    if len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    # Vectors from sentence-transformers with normalize_embeddings=True
    # are already unit vectors, so dot product = cosine similarity.
    return dot
