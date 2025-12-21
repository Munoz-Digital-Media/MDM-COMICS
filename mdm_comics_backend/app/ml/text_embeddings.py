"""
Text embeddings for CGC reference docs.

Default: deterministic hashed bag-of-words (no external deps).
Optional: if `sentence-transformers` is installed and a local model path is
provided, use it for higher-quality embeddings (no network fetches).
"""
import hashlib
import re
from pathlib import Path
from typing import Iterable, List, Optional

import logging
from pathlib import Path
from typing import Iterable, List, Optional

try:  # Optional dependency
    from sentence_transformers import SentenceTransformer  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    SentenceTransformer = None
logger = logging.getLogger(__name__)


def hash_embedding(text: str, dim: int = 128) -> List[float]:
    """
    Produce a stable hashed bag-of-words embedding.

    - Tokenizes on word characters.
    - Hashes each token with sha256 to pick an index in [0, dim).
    - Counts occurrences per bucket.
    """
    if not text:
        return [0.0] * dim

    tokens = re.findall(r"[a-zA-Z0-9]+", text.lower())
    buckets = [0.0] * dim
    for tok in tokens:
        h = hashlib.sha256(tok.encode("utf-8")).digest()
        idx = int.from_bytes(h[:4], "big") % dim
        buckets[idx] += 1.0
    return buckets


def merge_embeddings(vectors: List[List[float]]) -> List[float]:
    """Sum a list of embeddings (element-wise)."""
    if not vectors:
        return []
    dim = len(vectors[0])
    merged = [0.0] * dim
    for vec in vectors:
        for i, val in enumerate(vec):
            merged[i] += val
    return merged


class TextEmbedder:
    """
    Wrapper that uses a local sentence-transformers model if available,
    otherwise falls back to hashed bag-of-words.
    """

    def __init__(self, model_path: Optional[Path] = None, fallback_dim: int = 128):
        self.model = None
        self.fallback_dim = fallback_dim
        if model_path and SentenceTransformer:
            path = Path(model_path)
            checksum_path = path / "model.sha256"
            expected_checksum = checksum_path.read_text(encoding="utf-8").strip() if checksum_path.exists() else None
            if path.exists():
                if expected_checksum:
                    actual_checksum = self._dir_checksum(path)
                    if actual_checksum != expected_checksum:
                        logger.warning("Text encoder checksum mismatch; falling back to hashed BoW.")
                        self.model = None
                        return
                try:
                    self.model = SentenceTransformer(str(path))
                except Exception as exc:  # pragma: no cover - optional dependency
                    logger.warning("Failed to load sentence-transformers model; falling back: %s", exc)
                    self.model = None

    def embed(self, text: str) -> List[float]:
        if self.model:
            vec = self.model.encode([text], convert_to_numpy=True, normalize_embeddings=True)[0]
            return vec.tolist()
        return hash_embedding(text, dim=self.fallback_dim)

    def embed_many(self, texts: Iterable[str]) -> List[List[float]]:
        return [self.embed(t) for t in texts]

    def embed_and_merge(self, texts: Iterable[str]) -> List[float]:
        vectors = self.embed_many(texts)
        return merge_embeddings(vectors)

    def _dir_checksum(self, path: Path) -> str:
        """Compute a deterministic checksum over all files in a directory."""
        h = hashlib.sha256()
        for file_path in sorted(path.rglob("*")):
            if file_path.is_file():
                rel = file_path.relative_to(path).as_posix().encode("utf-8")
                h.update(rel)
                with file_path.open("rb") as f:
                    for chunk in iter(lambda: f.read(8192), b""):
                        h.update(chunk)
        return h.hexdigest()


__all__ = ["hash_embedding", "merge_embeddings", "TextEmbedder"]
