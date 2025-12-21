"""
PDF corpus loader for CGC reference guides.

Loads assets/cgc/pdf_corpus.json (base64-encoded PDFs) and assets/cgc/pdf_texts.json
(best-effort extracted text) so training/inference pipelines can pull raw bytes and
text without filesystem coupling.
"""
import base64
import json
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PDF_CORPUS = ROOT / "assets" / "cgc" / "pdf_corpus.json"
DEFAULT_PDF_TEXTS = ROOT / "assets" / "cgc" / "pdf_texts.json"


class PdfCorpus:
    def __init__(self, path: Path = DEFAULT_PDF_CORPUS, text_path: Path = DEFAULT_PDF_TEXTS):
        self.path = Path(path)
        self.text_path = Path(text_path)
        self._data = self._load()
        self._text = self._load_texts()

    @lru_cache(maxsize=1)
    def _load(self) -> Dict[str, Dict]:
        if not self.path.exists():
            return {}
        items = json.loads(self.path.read_text(encoding="utf-8"))
        return {item["slug"]: item for item in items if "slug" in item}

    @lru_cache(maxsize=1)
    def _load_texts(self) -> Dict[str, str]:
        if not self.text_path.exists():
            return {}
        items = json.loads(self.text_path.read_text(encoding="utf-8"))
        return {item["slug"]: item.get("text", "") for item in items if "slug" in item}

    def get(self, slug: str) -> Optional[Dict]:
        return self._data.get(slug)

    def content(self, slug: str) -> Optional[bytes]:
        meta = self.get(slug)
        if not meta:
            return None
        return base64.b64decode(meta["content_b64"])

    def text(self, slug: str) -> Optional[str]:
        return self._text.get(slug)

    def slugs(self):
        return list(self._data.keys())


__all__ = ["PdfCorpus"]
