"""
CGC label metadata registry for ML feature extraction.

Loads pre-scraped label metadata from assets/cgc/labels/cgc_labels.json and
exposes lightweight feature vectors (booleans + numeric launch metadata).
"""
import json
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LABEL_PATH = ROOT / "assets" / "cgc" / "labels" / "cgc_labels.json"


class LabelRegistry:
    """Lookup CGC label metadata and derive feature vectors."""

    def __init__(self, path: Path = DEFAULT_LABEL_PATH):
        self.path = Path(path)
        self._data = self._load_labels()

    @lru_cache(maxsize=1)
    def _load_labels(self) -> Dict[str, Dict]:
        if not self.path.exists():
            return {}
        items = json.loads(self.path.read_text(encoding="utf-8"))
        return {item["slug"]: item for item in items if "slug" in item}

    def get(self, slug: str) -> Optional[Dict]:
        """Return raw metadata by slug, if present."""
        return self._data.get(slug)

    def feature_vector(self, slug: str) -> Optional[Dict[str, float]]:
        """
        Return a flat feature vector for a label slug.

        Includes booleans as 0/1, plus optional launch_year/month and label_code.
        """
        meta = self.get(slug)
        if not meta:
            return None

        flags = meta.get("flags", {})
        fv = {
            "flag_universal": 1.0 if flags.get("universal") else 0.0,
            "flag_signature_series": 1.0 if flags.get("signature_series") else 0.0,
            "flag_witnessed_signature": 1.0 if flags.get("witnessed_signature") else 0.0,
            "flag_authenticated_unwitnessed": 1.0 if flags.get("authenticated_unwitnessed") else 0.0,
            "flag_qualified": 1.0 if flags.get("qualified") else 0.0,
            "flag_restored": 1.0 if flags.get("restored") else 0.0,
            "flag_conserved": 1.0 if flags.get("conserved") else 0.0,
            "flag_pedigree": 1.0 if flags.get("pedigree") else 0.0,
            "flag_custom_label": 1.0 if flags.get("custom_label") else 0.0,
            "flag_partial_submission": 1.0 if flags.get("partial_submission") else 0.0,
            "flag_no_grade": 1.0 if flags.get("no_grade") else 0.0,
        }

        launch_year = meta.get("launch_year")
        launch_month = meta.get("launch_month")
        if launch_year is not None:
            fv["launch_year"] = float(launch_year)
        if launch_month is not None:
            fv["launch_month"] = float(launch_month)

        label_code = meta.get("label_code")
        if label_code is not None:
            fv["label_code"] = float(label_code)

        status = meta.get("status", "active")
        fv["is_retired"] = 1.0 if status == "retired" else 0.0

        category = meta.get("category")
        if category:
            fv["is_custom_category"] = 1.0 if category == "custom" else 0.0

        return fv

    def slugs(self):
        return list(self._data.keys())


__all__ = ["LabelRegistry"]
