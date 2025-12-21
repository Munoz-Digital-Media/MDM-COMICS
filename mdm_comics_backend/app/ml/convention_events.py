"""
Convention event signals for signer-level features.

This loader ingests locally saved convention JSON (e.g., GalaxyCon pages) and
exposes per-signer aggregates like counts and price ranges for autographs/photo ops.
"""
import json
import statistics
from pathlib import Path
from typing import Dict, List, Optional


class ConventionEvents:
    def __init__(self, base_dir: Optional[Path] = None) -> None:
        root = Path(__file__).resolve().parents[3]
        self.base_dir = base_dir or (root / "assets")
        self.events = self._load_events()

    def _load_events(self) -> List[Dict]:
        events: List[Dict] = []
        if not self.base_dir.exists():
            return events
        # New convention JSONs
        for path in sorted((self.base_dir / "conventions").glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                payload["_source_path"] = str(path)
                events.append(payload)
            except Exception:
                continue
        # Legacy fallback
        for path in sorted((self.base_dir / "cgc").glob("galaxycon_*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                payload["_source_path"] = str(path)
                events.append(payload)
            except Exception:
                continue
        return events

    @staticmethod
    def _norm(name: str) -> str:
        return " ".join(name.split()).lower().strip()

    @classmethod
    def _matches(cls, target: str, candidate: str) -> bool:
        t = cls._norm(target)
        c = cls._norm(candidate)
        if not t or not c:
            return False
        return t == c or t in c or c in t

    def signer_features(self, signer_name: Optional[str]) -> Dict[str, float]:
        if not signer_name or not self.events:
            return {}

        auto_prices: List[float] = []
        photo_prices: List[float] = []
        group_photo_prices: List[float] = []
        mail_in_prices: List[float] = []
        event_hits = 0

        for event in self.events:
            matched = False
            for entry in event.get("autographs", []):
                if self._matches(signer_name, entry.get("name", "")) and entry.get("price_usd") is not None:
                    auto_prices.append(float(entry["price_usd"]))
                    matched = True
            for entry in event.get("photo_ops", []):
                if self._matches(signer_name, entry.get("name", "")) and entry.get("price_usd") is not None:
                    photo_prices.append(float(entry["price_usd"]))
                    matched = True
            for entry in event.get("group_photo_ops", []):
                if self._matches(signer_name, entry.get("name", "")) and entry.get("price_usd") is not None:
                    group_photo_prices.append(float(entry["price_usd"]))
                    matched = True
            for entry in event.get("mail_in_autographs", []):
                if self._matches(signer_name, entry.get("name", "")) and entry.get("price_usd") is not None:
                    mail_in_prices.append(float(entry["price_usd"]))
                    matched = True
            if matched:
                event_hits += 1

        def summarize(values: List[float]) -> Dict[str, float]:
            if not values:
                return {}
            return {
                "min": min(values),
                "median": statistics.median(values),
                "max": max(values),
                "count": float(len(values)),
            }

        features: Dict[str, float] = {
            "convention_event_hits": float(event_hits),
        }
        for prefix, values in [
            ("autograph_price", auto_prices),
            ("photo_op_price", photo_prices),
            ("group_photo_price", group_photo_prices),
            ("mail_in_autograph_price", mail_in_prices),
        ]:
            summary = summarize(values)
            for k, v in summary.items():
                features[f"{prefix}_{k}"] = v
        return {k: v for k, v in features.items() if v is not None}


__all__ = ["ConventionEvents"]
