"""
Convention pages refresher (GalaxyCon Columbus).

Fetches locally cached HTML pages and regenerates the parsed JSON so
ConventionEvents can surface signer-level features to the ML pipeline.
"""
import json
import subprocess
from pathlib import Path
from typing import Dict, List

import requests

from app.services.convention_parsers import parse_event

ROOT = Path(__file__).resolve().parents[3]
ASSETS = ROOT / "assets"
CONFIG_PATH = ASSETS / "conventions" / "config.json"


def load_config() -> List[Dict]:
    if CONFIG_PATH.exists():
        try:
            cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            return cfg.get("events", [])
        except Exception as e:
            print(f"[conventions] failed to read config: {e}")
    # fallback to legacy single event if config missing
    return [
        {
            "slug": "galaxycon_columbus",
            "name": "GalaxyCon Columbus",
            "parser": "galaxycon_shopify",
            "pages": {
                "event": "https://galaxycon.com/pages/galaxycon-columbus",
                "guests": "https://galaxycon.com/pages/galaxycon-columbus-guests",
                "autographs": "https://galaxycon.com/pages/galaxycon-columbus-autographs",
                "photo_ops": "https://galaxycon.com/pages/galaxycon-columbus-photo-ops",
                "group_photo_ops": "https://galaxycon.com/pages/galaxycon-columbus-group-photo-ops",
                "mail_in_autographs": "https://galaxycon.com/pages/galaxycon-columbus-mail-in-autographs",
            },
        }
    ]


def fetch_pages(slug: str, pages: Dict[str, str]) -> Dict[str, str]:
    base = ASSETS / "conventions" / slug
    base.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": "Mozilla/5.0 (ML-convention-cron)"}
    html_map: Dict[str, str] = {}
    for key, url in pages.items():
        dest = base / f"{slug}_{key}.html"
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            dest.write_bytes(resp.content)
            html_map[key] = dest.read_text(encoding="utf-8", errors="ignore")
        except Exception as e:
            print(f"[conventions] failed to fetch {url}: {e}")
    return html_map


def write_event_json(slug: str, payload: Dict) -> None:
    out = ASSETS / "conventions" / f"{slug}.json"
    out.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


async def run_convention_refresh_job() -> None:
    events = load_config()
    total = 0
    for event in events:
        slug = event.get("slug")
        parser = event.get("parser")
        name = event.get("name")
        pages = event.get("pages", {})
        if not slug or not parser or not pages:
            print(f"[conventions] skipping invalid event entry: {event}")
            continue
        html_map = fetch_pages(slug, pages)
        payload = parse_event(slug=slug, parser=parser, pages=html_map, name=name, event_config=event)
        write_event_json(slug, payload)
        total += 1
    print(f"[conventions] refresh complete for {total} event(s)")


__all__ = ["run_convention_refresh_job"]
