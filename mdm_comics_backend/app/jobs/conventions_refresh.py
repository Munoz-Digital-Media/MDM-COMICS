"""
Convention pages refresher (GalaxyCon Columbus).

Fetches locally cached HTML pages and regenerates the parsed JSON so
ConventionEvents can surface signer-level features to the ML pipeline.
"""
import subprocess
from pathlib import Path
from typing import List

import requests

ROOT = Path(__file__).resolve().parents[3]
ASSETS = ROOT / "assets" / "cgc"

PAGES = {
    "event": "https://galaxycon.com/pages/galaxycon-columbus",
    "guests": "https://galaxycon.com/pages/galaxycon-columbus-guests",
    "autographs": "https://galaxycon.com/pages/galaxycon-columbus-autographs",
    "photo_ops": "https://galaxycon.com/pages/galaxycon-columbus-photo-ops",
    "group_photo_ops": "https://galaxycon.com/pages/galaxycon-columbus-group-photo-ops",
    "mail_in_autographs": "https://galaxycon.com/pages/galaxycon-columbus-mail-in-autographs",
}


def fetch_pages() -> List[str]:
    ASSETS.mkdir(parents=True, exist_ok=True)
    saved: List[str] = []
    headers = {"User-Agent": "Mozilla/5.0 (ML-convention-cron)"}
    for key, url in PAGES.items():
        dest = ASSETS / f"galaxycon_columbus_{key}.html"
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            dest.write_bytes(resp.content)
            saved.append(str(dest))
        except Exception as e:
            print(f"[conventions] failed to fetch {url}: {e}")
    return saved


def run_parser() -> None:
    parser = ASSETS / "parse_galaxycon_columbus.py"
    if not parser.exists():
        print("[conventions] parser not found, skipping")
        return
    try:
        subprocess.run(
            ["python", str(parser)],
            cwd=str(ROOT),
            check=True,
        )
    except Exception as e:
        print(f"[conventions] parser error: {e}")


async def run_convention_refresh_job() -> None:
    saved = fetch_pages()
    print(f"[conventions] fetched {len(saved)} pages")
    run_parser()
    print("[conventions] refresh complete")


__all__ = ["run_convention_refresh_job"]
