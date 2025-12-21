"""
Parsers for convention pages.

Currently supports:
- galaxycon_shopify: GalaxyCon/Animate/Nightmare Weekend pages hosted on Shopify.
"""
import json
import re
from pathlib import Path
from typing import Dict, List, Optional


def _clean_hours(raw: str) -> str:
    raw = re.sub(r"<br\\s*/?>", " | ", raw)
    raw = re.sub(r"&nbsp;", " ", raw)
    raw = re.sub(r"<[^>]+>", " ", raw)
    return re.sub(r"\\s+", " ", raw).strip()


def _parse_event_meta(html: str, fallback_name: Optional[str] = None) -> Dict:
    date_match = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\\s+\\d{1,2}\\s*-\\s*\\d{1,2},\\s*\\d{4}", html, re.IGNORECASE)
    dates = date_match.group(0).upper() if date_match else None

    venue_match = re.search(
        r"Greater\\s+Columbus\\s+Convention\\s+Center.*?400\\s+North\\s+High\\s+St.*?Columbus,\\s*OH\\s*43215",
        html,
        re.IGNORECASE | re.DOTALL,
    )
    venue = "Greater Columbus Convention Center, 400 North High St, Columbus, OH 43215" if venue_match else None

    def extract_hours(label: str) -> Optional[str]:
        m = re.search(label + r".*?<p[^>]*>(.*?)</p>", html, re.IGNORECASE | re.DOTALL)
        if not m:
            return None
        return _clean_hours(m.group(1))

    event_hours = extract_hours("EVENT HOURS")
    exhibit_hours = extract_hours("EXHIBIT HALL HOURS")

    return {
        "name": fallback_name,
        "dates": dates,
        "venue": venue,
        "event_hours": event_hours,
        "exhibit_hours": exhibit_hours,
    }


def _parse_guests(html: str) -> List[Dict]:
    guests: List[Dict] = []
    for m in re.finditer(r"><span[^>]*>([A-Z][A-Za-z.'\\-]+(?:\\s+[A-Z][A-Za-z.'\\-]+)+)</span></", html):
        name = m.group(1).strip()
        guests.append({"name": name, "category": None, "source": "heading"})

    seen = set()
    deduped = []
    for g in guests:
        key = g["name"].lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(g)
    return deduped


def _parse_prices(html: str, context: str) -> List[Dict]:
    prices: List[Dict] = []
    for m in re.finditer(r"([A-Z][A-Za-z.'\\-]+(?:\\s+[A-Z][A-Za-z.'\\-]+)+)[^$]{0,80}\\$(\\d{2,4})", html):
        name = m.group(1).strip()
        amount = int(m.group(2))
        prices.append({"name": name, "price_usd": amount, "context": context})
    seen = set()
    deduped = []
    for p in prices:
        key = (p["name"].lower(), p["price_usd"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(p)
    return deduped


def parse_galaxycon_shopify(pages: Dict[str, str], fallback_name: Optional[str]) -> Dict:
    event_html = pages.get("event", "")
    payload = {
        "event": _parse_event_meta(event_html, fallback_name),
        "guests": _parse_guests(pages.get("guests", "")),
        "autographs": _parse_prices(pages.get("autographs", ""), "autographs"),
        "photo_ops": _parse_prices(pages.get("photo_ops", ""), "photo_ops"),
        "group_photo_ops": _parse_prices(pages.get("group_photo_ops", ""), "group_photo_ops"),
        "mail_in_autographs": _parse_prices(pages.get("mail_in_autographs", ""), "mail_in_autographs"),
    }
    return payload


PARSERS = {
    "galaxycon_shopify": parse_galaxycon_shopify,
}


def parse_event(slug: str, parser: str, pages: Dict[str, str], name: Optional[str]) -> Dict:
    fn = PARSERS.get(parser)
    if not fn:
        return {
            "event": {"name": name},
            "guests": [],
            "autographs": [],
            "photo_ops": [],
            "group_photo_ops": [],
            "mail_in_autographs": [],
            "debug": {"parser": parser, "error": "parser_not_found"},
        }
    payload = fn(pages, name)
    payload["slug"] = slug
    payload["source"] = parser
    return payload


__all__ = ["parse_event"]
