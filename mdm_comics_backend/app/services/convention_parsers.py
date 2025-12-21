"""
Parsers for convention pages.

Currently supports:
- galaxycon_shopify: GalaxyCon/Animate/Nightmare Weekend pages hosted on Shopify.
"""
import json
import re
from pathlib import Path
from datetime import date
from typing import Dict, List, Optional, Tuple


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


def parse_galaxycon_shopify(
    pages: Dict[str, str],
    fallback_name: Optional[str],
    event_config: Optional[Dict] = None,
) -> Dict:
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
    "frontrow_shopify_collections": None,  # placeholder, defined below
}


def parse_event(
    slug: str,
    parser: str,
    pages: Dict[str, str],
    name: Optional[str],
    event_config: Optional[Dict] = None,
) -> Dict:
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
    payload = fn(pages, name, event_config or {})
    payload["slug"] = slug
    payload["source"] = parser
    return payload


def parse_frontrow_shopify_collections(
    pages: Dict[str, str],
    fallback_name: Optional[str],
    event_config: Optional[Dict] = None,
) -> Dict:
    raw = pages.get("collections", "")
    events: List[Dict] = []
    default_year = None
    if event_config:
        default_year = event_config.get("default_year")

    def parse_range(text: str, year: Optional[int]) -> Tuple[Optional[str], Optional[str]]:
        # Examples: "Jan 10-11", "Jan 31-Feb 1", "Dec 27-28"
        parts = text.replace(" ", "").split("-")
        if not parts or not year:
            return None, None
        month_map = {
            "Jan": 1,
            "Feb": 2,
            "Mar": 3,
            "Apr": 4,
            "May": 5,
            "Jun": 6,
            "Jul": 7,
            "Aug": 8,
            "Sep": 9,
            "Oct": 10,
            "Nov": 11,
            "Dec": 12,
        }
        try:
            if len(parts) == 2:
                # same month, e.g., Jan10-11 or Jan31-Feb1 (handled below)
                left = parts[0]
                right = parts[1]
                # left contains month + day(s)
                for m_abbr, m_num in month_map.items():
                    if left.startswith(m_abbr):
                        day_start = int(left[len(m_abbr) :])
                        # If right starts with letters, it's a different month
                        if right[:3].isalpha():
                            m2 = month_map.get(right[:3])
                            day_end = int(right[3:])
                        else:
                            m2 = m_num
                            day_end = int(right)
                        start_iso = date(year, m_num, day_start).isoformat()
                        end_iso = date(year, m2, day_end).isoformat()
                        return start_iso, end_iso
            # fallback: single month-day
            return None, None
        except Exception:
            return None, None

    try:
        data = json.loads(raw)
        for c in data.get("collections", []):
            title = c.get("title", "")
            handle = c.get("handle", "")
            if "|" not in title or not handle:
                continue
            # Skip non-events
            if any(bad in title.lower() for bad in ["sponsor", "vendor", "vip", "home page"]):
                continue
            city, date_text = [p.strip() for p in title.split("|", 1)]
            start_iso, end_iso = parse_range(date_text, default_year)
            events.append(
                {
                    "name": city,
                    "date_text": date_text if not default_year else f"{date_text}, {default_year}",
                    "start_date_iso": start_iso,
                    "end_date_iso": end_iso,
                    "event_url": f"https://frontrowcardshow.com/collections/{handle}",
                    "source_title": title,
                }
            )
    except Exception:
        pass

    return {
        "event": {"name": fallback_name},
        "guests": [],
        "autographs": [],
        "photo_ops": [],
        "group_photo_ops": [],
        "mail_in_autographs": [],
        "events": events,
    }


PARSERS["frontrow_shopify_collections"] = parse_frontrow_shopify_collections


__all__ = ["parse_event"]
