#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Download PriceCharting Comics Data

Since PriceCharting doesn't have a bulk CSV download for comics,
this script queries the API with various search terms and filters
to collect all comic book data.
"""

import json
import csv
import time
import requests
from pathlib import Path

API_TOKEN = "278b25045864fcddf72f8a93e7f9b73733cad6ce"
BASE_URL = "https://www.pricecharting.com/api/products"
OUTPUT_FILE = Path(r"F:\apps\mdm_comics\backend\pricecharting_comics.csv")

# Major comic series/search terms to query
SEARCH_TERMS = [
    # Marvel
    "spider-man", "amazing spider-man", "x-men", "avengers", "iron man",
    "captain america", "hulk", "thor", "wolverine", "daredevil",
    "fantastic four", "guardians of the galaxy", "deadpool", "punisher",
    "ghost rider", "venom", "carnage", "black panther", "doctor strange",
    "silver surfer", "marvel",

    # DC
    "batman", "superman", "wonder woman", "justice league", "flash",
    "green lantern", "aquaman", "robin", "joker", "harley quinn",
    "nightwing", "teen titans", "suicide squad", "green arrow", "dc comics",
    "detective comics", "action comics",

    # Image
    "spawn", "walking dead", "invincible", "savage dragon", "witchblade",

    # Other Publishers
    "teenage mutant ninja turtles", "tmnt", "star wars", "transformers",
    "gi joe", "sonic", "archie", "simpsons", "buffy", "hellboy",
    "dark horse", "conan", "red sonja",

    # Generic searches to catch more
    "comic book", "#1", "#2", "#3", "annual", "variant",
]


def fetch_comics(query: str) -> list:
    """Fetch comics from PriceCharting API."""
    params = {
        "t": API_TOKEN,
        "q": query,
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        products = data.get("products", [])
        # Filter to only comic books
        comics = [p for p in products if p.get("genre") == "Comic Book"
                  or "Comic Books" in p.get("console-name", "")]
        return comics
    except Exception as e:
        print(f"  Error fetching '{query}': {e}")
        return []


def main():
    print("=" * 70)
    print("PRICECHARTING COMICS DOWNLOAD")
    print("=" * 70)

    all_comics = {}

    for i, term in enumerate(SEARCH_TERMS, 1):
        print(f"[{i}/{len(SEARCH_TERMS)}] Searching: {term}")

        comics = fetch_comics(term)
        new_count = 0

        for comic in comics:
            comic_id = comic.get("id")
            if comic_id and comic_id not in all_comics:
                all_comics[comic_id] = comic
                new_count += 1

        print(f"  Found {len(comics)} results, {new_count} new (total: {len(all_comics)})")

        # Rate limiting
        time.sleep(0.5)

    print(f"\n{'=' * 70}")
    print(f"Total unique comics collected: {len(all_comics)}")

    if not all_comics:
        print("No comics found!")
        return

    # Write to CSV
    print(f"Writing to {OUTPUT_FILE}...")

    # Get all possible fieldnames
    fieldnames = set()
    for comic in all_comics.values():
        fieldnames.update(comic.keys())
    fieldnames = sorted(fieldnames)

    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for comic in all_comics.values():
            writer.writerow(comic)

    print(f"Saved {len(all_comics)} comics to CSV!")
    print("=" * 70)

    # Print sample
    print("\nSample entries:")
    for i, comic in enumerate(list(all_comics.values())[:5]):
        name = comic.get("product-name", "Unknown")
        series = comic.get("console-name", "Unknown")
        loose = comic.get("loose-price", 0)
        if loose:
            loose = f"${loose/100:.2f}"
        print(f"  {i+1}. {name} ({series}) - {loose}")


if __name__ == "__main__":
    main()
