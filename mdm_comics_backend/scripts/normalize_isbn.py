"""
ISBN Normalization Script

Normalizes all ISBNs to ISBN-13 format for consistent matching.
- ISBN-10 (10 digits) -> converted to ISBN-13 with 978 prefix
- ISBN-13 (13 digits starting with 978/979) -> kept as-is
- Other formats -> best effort extraction
"""

import asyncio
import asyncpg
import time

def isbn10_to_isbn13(isbn10: str) -> str | None:
    """Convert ISBN-10 to ISBN-13 format."""
    # Keep only digits and X (check digit)
    isbn10 = ''.join(c for c in isbn10.upper() if c.isdigit() or c == 'X')

    if len(isbn10) != 10:
        return None

    # ISBN-10 X check digit = 10
    # Drop the old check digit, prefix with 978
    isbn13_base = '978' + isbn10[:9]

    # Calculate ISBN-13 check digit (alternating 1 and 3 weights)
    total = sum(int(d) * (1 if i % 2 == 0 else 3) for i, d in enumerate(isbn13_base))
    check = (10 - (total % 10)) % 10

    return isbn13_base + str(check)


def normalize_isbn(raw_isbn: str) -> str | None:
    """
    Normalize any ISBN format to ISBN-13 (digits only).

    Examples:
        '0-553-38169-9'      -> '9780553381696'
        '978-0-553-38169-6'  -> '9780553381696'
    """
    if not raw_isbn:
        return None

    # Strip all non-digit characters (except X for ISBN-10 check digit)
    cleaned = ''.join(c for c in raw_isbn.upper() if c.isdigit() or c == 'X')

    # ISBN-10: convert to ISBN-13
    if len(cleaned) == 10:
        return isbn10_to_isbn13(cleaned)

    # ISBN-13: validate prefix and return
    if len(cleaned) == 13 and cleaned.startswith(('978', '979')):
        return cleaned

    # Best effort: extract first 13 digits
    digits_only = ''.join(c for c in cleaned if c.isdigit())
    if len(digits_only) >= 10:
        # Try as ISBN-10 first
        result = isbn10_to_isbn13(digits_only[:10])
        if result:
            return result
        # Otherwise return first 13
        return digits_only[:13]

    return None


async def normalize_all_isbns():
    """Normalize all ISBNs in comic_issues table."""
    conn = await asyncpg.connect(
        'postgresql://postgres:UAzxIlGnYJEeIZnrsPGWdkLNYWoEEIAx@caboose.proxy.rlwy.net:50641/railway'
    )

    # Get records needing normalization (by ID to avoid deadlocks)
    print("Fetching records needing normalization...")
    rows = await conn.fetch("""
        SELECT id, isbn FROM comic_issues
        WHERE isbn IS NOT NULL
        AND LENGTH(isbn) > 0
        AND (isbn_normalized IS NULL OR isbn_normalized = '')
        ORDER BY id
    """)

    print(f"Found {len(rows):,} records to normalize")

    if not rows:
        print("Nothing to do!")
        await conn.close()
        return

    # Process one at a time to avoid deadlocks with GCD import
    batch_size = 100
    total = len(rows)
    updated = 0
    failed = 0

    for i in range(0, total, batch_size):
        batch = rows[i:i+batch_size]

        for row in batch:
            norm = normalize_isbn(row['isbn'])
            if norm:
                try:
                    await conn.execute(
                        "UPDATE comic_issues SET isbn_normalized = $1 WHERE id = $2",
                        norm, row['id']
                    )
                    updated += 1
                except Exception as e:
                    # Skip on conflict, continue
                    failed += 1
            else:
                failed += 1

        if (i + batch_size) % 1000 == 0 or i + batch_size >= total:
            print(f"  Progress: {min(i + batch_size, total):,} / {total:,} ({min(i + batch_size, total)/total*100:.1f}%)")

        # Small delay to avoid overwhelming the DB during GCD import
        await asyncio.sleep(0.01)

    print(f"\nDone! Updated: {updated:,}, Failed: {failed:,}")

    # Final stats
    stats = await conn.fetchrow("""
        SELECT
            COUNT(*) FILTER (WHERE isbn_normalized IS NOT NULL AND LENGTH(isbn_normalized) = 13) as isbn13,
            COUNT(*) FILTER (WHERE isbn_normalized IS NOT NULL AND LENGTH(isbn_normalized) != 13) as other,
            COUNT(*) FILTER (WHERE isbn IS NOT NULL AND LENGTH(isbn) > 0 AND isbn_normalized IS NULL) as missing
        FROM comic_issues
    """)

    print(f"\nFinal stats:")
    print(f"  ISBN-13 normalized: {stats['isbn13']:,}")
    print(f"  Other formats: {stats['other']:,}")
    print(f"  Failed to normalize: {stats['missing']:,}")

    await conn.close()


if __name__ == "__main__":
    asyncio.run(normalize_all_isbns())
