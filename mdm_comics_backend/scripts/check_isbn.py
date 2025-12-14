import asyncio
import asyncpg

async def check_isbn():
    conn = await asyncpg.connect('postgresql://postgres:UAzxIlGnYJEeIZnrsPGWdkLNYWoEEIAx@caboose.proxy.rlwy.net:50641/railway')

    # Sample some ISBNs
    print('=== SAMPLE ISBNs ===')
    samples = await conn.fetch("""
        SELECT isbn, series_name, issue_name
        FROM comic_issues
        WHERE isbn IS NOT NULL AND LENGTH(isbn) > 0
        LIMIT 5
    """)
    for row in samples:
        print(f"  {row['isbn']} -> {row['series_name']} #{row['issue_name']}")

    # Check all indexes on comic_issues
    print('\n=== ALL INDEXES ON comic_issues ===')
    idx = await conn.fetch("""
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = 'comic_issues'
    """)
    isbn_idx_exists = False
    upc_idx_exists = False
    for i in idx:
        name = i['indexname']
        if 'isbn' in name.lower():
            isbn_idx_exists = True
            print(f"  [ISBN] {name}")
        elif 'upc' in name.lower():
            upc_idx_exists = True
            print(f"  [UPC] {name}")

    if not isbn_idx_exists:
        print("  *** NO ISBN INDEX EXISTS ***")
    if not upc_idx_exists:
        print("  *** NO UPC INDEX EXISTS ***")

    # Check how GCD stores ISBN
    print('\n=== GCD ISBN FORMAT CHECK ===')
    isbn_lengths = await conn.fetch("""
        SELECT LENGTH(isbn) as len, COUNT(*) as cnt
        FROM comic_issues
        WHERE isbn IS NOT NULL AND LENGTH(isbn) > 0
        GROUP BY LENGTH(isbn)
        ORDER BY cnt DESC
        LIMIT 5
    """)
    for row in isbn_lengths:
        print(f"  Length {row['len']}: {row['cnt']:,} records")

    await conn.close()

if __name__ == "__main__":
    asyncio.run(check_isbn())
