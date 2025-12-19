"""
BCW SKU and Price Update Migration
Updates SKUs and prices based on spreadsheet data
"""
import asyncio
import sys
sys.path.insert(0, '.')

from sqlalchemy import text
from app.core.database import AsyncSessionLocal

# Price updates (using CURRENT SKU before rename)
PRICE_UPDATES = [
    ("MDM-BIN-SHT-YLW", 34.95),
    ("MDM-BIN-SHT-RED", 34.95),
    ("MDM-BIN-SHT-BLU", 34.95),
    ("MDM-BIN-SHT-GRN", 34.95),
    ("MDM-BIN-SHT-BLK", 34.95),
    ("MDM-BIN-LNG-BLK", 64.95),
    ("MDM-BIN-GRD-BLK", 52.95),
    ("MDM-LTC-GRD-01", 44.95),
    ("MDM-LTC-GRD-15", 114.95),
    ("MDM-LTC-GRD-30", 149.95),
    ("MDM-BAG-CUR", 7.45),
    ("MDM-BAG-CUR-TK", 7.45),
    ("MDM-BAG-SLV", 7.45),
    ("MDM-BAG-SLV-TK", 7.45),
    ("MDM-BAG-GLD", 7.95),
    ("MDM-BAG-GLD-TK", 7.95),
    ("MDM-BAG-TRS", 19.95),
    ("MDM-BAG-CUR-2M", 25.50),
    ("MDM-BAG-CUR-4M", 25.50),
    ("MDM-BAG-SLV-2M", 27.95),
    ("MDM-BAG-SLV-4M", 27.95),
    ("MDM-BAG-GLD-2M", 29.95),
    ("MDM-BAG-GLD-4M", 29.95),
    ("MDM-BAG-GRD-2M", 26.95),
    ("MDM-BAG-GRD-4M", 17.95),
    ("MDM-BRD-CUR", 12.95),
    ("MDM-BRD-MOD", 12.95),
    ("MDM-BRD-REG", 13.95),
    ("MDM-BRD-SLV", 13.95),
    ("MDM-BRD-GLD", 14.45),
    ("MDM-BRD-TRS", 28.45),
    ("MDM-DIV-BLK", 20.45),
    ("MDM-DIV-WHT", 20.45),
    ("MDM-PRT-BLU", 9.95),
    ("MDM-PRT-RED", 9.95),
    ("MDM-PRT-GRN", 9.95),
    ("MDM-PRT-YLW", 9.95),
    ("MDM-PRT-BLK", 9.95),
]

# SKU changes (old -> new)
SKU_CHANGES = [
    ("MDM-BIN-SHT-YLW", "MDM-SHT-YLW-BIN"),
    ("MDM-BIN-SHT-RED", "MDM-SHT-RED-BIN"),
    ("MDM-BIN-SHT-BLU", "MDM-SHT-BLU-BIN"),
    ("MDM-BIN-SHT-GRN", "MDM-SHT-GRN-BIN"),
    ("MDM-BIN-SHT-BLK", "MDM-SHT-BLK-BIN"),
    ("MDM-BIN-LNG-BLK", "MDM-LNG-BLK-BIN"),
    ("MDM-BIN-GRD-BLK", "MDM-BLK-GRD-BIN"),
    ("MDM-BAG-CUR", "MDM-CUR-BAG"),
    ("MDM-BAG-CUR-TK", "MDM-CUR-TK-BAG"),
    ("MDM-BAG-SLV", "MDM-SLV-BAG"),
    ("MDM-BAG-SLV-TK", "MDM-SLV-TK-BAG"),
    ("MDM-BAG-GLD", "MDM-GLD-BAG"),
    ("MDM-BAG-GLD-TK", "MDM-GLD-TK-BAG"),
    ("MDM-BAG-TRS", "MDM-TRS-BAG"),
    ("MDM-BAG-CUR-2M", "MDM-CUR-2M-BAG"),
    ("MDM-BAG-CUR-4M", "MDM-CUR-4M-BAG"),
    ("MDM-BAG-SLV-2M", "MDM-SLV-2M-BAG"),
    ("MDM-BAG-SLV-4M", "MDM-SLV-4M-BAG"),
    ("MDM-BAG-GLD-2M", "MDM-GLD-2M-BAG"),
    ("MDM-BAG-GLD-4M", "MDM-GLD-4M-BAG"),
    ("MDM-BAG-GRD-2M", "MDM-GRD-2M-BAG"),
    ("MDM-BAG-GRD-4M", "MDM-GRD-4M-BAG"),
    ("MDM-BRD-CUR", "MDM-CUR-BRD"),
    ("MDM-BRD-MOD", "MDM-MOD-BRD"),
    ("MDM-BRD-REG", "MDM-REG-BRD"),
    ("MDM-BRD-SLV", "MDM-SLV-BRD"),
    ("MDM-BRD-GLD", "MDM-GLD-BRD"),
    ("MDM-BRD-TRS", "MDM-TRS-BRD"),
    ("MDM-DIV-BLK", "MDM-BLK-DIV"),
    ("MDM-DIV-WHT", "MDM-WHT-DIV"),
    ("MDM-PRT-BLU", "MDM-BLU-PRT"),
    ("MDM-PRT-RED", "MDM-RED-PRT"),
    ("MDM-PRT-GRN", "MDM-GRN-PRT"),
    ("MDM-PRT-YLW", "MDM-YLW-PRT"),
    ("MDM-PRT-BLK", "MDM-BLK-PRT"),
]


async def run_migration():
    async with AsyncSessionLocal() as db:
        print("=" * 60)
        print("BCW SKU AND PRICE UPDATE MIGRATION")
        print("=" * 60)

        # Step 1: Update prices in products table
        print("\n[1/3] Updating prices in products table...")
        prices_updated = 0
        for sku, price in PRICE_UPDATES:
            result = await db.execute(
                text("UPDATE products SET price = :price WHERE sku = :sku"),
                {"sku": sku, "price": price}
            )
            if result.rowcount > 0:
                prices_updated += 1
                print(f"  OK {sku} -> ${price:.2f}")
            else:
                print(f"  SKIP {sku} not found in products")
        print(f"  Updated {prices_updated}/{len(PRICE_UPDATES)} prices")

        # Step 2: Update SKUs in products table
        print("\n[2/3] Updating SKUs in products table...")
        products_updated = 0
        for old_sku, new_sku in SKU_CHANGES:
            result = await db.execute(
                text("UPDATE products SET sku = :new_sku WHERE sku = :old_sku"),
                {"old_sku": old_sku, "new_sku": new_sku}
            )
            if result.rowcount > 0:
                products_updated += 1
                print(f"  OK {old_sku} -> {new_sku}")
            else:
                print(f"  SKIP {old_sku} not found in products")
        print(f"  Updated {products_updated}/{len(SKU_CHANGES)} product SKUs")

        # Step 3: Update SKUs in bcw_product_mappings table
        print("\n[3/3] Updating SKUs in bcw_product_mappings table...")
        mappings_updated = 0
        for old_sku, new_sku in SKU_CHANGES:
            result = await db.execute(
                text("UPDATE bcw_product_mappings SET mdm_sku = :new_sku WHERE mdm_sku = :old_sku"),
                {"old_sku": old_sku, "new_sku": new_sku}
            )
            if result.rowcount > 0:
                mappings_updated += 1
                print(f"  OK {old_sku} -> {new_sku}")
            else:
                print(f"  SKIP {old_sku} not found in mappings")
        print(f"  Updated {mappings_updated}/{len(SKU_CHANGES)} mapping SKUs")

        # Commit all changes
        await db.commit()

        print("\n" + "=" * 60)
        print("MIGRATION COMPLETE")
        print(f"  Prices updated: {prices_updated}")
        print(f"  Product SKUs updated: {products_updated}")
        print(f"  Mapping SKUs updated: {mappings_updated}")
        print("=" * 60)

        # Verify results
        print("\n[VERIFICATION] Sample of updated products:")
        result = await db.execute(text("""
            SELECT sku, name, price
            FROM products
            WHERE category = 'supplies' AND deleted_at IS NULL
            ORDER BY sku
            LIMIT 10
        """))
        for row in result.fetchall():
            print(f"  {row.sku}: ${row.price:.2f} - {row.name[:40]}")


if __name__ == "__main__":
    asyncio.run(run_migration())
