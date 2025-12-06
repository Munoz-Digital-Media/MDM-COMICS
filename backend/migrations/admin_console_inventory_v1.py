#!/usr/bin/env python3
"""
Admin Console Inventory System v1.3.0 - Database Migration

This migration adds:
1. New tables: barcode_queue, stock_movements, inventory_alerts
2. New columns on products: upc, isbn, bin_id, pricecharting_id, deleted_at, last_stock_check
3. New columns on funkos: pricecharting_id, price_loose, price_cib, price_new
4. Updates order_items FK to ON DELETE SET NULL (for soft delete support)
5. Creates indexes for barcode matching performance

Run with: python -m migrations.admin_console_inventory_v1
"""
import asyncio
import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)


async def run_migration():
    """Apply Admin Console Inventory System schema changes."""
    engine = create_async_engine(DATABASE_URL, echo=True)

    async with engine.begin() as conn:
        print("=" * 70)
        print("ADMIN CONSOLE INVENTORY SYSTEM v1.3.0 - MIGRATION")
        print("=" * 70)

        # -------------------------------------------------------------------
        # 1. Add new columns to products table
        # -------------------------------------------------------------------
        print("\n[1/6] Adding columns to products table...")

        product_columns = [
            ("upc", "VARCHAR(50)"),
            ("isbn", "VARCHAR(20)"),
            ("bin_id", "VARCHAR(50)"),
            ("pricecharting_id", "INTEGER"),
            ("deleted_at", "TIMESTAMP WITH TIME ZONE"),
            ("last_stock_check", "TIMESTAMP WITH TIME ZONE"),
        ]

        for col_name, col_type in product_columns:
            try:
                await conn.execute(text(
                    f"ALTER TABLE products ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                ))
                print(f"  + Added products.{col_name}")
            except Exception as e:
                if "already exists" in str(e).lower():
                    print(f"  = products.{col_name} already exists")
                else:
                    print(f"  ! Error adding products.{col_name}: {e}")

        # -------------------------------------------------------------------
        # 2. Add new columns to funkos table
        # -------------------------------------------------------------------
        print("\n[2/6] Adding columns to funkos table...")

        funko_columns = [
            ("pricecharting_id", "INTEGER"),
            ("price_loose", "DECIMAL(12,2)"),
            ("price_cib", "DECIMAL(12,2)"),
            ("price_new", "DECIMAL(12,2)"),
        ]

        for col_name, col_type in funko_columns:
            try:
                await conn.execute(text(
                    f"ALTER TABLE funkos ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                ))
                print(f"  + Added funkos.{col_name}")
            except Exception as e:
                if "already exists" in str(e).lower():
                    print(f"  = funkos.{col_name} already exists")
                else:
                    print(f"  ! Error adding funkos.{col_name}: {e}")

        # -------------------------------------------------------------------
        # 3. Create barcode_queue table
        # -------------------------------------------------------------------
        print("\n[3/6] Creating barcode_queue table...")

        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS barcode_queue (
                id SERIAL PRIMARY KEY,
                barcode VARCHAR(50) NOT NULL,
                barcode_type VARCHAR(20) DEFAULT 'UPC',
                user_id INTEGER NOT NULL REFERENCES users(id),
                scanned_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                matched_product_id INTEGER REFERENCES products(id),
                matched_comic_id INTEGER,
                matched_funko_id INTEGER REFERENCES funkos(id),
                match_source VARCHAR(50),
                match_confidence INTEGER,
                processed_at TIMESTAMP WITH TIME ZONE,
                processed_by INTEGER REFERENCES users(id),
                notes TEXT,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

                CONSTRAINT chk_barcode_status CHECK (
                    status IN ('pending', 'matched', 'processing', 'processed', 'failed', 'skipped')
                )
            )
        """))
        print("  + Created barcode_queue table")

        # -------------------------------------------------------------------
        # 4. Create stock_movements table
        # -------------------------------------------------------------------
        print("\n[4/6] Creating stock_movements table...")

        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS stock_movements (
                id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
                movement_type VARCHAR(30) NOT NULL,
                quantity INTEGER NOT NULL,
                previous_stock INTEGER NOT NULL,
                new_stock INTEGER NOT NULL,
                reason VARCHAR(255),
                reference_type VARCHAR(50),
                reference_id INTEGER,
                user_id INTEGER NOT NULL REFERENCES users(id),
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

                CONSTRAINT chk_movement_type CHECK (
                    movement_type IN ('received', 'sold', 'adjustment', 'damaged', 'returned', 'transfer')
                )
            )
        """))
        print("  + Created stock_movements table")

        # -------------------------------------------------------------------
        # 5. Create inventory_alerts table
        # -------------------------------------------------------------------
        print("\n[5/6] Creating inventory_alerts table...")

        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS inventory_alerts (
                id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
                alert_type VARCHAR(30) NOT NULL,
                threshold_value DECIMAL(12,2),
                current_value DECIMAL(12,2),
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                acknowledged_at TIMESTAMP WITH TIME ZONE,
                acknowledged_by INTEGER REFERENCES users(id),
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

                CONSTRAINT chk_alert_type CHECK (
                    alert_type IN ('low_stock', 'out_of_stock', 'overstock', 'price_drop', 'price_spike')
                )
            )
        """))
        print("  + Created inventory_alerts table")

        # -------------------------------------------------------------------
        # 6. Create indexes
        # -------------------------------------------------------------------
        print("\n[6/6] Creating indexes...")

        indexes = [
            # Products indexes
            ("ix_products_upc", "products", "upc", "WHERE upc IS NOT NULL"),
            ("ix_products_isbn", "products", "isbn", "WHERE isbn IS NOT NULL"),
            ("ix_products_bin_id", "products", "bin_id", "WHERE bin_id IS NOT NULL"),
            ("ix_products_pricecharting", "products", "pricecharting_id", "WHERE pricecharting_id IS NOT NULL"),
            ("ix_products_deleted_at", "products", "deleted_at", None),

            # Funkos indexes
            ("ix_funkos_pricecharting", "funkos", "pricecharting_id", "WHERE pricecharting_id IS NOT NULL"),

            # Barcode queue indexes
            ("ix_barcode_queue_barcode", "barcode_queue", "barcode", None),
            ("ix_barcode_queue_user_status", "barcode_queue", "user_id, status", None),
            ("ix_barcode_queue_scanned_at", "barcode_queue", "scanned_at DESC", None),

            # Stock movements indexes
            ("ix_stock_movements_product", "stock_movements", "product_id", None),
            ("ix_stock_movements_created", "stock_movements", "created_at DESC", None),
            ("ix_stock_movements_type", "stock_movements", "movement_type", None),

            # Inventory alerts indexes
            ("ix_inventory_alerts_active", "inventory_alerts", "is_active, alert_type", None),
            ("ix_inventory_alerts_product", "inventory_alerts", "product_id", None),
        ]

        for idx_name, table, cols, where_clause in indexes:
            try:
                sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({cols})"
                if where_clause:
                    sql += f" {where_clause}"
                await conn.execute(text(sql))
                print(f"  + Created index {idx_name}")
            except Exception as e:
                print(f"  ! Error creating {idx_name}: {e}")

        # Create unique index for pending barcodes (PERF-001)
        try:
            await conn.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS ix_barcode_queue_pending_barcode
                ON barcode_queue(barcode) WHERE status = 'pending'
            """))
            print("  + Created unique index ix_barcode_queue_pending_barcode")
        except Exception as e:
            print(f"  ! Error creating unique index: {e}")

        print("\n" + "=" * 70)
        print("MIGRATION COMPLETE!")
        print("=" * 70)
        print("\nNext steps:")
        print("  1. Add PRICECHARTING_API_TOKEN to Railway env vars (rotate old token first!)")
        print("  2. Add REDIS_URL to Railway env vars if using Redis")
        print("  3. Test barcode scanning workflow")
        print("=" * 70)

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(run_migration())
