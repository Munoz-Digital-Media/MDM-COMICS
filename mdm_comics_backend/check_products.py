import asyncio
from sqlalchemy import text
import sys
sys.path.insert(0, '.')
from app.core.database import AsyncSessionLocal

async def check():
    async with AsyncSessionLocal() as db:
        # Check products table
        result = await db.execute(text('''
            SELECT id, sku, name, category, price, stock, image_url, created_at
            FROM products
            WHERE deleted_at IS NULL
            ORDER BY created_at DESC
            LIMIT 20
        '''))
        products = result.fetchall()
        print('=== PRODUCTS TABLE (live for sale) ===')
        print(f'Count: {len(products)}')
        for p in products:
            name = p.name[:40] if p.name else 'NO NAME'
            print(f'  {p.sku}: {name} | ${p.price} | stock:{p.stock} | {p.category}')

        # Check total
        total = await db.execute(text('SELECT COUNT(*) FROM products WHERE deleted_at IS NULL'))
        print(f'\nTotal products: {total.scalar()}')

        # Check BCW mappings
        bcw = await db.execute(text('SELECT COUNT(*) FROM bcw_product_mappings'))
        print(f'BCW mappings: {bcw.scalar()}')

asyncio.run(check())
