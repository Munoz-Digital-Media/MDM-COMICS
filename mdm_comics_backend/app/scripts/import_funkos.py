"""
Import Funko POP data from JSON file into database.
Run with: python -m app.scripts.import_funkos
"""
import asyncio
import json
import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal, engine, Base
from app.models.funko import Funko, FunkoSeriesName


async def import_funkos(json_path: str):
    """Import Funkos from JSON file"""

    # Create tables if they don't exist
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Load JSON data
    print(f"Loading data from {json_path}...")
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f"Found {len(data)} Funko entries")

    async with AsyncSessionLocal() as db:
        # Cache for series names to avoid repeated lookups
        series_cache = {}

        # Get existing series
        result = await db.execute(select(FunkoSeriesName))
        for series in result.scalars().all():
            series_cache[series.name] = series

        # Get existing funko handles to avoid duplicates
        result = await db.execute(select(Funko.handle))
        existing_handles = {row[0] for row in result.all()}
        print(f"Found {len(existing_handles)} existing Funkos in database")

        imported = 0
        skipped = 0
        batch_size = 500

        for i, item in enumerate(data):
            handle = item.get('handle', '')

            if not handle or handle in existing_handles:
                skipped += 1
                continue

            # Get or create series
            series_list = item.get('series', [])
            funko_series = []

            for series_name in series_list:
                if series_name not in series_cache:
                    # Create new series
                    new_series = FunkoSeriesName(name=series_name)
                    db.add(new_series)
                    await db.flush()
                    series_cache[series_name] = new_series

                funko_series.append(series_cache[series_name])

            # Create Funko
            funko = Funko(
                handle=handle,
                title=item.get('title', ''),
                image_url=item.get('image', ''),
                series=funko_series
            )
            db.add(funko)
            existing_handles.add(handle)
            imported += 1

            # Commit in batches
            if imported % batch_size == 0:
                await db.commit()
                print(f"Imported {imported} Funkos...")

        # Final commit
        await db.commit()
        print(f"\nImport complete!")
        print(f"  Imported: {imported}")
        print(f"  Skipped (duplicates): {skipped}")
        print(f"  Total in database: {len(existing_handles)}")


if __name__ == "__main__":
    json_path = Path(__file__).parent.parent.parent / "funko_data.json"

    if not json_path.exists():
        print(f"Error: {json_path} not found")
        sys.exit(1)

    asyncio.run(import_funkos(str(json_path)))
