import asyncio
from sqlalchemy import text
from app.core.database import AsyncSessionLocal

async def main():
    async with AsyncSessionLocal() as db:
        result = await db.execute(text("UPDATE pipeline_checkpoints SET control_signal = 'stop', updated_at = NOW() WHERE job_name = 'sequential_enrichment' RETURNING is_running, state_data"))
        row = result.fetchone()
        await db.commit()
        if row:
            print(f'STOP signal sent. is_running={row[0]}, last_id={row[1]}')
        else:
            print('Not found')

asyncio.run(main())
