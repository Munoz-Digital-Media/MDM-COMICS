import asyncio
import pytest
import time
from app.adapters.metron_adapter import _metron_request_queue, start_metron_worker, stop_metron_worker

@pytest.mark.asyncio
async def test_metron_serialization():
    """
    Verify that Metron requests are processed serially with 1s spacing.
    Uses the global queue in MetronAdapter.
    """
    # Start worker
    await start_metron_worker()
    
    try:
        def mock_query(val):
            return val
            
        async def enqueue_request(val):
            loop = asyncio.get_running_loop()
            future = loop.create_future()
            await _metron_request_queue.put((mock_query, (val,), {}, future))
            return await future

        # Launch 5 concurrent requests
        start_time = time.time()
        tasks = [enqueue_request(i) for i in range(5)]
        
        completed = await asyncio.gather(*tasks)
        end_time = time.time()
        
        duration = end_time - start_time
        
        print(f"Processed {len(completed)} requests in {duration:.2f}s")
        
        assert len(completed) == 5
        assert duration >= 3.8
        
    finally:
        await stop_metron_worker()