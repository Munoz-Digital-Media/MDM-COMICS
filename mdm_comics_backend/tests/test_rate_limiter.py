import asyncio
import pytest
import time
from app.jobs.sequential_enrichment import metron_request_queue, metron_worker

@pytest.mark.asyncio
async def test_metron_serialization():
    """
    Verify that Metron requests are processed serially with 1s spacing.
    """
    # Start worker
    worker_task = asyncio.create_task(metron_worker())
    
    try:
        async def mock_query(val):
            return val
            
        async def enqueue_request(val):
            loop = asyncio.get_running_loop()
            future = loop.create_future()
            await metron_request_queue.put((mock_query, (val,), {}, future))
            return await future

        # Launch 5 concurrent requests
        # We expect them to finish at t=0, t=1, t=2, t=3, t=4
        start_time = time.time()
        tasks = [enqueue_request(i) for i in range(5)]
        
        completed = await asyncio.gather(*tasks)
        end_time = time.time()
        
        duration = end_time - start_time
        
        print(f"Processed {len(completed)} requests in {duration:.2f}s")
        
        assert len(completed) == 5
        # 5 items should take at least 4 intervals of 1s (plus execution time)
        # 1st item: immediate exec, sleep 1s
        # 2nd item: immediate exec, sleep 1s
        # ...
        # The gather returns when the Future is set.
        # Future is set BEFORE the sleep.
        # So:
        # T0: Item 1 start, Item 1 done (future set), Worker sleep start
        # T1: Worker sleep done, Item 2 start, Item 2 done (future set), Worker sleep start
        # ...
        # T4: Worker sleep done, Item 5 start, Item 5 done (future set)
        # So total time for gather() should be approx 4 seconds.
        assert duration >= 3.8  # Allow slight jitter
        
    finally:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
