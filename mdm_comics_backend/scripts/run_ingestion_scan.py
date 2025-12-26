import asyncio
import logging
import sys
import os

# Ensure backend path is in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.dirname(current_dir)
sys.path.append(backend_dir)

from app.core.database import async_session_maker
from app.services.cover_ingestion import get_cover_ingestion_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_scan(folder_path):
    """
    Manually trigger cover ingestion for a specific folder.
    """
    if not os.path.exists(folder_path):
        print(f"Error: Folder not found: {folder_path}")
        return

    print(f"Scanning folder: {folder_path}...")
    
    async with async_session_maker() as db:
        service = get_cover_ingestion_service(db)
        
        # Define progress callback
        def progress(current, total):
            print(f"Processing: {current}/{total}", end='\r')

        result = await service.ingest_folder(
            folder_path=folder_path,
            user_id=1, # System user
            progress_callback=progress
        )
        
    print("\n\n--- Scan Complete ---")
    print(f"Total Files: {result.total_files}")
    print(f"Processed:   {result.processed}")
    print(f"Queued:      {result.queued_for_review}")
    print(f"Skipped:     {result.skipped}")
    print(f"Errors:      {result.errors}")
    
    if result.queued_for_review > 0:
        print("\nSuccess! Items have been queued for Match Review.")
        print("Go to Admin > Match Review to approve them.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_ingestion_scan.py <path_to_folder>")
        print("Example: python run_ingestion_scan.py F:\\apps\\mdm_comics\\assets\\comic_book_covers\\image")
        sys.exit(1)
    
    folder_path = sys.argv[1]
    
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    asyncio.run(run_scan(folder_path))