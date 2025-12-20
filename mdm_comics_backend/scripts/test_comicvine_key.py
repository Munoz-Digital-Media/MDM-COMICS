import asyncio
import os
import sys
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.adapters.comicvine_adapter import create_comicvine_adapter, get_comicvine_client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_comicvine_key():
    print("=" * 60)
    print("COMIC VINE API KEY DIAGNOSTIC")
    print("=" * 60)

    # 1. Check Environment Variable
    api_key = os.getenv("COMIC_VINE_API_KEY")
    if api_key:
        masked_key = f"{api_key[:4]}...{api_key[-4:]}"
        print(f"[ENV] COMIC_VINE_API_KEY found: {masked_key}")
    else:
        print("[ENV] COMIC_VINE_API_KEY is NOT set in environment.")
        # Try to load from .env manually just in case
        try:
            from dotenv import load_dotenv
            load_dotenv()
            api_key = os.getenv("COMIC_VINE_API_KEY")
            if api_key:
                masked_key = f"{api_key[:4]}...{api_key[-4:]}"
                print(f"[DOTENV] Loaded from .env: {masked_key}")
            else:
                print("[DOTENV] Not found in .env file either.")
                return
        except ImportError:
            print("[DOTENV] python-dotenv not installed, skipping file check.")
            return

    # 2. Initialize Adapter
    print("\n[INIT] Initializing Adapter...")
    try:
        adapter = await create_comicvine_adapter()
        if not adapter:
            print("[ERROR] Failed to create adapter (key might be missing/empty).")
            return
        print("[INIT] Adapter created successfully.")
    except Exception as e:
        print(f"[ERROR] Adapter initialization crashed: {e}")
        return

    # 3. Perform Health Check (Simple API Call)
    print("\n[TEST] Running Health Check (fetch types)...")
    try:
        is_healthy = await adapter.health_check()
        if is_healthy:
            print("[PASS] Health Check: SUCCESS (API Key is valid and active)")
        else:
            print("[FAIL] Health Check: FAILED (API Key invalid or service down)")
            # Try to get more detail
            print("       Attempting detailed diagnostic request...")
            async with get_comicvine_client() as client:
                url = f"https://comicvine.gamespot.com/api/types/?api_key={api_key}&format=json&limit=1"
                resp = await client.get(url)
                print(f"       Status Code: {resp.status_code}")
                try:
                    data = resp.json()
                    print(f"       Error: {data.get('error')}")
                except:
                    print(f"       Body: {resp.text[:200]}")
            return
    except Exception as e:
        print(f"[ERROR] Health check crashed: {e}")
        return

    # 4. Perform Search Test (Complex API Call)
    print("\n[TEST] Running Search Test ('Amazing Spider-Man 300'வுகளை...)")
    try:
        result = await adapter.search_issues("Amazing Spider-Man 300", limit=1)
        if result.success:
            count = result.total_count
            print(f"[PASS] Search: SUCCESS (Found {count} results)")
            if result.records:
                record = result.records[0]
                norm = adapter.normalize(record)
                print(f"       Sample Match: {norm.get('series_name')} #{norm.get('issue_number')}")
                print(f"       Cover URL: {norm.get('cover_url')}")
            else:
                print("       (No records returned, but request succeeded)")
        else:
            print(f"[FAIL] Search: FAILED - {result.errors}")
    except Exception as e:
        print(f"[ERROR] Search test crashed: {e}")

    print("\n" + "=" * 60)
    print("DIAGNOSTIC COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(test_comicvine_key())
