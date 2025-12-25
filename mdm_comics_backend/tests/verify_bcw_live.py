import asyncio
import os
import logging
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# Import BCW Client
try:
    from app.services.bcw.browser_client import BCWBrowserClient
except ImportError as e:
    print(f"Import Error: {e}")
    print("Ensure you are running this script with the backend dependencies installed (pip install -r requirements.txt)")
    print("And Playwright browsers installed (playwright install chromium)")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("verify_bcw")

async def verify_bcw_integration():
    """
    Run a live verification of the BCW integration.
    1. Login
    2. Check Inventory
    3. Add to Cart
    4. Get Shipping Rates
    5. Clear Cart
    """
    
    # Check credentials
    username = os.getenv("BCW_USERNAME")
    password = os.getenv("BCW_PASSWORD")

    if not username or not password:
        logger.error("❌ Environment variables BCW_USERNAME and BCW_PASSWORD are required.")
        logger.info("Please set them in your environment or .env file.")
        return

    logger.info("🚀 Starting BCW Live Integration Test...")
    
    try:
        # Initialize client (headless=True for CI/Background execution)
        # Set headless=False if you want to watch the browser locally
        async with BCWBrowserClient(headless=True) as client:
            
            # --- STEP 1: LOGIN ---
            logger.info("--- Step 1: Login ---")
            try:
                success = await client.login(username, password)
                if success:
                    logger.info("✅ Login successful")
                else:
                    logger.error("❌ Login returned False")
                    return
            except Exception as e:
                logger.error(f"❌ Login failed with exception: {e}")
                return

            # --- STEP 2: INVENTORY CHECK ---
            logger.info("--- Step 2: Inventory Check ---")
            test_sku = "1-SIL-M4" # Valid SKU: Silver Comic Bags
            test_url = "https://www.bcwsupplies.com/silver-comic-bags"
            try:
                # Use direct URL navigation instead of search
                product = await client.go_to_product(test_url)
                if product:
                    logger.info(f"✅ Product found: {product.name} (SKU: {product.bcw_sku})")
                    logger.info(f"   Stock: {product.in_stock}, Qty: {product.available_qty}, Price: ${product.price}")
                else:
                    logger.error(f"❌ Product {test_sku} not found at {test_url}")
            except Exception as e:
                logger.error(f"❌ Inventory check failed: {e}")

            # --- STEP 3: ADD TO CART ---
            logger.info("--- Step 3: Add to Cart ---")
            try:
                # Clear cart first to ensure clean state
                await client.clear_cart()
                
                # Add to cart using direct URL
                success = await client.add_to_cart(test_sku, 1, product_url=test_url)
                if success:
                    logger.info(f"✅ Added {test_sku} to cart")
                else:
                    logger.error("❌ Failed to add item to cart")
            except Exception as e:
                logger.error(f"❌ Add to cart failed: {e}")

            # --- STEP 4: SHIPPING QUOTE ---
            logger.info("--- Step 4: Shipping Quote ---")
            # Use a dummy address
            test_address = {
                "name": "MDM Test",
                "address1": "123 Test St",
                "city": "New York",
                "state": "NY",
                "zip": "10001",
                "country": "US"
            }
            try:
                rates = await client.get_shipping_rates(test_address)
                if rates:
                    logger.info(f"✅ Retrieved {len(rates)} shipping options:")
                    for rate in rates:
                        logger.info(f"   🚚 {rate.carrier} {rate.service_name}: ${rate.price}")
                else:
                    logger.warning("⚠️ No shipping rates returned. Check if cart is empty or address is invalid.")
            except Exception as e:
                logger.error(f"❌ Shipping quote failed: {e}")

            # --- STEP 5: CLEANUP ---
            logger.info("--- Step 5: Cleanup ---")
            try:
                await client.clear_cart()
                logger.info("✅ Cart cleared")
            except Exception as e:
                logger.error(f"❌ Cleanup failed: {e}")

            logger.info("🎉 Verification Complete!")

    except Exception as e:
        logger.error(f"❌ Browser Client Error: {e}")

if __name__ == "__main__":
    # Fix for Windows SelectorEventLoopPolicy which does not support subprocesses
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(verify_bcw_integration())