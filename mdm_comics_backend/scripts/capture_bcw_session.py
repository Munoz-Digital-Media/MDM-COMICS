import asyncio
import json
import os
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from playwright.async_api import async_playwright

async def capture_session():
    """
    Launches a browser for the user to log in manually.
    Once logged in, captures cookies and saves them to 'bcw_session.json'.
    """
    print("\n" + "="*60)
    print("BCW SESSION CAPTURE UTILITY")
    print("="*60)
    print("1. A browser window will open.")
    print("2. Log in to BCW manually (solve the CAPTCHA!).")
    print("3. Once you reach the 'My Account' dashboard, this script will detect it")
    print("   and save your session cookies.")
    print("="*60 + "\n")

    async with async_playwright() as p:
        # Launch NON-HEADLESS browser so you can interact with it
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        print("🚀 Navigating to login page...")
        await page.goto("https://www.bcwsupplies.com/customer/account/login/")

        print("⏳ Waiting for successful login...")
        print("   (Please interact with the browser window now)")

        # Wait up to 5 minutes for the user to log in
        try:
            # We verify login by looking for the "Sign Out" link or the account dashboard URL
            print("🔍 Monitoring for login state (looking for 'Sign Out' or Dashboard URL)...")
            
            while True:
                # Check for the logout link
                logout_links = await page.query_selector_all("a[href*='customer/account/logout']")
                # Also check if the URL contains the account dashboard path
                current_url = page.url
                
                if logout_links or "/customer/account/" in current_url:
                    print("✅ Login detected via page state!")
                    break
                
                await asyncio.sleep(2)
                
                # Check if browser was closed by user
                if page.is_closed():
                    print("🚪 Browser closed. Capturing cookies now...")
                    break
            
            # Grab cookies
            cookies = await context.cookies()
            
            # Save to file
            output_file = "bcw_session.json"
            with open(output_file, "w") as f:
                json.dump(cookies, f, indent=2)
                
            print(f"\n🎉 SUCCESS! Session cookies saved to: {os.path.abspath(output_file)}")
            print("\nNEXT STEPS:")
            print("1. Open this JSON file.")
            print("2. Copy the ENTIRE content.")
            print("3. Paste it into a new Railway Secret Variable named: BCW_SESSION_COOKIES")
            
        except Exception as e:
            print(f"\n❌ Timed out or failed: {e}")
        
        await browser.close()

if __name__ == "__main__":
    # Windows event loop policy fix
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
    asyncio.run(capture_session())
