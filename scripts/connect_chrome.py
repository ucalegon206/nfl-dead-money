#!/usr/bin/env python3
"""
Smart approach: Use YOUR running Chrome browser via Remote Debugging Protocol

This connects to Chrome you're already using, so:
- No detection (it's already authenticated & trusted)
- No new IP signature
- Uses your real browser session
- Fastest & most reliable approach

Instructions:
1. Start Chrome with debugging enabled (see below)
2. Run this script
3. It will connect to your Chrome and scrape through it
"""

import logging
import sys
import time
from typing import Optional
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_chrome_debugging():
    """Check if Chrome is running with remote debugging"""
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('127.0.0.1', 9222))
    sock.close()
    return result == 0


def get_instructions():
    """Print instructions for starting Chrome with debugging"""
    return """
╔═══════════════════════════════════════════════════════════════════════════╗
║                    SETUP CHROME REMOTE DEBUGGING                         ║
╚═══════════════════════════════════════════════════════════════════════════╝

Chrome is not running with remote debugging. To use this approach:

1. CLOSE all Chrome windows

2. Run this command in Terminal:
   
   /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome \\
     --remote-debugging-port=9222 \\
     --user-data-dir=/tmp/chrome-debug &

   Or for a simple test (temporary profile):
   
   /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome \\
     --remote-debugging-port=9222 &

3. You'll see: [34641:34641:0718/140221.345654:ERROR:ipc_channel_posix.cc:326]
   This is normal - Chrome is running with debugging enabled.

4. Visit Spotrac in Chrome normally:
   https://www.spotrac.com/nfl/rankings/player/_/year/2024

5. Come back to Terminal and run this script:
   cd /Users/andrewsmith/Documents/portfolio/nfl-dead-money
   ./.venv/bin/python scripts/connect_chrome.py

6. The script will:
   - Connect to your running Chrome via port 9222
   - Use it to navigate Spotrac
   - Extract player rankings
   - No detection (it's your trusted browser session!)

═══════════════════════════════════════════════════════════════════════════

Why this works better than scrapers:
✅ Uses YOUR authenticated browser session
✅ No webdriver detection (it's real Chrome)
✅ Your IP is already trusted
✅ Cookies and auth are preserved
✅ JavaScript detection bypasses are unnecessary
✅ 100% more reliable than headless automation

═══════════════════════════════════════════════════════════════════════════
"""


def connect_and_scrape(year: int = 2024) -> Optional[pd.DataFrame]:
    """Connect to Chrome via remote debugging and scrape"""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.by import By
        from bs4 import BeautifulSoup
        
        logger.info("\n" + "="*70)
        logger.info("CONNECTING TO YOUR CHROME BROWSER")
        logger.info("="*70)
        
        options = Options()
        options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
        
        logger.info("  Connecting to Chrome on port 9222...")
        try:
            driver = webdriver.Chrome(options=options)
            logger.info("  ✅ Connected to your Chrome instance!")
        except Exception as e:
            logger.error(f"  ✗ Failed to connect: {e}")
            logger.error("\n" + get_instructions())
            return None
        
        url = f"https://www.spotrac.com/nfl/rankings/player/_/year/{year}/sort/cap_total"
        logger.info(f"\n  Loading: {url}")
        
        try:
            driver.get(url)
        except Exception as e:
            logger.error(f"  Error navigating: {e}")
            return None
        
        logger.info("  ⏳ Waiting for table to load...")
        
        # Wait for table
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.dataTable tbody tr"))
            )
            logger.info("  ✓ Table detected")
        except:
            logger.warning("  ⚠️  Table selector timeout, checking alternatives...")
        
        time.sleep(2)
        
        # Scroll to trigger lazy loading
        logger.info("  Scrolling to trigger lazy loading...")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(1)
        
        # Parse
        logger.info("  Extracting data...")
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Try multiple table selectors
        table = None
        for selector in ['table.dataTable', 'table[role="table"]', 'table']:
            table = soup.select_one(selector)
            if table and table.find('tbody'):
                break
        
        if not table or not table.find('tbody'):
            logger.error("  ✗ Table not found in page source")
            logger.error(f"  Page contains: {len(soup.find_all('table'))} tables")
            return None
        
        tbody = table.find('tbody')
        rows = []
        
        for tr in tbody.find_all('tr'):
            tds = tr.find_all('td')
            if len(tds) >= 3:
                try:
                    row = [td.get_text(strip=True) for td in tds[:10]]
                    if row and row[0]:  # Non-empty first column
                        rows.append(row)
                except:
                    pass
        
        if len(rows) < 100:
            logger.error(f"  ✗ Only {len(rows)} rows extracted (expected 500+)")
            return None
        
        logger.info(f"  ✅ Extracted {len(rows)} player records!")
        
        df = pd.DataFrame(rows)
        df['year'] = year
        
        logger.info("\n" + "="*70)
        logger.info(f"✅ SUCCESS! Extracted {len(df)} records for {year}")
        logger.info(f"Columns: {len(df.columns)}")
        logger.info(f"\nFirst 5 rows:\n{df.head()}")
        logger.info("="*70)
        
        return df
        
    except Exception as e:
        logger.error(f"  ✗ Error: {e}", exc_info=True)
        return None


def main():
    logger.info("\n" + "╔" + "="*68 + "╗")
    logger.info("║" + " CONNECT TO YOUR CHROME BROWSER ".center(68) + "║")
    logger.info("║" + " (Remote Debugging Protocol) ".center(68) + "║")
    logger.info("╚" + "="*68 + "╝")
    
    # Check if Chrome is running with debugging
    if not check_chrome_debugging():
        logger.error("\n❌ Chrome is not running with remote debugging\n")
        logger.info(get_instructions())
        return 1
    
    logger.info("✅ Chrome remote debugging detected on port 9222\n")
    
    # Try to scrape
    df = connect_and_scrape(2024)
    
    if df is not None:
        logger.info("\n✅ Success! Saving data...")
        output_path = Path('data/raw/player_rankings_2024_chrome_debug.csv')
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        logger.info(f"Saved to: {output_path}")
        return 0
    else:
        logger.error("\n❌ Failed to extract data")
        return 1


if __name__ == '__main__':
    from pathlib import Path
    sys.exit(main())
