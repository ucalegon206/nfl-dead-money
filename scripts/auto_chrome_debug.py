#!/usr/bin/env python3
"""
Automated Chrome Remote Debugging Setup + Scraper
Handles all steps automatically in one script.
"""

import subprocess
import time
import logging
import json
import socket
from pathlib import Path
import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def find_free_port(start=9222):
    """Find a free port starting from start"""
    port = start
    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('127.0.0.1', port))
            sock.close()
            if result != 0:  # Port is free
                return port
            port += 1
        except:
            port += 1


def start_chrome(debug_port=9222):
    """Start Chrome with remote debugging enabled"""
    logger.info(f"\n🚀 Starting Chrome with remote debugging on port {debug_port}...")
    
    # Find Chrome path
    import platform
    if platform.system() == "Darwin":  # macOS
        chrome_paths = [
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Chromium.app/Contents/MacOS/Chromium"
        ]
    elif platform.system() == "Windows":
        chrome_paths = [
            "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
            "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe"
        ]
    else:  # Linux
        chrome_paths = ["/usr/bin/google-chrome", "/usr/bin/chromium"]
    
    chrome_path = None
    for path in chrome_paths:
        if Path(path).exists():
            chrome_path = path
            break
    
    if not chrome_path:
        logger.error("❌ Chrome not found")
        return None
    
    # Kill any existing Chrome instances to avoid conflicts
    logger.info("  Cleaning up any existing Chrome instances...")
    subprocess.run("pkill -f 'Google Chrome'", shell=True, stderr=subprocess.DEVNULL)
    time.sleep(2)
    
    # Start Chrome with debugging
    cmd = [
        chrome_path,
        f"--remote-debugging-port={debug_port}",
        "--no-sandbox",
        "--disable-gpu",
        "--disable-dev-shm-usage"
    ]
    
    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info(f"✓ Chrome started (PID: {process.pid})")
        time.sleep(3)  # Let Chrome initialize
        return process
    except Exception as e:
        logger.error(f"❌ Failed to start Chrome: {e}")
        return None


def get_chrome_websocket_url(debug_port=9222):
    """Get WebSocket URL from Chrome's debugging endpoint"""
    for attempt in range(10):
        try:
            response = requests.get(f"http://127.0.0.1:{debug_port}/json/version", timeout=2)
            data = response.json()
            return data.get('webSocketDebuggerUrl')
        except:
            if attempt < 9:
                logger.info(f"  Waiting for Chrome... ({attempt + 1}/10)")
                time.sleep(1)
    return None


def scrape_with_chrome_debug(year=2024, debug_port=9222):
    """Scrape using Chrome remote debugging"""
    
    # Start Chrome
    chrome_process = start_chrome(debug_port)
    if not chrome_process:
        return None
    
    try:
        # Get WebSocket URL
        logger.info("🔗 Connecting to Chrome...")
        ws_url = get_chrome_websocket_url(debug_port)
        
        if not ws_url:
            logger.error("❌ Could not connect to Chrome")
            return None
        
        logger.info(f"✓ Connected: {ws_url.split('/')[-1][:20]}...")
        
        # Now use chrome-remote-interface or pyppeteer
        try:
            import pyppeteer
            logger.info("📖 Using pyppeteer to scrape...")
            
            import asyncio
            
            async def scrape():
                # Launch browser using existing Chrome
                browser = await pyppeteer.connect(browserWSEndpoint=ws_url)
                page = await browser.newPage()
                
                # Set viewport
                await page.setViewport({'width': 1920, 'height': 1080})
                
                # Navigate
                url = f"https://www.spotrac.com/nfl/rankings/player/_/year/{year}/sort/cap_total"
                logger.info(f"📄 Loading: {url}")
                await page.goto(url, waitUntil='networkidle2')
                
                logger.info("⏳ Waiting for table (30 seconds)...")
                try:
                    await page.waitForSelector('table.dataTable tbody tr', timeout=30000)
                    logger.info("✓ Table detected!")
                except:
                    logger.warning("⚠️  Table not detected, continuing...")
                
                # Scroll
                await page.evaluate('() => window.scrollTo(0, document.body.scrollHeight)')
                await asyncio.sleep(2)
                
                # Get HTML
                content = await page.content()
                
                # Parse
                soup = BeautifulSoup(content, 'html.parser')
                table = soup.select_one('table.dataTable')
                
                if not table:
                    logger.error("❌ No table found")
                    await browser.close()
                    return None
                
                rows = []
                for tr in table.select('tbody tr'):
                    tds = tr.find_all('td')
                    if len(tds) >= 3:
                        try:
                            row = [td.get_text(strip=True) for td in tds[:10]]
                            if row and row[0]:
                                rows.append(row)
                        except:
                            pass
                
                await browser.close()
                return rows
            
            # Run async scraper
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            rows = loop.run_until_complete(scrape())
            
            if rows:
                logger.info(f"✓ Extracted {len(rows)} rows")
                return rows
            else:
                logger.error("❌ Failed to extract data")
                return None
                
        except ImportError:
            logger.error("❌ pyppeteer not installed. Install with: pip install pyppeteer")
            return None
    
    finally:
        logger.info("\n🛑 Stopping Chrome...")
        chrome_process.terminate()
        chrome_process.wait(timeout=5)


def main():
    logger.info("\n" + "="*70)
    logger.info("AUTOMATED CHROME REMOTE DEBUGGING SCRAPER")
    logger.info("="*70)
    
    year = 2024
    debug_port = find_free_port(9222)
    
    logger.info(f"\nConfiguration:")
    logger.info(f"  Year: {year}")
    logger.info(f"  Debug port: {debug_port}")
    logger.info("\nThis script will:")
    logger.info("  1. Kill existing Chrome instances")
    logger.info("  2. Start Chrome with remote debugging")
    logger.info("  3. Load Spotrac player rankings")
    logger.info("  4. Extract table data")
    logger.info("  5. Clean up Chrome")
    
    input("\nPress Enter to start... (or Ctrl+C to cancel)")
    
    rows = scrape_with_chrome_debug(year, debug_port)
    
    if rows and len(rows) > 100:
        logger.info(f"\n✅ SUCCESS: Extracted {len(rows)} player records")
        # Save to CSV
        output_path = Path("data/raw") / f"player_rankings_{year}.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        import csv
        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(rows)
        
        logger.info(f"📁 Saved to: {output_path}")
    else:
        logger.error(f"\n❌ FAILED: Only extracted {len(rows) if rows else 0} rows")


if __name__ == "__main__":
    main()
