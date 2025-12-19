#!/usr/bin/env python3
"""
Firefox Diagnostic: Inspect actual page structure
"""

import logging
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

options = Options()
options.headless = False

driver = webdriver.Firefox(options=options)
url = "https://www.spotrac.com/nfl/rankings/player/_/year/2024/sort/cap_total"

logger.info(f"Loading {url}...")
driver.get(url)

logger.info("Waiting 10 seconds for page to load...")
time.sleep(10)

# Get page content
html = driver.page_source

logger.info(f"\n📊 PAGE ANALYSIS:")
logger.info(f"  Page size: {len(html)} bytes")

soup = BeautifulSoup(html, 'html.parser')

# Check for iframes
iframes = soup.find_all('iframe')
logger.info(f"  Iframes: {len(iframes)}")
for i, iframe in enumerate(iframes[:3]):
    logger.info(f"    {i}: {iframe.get('src', 'no src')[:100]}")

# Check for divs with data
divs_with_data = soup.find_all('div', {'data-testid': True})
logger.info(f"  Divs with data-testid: {len(divs_with_data)}")

# Check for role=table
role_tables = soup.find_all(role='table')
logger.info(f"  Elements with role='table': {len(role_tables)}")

# Check for class containing "table"
class_tables = []
for tag in soup.find_all():
    if tag.get('class'):
        if any('table' in cls.lower() for cls in tag.get('class', [])):
            class_tables.append(tag)
logger.info(f"  Elements with 'table' in class: {len(class_tables)}")

# Check for specific patterns
logger.info(f"\n🔍 SEARCHING FOR DATA PATTERNS:")

# Look for script tags with data
scripts = soup.find_all('script')
logger.info(f"  Script tags: {len(scripts)}")
for script in scripts:
    if script.string and 'player' in script.string.lower() and len(script.string) < 5000:
        logger.info(f"    Found player data in script (length: {len(script.string)})")

# Look for any player names
for name in ['Jalen Hurts', 'Josh Allen', 'Lamar Jackson']:
    if name in html:
        logger.info(f"  Found player name '{name}' in page")

# Check page structure
logger.info(f"\n📄 BODY STRUCTURE:")
body = soup.find('body')
if body:
    # Get immediate children
    children = [tag.name for tag in body.children if hasattr(tag, 'name') and tag.name]
    logger.info(f"  Body children: {children[:10]}")

logger.info("\n✅ Diagnostic complete. Keeping browser open for inspection...")
logger.info("Inspect the browser window to see what's actually rendered.")
input("Press Enter to close...")

driver.quit()
