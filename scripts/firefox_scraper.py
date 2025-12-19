#!/usr/bin/env python3
"""
Firefox Player Rankings Scraper

Uses Firefox instead of Chrome - avoids multi-instance Chrome conflicts.
Firefox has slightly different detection signatures, sometimes works better.
"""

import logging
import sys
import time
from pathlib import Path
from typing import Optional
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def scrape_with_firefox(year: int = 2024) -> Optional[pd.DataFrame]:
    """Scrape player rankings using Firefox"""
    try:
        from selenium import webdriver
        from selenium.webdriver.firefox.options import Options
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.by import By
        from bs4 import BeautifulSoup
        
        logger.info("\n" + "="*70)
        logger.info("FIREFOX PLAYER RANKINGS SCRAPER")
        logger.info("="*70)
        
        logger.info("\n🦊 Initializing Firefox...")
        
        options = Options()
        options.headless = False  # Show browser so you can see what's happening
        
        # Disable some detection mechanisms
        options.set_preference("dom.webdriver.enabled", False)
        options.set_preference("useAutomationExtension", False)
        
        # Random window size
        import random
        width = random.choice([1920, 1440, 1366])
        height = random.choice([1080, 900, 768])
        logger.info(f"  Window size: {width}x{height}")
        
        driver = webdriver.Firefox(options=options)
        
        url = f"https://www.spotrac.com/nfl/rankings/player/_/year/{year}/sort/cap_total"
        logger.info(f"\n📄 Loading: {url}")
        
        driver.get(url)
        
        logger.info("⏳ Waiting for table to load (up to 30 seconds)...")
        
        try:
            # Wait for table
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.dataTable tbody tr"))
            )
            logger.info("✓ Table detected!")
        except Exception as e:
            logger.warning(f"⚠️  Table detection timeout: {e}")
            logger.info("Continuing anyway...")
        
        # Give page time to fully render
        time.sleep(2)
        
        # Scroll to trigger lazy loading (safer approach)
        logger.info("📜 Scrolling to trigger lazy loading...")
        try:
            driver.execute_script("document.documentElement.scrollTop = document.documentElement.scrollHeight")
            time.sleep(2)
        except Exception as e:
            logger.debug(f"  Scroll error (continuing): {e}")
        
        # Parse with BeautifulSoup
        logger.info("🔍 Extracting data from page...")
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Find table - try multiple selectors
        table = None
        selectors = [
            'table.dataTable',
            'table[class*="dataTable"]',
            'div.dataTables_wrapper table',
            'table[role="table"]',
            'table'
        ]
        
        for selector in selectors:
            candidates = soup.select(selector)
            for candidate in candidates:
                if candidate.find('tbody'):
                    table = candidate
                    logger.info(f"  Found table using selector: {selector}")
                    break
            if table:
                break
        
        if not table:
            logger.error("❌ No table found in page")
            tables = soup.find_all('table')
            logger.error(f"  Page contains {len(tables)} tables total")
            for i, t in enumerate(tables[:3]):
                logger.debug(f"    Table {i}: {t.get('class')} - {len(t.find_all('tr'))} rows")
            driver.quit()
            return None
        
        tbody = table.find('tbody')
        if not tbody:
            logger.error("❌ Table has no tbody")
            driver.quit()
            return None
        
        # Extract rows
        rows = []
        for tr in tbody.find_all('tr'):
            tds = tr.find_all('td')
            if len(tds) >= 3:
                try:
                    row = [td.get_text(strip=True) for td in tds[:10]]
                    if row and row[0]:  # Non-empty first column
                        rows.append(row)
                except Exception as e:
                    logger.debug(f"  Skipped row: {e}")
        
        driver.quit()
        
        if len(rows) < 100:
            logger.error(f"❌ Only extracted {len(rows)} rows (expected 500+)")
            return None
        
        logger.info(f"\n✅ Successfully extracted {len(rows)} player records!")
        
        df = pd.DataFrame(rows)
        df['year'] = year
        
        logger.info(f"\n📊 Data summary:")
        logger.info(f"   Records: {len(df)}")
        logger.info(f"   Columns: {len(df.columns)}")
        logger.info(f"\n   First 5 rows:")
        for i, row in enumerate(df.head().values, 1):
            logger.info(f"     {i}. {row[:3]}")  # Show first 3 columns
        
        return df
        
    except ImportError as e:
        logger.error(f"❌ Missing dependency: {e}")
        logger.info("\nTo install geckodriver:")
        logger.info("  brew install geckodriver")
        return None
    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        return None


def main():
    logger.info("\n" + "╔" + "="*68 + "╗")
    logger.info("║" + " FIREFOX SPOTRAC SCRAPER ".center(68) + "║")
    logger.info("║" + " (No Chrome conflicts, simpler approach) ".center(68) + "║")
    logger.info("╚" + "="*68 + "╝")
    
    logger.info("\n📋 How this works:")
    logger.info("  1. Firefox opens in a normal window")
    logger.info("  2. Loads Spotrac player rankings page")
    logger.info("  3. You can watch it load in real-time")
    logger.info("  4. Script extracts the table data")
    logger.info("  5. Saves to CSV file")
    
    logger.info("\n⏱️  Estimated time: 30-60 seconds")
    
    # Scrape
    df = scrape_with_firefox(2024)
    
    if df is not None:
        logger.info("\n" + "="*70)
        logger.info("✅ SUCCESS!")
        logger.info("="*70)
        
        # Save
        output_dir = Path('data/raw')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_path = output_dir / 'player_rankings_2024_firefox.csv'
        df.to_csv(output_path, index=False)
        logger.info(f"\n📁 Saved to: {output_path}")
        logger.info(f"   Records: {len(df)}")
        
        return 0
    else:
        logger.error("\n" + "="*70)
        logger.error("❌ FAILED TO SCRAPE")
        logger.error("="*70)
        return 1


if __name__ == '__main__':
    sys.exit(main())
