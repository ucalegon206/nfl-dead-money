#!/usr/bin/env python3
"""
Spotrac Dead Money Scraper with Multiple Fallback Strategies

This script tries multiple approaches to get Spotrac data:
1. Direct HTTP scraping (often blocked)
2. Browser automation with Selenium
3. Manual import helper for CSV exports

Usage:
    python scripts/download_spotrac_data.py --year 2024 --method auto
    python scripts/download_spotrac_data.py --year 2024 --method manual
    python scripts/download_spotrac_data.py --start-year 2015 --end-year 2024
    
    # New: weekly team-cap snapshot (intended for cron/Airflow weekly runs)
    python scripts/download_spotrac_data.py --snapshot-team-cap --year 2025 --method auto

    # New: one-time historical player rankings snapshot (2011-2024)
    python scripts/download_spotrac_data.py --snapshot-player-rankings --start-year 2011 --end-year 2024 --method auto
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Optional, List
import pandas as pd
import requests
from bs4 import BeautifulSoup

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def scrape_spotrac_http(year: int) -> Optional[pd.DataFrame]:
    """
    Try direct HTTP scraping (often blocked by CloudFlare).
    
    Returns player-level dead money data.
    """
    url = f"https://www.spotrac.com/nfl/dead-money/{year}/"
    logger.info(f"Attempting HTTP scrape: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        if "challenge-platform" in response.text or "cf-browser-verification" in response.text:
            logger.warning("❌ CloudFlare challenge detected - HTTP method blocked")
            return None
        
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'class': 'datatable'})
        
        if not table:
            logger.warning("❌ Could not find data table")
            return None
        
        data = []
        rows = table.find('tbody').find_all('tr')
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 4:
                try:
                    player_cell = cols[0]
                    player_name = player_cell.text.strip()
                    
                    position = cols[1].text.strip() if len(cols) > 1 else ''
                    team = cols[2].text.strip() if len(cols) > 2 else ''
                    
                    # Parse dead cap hit
                    dead_cap_text = cols[3].text.strip() if len(cols) > 3 else '0'
                    dead_cap = float(dead_cap_text.replace('$', '').replace(',', '').replace('M', ''))
                    
                    data.append({
                        'player_name': player_name,
                        'position': position,
                        'team': team,
                        'year': year,
                        'dead_cap_hit': dead_cap
                    })
                except Exception as e:
                    logger.debug(f"Error parsing row: {e}")
                    continue
        
        if data:
            df = pd.DataFrame(data)
            logger.info(f"✓ Scraped {len(df)} player records for {year}")
            return df
        
        return None
        
    except requests.RequestException as e:
        logger.error(f"❌ HTTP request failed: {e}")
        return None


def scrape_team_cap_http(year: int) -> Optional[pd.DataFrame]:
    """
    Scrape Spotrac team cap tracker for a given year.
    Normalizes columns to link with team dimension.
    """
    url = f"https://www.spotrac.com/nfl/cap/{year}/"
    logger.info(f"Attempting HTTP scrape (Team Cap): {url}")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        if "challenge-platform" in response.text or "cf-browser-verification" in response.text:
            logger.warning("❌ CloudFlare challenge detected - HTTP method blocked")
            return None

        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'class': 'datatable'})
        if not table:
            logger.warning("❌ Could not find team cap data table")
            return None

        # Parse rows
        data = []
        tbody = table.find('tbody')
        rows = tbody.find_all('tr') if tbody else []

        def parse_money(text: str) -> float:
            text = text.strip().replace('$', '').replace(',', '').replace('M', '')
            try:
                return float(text)
            except:
                return 0.0

        for r in rows:
            tds = r.find_all('td')
            if len(tds) < 5:
                continue
            team_cell = tds[0]
            team_name = team_cell.text.strip()
            active_cap = parse_money(tds[1].text)
            dead_money = parse_money(tds[2].text)
            total_cap = parse_money(tds[3].text)
            cap_space = parse_money(tds[4].text) if len(tds) > 4 else 0.0

            data.append({
                'team_name': team_name,
                'team': team_name,  # will be normalized downstream
                'year': year,
                'active_cap_millions': active_cap,
                'dead_money_millions': dead_money,
                'salary_cap_millions': total_cap,
                'cap_space_millions': cap_space,
                'dead_cap_pct': (dead_money / total_cap * 100.0) if total_cap > 0 else 0.0,
            })

        if not data:
            return None

        df = pd.DataFrame(data)
        logger.info(f"✓ Scraped {len(df)} team cap rows for {year}")
        return df

    except requests.RequestException as e:
        logger.error(f"❌ HTTP request failed: {e}")
        return None


def scrape_spotrac_selenium(year: int) -> Optional[pd.DataFrame]:
    """
    Use Selenium for browser automation (bypasses CloudFlare).
    Requires: pip install selenium
    And ChromeDriver: brew install chromedriver (Mac) or download from chromedriver.chromium.org
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except ImportError:
        logger.warning("❌ Selenium not installed. Run: pip install selenium")
        return None
    
    url = f"https://www.spotrac.com/nfl/dead-money/{year}/"
    logger.info(f"Attempting Selenium scrape: {url}")
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in background
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)
        
        # Wait for table to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "datatable"))
        )
        
        # Give JS time to render
        time.sleep(2)
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        driver.quit()
        
        table = soup.find('table', {'class': 'datatable'})
        if not table:
            logger.warning("❌ Could not find data table")
            return None
        
        data = []
        rows = table.find('tbody').find_all('tr')
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 4:
                try:
                    player_name = cols[0].text.strip()
                    position = cols[1].text.strip()
                    team = cols[2].text.strip()
                    dead_cap_text = cols[3].text.strip()
                    dead_cap = float(dead_cap_text.replace('$', '').replace(',', '').replace('M', ''))
                    
                    data.append({
                        'player_name': player_name,
                        'position': position,
                        'team': team,
                        'year': year,
                        'dead_cap_hit': dead_cap
                    })
                except Exception as e:
                    continue
        
        if data:
            df = pd.DataFrame(data)
            logger.info(f"✓ Scraped {len(df)} player records via Selenium")
            return df
        
        return None
        
    except Exception as e:
        logger.error(f"❌ Selenium scraping failed: {e}")
        return None


def scrape_team_cap_selenium(year: int) -> Optional[pd.DataFrame]:
    """
    Scrape team cap table via Selenium.
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except ImportError:
        logger.warning("❌ Selenium not installed. Run: pip install selenium")
        return None

    url = f"https://www.spotrac.com/nfl/cap/{year}/"
    logger.info(f"Attempting Selenium scrape (Team Cap): {url}")

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")

    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "datatable")))
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        driver.quit()

        table = soup.find('table', {'class': 'datatable'})
        if not table:
            logger.warning("❌ Could not find team cap data table")
            return None

        data = []
        tbody = table.find('tbody')
        rows = tbody.find_all('tr') if tbody else []

        def parse_money(text: str) -> float:
            text = text.strip().replace('$', '').replace(',', '').replace('M', '')
            try:
                return float(text)
            except:
                return 0.0

        for r in rows:
            tds = r.find_all('td')
            if len(tds) < 5:
                continue
            team_name = tds[0].text.strip()
            active_cap = parse_money(tds[1].text)
            dead_money = parse_money(tds[2].text)
            total_cap = parse_money(tds[3].text)
            cap_space = parse_money(tds[4].text) if len(tds) > 4 else 0.0

            data.append({
                'team_name': team_name,
                'team': team_name,
                'year': year,
                'active_cap_millions': active_cap,
                'dead_money_millions': dead_money,
                'salary_cap_millions': total_cap,
                'cap_space_millions': cap_space,
                'dead_cap_pct': (dead_money / total_cap * 100.0) if total_cap > 0 else 0.0,
            })

        if not data:
            return None
        df = pd.DataFrame(data)
        logger.info(f"✓ Scraped {len(df)} team cap rows via Selenium")
        return df

    except Exception as e:
        logger.error(f"❌ Selenium scraping failed: {e}")
        return None


def scrape_player_rankings_http(year: int) -> Optional[pd.DataFrame]:
    """
    Scrape player rankings for a given year sorted by total cap (cap_total).
    Normalizes columns for linkage to player/team dimensions.
    """
    url = f"https://www.spotrac.com/nfl/rankings/player/_/year/{year}/sort/cap_total"
    logger.info(f"Attempting HTTP scrape (Player Rankings): {url}")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        if "challenge-platform" in response.text or "cf-browser-verification" in response.text:
            logger.warning("❌ CloudFlare challenge detected - HTTP method blocked")
            return None
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'class': 'datatable'})
        if not table:
            logger.warning("❌ Could not find player rankings table")
            return None

        data = []
        tbody = table.find('tbody')
        rows = tbody.find_all('tr') if tbody else []

        def parse_money(text: str) -> float:
            text = text.strip().replace('$', '').replace(',', '').replace('M', '')
            try:
                return float(text)
            except:
                return 0.0

        for r in rows:
            tds = r.find_all('td')
            if len(tds) < 5:
                continue
            player_name = tds[0].text.strip()
            position = tds[1].text.strip()
            team = tds[2].text.strip()
            cap_total = parse_money(tds[3].text)
            cap_hit = parse_money(tds[4].text) if len(tds) > 4 else 0.0

            data.append({
                'player_name': player_name,
                'position': position,
                'team': team,
                'year': year,
                'cap_total_millions': cap_total,
                'cap_hit_millions': cap_hit,
            })

        if not data:
            return None
        df = pd.DataFrame(data)
        logger.info(f"✓ Scraped {len(df)} player ranking rows for {year}")
        return df

    except requests.RequestException as e:
        logger.error(f"❌ HTTP request failed: {e}")
        return None


def manual_import_helper(year: int, output_path: Path):
    """
    Print instructions for manual CSV export from Spotrac.
    """
    url = f"https://www.spotrac.com/nfl/dead-money/{year}/"
    
    print("\n" + "="*80)
    print("MANUAL DOWNLOAD INSTRUCTIONS")
    print("="*80)
    print(f"\n1. Open this URL in your browser:")
    print(f"   {url}")
    print(f"\n2. Look for an 'Export' or 'CSV' button (often top-right of table)")
    print(f"   - OR - Use browser extension like 'Table Capture' or 'Data Scraper'")
    print(f"   - OR - Copy table and paste into Excel/Google Sheets, then export CSV")
    print(f"\n3. Save the CSV file to:")
    print(f"   {output_path}")
    print(f"\n4. Required columns:")
    print(f"   - player_name (or Player)")
    print(f"   - position (or Pos)")
    print(f"   - team (or Team)")
    print(f"   - dead_cap_hit (or Dead Cap Hit, Dead Money)")
    print(f"\n5. After saving, run:")
    print(f"   python scripts/download_spotrac_data.py --year {year} --verify")
    print("\n" + "="*80 + "\n")


def verify_csv(csv_path: Path) -> bool:
    """Verify manually downloaded CSV has correct format."""
    if not csv_path.exists():
        logger.error(f"❌ File not found: {csv_path}")
        return False
    
    try:
        df = pd.read_csv(csv_path)
        logger.info(f"✓ Loaded {len(df)} records from {csv_path}")
        
        # Check required columns (flexible naming)
        required = ['player_name', 'team', 'dead_cap_hit']
        missing = []
        
        # Try common column name variations
        col_map = {
            'player': 'player_name',
            'name': 'player_name',
            'pos': 'position',
            'position': 'position',
            'team': 'team',
            'dead cap hit': 'dead_cap_hit',
            'dead money': 'dead_cap_hit',
            'dead cap': 'dead_cap_hit'
        }
        
        df.columns = [col.lower().strip() for col in df.columns]
        df = df.rename(columns=col_map)
        
        for col in ['player_name', 'team', 'dead_cap_hit']:
            if col not in df.columns:
                missing.append(col)
        
        if missing:
            logger.error(f"❌ Missing required columns: {missing}")
            logger.info(f"Available columns: {df.columns.tolist()}")
            return False
        
        logger.info(f"✓ CSV format verified")
        logger.info(f"  Columns: {df.columns.tolist()}")
        logger.info(f"  Sample:\n{df.head(3)}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error reading CSV: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Download Spotrac dead money data")
    parser.add_argument('--year', type=int, help='Single year to download')
    parser.add_argument('--start-year', type=int, default=2015, help='Start year for range')
    parser.add_argument('--end-year', type=int, default=2024, help='End year for range')
    parser.add_argument('--method', choices=['auto', 'http', 'selenium', 'manual'],
                       default='auto', help='Scraping method')
    parser.add_argument('--verify', action='store_true', help='Verify existing CSV')
    parser.add_argument('--output-dir', type=Path, default=Path('data/raw'),
                       help='Output directory for CSV files')
    # New snapshot controls
    parser.add_argument('--snapshot-team-cap', action='store_true',
                        help='Snapshot team cap tracker for given year (weekly cron/Airflow)')
    parser.add_argument('--snapshot-player-rankings', action='store_true',
                        help='One-time snapshot of player rankings across years')
    
    args = parser.parse_args()
    
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    # Single year mode
    if args.year:
        years = [args.year]
    else:
        years = range(args.start_year, args.end_year + 1)
    
    for year in years:
        # Branch: weekly team-cap snapshot
        if args.snapshot_team_cap:
            output_path = args.output_dir / f"spotrac_team_cap_{year}_{time.strftime('%Y%m%d')}.csv"
            df = None
            if args.method in ['auto', 'http']:
                logger.info(f"\n{'='*60}\nSnapshot Team Cap (HTTP) for {year}")
                df = scrape_team_cap_http(year)
            if df is None and args.method in ['auto', 'selenium']:
                logger.info(f"\n{'='*60}\nSnapshot Team Cap (Selenium) for {year}")
                df = scrape_team_cap_selenium(year)
            if df is not None:
                df.to_csv(output_path, index=False)
                logger.info(f"✓ Saved team cap snapshot to {output_path}")
                time.sleep(3)
                continue
            logger.warning(f"\n⚠️  Team cap snapshot failed for {year}")
            manual_import_helper(year, output_path)
            continue

        # Branch: one-time historical player rankings snapshot
        if args.snapshot_player_rankings:
            output_path = args.output_dir / f"spotrac_player_rankings_{year}.csv"
            df = None
            if args.method in ['auto', 'http']:
                logger.info(f"\n{'='*60}\nPlayer Rankings (HTTP) for {year}")
                df = scrape_player_rankings_http(year)
            if df is None and args.method in ['auto', 'selenium']:
                # We can reuse generic Selenium approach if needed (not implemented here for rankings)
                logger.info(f"\n{'='*60}\nPlayer Rankings Selenium not implemented; using manual fallback if needed")
            if df is not None:
                df.to_csv(output_path, index=False)
                logger.info(f"✓ Saved player rankings to {output_path}")
                time.sleep(2)
                continue
            logger.warning(f"\n⚠️  Player rankings snapshot failed for {year}")
            manual_import_helper(year, output_path)
            continue

        # Default branch: player dead money scrape (existing behavior)
        output_path = args.output_dir / f"spotrac_dead_money_{year}.csv"
        
        # Verify mode
        if args.verify:
            verify_csv(output_path)
            continue
        
        # Try scraping dead money (player-level)
        df = None
        if args.method in ['auto', 'http']:
            logger.info(f"\n{'='*60}")
            logger.info(f"Trying HTTP scrape for {year}...")
            df = scrape_spotrac_http(year)
            if df is not None:
                df.to_csv(output_path, index=False)
                logger.info(f"✓ Saved to {output_path}")
                time.sleep(3)
                continue
        if args.method in ['auto', 'selenium'] and df is None:
            logger.info(f"\n{'='*60}")
            logger.info(f"Trying Selenium scrape for {year}...")
            df = scrape_spotrac_selenium(year)
            if df is not None:
                df.to_csv(output_path, index=False)
                logger.info(f"✓ Saved to {output_path}")
                time.sleep(3)
                continue
        # Fallback to manual
        if df is None or args.method == 'manual':
            logger.warning(f"\n⚠️  Automated scraping failed for {year}")
            manual_import_helper(year, output_path)


if __name__ == '__main__':
    main()
