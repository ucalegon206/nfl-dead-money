"""
Advanced Spotrac Scraper with Stealth Browser Emulation

Uses sophisticated anti-detection techniques:
- Stealth browser profiles (undetected-chromedriver)
- Human-like behavior (delays, mouse movements, scrolling)
- Realistic headers and cookies
- Request throttling and backoff
- Multiple browser fingerprint randomization
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict, List
import time
from datetime import datetime
import random
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StealthSpotracScraper:
    """
    Advanced Spotrac scraper with stealth browser emulation.
    
    Features:
    - Undetected ChromeDriver (bypasses Selenium detection)
    - Human-like behavior patterns
    - Realistic browser fingerprint
    - Request throttling and exponential backoff
    - Cookie and session management
    - JavaScript execution delays
    """
    
    # Realistic user agents (rotated per request)
    USER_AGENTS = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]
    
    # Realistic accept headers
    ACCEPT_HEADERS = [
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    ]
    
    def __init__(self, headless: bool = True, use_undetected: bool = True):
        """
        Initialize stealth scraper.
        
        Args:
            headless: Run browser in headless mode
            use_undetected: Use undetected-chromedriver (bypass detection)
        """
        self.headless = headless
        self.use_undetected = use_undetected
        self.driver = None
        self.session_cookies = {}
        self.request_count = 0
        self.last_request_time = 0
        
    def __enter__(self):
        self._initialize_driver()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver:
            self.driver.quit()
            
    def _get_random_user_agent(self) -> str:
        """Get random user agent for request"""
        return random.choice(self.USER_AGENTS)
    
    def _get_random_headers(self) -> Dict:
        """Generate realistic request headers"""
        return {
            'User-Agent': self._get_random_user_agent(),
            'Accept': random.choice(self.ACCEPT_HEADERS),
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        }
    
    def _human_like_delay(self, min_delay: float = 1.0, max_delay: float = 4.0):
        """Simulate human reading/thinking time"""
        delay = random.uniform(min_delay, max_delay)
        logger.info(f"  ⏳ Human-like delay: {delay:.2f}s")
        time.sleep(delay)
    
    def _initialize_driver(self):
        """Initialize stealth Chrome driver"""
        try:
            if self.use_undetected:
                logger.info("  🔍 Attempting undetected-chromedriver...")
                try:
                    import undetected_chrome as uc
                    self.driver = uc.Chrome(headless=self.headless, version_main=None)
                    logger.info("  ✓ Undetected ChromeDriver initialized")
                    return
                except ImportError:
                    logger.warning("  ⚠️  undetected-chrome not installed, falling back to Selenium")
                except Exception as e:
                    logger.warning(f"  ⚠️  undetected-chrome failed: {e}, falling back to Selenium")
            
            # Fallback: Regular Selenium with stealth features
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.chrome.service import Service
            
            options = Options()
            
            # Stealth options
            if self.headless:
                options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            options.add_argument("--window-size=1920,1080")
            
            # Performance and anti-detection
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-web-resources")
            options.add_argument("--disable-sync")
            options.add_argument("--disable-plugins")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-default-apps")
            options.add_argument("--start-maximized")
            
            # Network throttling simulation
            options.add_argument("--disable-blink-features")
            options.add_argument("--disable-component-extensions-with-background-pages")
            
            # User agent
            options.add_argument(f"user-agent={self._get_random_user_agent()}")
            
            # Additional privacy flags
            options.add_argument("--disable-background-networking")
            options.add_argument("--disable-preconnect")
            options.add_argument("--disable-offline-page-prefetch")
            
            self.driver = webdriver.Chrome(options=options)
            
            # Inject stealth JavaScript
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => false,
                    });
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5],
                    });
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['en-US', 'en'],
                    });
                    window.chrome = {
                        runtime: {}
                    };
                    Object.defineProperty(navigator, 'permissions', {
                        get: () => ({
                            query: () => Promise.resolve({ state: 'granted' })
                        })
                    });
                """
            })
            
            logger.info("✓ Stealth Selenium driver initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize driver: {e}")
            raise
    
    def _throttled_request(self, url: str):
        """Make request with human-like delays and exponential backoff"""
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.by import By
        
        # Add delay between requests
        if self.request_count > 0:
            min_delay = 2.0 + (self.request_count * 0.5)  # Increasing delays
            max_delay = 4.0 + (self.request_count * 0.5)
            actual_delay = random.uniform(min_delay, max_delay)
            logger.info(f"  ⏳ Request #{self.request_count}: waiting {actual_delay:.1f}s")
            time.sleep(actual_delay)
        
        self.request_count += 1
        
        logger.info(f"Loading: {url}")
        self.driver.get(url)
        
        # Human-like initial page inspection
        self._human_like_delay(0.5, 1.5)
        
        # Scroll to trigger lazy loading
        for i in range(3):
            scroll_amount = random.randint(300, 800)
            self.driver.execute_script(f"window.scrollBy(0, {scroll_amount})")
            self._human_like_delay(0.3, 0.8)
    
    def _wait_for_table(self, timeout: int = 30) -> bool:
        """Wait for table to render with sophisticated detection"""
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.by import By
        
        try:
            # Wait for table container
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.dataTable, table[role='table']"))
            )
            
            # Wait for rows to populate
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table tbody tr"))
            )
            
            # Extra wait for DataTables JS
            time.sleep(2)
            
            # Check if content is visible (not just in DOM)
            visible = self.driver.execute_script("""
                const table = document.querySelector('table');
                if (!table) return false;
                const rows = table.querySelectorAll('tbody tr');
                return rows.length > 0 && getComputedStyle(table).display !== 'none';
            """)
            
            if visible:
                logger.info("  ✓ Table detected and visible")
                return True
            else:
                logger.warning("  ⚠️  Table in DOM but not visible")
                return False
                
        except Exception as e:
            logger.error(f"  ✗ Table wait failed: {e}")
            return False
    
    def scrape_player_rankings(self, year: int, retries: int = 3) -> Optional[pd.DataFrame]:
        """
        Scrape player rankings with sophisticated retry logic.
        
        Args:
            year: NFL season year
            retries: Number of retry attempts
            
        Returns:
            DataFrame with player data or None if failed
        """
        from bs4 import BeautifulSoup
        
        url = f"https://www.spotrac.com/nfl/rankings/player/_/year/{year}/sort/cap_total"
        
        for attempt in range(retries):
            try:
                logger.info(f"\n{'='*60}")
                logger.info(f"ATTEMPT {attempt + 1}/{retries}: Player Rankings {year}")
                logger.info(f"{'='*60}")
                
                # Load page with throttling
                self._throttled_request(url)
                
                # Wait for table with timeout that increases on retry
                timeout = 15 + (attempt * 10)
                if not self._wait_for_table(timeout=timeout):
                    raise Exception("Table did not render within timeout")
                
                # Try JavaScript injection to trigger DataTables
                logger.info("  🔧 Triggering DataTables initialization...")
                self.driver.execute_script("""
                    // Force DataTables to reinitialize
                    if (window.$ && window.$.fn.dataTable) {
                        const tables = window.$.fn.dataTable.fnTables();
                        for (let table of tables) {
                            $(table).DataTable().draw();
                        }
                    }
                """)
                time.sleep(2)
                
                # Additional scroll to trigger any lazy loading
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(1)
                
                # Parse page
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                
                # Try multiple table selectors
                table = None
                selectors = [
                    'table.dataTable tbody',
                    'table[role="table"] tbody',
                    'table tbody',
                ]
                
                for selector in selectors:
                    rows = soup.select(selector + ' tr')
                    if len(rows) >= 100:  # Expect lots of players
                        table = soup.select_one(selector)
                        logger.info(f"  ✓ Found table with {len(rows)} rows using '{selector}'")
                        break
                
                if not table:
                    logger.error(f"  ✗ No table with sufficient rows found")
                    # Save screenshot for debugging
                    self.driver.save_screenshot(f"spotrac_player_debug_{year}_attempt{attempt}.png")
                    continue
                
                # Extract data
                rows_data = []
                for tr in table.find_all('tr'):
                    tds = tr.find_all('td')
                    if len(tds) >= 3:
                        try:
                            row = [td.get_text(strip=True) for td in tds[:10]]
                            if row and row[0]:  # Non-empty first column
                                rows_data.append(row)
                        except Exception as e:
                            logger.debug(f"  Skipped row: {e}")
                
                if len(rows_data) < 100:
                    logger.error(f"  ✗ Only {len(rows_data)} rows extracted (expected 500+)")
                    self.driver.save_screenshot(f"spotrac_player_debug_{year}_attempt{attempt}.png")
                    continue
                
                logger.info(f"  ✓ Successfully extracted {len(rows_data)} player records")
                
                # Create DataFrame
                df = pd.DataFrame(rows_data)
                df['year'] = year
                logger.info(f"  ✓ Player ranking data for {year}: {len(df)} records")
                
                return df
                
            except Exception as e:
                logger.error(f"  ✗ Attempt {attempt + 1} failed: {e}")
                
                # Exponential backoff on retry
                if attempt < retries - 1:
                    wait_time = (2 ** attempt) * random.uniform(1, 2)
                    logger.info(f"  ⏳ Backing off for {wait_time:.1f}s before retry...")
                    time.sleep(wait_time)
        
        logger.error(f"Failed to scrape player rankings after {retries} attempts")
        return None
    
    def scrape_team_cap(self, year: int) -> Optional[pd.DataFrame]:
        """Scrape team cap data with stealth browser"""
        from bs4 import BeautifulSoup
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.by import By
        
        url = f"https://www.spotrac.com/nfl/cap/{year}/"
        
        try:
            logger.info(f"\nScraping team cap: {year}")
            self._throttled_request(url)
            
            # Wait for table
            if not self._wait_for_table(timeout=20):
                raise Exception("Table did not load")
            
            # Parse
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            table = soup.find('table', {'class': 'dataTable'})
            
            if not table:
                raise Exception("Team cap table not found")
            
            tbody = table.find('tbody')
            rows_data = []
            
            for tr in tbody.find_all('tr'):
                tds = tr.find_all('td')
                if len(tds) >= 5:
                    row = [td.get_text(strip=True) for td in tds[:7]]
                    rows_data.append(row)
            
            if len(rows_data) < 25:  # Should have at least most NFL teams
                raise Exception(f"Only {len(rows_data)} team records found")
            
            df = pd.DataFrame(rows_data)
            df['year'] = year
            logger.info(f"  ✓ Team cap data: {len(df)} teams")
            
            return df
            
        except Exception as e:
            logger.error(f"  ✗ Team cap scrape failed: {e}")
            return None


def test_stealth_scraper():
    """Test stealth scraper on player rankings"""
    logger.info("Starting Stealth Spotrac Scraper Test")
    
    try:
        with StealthSpotracScraper(headless=False, use_undetected=True) as scraper:
            # Test player rankings for 2024
            df = scraper.scrape_player_rankings(2024, retries=3)
            
            if df is not None:
                logger.info(f"\n✅ SUCCESS: Extracted {len(df)} player records")
                logger.info(f"Columns: {df.columns.tolist()}")
                logger.info(f"\nSample data:\n{df.head()}")
            else:
                logger.error("\n❌ FAILED: Could not extract player data")
                
    except Exception as e:
        logger.error(f"Scraper error: {e}", exc_info=True)


if __name__ == '__main__':
    test_stealth_scraper()
