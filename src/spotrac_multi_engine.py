"""
Multi-Engine Spotrac Scraper

Tries multiple browser engines and approaches:
1. Chrome Remote Debugging Protocol (direct connection to running Chrome)
2. Puppeteer/pyppeteer (JavaScript-native automation)
3. Geckodriver Firefox (different engine, different detection)
4. Selenium with aggressive fingerprint randomization
"""

import logging
import asyncio
import random
import time
from pathlib import Path
from typing import Optional
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MultiEngineSpotracScraper:
    """Try multiple browser engines to bypass detection"""
    
    def __init__(self):
        self.driver = None
        self.session = None
        
    async def try_puppeteer(self, year: int) -> Optional[pd.DataFrame]:
        """Try Puppeteer (JavaScript-native, less detectable)"""
        logger.info("\n🔧 Attempting Puppeteer approach...")
        try:
            from pyppeteer import launch
            from bs4 import BeautifulSoup
            
            logger.info("  Launching Puppeteer browser...")
            browser = await launch(
                headless=True,
                args=['--no-sandbox', '--disable-setuid-sandbox'],
                ignoreHTTPSErrors=True,
            )
            
            page = await browser.newPage()
            
            # Set realistic user agent
            await page.setUserAgent(self._get_user_agent())
            
            # Set realistic viewport
            await page.setViewport({'width': 1920, 'height': 1080})
            
            url = f"https://www.spotrac.com/nfl/rankings/player/_/year/{year}/sort/cap_total"
            logger.info(f"  Loading: {url}")
            
            # Navigate with longer timeout
            await page.goto(url, {'waitUntil': 'networkidle2', 'timeout': 30000})
            
            # Wait for table
            logger.info("  Waiting for table to render...")
            try:
                await page.waitForSelector('table.dataTable tbody tr', {'timeout': 15000})
            except:
                logger.warning("  Table selector not found, trying alternatives...")
                
            # Scroll to trigger lazy loading
            await page.evaluate('() => window.scrollTo(0, document.body.scrollHeight)')
            await asyncio.sleep(2)
            
            # Extract content
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            # Try to find table
            table = soup.find('table', {'class': 'dataTable'})
            if not table:
                table = soup.find('table', {'role': 'table'})
            
            if not table:
                logger.error("  ✗ No table found")
                await browser.close()
                return None
            
            tbody = table.find('tbody')
            if not tbody:
                logger.error("  ✗ No tbody found")
                await browser.close()
                return None
            
            # Extract rows
            rows = []
            for tr in tbody.find_all('tr'):
                tds = tr.find_all('td')
                if len(tds) >= 3:
                    row = [td.get_text(strip=True) for td in tds[:10]]
                    rows.append(row)
            
            await browser.close()
            
            if len(rows) < 100:
                logger.error(f"  ✗ Only {len(rows)} rows (expected 500+)")
                return None
            
            logger.info(f"  ✅ Puppeteer success! {len(rows)} records")
            df = pd.DataFrame(rows)
            df['year'] = year
            return df
            
        except ImportError:
            logger.warning("  ⚠️  pyppeteer not installed")
            return None
        except Exception as e:
            logger.error(f"  ✗ Puppeteer failed: {e}")
            return None
    
    def try_firefox(self, year: int) -> Optional[pd.DataFrame]:
        """Try Firefox with Geckodriver (different engine, different detection)"""
        logger.info("\n🦊 Attempting Firefox approach...")
        try:
            from selenium import webdriver
            from selenium.webdriver.firefox.options import Options
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.common.by import By
            from bs4 import BeautifulSoup
            
            options = Options()
            options.headless = True
            options.add_argument('--width=1920')
            options.add_argument('--height=1080')
            options.set_preference('general.useragent.override', self._get_user_agent())
            
            logger.info("  Launching Firefox...")
            driver = webdriver.Firefox(options=options)
            
            url = f"https://www.spotrac.com/nfl/rankings/player/_/year/{year}/sort/cap_total"
            logger.info(f"  Loading: {url}")
            driver.get(url)
            
            # Wait for table
            logger.info("  Waiting for table...")
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table.dataTable tbody tr"))
                )
            except:
                logger.warning("  Timeout waiting for table")
            
            time.sleep(2)
            
            # Scroll
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(1)
            
            # Parse
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            table = soup.find('table', {'class': 'dataTable'})
            
            if not table or not table.find('tbody'):
                logger.error("  ✗ Table not found")
                driver.quit()
                return None
            
            rows = []
            for tr in table.find('tbody').find_all('tr'):
                tds = tr.find_all('td')
                if len(tds) >= 3:
                    row = [td.get_text(strip=True) for td in tds[:10]]
                    rows.append(row)
            
            driver.quit()
            
            if len(rows) < 100:
                logger.error(f"  ✗ Only {len(rows)} rows")
                return None
            
            logger.info(f"  ✅ Firefox success! {len(rows)} records")
            df = pd.DataFrame(rows)
            df['year'] = year
            return df
            
        except ImportError:
            logger.warning("  ⚠️  geckodriver not found in PATH")
            return None
        except Exception as e:
            logger.error(f"  ✗ Firefox failed: {e}")
            return None
    
    def try_chrome_remote_debug(self, year: int) -> Optional[pd.DataFrame]:
        """Try connecting to Chrome via remote debugging protocol"""
        logger.info("\n🔌 Attempting Chrome Remote Debugging Protocol...")
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.common.by import By
            from bs4 import BeautifulSoup
            
            # Try to connect to running Chrome instance
            options = Options()
            options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
            
            logger.info("  Connecting to Chrome on port 9222...")
            try:
                driver = webdriver.Chrome(options=options)
                logger.info("  ✅ Connected to existing Chrome instance!")
            except:
                logger.warning("  ⚠️  No Chrome instance on port 9222")
                logger.info("     To use this method, start Chrome with:")
                logger.info("     /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-debug")
                return None
            
            url = f"https://www.spotrac.com/nfl/rankings/player/_/year/{year}/sort/cap_total"
            logger.info(f"  Loading: {url}")
            driver.get(url)
            
            # Wait for table
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table.dataTable tbody tr"))
                )
            except:
                logger.warning("  Table not found")
            
            time.sleep(2)
            
            # Parse
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            table = soup.find('table', {'class': 'dataTable'})
            
            if not table or not table.find('tbody'):
                logger.error("  ✗ Table not found")
                return None
            
            rows = []
            for tr in table.find('tbody').find_all('tr'):
                tds = tr.find_all('td')
                if len(tds) >= 3:
                    row = [td.get_text(strip=True) for td in tds[:10]]
                    rows.append(row)
            
            if len(rows) < 100:
                logger.error(f"  ✗ Only {len(rows)} rows")
                return None
            
            logger.info(f"  ✅ Remote Debug success! {len(rows)} records")
            df = pd.DataFrame(rows)
            df['year'] = year
            return df
            
        except Exception as e:
            logger.error(f"  ✗ Remote debug failed: {e}")
            return None
    
    def try_selenium_aggressive(self, year: int) -> Optional[pd.DataFrame]:
        """Try Selenium with aggressive fingerprint randomization"""
        logger.info("\n⚡ Attempting Selenium with aggressive evasion...")
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.common.by import By
            from bs4 import BeautifulSoup
            
            options = Options()
            options.headless = True
            
            # Aggressive anti-detection
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            # Random window size
            width = random.choice([1920, 1440, 1366, 1680])
            height = random.choice([1080, 900, 768, 1050])
            options.add_argument(f"--window-size={width},{height}")
            
            # Random user agent
            options.add_argument(f"user-agent={self._get_user_agent()}")
            
            # Additional flags
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            
            logger.info(f"  Launching Chrome ({width}x{height})...")
            driver = webdriver.Chrome(options=options)
            
            # Inject anti-detection scripts
            driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', { get: () => false });
                    Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
                    Object.defineProperty(navigator, 'languages', { get: () => ['en-US'] });
                    window.chrome = { runtime: {} };
                    
                    // Spoof canvas
                    const canvas = HTMLCanvasElement.prototype;
                    const ctx = canvas.getContext;
                    canvas.getContext = function(contextType) {
                        const context = ctx.call(this, contextType);
                        if (contextType === '2d') {
                            context.fillText = () => {};
                        }
                        return context;
                    };
                """
            })
            
            url = f"https://www.spotrac.com/nfl/rankings/player/_/year/{year}/sort/cap_total"
            logger.info(f"  Loading: {url}")
            driver.get(url)
            
            # Random delay
            time.sleep(random.uniform(1, 3))
            
            # Wait for table
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table.dataTable tbody tr"))
                )
            except:
                logger.warning("  Table load timeout")
            
            time.sleep(random.uniform(1, 2))
            
            # Scroll
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(1)
            
            # Parse
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            table = soup.find('table', {'class': 'dataTable'})
            
            if not table or not table.find('tbody'):
                logger.error("  ✗ Table not found")
                driver.quit()
                return None
            
            rows = []
            for tr in table.find('tbody').find_all('tr'):
                tds = tr.find_all('td')
                if len(tds) >= 3:
                    row = [td.get_text(strip=True) for td in tds[:10]]
                    rows.append(row)
            
            driver.quit()
            
            if len(rows) < 100:
                logger.error(f"  ✗ Only {len(rows)} rows")
                return None
            
            logger.info(f"  ✅ Selenium success! {len(rows)} records")
            df = pd.DataFrame(rows)
            df['year'] = year
            return df
            
        except Exception as e:
            logger.error(f"  ✗ Selenium failed: {e}")
            return None
    
    def _get_user_agent(self) -> str:
        """Random realistic user agent"""
        agents = [
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        ]
        return random.choice(agents)
    
    async def scrape_player_rankings_multiengine(self, year: int) -> Optional[pd.DataFrame]:
        """Try all engines in sequence"""
        logger.info("\n" + "="*70)
        logger.info(f"MULTI-ENGINE SCRAPING: Player Rankings {year}")
        logger.info("="*70)
        
        # Try each approach
        approaches = [
            ("Chrome Remote Debug", self.try_chrome_remote_debug),
            ("Puppeteer (JavaScript)", self.try_puppeteer),
            ("Firefox (Geckodriver)", self.try_firefox),
            ("Selenium (Aggressive)", self.try_selenium_aggressive),
        ]
        
        for name, method in approaches:
            logger.info(f"\n{'─'*70}")
            try:
                if asyncio.iscoroutinefunction(method):
                    result = await method(year)
                else:
                    result = method(year)
                
                if result is not None:
                    logger.info(f"\n{'='*70}")
                    logger.info(f"✅ SUCCESS via {name}!")
                    logger.info(f"{'='*70}")
                    return result
            except Exception as e:
                logger.error(f"  Exception: {e}")
                continue
        
        logger.info(f"\n{'='*70}")
        logger.error("❌ All approaches failed")
        logger.info(f"{'='*70}")
        return None


async def main():
    scraper = MultiEngineSpotracScraper()
    
    logger.info("\n" + "╔" + "="*68 + "╗")
    logger.info("║" + " MULTI-ENGINE SPOTRAC SCRAPER ".center(68) + "║")
    logger.info("║" + " Tries 4 different approaches to bypass detection ".center(68) + "║")
    logger.info("╚" + "="*68 + "╝")
    
    df = await scraper.scrape_player_rankings_multiengine(2024)
    
    if df is not None:
        logger.info(f"\n✅ EXTRACTION SUCCESSFUL")
        logger.info(f"   Records: {len(df)}")
        logger.info(f"   Columns: {df.columns.tolist()}")
        logger.info(f"\n{df.head()}")
    else:
        logger.error(f"\n❌ EXTRACTION FAILED")
        logger.info("\nNext options:")
        logger.info("  1. Try manual export from Spotrac (5 min, 100% reliable)")
        logger.info("  2. Use team-level data we have (complete & validated)")
        logger.info("  3. Start Chrome with debugging:")
        logger.info("     /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome \\")
        logger.info("       --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-debug")
        logger.info("     Then re-run this script")


if __name__ == '__main__':
    asyncio.run(main())
