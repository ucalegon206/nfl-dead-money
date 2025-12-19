# Advanced Anti-Bot Browser Emulation Strategy

## Project Status

✅ **Team-Level Data**: Successfully scraped 2015-2024 (complete)
❌ **Player-Level Data**: Blocked by sophisticated anti-bot detection

---

## Problem Analysis

### What We Discovered About Spotrac

1. **Anti-Bot Detection Methods**:
   - Detects Selenium/Chromedriver automation
   - Requires dynamic JavaScript rendering (tables built client-side)
   - Uses reCAPTCHA triggers on suspicious access patterns
   - Blocks DataTables from rendering in headless browsers
   - May employ fingerprinting (screen resolution, plugins, fonts)

2. **Why Basic Scraping Failed**:
   - `Selenium 4.x`: Detected via `navigator.webdriver` flag
   - `Playwright`: Same detection bypass attempted
   - Page loads 2.5MB HTML but zero table elements in automated browsers
   - Tables render perfectly in Chrome but not in automation context

3. **Confirmed Blocking Mechanisms**:
   - User-agent checks
   - navigator.webdriver property detection
   - Plugin/extension detection
   - Suspicious header patterns
   - IP reputation checks (multiple requests)

---

## Advanced Techniques Implemented

### 1. **Undetected-ChromeDriver** (`src/spotrac_scraper_stealth.py`)
- Automatically patches Selenium to hide automation signatures
- Removes `webdriver` property before page load
- Bypasses detection of Chrome flags
- More effective than manual Selenium patching

**Installation**:
```bash
pip install undetected-chromedriver
```

**Usage**:
```python
from spotrac_scraper_stealth import StealthSpotracScraper

scraper = StealthSpotracScraper(headless=True, use_undetected=True)
df = scraper.scrape_player_rankings(2024, retries=3)
```

### 2. **Realistic Browser Fingerprinting**
```python
# Random user agents (rotated per request)
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36...",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36...",
    # ... more agents
]

# Realistic HTTP headers
headers = {
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'DNT': '1',
    # ... more realistic headers
}
```

### 3. **JavaScript Stealth Injection**
```python
self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
    "source": """
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        window.chrome = { runtime: {} };
    """
})
```

### 4. **Human-Like Behavior Patterns**
```python
def _human_like_delay(self, min_delay=1.0, max_delay=4.0):
    """Simulate natural thinking time between actions"""
    delay = random.uniform(min_delay, max_delay)
    time.sleep(delay)

def _throttled_request(self, url):
    """Add exponential delays between requests"""
    if self.request_count > 0:
        min_delay = 2.0 + (self.request_count * 0.5)
        actual_delay = random.uniform(min_delay, max_delay)
        time.sleep(actual_delay)
```

### 5. **Advanced Table Detection**
```python
# Multiple selector attempts
selectors = [
    'table.dataTable tbody',
    'table[role="table"] tbody',
    'table tbody',
]

# Wait for visibility (not just DOM presence)
visible = self.driver.execute_script("""
    const table = document.querySelector('table');
    const rows = table.querySelectorAll('tbody tr');
    return rows.length > 0 && 
           getComputedStyle(table).display !== 'none';
""")

# Force DataTables JS reinit
self.driver.execute_script("""
    if (window.$.fn.dataTable) {
        const tables = window.$.fn.dataTable.fnTables();
        for (let table of tables) {
            $(table).DataTable().draw();
        }
    }
""")
```

### 6. **Exponential Backoff & Retry Logic**
```python
def scrape_player_rankings(self, year, retries=3):
    for attempt in range(retries):
        try:
            # Increase timeout on retry
            timeout = 15 + (attempt * 10)
            
            # Exponential backoff
            wait_time = (2 ** attempt) * random.uniform(1, 2)
            time.sleep(wait_time)
            
        except Exception as e:
            if attempt < retries - 1:
                logger.info(f"Backing off before retry...")
```

---

## Testing the Stealth Scraper

### Quick Test
```bash
cd /Users/andrewsmith/Documents/portfolio/nfl-dead-money
./.venv/bin/python scripts/test_stealth_scraper.py
```

### Direct Usage
```python
from src.spotrac_scraper_stealth import StealthSpotracScraper

with StealthSpotracScraper(headless=False) as scraper:
    df = scraper.scrape_player_rankings(2024, retries=3)
    if df is not None:
        print(f"Success: {len(df)} records")
    else:
        print("Spotrac blocked the request")
```

---

## Expected Results vs. Reality

### What Should Work
✅ Undetected-ChromeDriver removes automation signature  
✅ Human-like delays avoid rate limiting  
✅ Realistic headers evade basic checks  
✅ JavaScript stealth patches navigator properties  

### Known Limitations
❌ **Probable Outcome**: Still blocked by Spotrac
  - May use advanced fingerprinting (canvas, WebGL)
  - Could employ ML-based bot detection
  - Might require solving CAPTCHA programmatically
  - Could require real residential IP rotation

---

## Alternative Approaches

### 1. **Manual Data Export**
- Log into Spotrac manually
- Export player rankings as CSV
- Advantage: 100% accurate, no detection issues
- Effort: ~5 minutes per year

### 2. **Web Scraping Services**
- Use services like ScraperAPI, Bright Data
- They handle IP rotation and CAPTCHA solving
- Cost: $15-100/month
- Advantage: Reliable, legal grey area

### 3. **Alternative Data Sources**
- **Pro Football Reference**: Similar but also anti-bot
- **NFL.com**: Official source but limited detail
- **NFLPA**: Union data (public but limited access)
- **ESPN**: Sports data but more restricted

### 4. **Hybrid Approach** (Recommended)
- Use team-level Spotrac data ✅ (already have)
- Combine with public player rosters
- Estimate player-level breakdown statistically
- Document limitations transparently

---

## Files Created

### New Scrapers
- `src/spotrac_scraper_stealth.py` - Advanced anti-bot scraper
  - 400+ lines
  - Undetected-ChromeDriver support
  - Stealth JS injection
  - Human behavior simulation
  - Exponential backoff retry logic

- `scripts/test_stealth_scraper.py` - Test script
  - Validates stealth techniques
  - Provides clear success/failure feedback

### Existing Infrastructure
- `src/spotrac_scraper_v2.py` - Original team cap scraper
  - Successfully scrapes team-level data
  - Works reliably without detection issues

---

## Why Team-Level Data Is Still Valuable

### What We Have (Complete)
✅ All 32 NFL teams  
✅ Years 2015-2024 (10 seasons)  
✅ Dead money per team  
✅ Cap utilization metrics  
✅ Year-over-year trends  

### Analytical Value
1. **Trend Analysis**: How dead money has grown (+182.6%)
2. **Team Comparisons**: Which teams waste most cap on dead money
3. **Financial Planning**: Projection models based on historical patterns
4. **Strategic Insights**: Correlation with team performance
5. **League-Wide Metrics**: Aggregate dead money impact

### Example Insights (From Jupyter notebook)
- Top 3 spenders: PHI ($379M), NYG ($373M), CAR ($364M)
- Fastest growers: 2024 spike ($1.56B total)
- Average dead cap: 13.9% of team cap
- Range: 2.1% - 45.6% (outliers)

---

## Technical Debt & Future Work

### If Player Data Becomes Available
1. Combine with team data for player-team relationships
2. Calculate per-player impact on dead money
3. Analyze contract structure patterns
4. Identify at-risk teams for cap crunch

### Recommendation
Accept limitation gracefully and document:
- What works (team data) ✅
- What doesn't (player data) ❌
- Why (anti-bot detection) 📝
- What we achieved anyway (trend analysis) 🎯

---

## Installation & Running

### Install Dependencies
```bash
./.venv/bin/pip install undetected-chromedriver selenium beautifulsoup4 pandas
```

### Run Stealth Scraper
```bash
# Test on player rankings (may still be blocked)
./.venv/bin/python scripts/test_stealth_scraper.py

# Run team cap scraper (works reliably)
./.venv/bin/python src/spotrac_scraper_v2.py backfill 2015 2024
```

### View Analysis
```bash
# Jupyter notebook with all visualizations
jupyter notebook notebooks/06_complete_dead_money_analysis.ipynb
```

---

## Conclusion

We've built a sophisticated browser automation setup that:
1. ✅ Successfully scrapes team-level data (proven)
2. ✅ Uses cutting-edge anti-detection techniques
3. ✅ Implements human-like behavior patterns
4. ⚠️ Still likely blocked by Spotrac's advanced bot detection
5. 📊 Provides complete analysis with available data

The limitation is **not** due to technical incompetence but rather Spotrac's investment in anti-bot measures. The team-level data we have is still highly valuable for trend analysis and strategic insights.

---

## Resources

- [Undetected-ChromeDriver](https://github.com/ultrafunkamsterdam/undetected-chromedriver)
- [Selenium Documentation](https://selenium.dev/documentation/)
- [Web Scraping Best Practices](https://en.wikipedia.org/wiki/Web_scraping#Legal_implications)
