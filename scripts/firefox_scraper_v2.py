#!/usr/bin/env python3
"""
Firefox Player Rankings Scraper - Extract from Script Tags
The data is embedded in JavaScript, not in an HTML table.
"""

import logging
import json
import re
import time
from pathlib import Path
from typing import Optional
import pandas as pd
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def scrape_with_firefox(year: int = 2024) -> Optional[pd.DataFrame]:
    """Scrape player rankings using Firefox - extract from script tags"""
    
    logger.info("\n" + "="*70)
    logger.info("FIREFOX PLAYER RANKINGS SCRAPER (Script-based)")
    logger.info("="*70)
    
    logger.info("\n🦊 Initializing Firefox...")
    
    options = Options()
    options.headless = False
    options.set_preference("dom.webdriver.enabled", False)
    
    driver = webdriver.Firefox(options=options)
    
    try:
        url = f"https://www.spotrac.com/nfl/rankings/player/_/year/{year}/sort/cap_total"
        logger.info(f"\n📄 Loading: {url}")
        
        driver.get(url)
        
        logger.info("⏳ Waiting for page to fully load (15 seconds)...")
        time.sleep(15)
        
        # Get page source
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract data from script tags
        logger.info("🔍 Searching for player data in scripts...")
        
        rows = []
        
        # Helper: try to coerce JavaScript-like text to JSON/Python safely
        import ast
        def try_parse_any(text: str):
            # First, try json
            try:
                return json.loads(text)
            except Exception:
                pass
            # Try Python literal (replace JS booleans/null)
            try:
                safe = (
                    text.replace('true', 'True')
                        .replace('false', 'False')
                        .replace('null', 'None')
                )
                return ast.literal_eval(safe)
            except Exception:
                return None
        
        # Strategy:
        # 1) Look for large array/object literals in scripts and parse them
        # 2) Inspect parsed structures for player-like records
        script_candidates = soup.find_all('script')
        logger.info(f"  Inspecting {len(script_candidates)} script tags for embedded data...")
        
        def extract_rows_from_obj(obj):
            extracted = []
            if isinstance(obj, dict):
                # Look for arrays in common keys
                for k, v in obj.items():
                    if isinstance(v, (list, tuple)):
                        extracted += extract_rows_from_obj(v)
                    elif isinstance(v, dict):
                        extracted += extract_rows_from_obj(v)
                # Also, if dict itself looks like a player row
                keys = {str(k).lower() for k in obj.keys()}
                if {'name', 'team'}.issubset(keys) or {'player', 'team'}.issubset(keys) or {'player', 'pos'}.issubset(keys):
                    name = obj.get('name') or obj.get('player') or obj.get('Player')
                    team = obj.get('team') or obj.get('Team')
                    pos = obj.get('position') or obj.get('pos') or obj.get('Position')
                    # Value fields often named cap, cap_hit, cap_total, value, amount
                    val = obj.get('cap_total') or obj.get('cap') or obj.get('cap_hit') or obj.get('value') or obj.get('amount')
                    # Clean potential currency strings
                    if isinstance(val, str):
                        val_num = re.sub(r'[^0-9\.-]', '', val)
                        try:
                            val = float(val_num)
                        except Exception:
                            val = None
                    extracted.append([name, team, pos, val, year])
                return extracted
            elif isinstance(obj, (list, tuple)):
                for item in obj:
                    extracted += extract_rows_from_obj(item)
                return extracted
            else:
                return extracted
        
        # Scan scripts for JSON-ish arrays/objects
        for script in script_candidates:
            content = script.string or ''
            if not content:
                continue
            # Quickly skip tiny scripts
            if len(content) < 100:
                continue
            # Find large bracketed structures by naive bracket matching
            # Limit to reasonable sizes to avoid huge memory
            starts = [m.start() for m in re.finditer(r'[\[{]', content)]
            for s in starts:
                # Only consider segments that look like arrays/objects with quotes inside soon after
                if '"' not in content[s:s+200] and "'" not in content[s:s+200]:
                    continue
                # Try to expand to matching closing bracket with a simple stack
                stack = []
                end = None
                for i, ch in enumerate(content[s:], start=s):
                    if ch in '[{':
                        stack.append(ch)
                    elif ch in '}]':
                        if not stack:
                            break
                        opener = stack.pop()
                        if (opener == '[' and ch != ']') or (opener == '{' and ch != '}'):
                            break
                        if not stack:
                            end = i + 1
                            break
                    # Hard stop if segment is too large
                    if i - s > 400000:
                        break
                if end and end - s > 100:  # meaningful segment
                    segment = content[s:end]
                    parsed = try_parse_any(segment)
                    if parsed is not None:
                        new_rows = extract_rows_from_obj(parsed)
                        if new_rows:
                            rows.extend(new_rows)
            # Early exit if we've captured enough
            if len(rows) > 200:
                break
        
        # Method 2: Extract from visible text if table is rendered
        if len(rows) == 0:
            logger.info("  No JSON data found, checking for rendered text...")
            
            # Heuristic parsing from page text
            main = soup.find('main') or soup
            text_content = main.get_text("\n", strip=True)
            lines = [ln.strip() for ln in text_content.split('\n') if ln.strip()]
            
            # Regex to capture rank, player, team, position, currency value
            pat = re.compile(
                r"^(?:(?P<rank>\d{1,3})\s+)?(?P<player>[A-Z][a-zA-Z.'\-]+(?:\s+[A-Z][a-zA-Z.'\-]+)+)\s+(?P<team>[A-Z]{2,3})\s+(?P<pos>[A-Z]{1,4})[^$]*\$(?P<value>[0-9,]+(?:\.[0-9]{2})?)"
            )
            seen = set()
            for ln in lines:
                if '$' not in ln:
                    continue
                m = pat.search(ln)
                if not m:
                    continue
                player = m.group('player')
                team = m.group('team')
                pos = m.group('pos')
                value = m.group('value')
                key = (player, team, pos, value)
                if key in seen:
                    continue
                seen.add(key)
                try:
                    val_num = float(re.sub(r'[^0-9\.-]', '', value))
                except Exception:
                    val_num = None
                rows.append([player, team, pos, val_num, year])
        
        driver.quit()
        
        if len(rows) < 10:
            logger.warning(f"⚠️  Only extracted {len(rows)} rows, page structure may have changed")
            logger.info("\n📊 Script content analysis:")
            
            # Log script content for debugging
            for i, script in enumerate(soup.find_all('script')[:5]):
                if script.string:
                    content = script.string[:200]
                    logger.info(f"  Script {i}: {content.replace(chr(10), ' ')[:100]}...")
            
            return None
        
        logger.info(f"✓ Extracted {len(rows)} rows")
        
        # Create DataFrame
        df = pd.DataFrame(rows, columns=['Player', 'Team', 'Position', 'Value', 'Year'])
        logger.info(f"\n📊 Data Summary:")
        logger.info(f"  Rows: {len(df)}")
        logger.info(f"  Columns: {list(df.columns)}")
        
        return df
    
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        driver.quit()
        return None


def main():
    logger.info("\n🎯 SPOTRAC PLAYER RANKINGS EXTRACTION")
    logger.info("Using Firefox browser with JavaScript-embedded data")
    
    year = 2024
    
    df = scrape_with_firefox(year)
    
    if df is not None and len(df) > 100:
        logger.info(f"\n✅ SUCCESS!")
        logger.info(f"Extracted {len(df)} player records for {year}")
        
        # Save to CSV
        output_path = Path("data/raw") / f"player_rankings_{year}.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        df.to_csv(output_path, index=False)
        logger.info(f"📁 Saved to: {output_path}")
        
        # Show sample
        logger.info("\n📋 Sample data:")
        logger.info(df.head())
    else:
        logger.error(f"\n❌ FAILED: Extracted {len(df) if df is not None else 0} rows (need 100+)")
        logger.error("\nNote: The page structure may be protected or use complex JavaScript")
        logger.error("Try inspecting the browser window manually to see the actual data.")


if __name__ == "__main__":
    main()
