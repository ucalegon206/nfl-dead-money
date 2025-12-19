#!/usr/bin/env python3
"""
Guided Firefox Player Rankings Scraper
- Opens Spotrac rankings in Firefox (headful)
- Lets you interact (scroll/solve prompts)
- On Enter, extracts rows via in-page JavaScript
"""

import logging
import time
from pathlib import Path
import json
import pandas as pd
from selenium import webdriver
from selenium.webdriver.firefox.options import Options

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

JS_EXTRACT = r"""
(() => {
  function unique(arr) {
    const seen = new Set();
    const out = [];
    for (const r of arr) {
      const key = JSON.stringify(r);
      if (!seen.has(key)) { seen.add(key); out.push(r); }
    }
    return out;
  }
  const currencyRe = /\$[0-9,.]+/;
  const posRe = /^(QB|WR|RB|TE|CB|S|FS|SS|LB|OLB|ILB|MLB|DE|DT|EDGE|OT|OG|C|DL|DB|K|P|LS)$/;
  const section = document.querySelector('main') || document.body;
  const anchors = Array.from(section.querySelectorAll('a'))
    .filter(a => (a.getAttribute('href')||'').includes('/nfl/') && (a.textContent||'').trim().split(/\s+/).length >= 2);

  const rows = [];
  for (const a of anchors) {
    const name = (a.textContent||'').trim();
    let container = a.closest('tr');
    if (!container) container = a.closest('li');
    if (!container) container = a.parentElement;
    const text = (container ? container.innerText : a.parentElement.innerText) || '';
    const m = text.match(currencyRe);
    const val = m ? m[0] : null;
    let team = null, pos = null;
    const tokens = text.split(/\s+/);
    for (const t of tokens) {
      if (!team && /^[A-Z]{2,3}$/.test(t)) team = t;
      if (!pos && posRe.test(t)) pos = t;
    }
    if (val) rows.push([name, team, pos, val]);
  }
  return unique(rows);
})()
"""

def run(year=2024):
  log.info("\n🦊 Launching Firefox (guided mode)...")
  opts = Options()
  opts.headless = False
  opts.set_preference("dom.webdriver.enabled", False)
  driver = webdriver.Firefox(options=opts)
  try:
    url = f"https://www.spotrac.com/nfl/rankings/player/_/year/{year}/sort/cap_total"
    log.info(f"📄 Loading: {url}")
    driver.get(url)
    log.info("⏳ Give the page 10-20s to stabilize...")
    time.sleep(12)
    log.info("👉 If needed, scroll or interact, then press Enter here...")
    input()

    # Try extraction multiple times with small waits
    all_rows = []
    for i in range(3):
      rows = driver.execute_script(JS_EXTRACT)
      if rows is None:
        rows = []
      log.info(f"  Attempt {i+1}: found {len(rows)} rows")
      all_rows.extend(rows)
      time.sleep(2)
    # dedupe
    seen = set()
    dedup = []
    for r in all_rows:
      k = tuple(r)
      if k not in seen:
        seen.add(k)
        dedup.append(r)

    log.info(f"✅ Total unique rows: {len(dedup)}")
    # Always dump page artifacts for offline parsing
    try:
      html = driver.page_source
      main_text = driver.execute_script("return (document.querySelector('main')||document.body).innerText;") or ''
      # Dump all script tag contents as well
      scripts = driver.execute_script("return Array.from(document.scripts).map(s => s.textContent || '');") or []
    except Exception:
      html = driver.page_source
      main_text = ''
      scripts = []
    raw_dir = Path('data/raw')
    raw_dir.mkdir(parents=True, exist_ok=True)
    html_path = raw_dir / f'spotrac_{year}_raw.html'
    txt_path = raw_dir / f'spotrac_{year}_raw.txt'
    scripts_path = raw_dir / f'spotrac_{year}_scripts.jsonl'
    html_path.write_text(html)
    txt_path.write_text(main_text)
    # Write scripts as JSON Lines
    import json
    with scripts_path.open('w') as f:
      for idx, content in enumerate(scripts):
        obj = {"index": idx, "length": len(content or ''), "content": content or ''}
        f.write(json.dumps(obj) + "\n")
    log.info(f"📝 Dumped HTML to: {html_path}")
    log.info(f"📝 Dumped text to: {txt_path}")
    log.info(f"📝 Dumped scripts to: {scripts_path}")

    if len(dedup) < 50:
      log.error("Too few rows; data may be hidden. Try more scrolling; raw dumps saved.")
      return None

    # Normalize and save
    out = []
    for name, team, pos, val in dedup:
      try:
        val_num = float(''.join(ch for ch in val if ch.isdigit() or ch=='.'))
      except Exception:
        val_num = None
      out.append({
        'Player': name,
        'Team': team,
        'Position': pos,
        'CapValue': val_num,
        'Year': year
      })

    df = pd.DataFrame(out)
    p = Path('data/raw') / f'player_rankings_{year}.csv'
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False)
    log.info(f"📁 Saved: {p} ({len(df)} rows)")
    return df
  finally:
    try:
      driver.quit()
    except Exception:
      pass

if __name__ == '__main__':
  run(2024)
