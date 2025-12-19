#!/usr/bin/env python3
"""
Offline parser for Spotrac rankings using dumped scripts JSONL
- Input: data/raw/spotrac_<year>_scripts.jsonl (from firefox_scraper_guided.py)
- Output: data/raw/player_rankings_<year>.csv
"""
import json
import re
import ast
import sys
from pathlib import Path
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


def try_parse_any(text: str):
    try:
        return json.loads(text)
    except Exception:
        pass
    try:
        safe = text.replace('true','True').replace('false','False').replace('null','None')
        return ast.literal_eval(safe)
    except Exception:
        return None


def extract_rows_from_obj(obj, year):
    rows = []
    if isinstance(obj, dict):
        keys = {str(k).lower() for k in obj.keys()}
        # direct dict looks like a player row
        if {'name','team'}.issubset(keys) or {'player','team'}.issubset(keys):
            name = obj.get('name') or obj.get('player') or obj.get('Player')
            team = obj.get('team') or obj.get('Team')
            pos = obj.get('position') or obj.get('pos') or obj.get('Position')
            val = obj.get('cap_total') or obj.get('cap') or obj.get('cap_hit') or obj.get('value') or obj.get('amount')
            if isinstance(val, str):
                num = re.sub(r'[^0-9\.-]','', val)
                try:
                    val = float(num)
                except Exception:
                    val = None
            rows.append([name, team, pos, val, year])
        # search nested
        for v in obj.values():
            rows.extend(extract_rows_from_obj(v, year))
    elif isinstance(obj, (list, tuple)):
        for it in obj:
            rows.extend(extract_rows_from_obj(it, year))
    return rows


def scan_script_content(content: str, year: int):
    rows = []
    # fast skip
    if len(content) < 100:
        return rows
    # find bracketed segments, attempt parse
    starts = [m.start() for m in re.finditer(r'[\[{]', content)]
    for s in starts:
        if '"' not in content[s:s+200] and "'" not in content[s:s+200]:
            continue
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
            if i - s > 400000:
                break
        if end and end - s > 100:
            seg = content[s:end]
            parsed = try_parse_any(seg)
            if parsed is not None:
                new_rows = extract_rows_from_obj(parsed, year)
                if new_rows:
                    rows.extend(new_rows)
        if len(rows) > 2000:
            break
    return rows


def parse_dump(year: int):
    scripts_path = Path('data/raw') / f'spotrac_{year}_scripts.jsonl'
    if not scripts_path.exists():
        log.error(f"Scripts dump not found: {scripts_path}")
        sys.exit(1)

    rows = []
    total_scripts = 0
    with scripts_path.open() as f:
        for line in f:
            try:
                obj = json.loads(line)
            except Exception:
                continue
            total_scripts += 1
            content = obj.get('content') or ''
            rows.extend(scan_script_content(content, year))
    log.info(f"Scanned {total_scripts} scripts; extracted {len(rows)} raw rows")

    # Clean + dedupe
    cleaned = []
    for name, team, pos, val, yr in rows:
        if not name:
            continue
        cleaned.append([
            str(name).strip(),
            (str(team).strip() if team else None),
            (str(pos).strip() if pos else None),
            (float(val) if isinstance(val, (int,float)) else None),
            yr
        ])
    seen = set(); out = []
    for r in cleaned:
        k = (r[0], r[3])
        if k not in seen:
            seen.add(k)
            out.append(r)

    df = pd.DataFrame(out, columns=['Player','Team','Position','CapValue','Year'])
    log.info(f"Rows after clean/dedupe: {len(df)}")
    if len(df) < 50:
        log.warning("Low row count; consider re-running guided scraper and scrolling more.")

    out_path = Path('data/raw') / f'player_rankings_{year}.csv'
    df.to_csv(out_path, index=False)
    log.info(f"Saved CSV: {out_path}")


if __name__ == '__main__':
    year = int(sys.argv[1]) if len(sys.argv)>1 else 2024
    parse_dump(year)
