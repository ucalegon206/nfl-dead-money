#!/usr/bin/env python3
"""
Parse Spotrac rankings raw text (dumped by firefox_scraper_guided.py) into CSV
"""
import re
import sys
from pathlib import Path
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

NAME_RE = re.compile(r"([A-Z][a-zA-Z.'\-]+(?:\s+[A-Z][a-zA-Z.'\-]+)+)")
CURR_RE = re.compile(r"\$([0-9][0-9,]*(?:\.[0-9]{2})?)")
TEAM_RE = re.compile(r"\b([A-Z]{2,3})\b")
POS_RE = re.compile(r"\b(QB|WR|RB|TE|CB|S|FS|SS|LB|OLB|ILB|MLB|DE|DT|EDGE|OT|OG|C|DL|DB|K|P|LS)\b")


def parse_text_file(year: int):
    txt_path = Path('data/raw') / f'spotrac_{year}_raw.txt'
    if not txt_path.exists():
        log.error(f"Raw text not found: {txt_path}")
        sys.exit(1)

    lines = [ln.strip() for ln in txt_path.read_text(errors='ignore').splitlines()]

    rows = []
    for i, ln in enumerate(lines):
        if '$' not in ln:
            continue
        mcur = CURR_RE.search(ln)
        if not mcur:
            continue
        val = mcur.group(1)
        name = None
        team = None
        pos = None

        # Look in same line for name, team, pos
        mname = NAME_RE.search(ln)
        if mname:
            name = mname.group(1)
        mteam = TEAM_RE.findall(ln)
        for t in mteam or []:
            if len(t) in (2,3):
                team = t; break
        mpos = POS_RE.search(ln)
        if mpos:
            pos = mpos.group(1)

        # If not found, look back a couple lines
        if not name and i>0:
            for j in range(1,4):
                prev = lines[i-j]
                mname2 = NAME_RE.search(prev)
                if mname2:
                    name = mname2.group(1); break
                mpos2 = POS_RE.search(prev)
                if mpos2 and not pos:
                    pos = mpos2.group(1)
                mteam2 = TEAM_RE.findall(prev)
                for t in mteam2 or []:
                    if len(t) in (2,3) and not team:
                        team = t; break

        if name:
            try:
                val_num = float(''.join(ch for ch in val if ch.isdigit() or ch=='.'))
            except Exception:
                val_num = None
            rows.append([name, team, pos, val_num, year])

    # Deduplicate by (name, value)
    seen = set(); out = []
    for r in rows:
        k = (r[0], r[3])
        if k not in seen:
            seen.add(k)
            out.append(r)

    df = pd.DataFrame(out, columns=['Player','Team','Position','CapValue','Year'])
    log.info(f"Parsed rows: {len(df)}")
    out_path = Path('data/raw') / f'player_rankings_{year}.csv'
    df.to_csv(out_path, index=False)
    log.info(f"Saved CSV: {out_path}")


if __name__ == '__main__':
    year = int(sys.argv[1]) if len(sys.argv)>1 else 2024
    parse_text_file(year)
