"""
Contracts ingestion and dead money computation utilities.

Supports importing manually downloaded CSVs (e.g., Spotrac/OTC exports) or
our sample file(s), normalizes to a standard schema, merges with PFR rosters,
and computes team-level dead money rollups.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import numpy as np
import pandas as pd

# Known team abbreviations (PFR/legacy variants included)
TEAM_ABBRS = {
    'ARI','ATL','BAL','BUF','CAR','CHI','CIN','CLE','DAL','DEN','DET','GNB','GB','HOU','IND','JAX','JAC','KAN','KC',
    'LAC','LAR','LVR','LV','MIA','MIN','NOR','NO','NWE','NE','NYG','NYJ','PHI','PIT','SFO','SF','SEA','TAM','TB','TEN','WAS'
}

FULL_TO_ABBR = {
    'ARIZONA CARDINALS':'ARI','ATLANTA FALCONS':'ATL','BALTIMORE RAVENS':'BAL','BUFFALO BILLS':'BUF','CAROLINA PANTHERS':'CAR',
    'CHICAGO BEARS':'CHI','CINCINNATI BENGALS':'CIN','CLEVELAND BROWNS':'CLE','DALLAS COWBOYS':'DAL','DENVER BRONCOS':'DEN',
    'DETROIT LIONS':'DET','GREEN BAY PACKERS':'GNB','HOUSTON TEXANS':'HOU','INDIANAPOLIS COLTS':'IND','JACKSONVILLE JAGUARS':'JAX',
    'KANSAS CITY CHIEFS':'KAN','LOS ANGELES CHARGERS':'LAC','LOS ANGELES RAMS':'LAR','LAS VEGAS RAIDERS':'LVR','MIAMI DOLPHINS':'MIA',
    'MINNESOTA VIKINGS':'MIN','NEW ORLEANS SAINTS':'NOR','NEW ENGLAND PATRIOTS':'NWE','NEW YORK GIANTS':'NYG','NEW YORK JETS':'NYJ',
    'PHILADELPHIA EAGLES':'PHI','PITTSBURGH STEELERS':'PIT','SAN FRANCISCO 49ERS':'SFO','SEATTLE SEAHAWKS':'SEA',
    'TAMPA BAY BUCCANEERS':'TAM','TENNESSEE TITANS':'TEN','WASHINGTON COMMANDERS':'WAS',
}

NAME_SUFFIX_RE = re.compile(r"\b(JR\.?|SR\.?|III|II|IV)\b", re.IGNORECASE)
NON_ALPHA_RE = re.compile(r"[^A-Z0-9]+")


def parse_money(val) -> float:
    if pd.isna(val):
        return 0.0
    s = str(val).strip()
    if s in {'', '—', '-', 'NaN', 'nan', 'None'}:
        return 0.0
    s = s.replace('$', '').replace(',', '')
    try:
        return float(s)
    except ValueError:
        # Try parentheses for negatives
        s2 = s.strip('()')
        try:
            return -float(s2) if s.startswith('(') and s.endswith(')') else float(s2)
        except Exception:
            return 0.0


def normalize_team(team: str) -> Optional[str]:
    if pd.isna(team):
        return None
    t = str(team).strip().upper()
    if t in TEAM_ABBRS:
        # Normalize common variants
        if t == 'GB':
            return 'GNB'
        if t == 'KC':
            return 'KAN'
        if t == 'NO':
            return 'NOR'
        if t == 'NE':
            return 'NWE'
        if t == 'TB':
            return 'TAM'
        if t == 'SF':
            return 'SFO'
        if t == 'LV':
            return 'LVR'
        return t
    return FULL_TO_ABBR.get(t)


def normalize_name(name: str) -> str:
    if pd.isna(name):
        return ''
    s = str(name).upper().strip()
    s = NAME_SUFFIX_RE.sub('', s)
    s = NON_ALPHA_RE.sub(' ', s)
    s = ' '.join(s.split())
    return s


def standardize_contracts(df: pd.DataFrame, source: str = 'external') -> pd.DataFrame:
    col_map_candidates = {
        'player':['player','player_name','Player','Player Name'],
        'team':['team','Team','Tm'],
        'year':['year','Year','season','Season'],
        'dead_money':['dead_money','Dead Money','Dead Cap','Dead Cap Hit','dead_cap_hit','Dead$','DeadCap'],
        'designation':['designation','Designation','Type','Status'],
    }
    # Build rename map based on available columns
    rename = {}
    for std, cands in col_map_candidates.items():
        for c in cands:
            if c in df.columns:
                rename[c] = std
                break
    sdf = df.rename(columns=rename).copy()

    # Ensure required cols exist
    for req in ['player','team','year','dead_money']:
        if req not in sdf.columns:
            sdf[req] = np.nan

    # Clean
    sdf['player_norm'] = sdf['player'].map(normalize_name)
    sdf['team'] = sdf['team'].map(normalize_team)
    sdf['year'] = pd.to_numeric(sdf['year'], errors='coerce').astype('Int64')
    sdf['dead_money'] = sdf['dead_money'].map(parse_money).astype(float)
    sdf['designation'] = sdf.get('designation', pd.Series([np.nan]*len(sdf)))
    sdf['source'] = source
    # Drop rows with no team/year or zero dead money
    sdf = sdf.dropna(subset=['team','year'])
    sdf = sdf[sdf['dead_money'] > 0]
    return sdf[['player','player_norm','team','year','dead_money','designation','source']]


def load_contract_csvs(path_glob: str) -> pd.DataFrame:
    paths = sorted(Path().glob(path_glob))
    frames = []
    for p in paths:
        try:
            df = pd.read_csv(p)
            frames.append(standardize_contracts(df, source=p.name))
        except Exception:
            continue
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame(columns=['player','player_norm','team','year','dead_money','designation','source'])


def merge_with_rosters(contracts_df: pd.DataFrame, rosters_df: pd.DataFrame) -> pd.DataFrame:
    # Normalize roster names and teams
    r = rosters_df.copy()
    # PFR roster 'Player' col contains names; ensure we have it
    name_col = 'Player' if 'Player' in r.columns else r.columns[0]
    r['player_norm'] = r[name_col].map(normalize_name)
    if 'team' not in r.columns:
        # try 'Tm'
        if 'Tm' in r.columns:
            r['team'] = r['Tm']
        else:
            r['team'] = np.nan
    if 'year' not in r.columns:
        # try 'Season' or fallback
        r['year'] = r.get('Season', np.nan)
    r['team'] = r['team'].map(normalize_team)
    r['year'] = pd.to_numeric(r['year'], errors='coerce').astype('Int64')

    # Merge on normalized name + team + year
    merged = contracts_df.merge(r[['player_norm','team','year']], on=['player_norm','team','year'], how='left', indicator=True)
    return merged


def compute_team_dead_money(contracts_df: pd.DataFrame) -> pd.DataFrame:
    grp = (contracts_df
           .groupby(['team','year'], dropna=False)['dead_money']
           .sum()
           .reset_index()
           .sort_values(['year','dead_money'], ascending=[True, False]))
    return grp


def ingest_and_compute(
    contracts_glob: str = 'data/raw/contracts/dead_money_*.csv',
    roster_combined_csv: Optional[str] = 'data/raw/pfr/combined_rosters_2015_2024.csv',
    fallback_roster_2024: Optional[str] = 'data/raw/pfr/rosters_2024.csv',
    out_player_csv: str = 'data/processed/player_dead_money.csv',
    out_team_csv: str = 'data/processed/team_dead_money.csv',
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """End-to-end: load contracts, merge with rosters, compute team rollups, save.

    If no real contracts found, falls back to sample file(s) in data/raw/.
    """
    Path(out_player_csv).parent.mkdir(parents=True, exist_ok=True)

    contracts_df = load_contract_csvs(contracts_glob)
    if contracts_df.empty:
        # Try our sample player dead money
        sample = Path('data/raw/player_dead_money_sample.csv')
        if sample.exists():
            contracts_df = standardize_contracts(pd.read_csv(sample), source=sample.name)
    if contracts_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Load roster(s)
    roster_df = pd.DataFrame()
    if roster_combined_csv and Path(roster_combined_csv).exists():
        roster_df = pd.read_csv(roster_combined_csv)
    elif fallback_roster_2024 and Path(fallback_roster_2024).exists():
        roster_df = pd.read_csv(fallback_roster_2024)

    merged = merge_with_rosters(contracts_df, roster_df) if not roster_df.empty else contracts_df.copy()

    # Save player-level
    merged.to_csv(out_player_csv, index=False)

    # Team-level rollup
    team = compute_team_dead_money(merged)
    team.to_csv(out_team_csv, index=False)

    return merged, team
