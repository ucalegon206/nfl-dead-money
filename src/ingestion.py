"""
Ingestion layer for raw → staging tables.

Goals:
- Load raw CSVs (Spotrac team cap, player rankings, dead money)
- Apply light schema normalization (column names, types)
- Write staging tables under data/staging/
"""

from pathlib import Path
from typing import Optional
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_DIR = Path("data/raw")
STAGING_DIR = Path("data/staging")
STAGING_DIR.mkdir(parents=True, exist_ok=True)


def _to_float(val) -> float:
    try:
        return float(val)
    except Exception:
        return 0.0


def stage_spotrac_team_cap(year: int, snapshot_date: Optional[str] = None) -> Path:
    """Normalize and stage Spotrac team cap snapshot for given year."""
    fname = f"spotrac_team_cap_{year}_{snapshot_date}.csv" if snapshot_date else None
    # fallback to latest file if snapshot_date not provided
    raw_path = RAW_DIR / (fname if fname else f"spotrac_team_cap_{year}_{_latest_snapshot_suffix(year)}.csv")
    if not raw_path.exists():
        logger.warning("Team cap raw file not found: %s", raw_path)
        return raw_path
    df = pd.read_csv(raw_path)
    # Normalize columns
    col_map = {
        'team_name': 'team_name',
        'team': 'team_name',
        'year': 'year',
        'active_cap_millions': 'active_cap_millions',
        'dead_money_millions': 'dead_money_millions',
        'salary_cap_millions': 'salary_cap_millions',
        'cap_space_millions': 'cap_space_millions',
        'dead_cap_pct': 'dead_cap_pct',
    }
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns=col_map)
    # Types
    for c in ['active_cap_millions','dead_money_millions','salary_cap_millions','cap_space_millions','dead_cap_pct']:
        if c in df.columns:
            df[c] = df[c].apply(_to_float)
    df['year'] = pd.to_numeric(df.get('year'), errors='coerce').astype('Int64')
    # Write staging
    out_path = STAGING_DIR / f"stg_spotrac_team_cap_{year}.csv"
    df.to_csv(out_path, index=False)
    logger.info("Staged team cap: %s (%d rows)", out_path, len(df))
    return out_path


def stage_spotrac_player_rankings(year: int) -> Path:
    """Normalize and stage Spotrac player rankings for a given year."""
    raw_path = RAW_DIR / f"spotrac_player_rankings_{year}.csv"
    if not raw_path.exists():
        logger.warning("Player rankings raw file not found: %s", raw_path)
        return raw_path
    df = pd.read_csv(raw_path)
    col_map = {
        'player_name': 'player_name',
        'position': 'position',
        'team': 'team',
        'year': 'year',
        'cap_total_millions': 'cap_total_millions',
        'cap_hit_millions': 'cap_hit_millions',
    }
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns=col_map)
    for c in ['cap_total_millions','cap_hit_millions']:
        if c in df.columns:
            df[c] = df[c].apply(_to_float)
    df['year'] = pd.to_numeric(df.get('year'), errors='coerce').astype('Int64')
    out_path = STAGING_DIR / f"stg_spotrac_player_rankings_{year}.csv"
    df.to_csv(out_path, index=False)
    logger.info("Staged player rankings: %s (%d rows)", out_path, len(df))
    return out_path


def stage_spotrac_dead_money(year: int) -> Path:
    """Normalize and stage Spotrac player dead money for a given year."""
    raw_path = RAW_DIR / f"spotrac_dead_money_{year}.csv"
    if not raw_path.exists():
        logger.warning("Dead money raw file not found: %s", raw_path)
        return raw_path
    df = pd.read_csv(raw_path)
    col_map = {
        'player_name': 'player_name',
        'position': 'position',
        'team': 'team',
        'year': 'year',
        'dead_cap_hit': 'dead_cap_millions',
    }
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns=col_map)
    df['dead_cap_millions'] = df['dead_cap_millions'].apply(_to_float)
    df['year'] = pd.to_numeric(df.get('year'), errors='coerce').astype('Int64')
    out_path = STAGING_DIR / f"stg_spotrac_dead_money_{year}.csv"
    df.to_csv(out_path, index=False)
    logger.info("Staged dead money: %s (%d rows)", out_path, len(df))
    return out_path


def _latest_snapshot_suffix(year: int) -> str:
    # naive fallback: pick most recent file by glob; kept simple here
    import glob
    files = glob.glob(str(RAW_DIR / f"spotrac_team_cap_{year}_*.csv"))
    if not files:
        return ""
    return Path(sorted(files)[-1]).stem.split(f"spotrac_team_cap_{year}_")[-1]
