"""
Scrape player compensation data from Pro Football Reference.

Extracts contract info from player pages and rosters, normalizes to
compensation data model, outputs normalized schema.
"""

from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Optional, Dict, List
import logging

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

from .compensation_model import CompensationDataModel, Player, PlayerContract, PlayerCapImpact

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PFR_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
}


def parse_salary_string(s: str) -> float:
    """Convert salary string like '$5,234,000' to millions (5.234)."""
    if pd.isna(s) or not s:
        return 0.0
    s = str(s).strip().replace('$', '').replace(',', '')
    try:
        return float(s) / 1_000_000
    except ValueError:
        return 0.0


def scrape_pfr_player_salary_page(player_url: str) -> Dict:
    """
    Scrape individual player page from PFR for salary/contract info.
    
    Returns dict with player_name, team, position, salary data.
    """
    try:
        resp = requests.get(player_url, headers=PFR_HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'lxml')
        
        # Extract header info
        h1 = soup.find('h1')
        player_name = h1.text.strip() if h1 else 'Unknown'
        
        # Find salary table (usually near top of page)
        salary_data = {}
        tables = soup.find_all('table')
        for tbl in tables:
            # Look for table with salary/contract info
            if 'salary' in tbl.get('id', '').lower() or 'contract' in tbl.get('id', '').lower():
                try:
                    df = pd.read_html(str(tbl))[0]
                    salary_data['table'] = df
                except ValueError:
                    continue
        
        return {
            'player_name': player_name,
            'salary_data': salary_data,
        }
    except Exception as e:
        logger.error(f"Error scraping {player_url}: {e}")
        return {}


def scrape_pfr_2024_compensation(
    roster_csv_path: str = 'data/raw/pfr/rosters_2024.csv',
    output_dir: str = 'data/processed/compensation',
) -> CompensationDataModel:
    """
    Scrape compensation data for 2024 NFL season from PFR rosters.
    
    Process:
    1. Load 2024 roster from CSV (already scraped)
    2. For each player, build minimal compensation record from roster data
    3. Normalize to compensation data model
    4. Output three tables: dim_players, fact_contracts, mart_cap_impact
    
    Note: PFR doesn't easily expose full contract details on team roster pages.
    This extracts what's available (position, team, etc) and serves as base
    for later enhancement with external data sources.
    """
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    model = CompensationDataModel()
    
    # Load roster
    if not Path(roster_csv_path).exists():
        logger.error(f"Roster file not found: {roster_csv_path}")
        return model
    
    roster_df = pd.read_csv(roster_csv_path)
    logger.info(f"Loaded {len(roster_df)} players from {roster_csv_path}")
    
    # Map columns (PFR roster columns vary)
    name_col = 'Player' if 'Player' in roster_df.columns else roster_df.columns[0]
    pos_col = 'Pos' if 'Pos' in roster_df.columns else 'Position'
    team_col = 'team' if 'team' in roster_df.columns else 'Tm'
    year_col = 'year' if 'year' in roster_df.columns else 'Year'
    
    for idx, row in roster_df.iterrows():
        try:
            player_name = str(row[name_col]).strip()
            position = str(row[pos_col]).strip() if pd.notna(row.get(pos_col)) else 'UNK'
            team = str(row[team_col]).strip() if pd.notna(row.get(team_col)) else 'UNK'
            year = int(row[year_col]) if pd.notna(row.get(year_col)) else 2024
            
            # Create player ID (simple hash of name + team + year)
            player_id = f"PFR_{re.sub(r'[^A-Z0-9]', '', player_name.upper())}_{team}_{year}"
            
            # Add player to model
            player = Player(
                player_id=player_id,
                player_name=player_name,
                position=position,
            )
            model.add_player(player)
            
            # Add minimal contract record (we don't have full salary from roster)
            # This is a placeholder; real data would come from Spotrac/OTC
            contract = PlayerContract(
                contract_id=f"{player_id}_base",
                player_id=player_id,
                team=team,
                year=year,
                salary_type='base_salary',
                amount_millions=0.0,  # Placeholder
                status='active',
            )
            model.add_contract(contract)
            
            # Compute impact (will be 0 without real salary data)
            impact = model.compute_cap_impact_from_contracts(player_id, team, year)
            model.add_cap_impact(impact)
            
        except Exception as e:
            logger.warning(f"Error processing row {idx}: {e}")
            continue
    
    # Export
    model.export_all(output_dir)
    logger.info(f"Exported compensation data to {output_dir}")
    
    return model


def merge_compensation_with_external_source(
    model: CompensationDataModel,
    external_csv_path: str,
    year: int = 2024,
) -> CompensationDataModel:
    """
    Merge external contract source (Spotrac/OTC export) with PFR roster model.
    
    External CSV should have columns: player_name, team, salary_type, amount_millions
    """
    if not Path(external_csv_path).exists():
        logger.warning(f"External source not found: {external_csv_path}")
        return model
    
    external_df = pd.read_csv(external_csv_path)
    logger.info(f"Loaded {len(external_df)} records from {external_csv_path}")
    
    for _, row in external_df.iterrows():
        try:
            player_name = str(row.get('player_name', '')).strip()
            team = str(row.get('team', '')).strip()
            salary_type = str(row.get('salary_type', 'base_salary')).strip()
            amount_millions = float(row.get('amount_millions', 0))
            
            # Find matching player in model
            matching_players = model.players_df[
                (model.players_df['player_name'].str.contains(player_name, case=False, na=False)) &
                (model.players_df['team'] == team) if 'team' in model.players_df.columns else True
            ]
            
            if matching_players.empty:
                logger.warning(f"No match for {player_name} ({team})")
                continue
            
            player_id = matching_players.iloc[0]['player_id']
            
            # Add contract
            contract = PlayerContract(
                contract_id=f"{player_id}_{salary_type}",
                player_id=player_id,
                team=team,
                year=year,
                salary_type=salary_type,
                amount_millions=amount_millions,
            )
            model.add_contract(contract)
            
            # Recompute cap impact
            impact = model.compute_cap_impact_from_contracts(player_id, team, year)
            model.add_cap_impact(impact)
            
        except Exception as e:
            logger.warning(f"Error merging row: {e}")
            continue
    
    return model
