"""
Data collection utilities for NFL dead money analysis.

This module contains functions to:
- Scrape dead money data from Spotrac
- Fetch player statistics from various sources
- Clean and standardize data formats
- Save data to appropriate locations
"""

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from typing import Optional, List, Dict
import time
from pathlib import Path
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# NFL team abbreviations mapping
NFL_TEAMS = {
    'arizona-cardinals': 'ARI',
    'atlanta-falcons': 'ATL',
    'baltimore-ravens': 'BAL',
    'buffalo-bills': 'BUF',
    'carolina-panthers': 'CAR',
    'chicago-bears': 'CHI',
    'cincinnati-bengals': 'CIN',
    'cleveland-browns': 'CLE',
    'dallas-cowboys': 'DAL',
    'denver-broncos': 'DEN',
    'detroit-lions': 'DET',
    'green-bay-packers': 'GB',
    'houston-texans': 'HOU',
    'indianapolis-colts': 'IND',
    'jacksonville-jaguars': 'JAX',
    'kansas-city-chiefs': 'KC',
    'las-vegas-raiders': 'LV',
    'los-angeles-chargers': 'LAC',
    'los-angeles-rams': 'LAR',
    'miami-dolphins': 'MIA',
    'minnesota-vikings': 'MIN',
    'new-england-patriots': 'NE',
    'new-orleans-saints': 'NO',
    'new-york-giants': 'NYG',
    'new-york-jets': 'NYJ',
    'philadelphia-eagles': 'PHI',
    'pittsburgh-steelers': 'PIT',
    'san-francisco-49ers': 'SF',
    'seattle-seahawks': 'SEA',
    'tampa-bay-buccaneers': 'TB',
    'tennessee-titans': 'TEN',
    'washington-commanders': 'WAS'
}


def scrape_spotrac_dead_money(year: int, save_path: Optional[str] = None) -> pd.DataFrame:
    """
    Scrape dead money data from Spotrac for a specific year.
    
    NOTE: Spotrac blocks automated scraping. You have two options:
    1. Manually download CSV from https://www.spotrac.com/nfl/cap/{year}/ 
    2. Use the load_manual_data() function after saving CSV files
    
    Args:
        year: NFL season year (e.g., 2024)
        save_path: Optional path to save the data as CSV
        
    Returns:
        DataFrame with columns: team, year, dead_money, salary_cap, dead_cap_pct
    """
    url = f"https://www.spotrac.com/nfl/cap/{year}/"
    logger.info(f"Scraping Spotrac dead money data for {year}")
    logger.warning("NOTE: Spotrac may block automated requests. Consider manual download.")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.spotrac.com/nfl/'
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the table with cap data
        table = soup.find('table', {'class': 'datatable'})
        if not table:
            logger.error(f"Could not find data table for {year}")
            return pd.DataFrame()
        
        # Extract data
        data = []
        rows = table.find('tbody').find_all('tr')
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 5:
                team_cell = cols[0]
                team_link = team_cell.find('a')
                team_name = team_link.text.strip() if team_link else team_cell.text.strip()
                
                # Extract monetary values (remove $, commas, convert to float)
                def clean_money(text):
                    return float(text.replace('$', '').replace(',', '').replace('M', ''))
                
                try:
                    active_cap = clean_money(cols[1].text.strip()) if len(cols) > 1 else 0
                    dead_money = clean_money(cols[2].text.strip()) if len(cols) > 2 else 0
                    total_cap = clean_money(cols[3].text.strip()) if len(cols) > 3 else 0
                    
                    data.append({
                        'team': team_name,
                        'year': year,
                        'active_cap': active_cap,
                        'dead_money': dead_money,
                        'total_cap': total_cap,
                        'dead_cap_pct': (dead_money / total_cap * 100) if total_cap > 0 else 0
                    })
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Error parsing row for {team_name}: {e}")
                    continue
        
        df = pd.DataFrame(data)
        logger.info(f"Scraped {len(df)} teams for {year}")
        
        if save_path:
            Path(save_path).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(save_path, index=False)
            logger.info(f"Saved data to {save_path}")
        
        return df
        
    except requests.RequestException as e:
        logger.error(f"Error fetching data for {year}: {e}")
        return pd.DataFrame()


def scrape_spotrac_multiple_years(start_year: int, end_year: int, 
                                   save_dir: str = '../data/raw') -> pd.DataFrame:
    """
    Scrape dead money data for multiple years.
    
    Args:
        start_year: Starting year (inclusive)
        end_year: Ending year (inclusive)
        save_dir: Directory to save individual year files
        
    Returns:
        Combined DataFrame for all years
    """
    all_data = []
    
    for year in range(start_year, end_year + 1):
        save_path = f"{save_dir}/dead_money_{year}.csv"
        df = scrape_spotrac_dead_money(year, save_path)
        
        if not df.empty:
            all_data.append(df)
        
        # Be respectful to the server
        time.sleep(2)
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_path = f"{save_dir}/dead_money_all_years.csv"
        combined_df.to_csv(combined_path, index=False)
        logger.info(f"Saved combined data to {combined_path}")
        return combined_df
    
    return pd.DataFrame()


def load_manual_data(data_dir: str = '../data/raw') -> pd.DataFrame:
    """
    Load manually downloaded dead money CSV files.
    
    Instructions for manual download:
    1. Visit https://www.spotrac.com/nfl/cap/2024/ (change year as needed)
    2. Copy the table data or use browser dev tools to save
    3. Save as CSV with columns: team, year, active_cap, dead_money, total_cap
    4. Place in data/raw directory
    
    Args:
        data_dir: Directory containing CSV files
        
    Returns:
        Combined DataFrame from all CSV files
    """
    import glob
    
    csv_files = glob.glob(f"{data_dir}/dead_money_*.csv")
    
    if not csv_files:
        logger.warning(f"No CSV files found in {data_dir}")
        return pd.DataFrame()
    
    all_data = []
    for file in csv_files:
        df = pd.read_csv(file)
        all_data.append(df)
        logger.info(f"Loaded {file}")
    
    combined = pd.concat(all_data, ignore_index=True)
    logger.info(f"Loaded {len(combined)} total records")
    return combined


def create_sample_data(start_year: int = 2015, end_year: int = 2024, 
                       save_path: str = '../data/raw/dead_money_sample.csv') -> pd.DataFrame:
    """
    Create realistic sample dead money data for testing and exploration.
    Based on actual NFL trends: dead money has been increasing over time.
    
    Args:
        start_year: Starting year
        end_year: Ending year
        save_path: Path to save the sample data
        
    Returns:
        DataFrame with sample dead money data
    """
    np.random.seed(42)
    
    teams = ['ARI', 'ATL', 'BAL', 'BUF', 'CAR', 'CHI', 'CIN', 'CLE',
             'DAL', 'DEN', 'DET', 'GB', 'HOU', 'IND', 'JAX', 'KC',
             'LAC', 'LAR', 'LV', 'MIA', 'MIN', 'NE', 'NO', 'NYG',
             'NYJ', 'PHI', 'PIT', 'SF', 'SEA', 'TB', 'TEN', 'WAS']
    
    data = []
    
    for year in range(start_year, end_year + 1):
        # Salary cap increases over time (approximate real values)
        base_cap = 120 + (year - 2015) * 10  # $120M in 2015, increasing by ~$10M/year
        
        for team in teams:
            # Dead money varies but has been trending up
            # Average around 10-15% of cap, some teams much higher
            base_dead_pct = 8 + (year - 2015) * 0.5  # Increasing trend
            team_modifier = np.random.normal(1.0, 0.4)  # Some teams worse than others
            dead_cap_pct = max(2, min(30, base_dead_pct * team_modifier))
            
            dead_money = (dead_cap_pct / 100) * base_cap
            active_cap = base_cap - dead_money
            
            data.append({
                'team': team,
                'year': year,
                'active_cap': round(active_cap, 2),
                'dead_money': round(dead_money, 2),
                'total_cap': base_cap,
                'dead_cap_pct': round(dead_cap_pct, 2)
            })
    
    df = pd.DataFrame(data)
    
    if save_path:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(save_path, index=False)
        logger.info(f"Created sample data: {len(df)} records saved to {save_path}")
    
    return df


def create_sample_player_data(start_year: int = 2015, end_year: int = 2024,
                              save_path: str = '../data/raw/player_dead_money_sample.csv') -> pd.DataFrame:
    """
    Create realistic sample player-level dead money data.
    Simulates notable players who generated significant dead cap hits.
    
    Args:
        start_year: Starting year
        end_year: Ending year
        save_path: Path to save the sample data
        
    Returns:
        DataFrame with player dead money data
    """
    np.random.seed(42)
    
    # Sample of realistic player names and positions
    positions = ['QB', 'WR', 'RB', 'DE', 'LB', 'CB', 'OL', 'DT', 'TE', 'S']
    
    # Common first/last names for variety
    first_names = ['Tom', 'Aaron', 'Russell', 'Cam', 'Matt', 'Matthew', 'Kirk', 
                   'Julio', 'DeAndre', 'Larry', 'Calvin', 'Odell', 'DeShaun',
                   'Le\'Veon', 'Todd', 'David', 'Von', 'Khalil', 'Joey',
                   'Patrick', 'Richard', 'Stephon', 'Jalen', 'Davante']
    
    last_names = ['Johnson', 'Smith', 'Williams', 'Brown', 'Jones', 'Miller',
                  'Davis', 'Garcia', 'Rodriguez', 'Wilson', 'Martinez', 'Anderson',
                  'Taylor', 'Thomas', 'Moore', 'Jackson', 'Martin', 'Lee',
                  'White', 'Harris', 'Clark', 'Lewis', 'Robinson', 'Walker']
    
    teams = ['ARI', 'ATL', 'BAL', 'BUF', 'CAR', 'CHI', 'CIN', 'CLE',
             'DAL', 'DEN', 'DET', 'GB', 'HOU', 'IND', 'JAX', 'KC',
             'LAC', 'LAR', 'LV', 'MIA', 'MIN', 'NE', 'NO', 'NYG',
             'NYJ', 'PHI', 'PIT', 'SF', 'SEA', 'TB', 'TEN', 'WAS']
    
    data = []
    player_id = 1
    
    # Generate some "dead money kings" - players with multiple high dead cap hits
    num_kings = 10
    for i in range(num_kings):
        player_name = f"{np.random.choice(first_names)} {np.random.choice(last_names)}"
        position = np.random.choice(['QB', 'WR', 'DE', 'LB'] if i < 5 else positions)
        
        # Kings appear multiple times (traded/released multiple times)
        num_appearances = np.random.randint(2, 5)
        years_active = np.random.choice(range(start_year, end_year), num_appearances, replace=False)
        
        for year in sorted(years_active):
            team = np.random.choice(teams)
            
            # Kings have large dead cap hits
            base_dead_cap = 15 + np.random.exponential(8)
            dead_cap_hit = min(base_dead_cap, 45)  # Cap at realistic max
            
            data.append({
                'player_id': f'P{player_id:04d}',
                'player_name': player_name,
                'position': position,
                'team': team,
                'year': year,
                'dead_cap_hit': round(dead_cap_hit, 2),
                'is_king': True
            })
        
        player_id += 1
    
    # Generate regular players with dead cap hits
    num_regular_players = 150
    for i in range(num_regular_players):
        player_name = f"{np.random.choice(first_names)} {np.random.choice(last_names)} {i}"
        position = np.random.choice(positions)
        team = np.random.choice(teams)
        year = np.random.randint(start_year, end_year + 1)
        
        # Regular players have smaller dead cap hits
        # Position affects size: QB/WR/DE tend to have larger contracts
        if position in ['QB', 'DE', 'WR']:
            base_dead_cap = 3 + np.random.exponential(4)
        else:
            base_dead_cap = 1 + np.random.exponential(2)
        
        dead_cap_hit = min(base_dead_cap, 30)
        
        data.append({
            'player_id': f'P{player_id:04d}',
            'player_name': player_name,
            'position': position,
            'team': team,
            'year': year,
            'dead_cap_hit': round(dead_cap_hit, 2),
            'is_king': False
        })
        
        player_id += 1
    
    df = pd.DataFrame(data)
    df = df.sort_values(['year', 'dead_cap_hit'], ascending=[True, False])
    
    if save_path:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(save_path, index=False)
        logger.info(f"Created player sample data: {len(df)} records saved to {save_path}")
    
    return df


def fetch_contract_data(team: str, year: int) -> Optional[pd.DataFrame]:
    """
    Fetch contract information for a team in a given year.
    
    Args:
        team: Team abbreviation
        year: NFL season year
        
    Returns:
        DataFrame with contract details or None if unavailable
    """
    # TODO: Implement contract data fetching
    pass
