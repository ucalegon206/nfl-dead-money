# How to Get Spotrac Dead Money Data

Spotrac doesn't have a public API and actively blocks automated scraping. Here are your options:

## Option 1: Manual Download (Recommended - Most Reliable)

### Team Dead Money by Year
1. Visit https://www.spotrac.com/nfl/cap/2024/ (change year as needed)
2. Scroll to the "Team Salary Cap Tracker" table
3. Click "Export" or manually copy the table
4. Save as CSV to `data/raw/spotrac_team_cap_YYYY.csv`

**Key columns:**
- Team
- Active Cap
- Dead Money
- Cap Space
- Salary Cap

### Player Dead Money
1. Visit https://www.spotrac.com/nfl/dead-money/2024/
2. This shows all players with dead cap charges
3. Export table or copy data
4. Save as `data/raw/spotrac_player_dead_money_YYYY.csv`

**Key columns:**
- Player
- Position  
- Team(s)
- Dead Cap Hit
- Years Remaining (if shown)

### Player Contract Details
For individual player contracts with full breakdown:
1. Visit https://www.spotrac.com/nfl/rankings/
2. Search for specific player
3. Contract page shows year-by-year breakdown:
   - Base Salary
   - Signing Bonus
   - Roster Bonus
   - Cap Hit
   - Dead Cap
   - Guarantees

## Option 2: Automated Scraping (May Be Blocked)

We have a scraper in `src/data_collection.py`:

```python
from src.data_collection import scrape_spotrac_dead_money

# Try scraping (may fail due to bot detection)
df = scrape_spotrac_dead_money(year=2024, save_path="data/raw/spotrac_2024.csv")
```

**Limitations:**
- Spotrac uses CloudFlare bot protection
- May require CAPTCHA solving
- Rate limiting applies
- May need browser automation (Selenium/Playwright)

## Option 3: Use Existing CSV Export Tool

Some Spotrac pages have a built-in CSV export:
1. Navigate to the data page
2. Look for "Export to CSV" button (usually top-right)
3. Download and place in `data/raw/`

## Option 4: Browser Extension (Advanced)

Use a browser extension to export table data:
- Table Capture (Chrome/Firefox)
- Data Scraper (Chrome)
- Copy Tables (Firefox)

Steps:
1. Install extension
2. Navigate to Spotrac page
3. Use extension to extract table
4. Export as CSV

## Option 5: Alternative Source - Over The Cap (OTC)

Over The Cap (https://overthecap.com) has similar data:
- Often easier to scrape
- More dev-friendly structure
- May have export options

Example URLs:
- Team cap: https://overthecap.com/salary-cap-space/
- Player contracts: https://overthecap.com/contracts/
- Dead money: https://overthecap.com/dead-money/

## Integration After Download

Once you have CSV files in `data/raw/`, use our pipeline:

```python
from src.pipeline_tasks import merge_dead_money

# Update the dead money CSV path in pipeline_tasks.py
merge_dead_money(dead_money_csv="data/raw/spotrac_player_dead_money_2024.csv")
```

Or use the contracts loader:

```python
from src.contracts_loader import load_spotrac_csv

df = load_spotrac_csv("data/raw/spotrac_contracts_2024.csv")
```

## Data Quality Notes

After importing Spotrac data, run data quality checks:

```python
from src.pipeline_tasks import run_data_quality

run_data_quality()
```

Expected validation results:
- Player totals should match team aggregates
- Dead money should be ≥ $0
- Years should be 2015-2024 (or current range)
- All teams should have entries

## Recommended Approach

**For this project, I recommend:**

1. **Start with manual download** of 1-2 recent years to validate the pipeline
2. **Test integration** with our existing data model
3. **Build a robust scraper** only if you need regular updates
4. Consider **Over The Cap** as it's often more scrape-friendly

## Need Help?

- Check `src/data_collection.py` for existing scraper code
- See `src/contracts_loader.py` for CSV import utilities
- Review `data/raw/player_dead_money_sample.csv` for expected format
