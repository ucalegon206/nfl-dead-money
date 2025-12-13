# NFL Dead Money Analysis

An exploratory data analysis project to investigate the impact and predictability of NFL dead money cap hits.

## Research Questions

1. **Problem Scale**: How bad is the problem of "dead money" in the NFL?
   - Historical trends and magnitudes
   - Impact on team performance
   - Teams most affected

2. **Previous Solutions**: What has been done to help avoid signing players likely to become dead money?
   - Current evaluation methods
   - Success rates
   - Gaps in existing approaches

3. **Pattern Recognition**: Can we find patterns among players who ended up being unproductive?
   - Player characteristics
   - Contract structures
   - Performance indicators
   - Career trajectory markers

## Project Structure

```
nfl-dead-money/
├── notebooks/           # Jupyter notebooks for exploration
│   ├── 01_problem_scale.ipynb
│   ├── 02_historical_analysis.ipynb
│   └── 03_pattern_discovery.ipynb
├── data/               # Raw and processed data
│   ├── raw/
│   └── processed/
├── src/                # Reusable code modules
│   ├── data_collection.py
│   ├── data_processing.py
│   └── visualization.py
├── requirements.txt    # Python dependencies
└── README.md          # This file
```

## Setup

1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On macOS/Linux
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Start Jupyter:
   ```bash
   jupyter notebook
   ```

## Data Sources (To Be Determined)

- NFL salary cap data
- Player contract information
- Performance statistics
- Team records and outcomes

## Next Steps

- [ ] Identify and access reliable data sources
- [ ] Define "dead money" operationally for analysis
- [ ] Establish baseline metrics for problem scale
- [ ] Begin exploratory data analysis
