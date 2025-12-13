# Production Data Pipeline Setup Guide

## Current Status
🟡 **Exploration Phase Complete** - Ready to transition to production infrastructure

## Quick Links
- [Productionization Roadmap](.github/productionization-roadmap.md)
- [Airflow DAG Template](dags/nfl_dead_money_pipeline.py)
- [dbt Project](dbt/)
- [Data Validation Framework](src/data_validation.py)

---

## Phase 2: Production Implementation Checklist

### Step 1: Choose Your Data Warehouse
Select one based on your team's expertise and budget:

```
┌─────────────────────────────────────────────────────────────┐
│ Option A: Snowflake (Recommended)                          │
│ - Native Iceberg support                                    │
│ - Great dbt integration (dbt Cloud available)              │
│ - Easy Airflow integration                                  │
│ - Cost: ~$300-500/month for dev environment               │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ Option B: BigQuery                                          │
│ - Serverless (no infrastructure to manage)                 │
│ - Excellent dbt integration                                │
│ - Pay per query (~$6-8 per TB scanned)                     │
│ - Cost: ~$50-200/month depending on usage                 │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ Option C: Databricks (Advanced)                            │
│ - Apache Spark + Delta Lake                                │
│ - Python + SQL support                                     │
│ - ML integrations ready                                    │
│ - Cost: ~$200-600/month                                    │
└─────────────────────────────────────────────────────────────┘
```

### Step 2: Set Up Airflow

**Option A: Managed (Recommended for MVP)**
```bash
# AWS MWAA (Managed Workflows for Apache Airflow)
# Set up in AWS console: ~$300-400/month
# Includes: Auto-scaling, monitoring, backup
```

**Option B: Self-Hosted**
```bash
# Using Docker Compose
docker-compose up -d

# Initial setup:
airflow db init
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --email admin@example.com \
    --role Admin \
    --password password
```

**Option C: Development (Local Testing)**
```bash
# Install Airflow locally
pip install apache-airflow[celery]

# Run:
airflow standalone  # Runs scheduler + webserver
```

### Step 3: Configure dbt

```bash
# Install dbt
pip install dbt-core dbt-snowflake  # or dbt-bigquery, dbt-databricks

# Initialize project (if not already done)
dbt init nfl_dead_money

# Configure profiles.yml
# Location: ~/.dbt/profiles.yml
# Add credentials for your data warehouse

# Test connection
dbt debug

# Run models
dbt run

# Run tests
dbt test
```

### Step 4: Set Up Iceberg

**For Snowflake:**
```sql
-- Create Iceberg table format (supported in Snowflake 2024+)
CREATE TABLE raw.player_dead_money_v2 (
    player_id STRING,
    player_name STRING,
    dead_cap_hit DECIMAL(10,2),
    year INT
)
CLUSTERING BY (year, player_name)
ICEBERG;

-- Enable time travel
ALTER TABLE raw.player_dead_money_v2 SET ICEBERG COPY GRANTS;
```

**For BigQuery:**
```python
# BigQuery Iceberg tables via Apache Iceberg library
from google.cloud import bigquery
import iceberg

# Create catalog connection
# Iceberg support via Apache Iceberg integration
```

### Step 5: Implement Real Data Collection

Current: Mock data in `src/data_collection.py`

**TO DO:**
1. Choose Spotrac collection method:
   - [ ] Manual CSV export (simplest, fastest MVP)
   - [ ] API integration (if available)
   - [ ] Web scraper with proper User-Agent (robust)

2. Update `src/data_collection.py`:
   ```python
   def scrape_spotrac_for_production():
       """
       Real implementation with:
       - Retry logic
       - Rate limiting
       - Error handling
       - Logging
       """
   ```

3. Test data validation:
   ```bash
   pytest tests/test_data_collection.py
   pytest tests/test_data_validation.py
   ```

### Step 6: Create Test Suite

```bash
# Add dbt tests
cat > dbt/tests/unique_players.sql << 'EOF'
select player_id, team, year, count(*)
from {{ ref('stg_player_dead_money') }}
group by 1, 2, 3
having count(*) > 1
EOF

# Run dbt tests
dbt test
```

### Step 7: Deploy DAG

```bash
# Copy DAG to Airflow DAGs folder
cp dags/nfl_dead_money_pipeline.py /airflow/dags/

# Trigger DAG (test)
airflow dags test nfl_dead_money_pipeline 2025-01-01

# Unpause for production
airflow dags unpause nfl_dead_money_pipeline
```

---

## Deployment Timeline

**Week 1: Infrastructure**
- [ ] Select data warehouse
- [ ] Set up Airflow environment
- [ ] Create dev/prod environments

**Week 2: Data Pipeline**
- [ ] Configure dbt
- [ ] Deploy staging models
- [ ] Write dbt tests

**Week 3: Integration**
- [ ] Integrate Airflow + dbt
- [ ] Implement real data collection
- [ ] Iceberg configuration

**Week 4: Testing & Launch**
- [ ] End-to-end pipeline test
- [ ] Backfill historical data
- [ ] Monitoring & alerting setup

---

## Monitoring & Operations

### Key Metrics to Track
```
- Pipeline execution time: <30 minutes
- Data freshness: <24 hours
- Test pass rate: >99%
- Data warehouse cost: <$500/month (MVP)
```

### Alerting Setup
```python
# Slack notifications on failure
SLACK_CONN_ID = "slack_connection"
notify_on_failure = SlackAPIPostOperator(
    task_id='notify_failure',
    text="DAG failed: {{ ds }} {{ task_instance.log_url }}"
)
```

### Daily Validation Dashboard
```python
# Query for monitoring
SELECT 
    DATE(dbt_loaded_at) as load_date,
    COUNT(*) as events_loaded,
    SUM(dead_cap_hit) as total_cap,
    COUNT(DISTINCT player_id) as unique_players
FROM marts.fct_dead_money_events
WHERE DATE(dbt_loaded_at) = CURRENT_DATE()
GROUP BY 1
```

---

## Common Issues & Troubleshooting

### Issue: Spotrac blocks scraper
**Solution**: 
- Use rotating proxies
- Add random delays between requests
- Consider manual export approach for MVP

### Issue: dbt model failures
**Solution**:
- Check staging data with `dbt run --select stg_*`
- Review dbt logs: `dbt run --debug`
- Validate schema changes in Iceberg

### Issue: Pipeline takes too long
**Solution**:
- Add Iceberg partitioning
- Use incremental dbt models
- Parallelize Airflow tasks

---

## Next Steps

1. **Confirm real data source** - Test Spotrac access
2. **Set up development environment** - Local Airflow + dbt
3. **Deploy staging pipeline** - First week of Phase 2
4. **Run backfill** - Historical 2015-2024 data
5. **Activate monitoring** - Before production launch

---

## Questions? 

Reference the productionization roadmap or check comments in:
- `dags/nfl_dead_money_pipeline.py`
- `dbt/dbt_project.yml`
- `src/data_validation.py`
