# NFL Dead Money - Productionization Roadmap

## Current Phase: Exploration & Proof of Concept ✅
- [x] Identify data sources (Spotrac, Over The Cap)
- [x] Create sample data reflecting real trends
- [x] Build initial analysis notebooks
- [x] Establish data validation framework
- [ ] Confirm optimal real data source

## Phase 2: Production Data Pipeline (Next)

### Timeline Estimate
- Weeks 1-2: Airflow DAG development
- Weeks 2-3: dbt models and tests
- Weeks 3-4: Iceberg setup and optimization
- Week 4-5: Integration testing and deployment

### Key Decisions Needed
1. **Data Warehouse**: Snowflake vs BigQuery vs Databricks?
2. **Airflow Hosting**: Cloud-managed (MWAA) vs self-hosted?
3. **Real Data Source**: Manual Spotrac exports vs API vs web scraping?
4. **Update Frequency**: Daily vs weekly vs manual trigger?
5. **Historical Data**: How far back do we need? (Current: 2015-2024)

### Budget Considerations
- Airflow infrastructure: ~$300-500/month
- Data warehouse: ~$100-300/month depending on usage
- Iceberg storage: ~$20-50/month for raw data
- Total estimated: ~$420-850/month

### Risk Mitigation
- [ ] Verify Spotrac data access & reliability
- [ ] Test scraper robustness (CAPTCHAs, IP blocking)
- [ ] Plan for Iceberg schema changes
- [ ] Document manual intervention procedures

## Phase 3: ML & Prediction Models (Future)

### Objectives
- Predict which player signings will generate excessive dead money
- Identify patterns in high dead-money teams
- Build early warning system for contract risk

### Data Requirements
- Player performance metrics (stats, injuries)
- Team roster turnover rates
- Draft pick performance
- Free agent market data

## Success Criteria
- ✅ Automated data collection with 99.5% uptime
- ✅ Data quality validations passing 99.9% of the time
- ✅ 24-hour data freshness from source
- ✅ Predictive model achieving 70%+ accuracy
