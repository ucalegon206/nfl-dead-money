-- [TODO: PRODUCTIONIZATION - PHASE 2]
-- Staging model: Clean and standardize team dead money data

{{ config(
    materialized='table',
    schema='staging',
    tags=['staging', 'team_data']
) }}

SELECT
    team,
    CAST(year AS INT) as year,
    CAST(active_cap AS DECIMAL(10,2)) as active_cap_million,
    CAST(dead_money AS DECIMAL(10,2)) as dead_money_million,
    CAST(total_cap AS DECIMAL(10,2)) as total_cap_million,
    CAST(dead_cap_pct AS DECIMAL(5,2)) as dead_cap_percentage,
    CURRENT_TIMESTAMP() as dbt_loaded_at
    
FROM {{ source('raw', 'team_dead_money_raw') }}

WHERE team IS NOT NULL
  AND year BETWEEN 2015 AND CURRENT_YEAR()
