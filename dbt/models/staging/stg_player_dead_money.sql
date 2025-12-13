-- [TODO: PRODUCTIONIZATION - PHASE 2]
-- Staging model: Clean and standardize player dead money data
-- Target: Remove duplicates, standardize types, add load timestamp

{{ config(
    materialized='table',
    schema='staging',
    tags=['staging', 'player_data']
) }}

SELECT
    player_id,
    player_name,
    position,
    team,
    CAST(year AS INT) as year,
    CAST(dead_cap_hit AS DECIMAL(10,2)) as dead_cap_hit,
    CURRENT_TIMESTAMP() as dbt_loaded_at,
    ROW_NUMBER() OVER (PARTITION BY player_id, team, year ORDER BY _sdc_extracted_at DESC) as rn
    
FROM {{ source('raw', 'player_dead_money_raw') }}

WHERE rn = 1  -- Remove duplicates, keep latest
  AND player_id IS NOT NULL
  AND dead_cap_hit > 0
