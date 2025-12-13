-- [TODO: PRODUCTIONIZATION - PHASE 2]
-- Mart model: Dimension table - Teams

{{ config(
    materialized='table',
    schema='marts',
    tags=['mart', 'dimensions']
) }}

SELECT DISTINCT
    team,
    CASE 
        WHEN team = 'ARI' THEN 'Arizona Cardinals'
        WHEN team = 'ATL' THEN 'Atlanta Falcons'
        WHEN team = 'BAL' THEN 'Baltimore Ravens'
        -- TODO: Add all 32 teams
        ELSE team
    END as team_name,
    CURRENT_TIMESTAMP() as dbt_created_at
    
FROM {{ ref('stg_team_dead_money') }}

ORDER BY team
