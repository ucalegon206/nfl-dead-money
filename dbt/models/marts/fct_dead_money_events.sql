-- [TODO: PRODUCTIONIZATION - PHASE 2]
-- Mart model: Fact table - Dead money events

{{ config(
    materialized='table',
    schema='marts',
    tags=['mart', 'facts']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['player_id', 'team', 'year']) }} as dead_money_event_id,
    player_id,
    player_name,
    position,
    team,
    year,
    dead_cap_hit,
    team_total_dead_money,
    total_cap_million,
    dead_cap_percentage,
    pct_of_team_dead_money,
    impact_level,
    CURRENT_TIMESTAMP() as dbt_created_at
    
FROM {{ ref('int_player_team_analysis') }}

ORDER BY year DESC, dead_cap_hit DESC
