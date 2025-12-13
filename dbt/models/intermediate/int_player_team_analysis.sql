-- [TODO: PRODUCTIONIZATION - PHASE 2]
-- Intermediate model: Join player and team data with validation metrics

{{ config(
    materialized='table',
    schema='intermediate',
    tags=['intermediate', 'enriched']
) }}

WITH player_data AS (
    SELECT * FROM {{ ref('stg_player_dead_money') }}
),

team_data AS (
    SELECT * FROM {{ ref('stg_team_dead_money') }}
),

joined AS (
    SELECT
        p.player_id,
        p.player_name,
        p.position,
        p.team,
        p.year,
        p.dead_cap_hit,
        t.dead_money_million as team_total_dead_money,
        t.total_cap_million,
        t.dead_cap_percentage,
        ROUND(p.dead_cap_hit / NULLIF(t.dead_money_million, 0) * 100, 2) as pct_of_team_dead_money,
        CASE 
            WHEN p.dead_cap_hit > 10 THEN 'Major'
            WHEN p.dead_cap_hit > 5 THEN 'Significant'
            ELSE 'Minor'
        END as impact_level
    FROM player_data p
    LEFT JOIN team_data t 
        ON p.team = t.team 
        AND p.year = t.year
)

SELECT * FROM joined
