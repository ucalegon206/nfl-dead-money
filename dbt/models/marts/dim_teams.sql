-- [TODO: PRODUCTIONIZATION - PHASE 2]
-- Mart model: Dimension table - Teams

{{ config(
    materialized='table',
    schema='marts',
    tags=['mart', 'dimensions']
) }}

with teams as (
    select distinct team as team_code, team_name
    from {{ ref('stg_spotrac_team_cap') }}
)

select team_code as team,
             team_name,
             CURRENT_TIMESTAMP() as dbt_created_at
from teams
order by team_code
