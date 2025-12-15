
  
    
    

    create  table
      "nfl_dead_money"."main_marts"."dim_teams__dbt_tmp"
  
    as (
      -- [TODO: PRODUCTIONIZATION - PHASE 2]
-- Mart model: Dimension table - Teams



with teams as (
    select distinct team as team_code, team_name
    from "nfl_dead_money"."main_staging"."stg_spotrac_team_cap"
)

select team_code as team,
             team_name,
             CURRENT_TIMESTAMP() as dbt_created_at
from teams
order by team_code
    );
  
  