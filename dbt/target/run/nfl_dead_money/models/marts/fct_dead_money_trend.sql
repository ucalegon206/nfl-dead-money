
  
    
    

    create  table
      "nfl_dead_money"."main_marts"."fct_dead_money_trend__dbt_tmp"
  
    as (
      

-- Dead money trend over time: total NFL dead money by year
with team_cap as (
  select
    year,
    team,
    dead_money_millions,
    salary_cap_millions
  from "nfl_dead_money"."main_staging"."stg_spotrac_team_cap"
)

select
  year,
  round(sum(dead_money_millions)::numeric, 2) as total_dead_money_millions,
  round(avg(dead_money_millions)::numeric, 2) as avg_dead_money_per_team,
  round(max(dead_money_millions)::numeric, 2) as max_dead_money_millions,
  count(distinct team) as teams_with_data
from team_cap
group by year
order by year
    );
  
  