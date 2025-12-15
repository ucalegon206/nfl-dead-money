

-- Dead money by team and year: for team-level heatmap/line chart
with team_cap as (
  select
    year,
    team,
    team_name,
    dead_money_millions,
    salary_cap_millions,
    (case when salary_cap_millions > 0 
      then round((dead_money_millions / salary_cap_millions * 100)::numeric, 2)
      else 0 end) as dead_cap_pct
  from "nfl_dead_money"."main_staging"."stg_spotrac_team_cap"
)

select
  year,
  team,
  team_name,
  round(dead_money_millions::numeric, 2) as dead_money_millions,
  round(salary_cap_millions::numeric, 2) as salary_cap_millions,
  dead_cap_pct,
  row_number() over (partition by year order by dead_money_millions desc) as rank_in_year
from team_cap
order by year, rank_in_year