{{ config(materialized='table', schema='marts') }}

-- Dead money by player: for player-level analysis with percentile ranks
-- Filter: only include players with > $1M dead cap (avoid noise from minor charges)
with player_dm as (
  select
    player_name,
    team,
    year,
    dead_cap_millions
  from {{ ref('stg_spotrac_dead_money') }}
  where dead_cap_millions > 1.0
),

team_totals as (
  select
    team,
    year,
    sum(dead_cap_millions) as team_total_dead_money_millions
  from player_dm
  group by team, year
),

nfl_stats as (
  select
    year,
    sum(dead_cap_millions) as nfl_total_dead_money_millions,
    avg(dead_cap_millions) as nfl_avg_dead_cap,
    stddev_pop(dead_cap_millions) as nfl_stddev_dead_cap,
    percentile_cont(0.75) within group (order by dead_cap_millions) as p75_dead_cap,
    percentile_cont(0.90) within group (order by dead_cap_millions) as p90_dead_cap,
    percentile_cont(0.95) within group (order by dead_cap_millions) as p95_dead_cap
  from player_dm
  group by year
),

player_with_percentile as (
  select
    p.player_name,
    p.team,
    p.year,
    p.dead_cap_millions,
    t.team_total_dead_money_millions,
    round((p.dead_cap_millions / t.team_total_dead_money_millions * 100)::numeric, 2) as pct_of_team_dead_money,
    n.nfl_total_dead_money_millions,
    round((p.dead_cap_millions / n.nfl_total_dead_money_millions * 100)::numeric, 4) as pct_of_nfl_dead_money,
    percent_rank() over (partition by p.year order by p.dead_cap_millions) as percentile_rank,
    round((percent_rank() over (partition by p.year order by p.dead_cap_millions) * 100)::numeric, 1) as nfl_percentile
  from player_dm p
  join team_totals t on p.team = t.team and p.year = t.year
  join nfl_stats n on p.year = n.year
)

select
  player_name,
  team,
  year,
  round(dead_cap_millions::numeric, 2) as dead_cap_millions,
  round(team_total_dead_money_millions::numeric, 2) as team_total_dead_money_millions,
  pct_of_team_dead_money,
  round(nfl_total_dead_money_millions::numeric, 2) as nfl_total_dead_money_millions,
  pct_of_nfl_dead_money,
  nfl_percentile,
  row_number() over (partition by year order by dead_cap_millions desc) as rank_in_year
from player_with_percentile
order by year, dead_cap_millions desc
