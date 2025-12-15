

-- Stage raw Spotrac player dead money data
-- Source: data/staging/stg_spotrac_dead_money.csv (from ingestion layer)
-- Cleans column names, casts to correct types, applies light validation

-- For development: source from staging CSV directly
-- In production, would source from database table
select
  player_name,
  team,
  cast(year as integer) as year,
  cast(dead_cap_hit as decimal(10, 2)) as dead_cap_millions,
  position,
  is_king
from "nfl_dead_money"."main"."spotrac_dead_money"
where dead_cap_hit > 0
order by year, team, dead_cap_hit desc