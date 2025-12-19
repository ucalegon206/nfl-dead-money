-- Staging model for player rankings CSVs written by snapshot script
-- Reads all CSVs under data/raw/player_rankings_*.csv via DuckDB globbing

with src as (
    select *
    from read_csv_auto('../../data/raw/player_rankings_*.csv',
                       header=True,
                       ignore_errors=True)
)

select
  cast(Player as varchar) as player,
  cast(Team as varchar) as team,
  cast(Position as varchar) as position,
  try_cast(CapValue as double) as cap_value,
  try_cast(Year as integer) as year
from src
where player is not null and player <> ''
