{{ config(materialized='table', schema='staging') }}

with seed as (
  select * from {{ source('spotrac','spotrac_team_cap') }}
),
normalized as (
  select
    team_name,
    team,
    year,
    active_cap_millions,
    dead_money_millions,
    salary_cap_millions,
    cap_space_millions,
    dead_cap_pct
  from seed
)

select * from normalized