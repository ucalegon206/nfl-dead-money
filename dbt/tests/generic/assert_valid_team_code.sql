-- tests/generic/assert_valid_team_code.sql
-- Generic test to assert all team codes are valid NFL codes

{% test assert_valid_team_code(model, column_name) %}
  select *
  from {{ model }}
  where {{ column_name }} not in (
    'ARI','ATL','BAL','BUF','CAR','CHI','CIN','CLE',
    'DAL','DEN','DET','GB','HOU','IND','JAX','KC',
    'LAC','LAR','LV','MIA','MIN','NE','NO','NYG',
    'NYJ','PHI','PIT','SF','SEA','TB','TEN','WAS'
  )
{% endtest %}
