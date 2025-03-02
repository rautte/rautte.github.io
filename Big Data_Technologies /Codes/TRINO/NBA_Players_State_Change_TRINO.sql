WITH source AS (
  SELECT 
    player_name,
    is_active,
    seasons,
    years_since_last_active,
    current_season
  FROM 
    bootcamp.nba_players
  WHERE 
    player_name IS NOT NULL AND
    is_active IS NOT NULL AND 
    seasons IS NOT NULL AND
    years_since_last_active IS NOT NULL AND
    current_season IS NOT NULL
  GROUP BY 
    1,2,3,4,5
),
players AS (
  SELECT
    player_name,
    is_active,
    ARRAY_AGG(t.years) AS active_seasons,
    years_since_last_active,
    current_season,
    ROW_NUMBER() OVER (PARTITION BY player_name ORDER BY current_season) AS ord
  FROM
    source,
    UNNEST (seasons) AS t (years,b,c,d,e,f,g)
  GROUP BY 
    1,2,4,5
),
past_active AS (
  SELECT 
    p1.player_name,
    COALESCE(LAG(p2.is_active) OVER (PARTITION BY p2.player_name ORDER BY p2.current_season), False) AS was_active,
    p1.is_active,
    p1.active_seasons,
    p1.current_season,
    p1.years_since_last_active,
    p1.ord
  FROM players p1 JOIN players p2
    ON p1.player_name = p2.player_name 
      AND p1.ord = p2.ord
)

SELECT 
  player_name,
  active_seasons,
  is_active,
  current_season,
  CASE
    WHEN ord = 1 
      THEN 'New'
    WHEN was_active = False AND is_active = True
      THEN 'Returned from Retirement'
    WHEN was_active = True AND is_active = True
      THEN 'Continued Playing'
    WHEN was_active = False AND is_active = False 
      THEN 'Stayed Retired'
    WHEN was_active = True AND is_active = False
      THEN 'Retired'
  END AS player_state,
  years_since_last_active AS inactive_years
FROM past_active
ORDER BY 1,4
