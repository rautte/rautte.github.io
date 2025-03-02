WITH player_pts AS ( 
  SELECT
    game_id,
    team_id,
    COALESCE(team_abbreviation, 'Ref_teamID') AS team_abbreviation,
    COALESCE(team_city, 'Ref_teamID') AS team_city,
    player_id,
    COALESCE(player_name, 'Ref_playerID') AS player_name,
    pts
  FROM
    bootcamp.nba_game_details
  WHERE 
    game_id IS NOT NULL
      AND team_id IS NOT NULL
      AND player_id IS NOT NULL
  GROUP BY 
    1,2,3,4,5,6,7
),
game_team_player AS (
  SELECT 
    COALESCE(CAST(n.game_date_est AS VARCHAR), 'Unknown') AS game_date_est,
    n.season,
    p.game_id,
    p.team_id,
    p.team_abbreviation,
    p.team_city,
    CASE 
      WHEN n.home_team_id = p.team_id
        THEN 'Home'
      WHEN n.visitor_team_id = p.team_id
        THEN 'Away'
    END AS game_at,
    p.player_id,
    p.player_name,
    p.pts AS player_points,
    CASE 
      WHEN n.home_team_id = p.team_id
        THEN n.pts_home
      WHEN n.visitor_team_id = p.team_id
        THEN n.pts_away
    END AS team_points,
    CASE
      WHEN n.home_team_id = p.team_id 
           AND n.home_team_wins = 1
        THEN 1
      WHEN n.visitor_team_id = p.team_id 
           AND n.home_team_wins = 1
        THEN 0
      WHEN n.home_team_id = p.team_id 
           AND n.home_team_wins = 0
        THEN 0
      WHEN n.visitor_team_id = p.team_id 
           AND n.home_team_wins = 0
        THEN 1
    END AS team_wins
  FROM
    bootcamp.nba_games n JOIN player_pts p
     ON p.game_id = n.game_id
  WHERE 
    n.season IS NOT NULL
      AND n.home_team_id IS NOT NULL
      AND n.visitor_team_id IS NOT NULL
      AND n.home_team_wins IS NOT NULL
      AND n.pts_away IS NOT NULL
      AND n.pts_home IS NOT NULL
)

  SELECT 
    CASE 
      WHEN season IS NULL
          AND player_id IS NOT NULL
          AND team_id IS NOT NULL
        THEN 'For_Player_Team'
      WHEN season IS NULL
          AND player_id IS NULL
          AND team_id IS NOT NULL
        THEN 'For_Team'
      WHEN season IS NOT NULL
        THEN CAST(season AS VARCHAR)
    END AS season,
    CASE 
      WHEN team_id IS NULL
        THEN 'For_Player_Season'
      ELSE CAST(team_id AS VARCHAR)
    END AS team_id,
    CASE 
      WHEN team_abbreviation IS NULL
        THEN 'For_Player_Season'
      ELSE team_abbreviation
    END AS team_abbreviation,
    CASE 
      WHEN player_id IS NULL
        THEN 'For_Team'
      ELSE CAST(player_id AS VARCHAR)
    END AS player_id,
    CASE 
      WHEN player_name IS NULL
        THEN 'For_Team'
      ELSE player_name
    END AS player_name,
    SUM(player_points) AS player_points,
    SUM(team_points) AS team_points,
    SUM(team_wins) AS team_wins
  FROM 
    game_team_player
  GROUP BY 
    GROUPING SETS (
    (player_id, player_name, team_id, team_abbreviation),
    (player_id, player_name, season),
    (team_id, team_abbreviation)
    )

