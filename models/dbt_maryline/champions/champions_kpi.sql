WITH position_counts AS (
  SELECT
    championid,
    champion_name,
    position,
    COUNT(*) AS nb_games_position
  FROM `projetlol-489709.projet2.all_datas`
  GROUP BY championid, champion_name, position
),

dominant_position AS (
  SELECT
    championid,
    champion_name,
    position AS dominant_position
  FROM (
    SELECT
      championid,
      champion_name,
      position,
      nb_games_position,
      ROW_NUMBER() OVER(
        PARTITION BY championid
        ORDER BY nb_games_position DESC
      ) AS rn
    FROM position_counts
  )
  WHERE rn = 1
),

bans_per_match AS (
  SELECT DISTINCT
    match_id,
    name AS champion_name
  FROM `projetlol-489709.projet2.all_datas`
  WHERE name IS NOT NULL
),

bans_count AS (
  SELECT
    champion_name,
    COUNT(match_id) AS nb_bans
  FROM bans_per_match
  GROUP BY champion_name
)

SELECT
  a.championid,
  a.champion_name,
  d.dominant_position,

  COUNT(DISTINCT a.match_id) AS nb_games,
  
  COUNT(DISTINCT IF(a.player_win = 1, a.match_id, NULL)) AS nb_win,

  ROUND(
    COUNT(DISTINCT IF(a.player_win = 1, a.match_id, NULL)) /
    COUNT(DISTINCT a.match_id),
    4
  ) AS win_rate,

  ROUND(
    COUNT(DISTINCT a.match_id) /
    (SELECT COUNT(DISTINCT match_id)
     FROM `projetlol-489709.projet2.all_datas`),
    4
  ) AS pick_rate,

  ROUND(SUM(a.goldearned),0) AS tot_goldearned,
  ROUND(AVG(a.gold_per_min),2) AS avg_gold_per_min,

  ROUND(SUM(a.kills),0) AS tot_kills,
  ROUND(SUM(a.deaths),0) AS tot_deaths,
  ROUND(SUM(a.assists),0) AS tot_assists,

  ROUND(AVG(a.kda),2) AS avg_kda,
  ROUND(AVG(a.damage_to_champs),0) AS avg_damage_dealt,
  ROUND(AVG(a.damage_taken),0) AS avg_damage_taken,

  ROUND(AVG(a.duration_mins),2) AS avg_match_duration,
  ROUND(AVG(a.visionscore),0) AS avg_visionscore,

  COALESCE(b.nb_bans,0) AS nb_bans

FROM `projetlol-489709.projet2.all_datas` a

LEFT JOIN dominant_position d
ON a.championid = d.championid

LEFT JOIN bans_count b
ON a.champion_name = b.champion_name

GROUP BY
  a.championid,
  a.champion_name,
  d.dominant_position,
  b.nb_bans

HAVING nb_games > 100

ORDER BY win_rate DESC