SELECT
  championid,
  champion_name,

  DATE_TRUNC(match_datetime, MONTH) AS month,

  COUNT(DISTINCT match_id) AS nb_games,

  COUNT(DISTINCT IF(player_win = 1, match_id, NULL)) AS nb_wins,

  ROUND(
    COUNT(DISTINCT IF(player_win = 1, match_id, NULL)) /
    COUNT(DISTINCT match_id),
    4
  ) AS win_rate

FROM `projetlol-489709.projet2.all_datas`

GROUP BY
  championid,
  champion_name,
  month

HAVING nb_games > 50

ORDER BY
  champion_name,
  month