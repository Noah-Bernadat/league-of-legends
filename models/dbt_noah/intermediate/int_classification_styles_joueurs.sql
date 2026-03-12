{{ config(materialized='view') }}

WITH stats_union AS (

    SELECT * FROM `projetlol-489709.projet2.stats1`
    UNION ALL
    SELECT * FROM `projetlol-489709.projet2.stats2`

)

SELECT
  p.id,
  p.matchid,
  p.player,
  p.championid,
  c.name AS champion_name,
  p.role,
  p.position,

  CASE
    WHEN p.position = 'TOP' THEN 'Toplane'
    WHEN p.position = 'JUNGLE' THEN 'Jungle'
    WHEN p.position = 'MID' THEN 'Midlane'
    WHEN p.role = 'DUO_CARRY' AND p.position = 'BOT' THEN 'ADC'
    WHEN p.role = 'DUO_SUPPORT' AND p.position = 'BOT' THEN 'Support'
    ELSE 'Autres'
  END AS vrai_role,

  s.win,
  s.kills,
  s.deaths,
  s.assists,

  s.totdmgtochamp,
  s.totdmgtaken,
  s.totheal,

  s.totminionskilled,
  s.neutralminionskilled,

  s.goldearned,
  s.visionscore,

  s.wardsplaced,
  s.wardskilled

FROM `projetlol-489709.projet2.participants` p

LEFT JOIN stats_union s
  ON p.id = s.id

LEFT JOIN `projetlol-489709.projet2.champs` c
  ON p.championid = c.id