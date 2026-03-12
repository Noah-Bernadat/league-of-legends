WITH styles AS (
  SELECT
    champion_name,

    CASE
      WHEN totdmgtaken > 30000 THEN 'Tank'
      WHEN totheal > 5000 THEN 'Support'
      WHEN totdmgtochamp > 25000 THEN 'DPS'
      WHEN (totminionskilled + neutralminionskilled) > 250 THEN 'Farmer'
      ELSE 'Polyvalent'
    END AS player_style

  FROM `projetlol-489709.dbt_nbernadat.v_player_match_stats`
),

agg AS (
  SELECT
    champion_name,
    player_style,
    COUNT(*) AS nb
  FROM styles
  GROUP BY champion_name, player_style
),

ranked AS (
  SELECT *,
  ROW_NUMBER() OVER(PARTITION BY champion_name ORDER BY nb DESC) AS r
  FROM agg
)

SELECT
  champion_name,
  player_style AS dominant_style,
  nb
FROM ranked
WHERE r = 1
ORDER BY nb DESC