
--perfs des champions par role

WITH base AS (
    SELECT * FROM {{ ref('int_player_match_stats') }}
)

SELECT
    clean_role,
    champion_name,
    COUNT(*) AS games_played,
    ROUND(AVG(win) * 100, 1)   AS winrate_pct,             --winrate
    ROUND(AVG(kills), 1)       AS avg_kills,               --kda
    ROUND(AVG(deaths), 1)      AS avg_deaths,
    ROUND(AVG(assists), 1)     AS avg_assists,
    ROUND(AVG(kda), 2)         AS avg_kda,
    ROUND(AVG(dmg_to_champs_per_min), 1)    AS avg_dmg_per_min,         --stats clés/min
    ROUND(AVG(gold_per_min), 1)   AS avg_gold_per_min,
    ROUND(AVG(cs_per_min), 1)     AS avg_cs_per_min,
    ROUND(AVG(vision_per_min), 2) AS avg_vision_per_min,
    ROUND(AVG(obj_dmg_per_min), 1) AS avg_obj_dmg_per_min,
    SUM(doublekills)    AS total_double_kills,
    SUM(triplekills)    AS total_triple_kills,
    SUM(quadrakills)    AS total_quadra_kills,
    SUM(pentakills)     AS total_penta_kills

FROM base
GROUP BY clean_role, champion_name

HAVING COUNT(*) >= 30

ORDER BY clean_role, winrate_pct DESC