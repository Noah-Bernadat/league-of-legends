
--comparaison gagnants vs perdants par role

WITH base AS (
    SELECT * FROM {{ ref('int_player_match_stats') }}
),

win_impact AS(
SELECT
    'win_impact' AS win_type,
    clean_role,
    CAST(win AS STRING) AS category,
    NULL AS category_order,
    COUNT(*) AS games,
    ROUND(AVG(win)*100,1) AS winrate_pct,
    ROUND(AVG(kills), 1) AS avg_kills,                          --kda
    ROUND(AVG(deaths), 1) AS avg_deaths,
    ROUND(AVG(assists), 1) AS avg_assists,
    ROUND(AVG(kda), 2) AS avg_kda,
    ROUND(AVG(dmg_to_champs_per_min), 1) AS avg_dmg_per_min,    --dmg/min
    ROUND(AVG(dmg_taken_per_min), 1) AS avg_dmg_taken_per_min,
    ROUND(AVG(obj_dmg_per_min), 1) AS avg_obj_dmg_per_min,      --obj/min
    ROUND(AVG(turret_dmg_per_min), 1) AS avg_turret_dmg_per_min,
    ROUND(AVG(gold_per_min), 1) AS avg_gold_per_min,            --eco/min
    ROUND(AVG(cs_per_min), 1) AS avg_cs_per_min,
    ROUND(AVG(vision_per_min), 2) AS avg_vision_per_min         --vision/min

FROM base
GROUP BY clean_role, win
),

--en retard vs normal
behind_impact AS (
    SELECT
        'behind_impact'         AS win_type,
        clean_role,
        CASE WHEN kda < 1 THEN 'behind' ELSE 'ok' END AS category,
        NULL                    AS category_order,
        COUNT(*)                AS games,
        ROUND(AVG(win) * 100, 1)                AS winrate_pct,
        ROUND(AVG(kills), 1)                    AS avg_kills,
        ROUND(AVG(deaths), 1)                   AS avg_deaths,
        ROUND(AVG(assists), 1)                  AS avg_assists,
        ROUND(AVG(kda), 2)                      AS avg_kda,
        ROUND(AVG(dmg_to_champs_per_min), 1)    AS avg_dmg_per_min,
        ROUND(AVG(dmg_taken_per_min), 1)        AS avg_dmg_taken_per_min,
        ROUND(AVG(obj_dmg_per_min), 1)          AS avg_obj_dmg_per_min,
        ROUND(AVG(turret_dmg_per_min), 1)       AS avg_turret_dmg_per_min,
        ROUND(AVG(gold_per_min), 1)             AS avg_gold_per_min,
        ROUND(AVG(cs_per_min), 1)               AS avg_cs_per_min,
        ROUND(AVG(vision_per_min), 2)           AS avg_vision_per_min
    FROM base
    GROUP BY clean_role, category
),

--tranches de morts

deaths_brackets AS (
    SELECT
        'deaths_bracket'        AS win_type,
        clean_role,
        CASE
            WHEN deaths BETWEEN 0 AND 3   THEN '0-3 deaths'
            WHEN deaths BETWEEN 4 AND 6   THEN '4-6 deaths'
            WHEN deaths BETWEEN 7 AND 9   THEN '7-9 deaths'
            WHEN deaths >= 10              THEN '10+ deaths'
        END AS category,
        CASE
            WHEN deaths BETWEEN 0 AND 3   THEN 1
            WHEN deaths BETWEEN 4 AND 6   THEN 2
            WHEN deaths BETWEEN 7 AND 9   THEN 3
            WHEN deaths >= 10              THEN 4
        END AS category_order,
        COUNT(*)                AS games,
        ROUND(AVG(win) * 100, 1)                AS winrate_pct,
        ROUND(AVG(kills), 1)                    AS avg_kills,
        ROUND(AVG(deaths), 1)                   AS avg_deaths,
        ROUND(AVG(assists), 1)                  AS avg_assists,
        ROUND(AVG(kda), 2)                      AS avg_kda,
        ROUND(AVG(dmg_to_champs_per_min), 1)    AS avg_dmg_per_min,
        ROUND(AVG(dmg_taken_per_min), 1)        AS avg_dmg_taken_per_min,
        ROUND(AVG(obj_dmg_per_min), 1)          AS avg_obj_dmg_per_min,
        ROUND(AVG(turret_dmg_per_min), 1)       AS avg_turret_dmg_per_min,
        ROUND(AVG(gold_per_min), 1)             AS avg_gold_per_min,
        ROUND(AVG(cs_per_min), 1)               AS avg_cs_per_min,
        ROUND(AVG(vision_per_min), 2)           AS avg_vision_per_min
    FROM base
    GROUP BY clean_role, category, category_order
)

SELECT * FROM win_impact
UNION ALL
SELECT * FROM behind_impact
UNION ALL
SELECT * FROM deaths_brackets

ORDER BY win_type, clean_role, category