
--facteur clé de victoire pour chaque role


WITH base AS (
    SELECT * FROM {{ ref('int_player_match_stats') }}
),
correlations AS (

SELECT
    'correlation'   AS analysis_type,
    clean_role,
    ROUND(CORR(kills, win), 3)              AS corr_kills,
    ROUND(CORR(deaths, win), 3)             AS corr_deaths,
    ROUND(CORR(assists, win), 3)            AS corr_assists,
    ROUND(CORR(totdmgtochamp, win), 3)      AS corr_dmg_to_champs,
    ROUND(CORR(totdmgtaken, win), 3)        AS corr_dmg_taken,
    ROUND(CORR(totheal, win), 3)            AS corr_heal,
    ROUND(CORR(dmgtoobj, win), 3)           AS corr_obj_dmg,
    ROUND(CORR(dmgtoturrets, win), 3)       AS corr_turret_dmg,
    ROUND(CORR(goldearned, win), 3)         AS corr_gold,
    ROUND(CORR(visionscore, win), 3)        AS corr_vision,
    ROUND(CORR(totminionskilled + neutralminionskilled, win), 3) AS corr_cs,
    ROUND(CORR(wardskilled, win), 3)        AS corr_wards_killed,
    ROUND(CORR(firstblood, win), 3)         AS corr_first_blood,
    ROUND(CORR(totcctimedealt, win), 3)     AS corr_cc_time,
    NULL                    AS first_blood_rate_pct,            --colonne 1st blood vides pour cette partie
    NULL                    AS winrate_after_fb,
    NULL                    AS fb_games

FROM base
GROUP BY clean_role
),

fb_rate AS (
    SELECT
        clean_role,
        ROUND(AVG(firstblood) * 100, 1) AS first_blood_rate_pct
    FROM base
    GROUP BY clean_role
),

fb_winrate AS (
    SELECT
        clean_role,
        ROUND(AVG(win) * 100, 1) AS winrate_after_fb,
        COUNT(*) AS fb_games
    FROM base
    WHERE firstblood = 1
    GROUP BY clean_role
),

firstblood AS (
    SELECT
        'firstblood'            AS analysis_type,
        r.clean_role,

        -- Colonnes corrélation vides pour cette partie
        NULL AS corr_kills, NULL AS corr_deaths, NULL AS corr_assists,
        NULL AS corr_dmg_to_champs, NULL AS corr_dmg_taken, NULL AS corr_heal,
        NULL AS corr_obj_dmg, NULL AS corr_turret_dmg, NULL AS corr_gold,
        NULL AS corr_vision, NULL AS corr_cs, NULL AS corr_wards_killed,
        NULL AS corr_first_blood, NULL AS corr_cc_time,

        -- Colonnes FB
        r.first_blood_rate_pct,
        w.winrate_after_fb,
        w.fb_games

    FROM fb_rate r
    LEFT JOIN fb_winrate w ON r.clean_role = w.clean_role
)

SELECT * FROM correlations
UNION ALL
SELECT * FROM firstblood

ORDER BY analysis_type, clean_role