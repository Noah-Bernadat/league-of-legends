
--moyenne stats pour chaque role

WITH base AS (
    SELECT * FROM {{ ref('int_player_match_stats') }}
)

SELECT
    clean_role,
    COUNT(*) AS total_games,                                        
    ROUND(AVG(win) * 100, 1) AS winrate_pct,                    --winrate
    ROUND(AVG(kills), 1) AS avg_kills,                          --kda
    ROUND(AVG(deaths), 1) AS avg_deaths,
    ROUND(AVG(assists), 1) AS avg_assists,
    ROUND(AVG(kda), 2) AS avg_kda,
    ROUND(AVG(dmg_to_champs_per_min), 1) AS avg_dmg_per_min,    --dmg/min
    ROUND(AVG(dmg_taken_per_min), 1) AS avg_dmg_taken_per_min,
    ROUND(AVG(heal_per_min), 1) AS avg_heal_per_min,
    ROUND(AVG(obj_dmg_per_min), 1) AS avg_obj_dmg_per_min,
    ROUND(AVG(turret_dmg_per_min), 1) AS avg_turret_dmg_per_min,
    ROUND(AVG(gold_per_min), 1) AS avg_gold_per_min,            --eco et farm/min
    ROUND(AVG(cs_per_min), 1) AS avg_cs_per_min,
    ROUND(AVG(vision_per_min), 2) AS avg_vision_per_min,        --vision/min
    ROUND(AVG(totcctimedealt), 1) AS avg_cc_time,               --cc
    ROUND(AVG(firstblood) * 100, 1) AS first_blood_pct          --1st boold

FROM base
GROUP BY clean_role
ORDER BY avg_dmg_per_min DESC