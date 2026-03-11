
--moyenne stats pour chaque role

WITH base AS (
    SELECT * FROM {{ ref('int_player_match_stats') }}
),

profiles AS (
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
),

min_max AS (
    SELECT
        MIN(avg_dmg_per_min) AS min_dmg, MAX(avg_dmg_per_min) AS max_dmg,
        MIN(avg_dmg_taken_per_min) AS min_taken, MAX(avg_dmg_taken_per_min) AS max_taken,
        MIN(avg_gold_per_min) AS min_gold, MAX(avg_gold_per_min) AS max_gold,
        MIN(avg_kda) AS min_kda, MAX(avg_kda) AS max_kda,
        MIN(avg_vision_per_min) AS min_vision, MAX(avg_vision_per_min) AS max_vision,
        MIN(avg_heal_per_min) AS min_heal, MAX(avg_heal_per_min) AS max_heal,
        MIN(avg_obj_dmg_per_min) AS min_obj, MAX(avg_obj_dmg_per_min) AS max_obj,
        MIN(avg_cs_per_min) AS min_cs, MAX(avg_cs_per_min) AS max_cs,
        MIN(avg_cc_time) AS min_cc, MAX(avg_cc_time) AS max_cc
    FROM profiles
)

SELECT
    p.*,

    
    ROUND(SAFE_DIVIDE(p.avg_dmg_per_min - mm.min_dmg, mm.max_dmg - mm.min_dmg) * 100, 1)             AS norm_dmg_per_min,
    ROUND(SAFE_DIVIDE(p.avg_dmg_taken_per_min - mm.min_taken, mm.max_taken - mm.min_taken) * 100, 1)  AS norm_dmg_taken_per_min,
    ROUND(SAFE_DIVIDE(p.avg_gold_per_min - mm.min_gold, mm.max_gold - mm.min_gold) * 100, 1)          AS norm_gold_per_min,
    ROUND(SAFE_DIVIDE(p.avg_kda - mm.min_kda, mm.max_kda - mm.min_kda) * 100, 1)                     AS norm_kda,
    ROUND(SAFE_DIVIDE(p.avg_vision_per_min - mm.min_vision, mm.max_vision - mm.min_vision) * 100, 1)  AS norm_vision_per_min,
    ROUND(SAFE_DIVIDE(p.avg_heal_per_min - mm.min_heal, mm.max_heal - mm.min_heal) * 100, 1)          AS norm_heal_per_min,
    ROUND(SAFE_DIVIDE(p.avg_obj_dmg_per_min - mm.min_obj, mm.max_obj - mm.min_obj) * 100, 1)          AS norm_obj_dmg_per_min,
    ROUND(SAFE_DIVIDE(p.avg_cs_per_min - mm.min_cs, mm.max_cs - mm.min_cs) * 100, 1)                  AS norm_cs_per_min,
    ROUND(SAFE_DIVIDE(p.avg_cc_time - mm.min_cc, mm.max_cc - mm.min_cc) * 100, 1)                     AS norm_cc_time                       --valeurs normalisées 0-100 pour essayer de créer un radar chart

FROM profiles p
CROSS JOIN min_max mm
ORDER BY p.avg_dmg_per_min DESC