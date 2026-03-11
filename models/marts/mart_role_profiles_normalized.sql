WITH base AS (
    SELECT * FROM {{ ref('mart_role_profiles') }}
),


min_max AS (                                                --récup les min et max de chq métrique
    SELECT
        MIN(avg_dmg_per_min)           AS min_dmg,
        MAX(avg_dmg_per_min)           AS max_dmg,
        MIN(avg_dmg_taken_per_min)     AS min_taken,
        MAX(avg_dmg_taken_per_min)     AS max_taken,
        MIN(avg_gold_per_min)          AS min_gold,
        MAX(avg_gold_per_min)          AS max_gold,
        MIN(avg_kda)                   AS min_kda,
        MAX(avg_kda)                   AS max_kda,
        MIN(avg_vision_per_min)        AS min_vision,
        MAX(avg_vision_per_min)        AS max_vision,
        MIN(avg_heal_per_min)          AS min_heal,
        MAX(avg_heal_per_min)          AS max_heal,
        MIN(avg_obj_dmg_per_min)       AS min_obj,
        MAX(avg_obj_dmg_per_min)       AS max_obj,
        MIN(avg_cs_per_min)            AS min_cs,
        MAX(avg_cs_per_min)            AS max_cs,
        MIN(avg_cc_time)               AS min_cc,
        MAX(avg_cc_time)               AS max_cc
    FROM base
)

SELECT
    b.clean_role,                                           --metriques normalisées 0-100

  
    ROUND(SAFE_DIVIDE(b.avg_dmg_per_min - mm.min_dmg, mm.max_dmg - mm.min_dmg) * 100, 1)             AS norm_dmg_per_min,
    ROUND(SAFE_DIVIDE(b.avg_dmg_taken_per_min - mm.min_taken, mm.max_taken - mm.min_taken) * 100, 1)  AS norm_dmg_taken_per_min,
    ROUND(SAFE_DIVIDE(b.avg_gold_per_min - mm.min_gold, mm.max_gold - mm.min_gold) * 100, 1)          AS norm_gold_per_min,
    ROUND(SAFE_DIVIDE(b.avg_kda - mm.min_kda, mm.max_kda - mm.min_kda) * 100, 1)                     AS norm_kda,
    ROUND(SAFE_DIVIDE(b.avg_vision_per_min - mm.min_vision, mm.max_vision - mm.min_vision) * 100, 1)  AS norm_vision_per_min,
    ROUND(SAFE_DIVIDE(b.avg_heal_per_min - mm.min_heal, mm.max_heal - mm.min_heal) * 100, 1)          AS norm_heal_per_min,
    ROUND(SAFE_DIVIDE(b.avg_obj_dmg_per_min - mm.min_obj, mm.max_obj - mm.min_obj) * 100, 1)          AS norm_obj_dmg_per_min,
    ROUND(SAFE_DIVIDE(b.avg_cs_per_min - mm.min_cs, mm.max_cs - mm.min_cs) * 100, 1)                  AS norm_cs_per_min,
    ROUND(SAFE_DIVIDE(b.avg_cc_time - mm.min_cc, mm.max_cc - mm.min_cc) * 100, 1)                     AS norm_cc_time,

    
    b.avg_dmg_per_min,
    b.avg_dmg_taken_per_min,
    b.avg_gold_per_min,
    b.avg_kda,
    b.avg_vision_per_min,
    b.avg_heal_per_min,
    b.avg_obj_dmg_per_min,
    b.avg_cs_per_min,
    b.avg_cc_time

FROM base b
CROSS JOIN min_max mm

ORDER BY b.avg_dmg_per_min DESC