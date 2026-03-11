
--facteur clé de victoire pour chaque role


WITH base AS (
    SELECT * FROM {{ ref('int_player_match_stats') }}
)

SELECT
    clean_role,

    ROUND(CORR(kills, win), 3) AS corr_kills,
    ROUND(CORR(deaths, win), 3) AS corr_deaths,
    ROUND(CORR(assists, win), 3) AS corr_assists,
    ROUND(CORR(totdmgtochamp, win), 3) AS corr_dmg_to_champs,
    ROUND(CORR(totdmgtaken, win), 3) AS corr_dmg_taken,
    ROUND(CORR(totheal, win), 3) AS corr_heal,
    ROUND(CORR(dmgtoobj, win), 3) AS corr_obj_dmg,
    ROUND(CORR(dmgtoturrets, win), 3) AS corr_turret_dmg,
    ROUND(CORR(goldearned, win), 3) AS corr_gold,
    ROUND(CORR(visionscore, win), 3) AS corr_vision,
    ROUND(CORR(total_cs, win), 3) AS corr_cs,
    ROUND(CORR(wardskilled, win), 3) AS corr_wards_killed,
    ROUND(CORR(firstblood, win), 3) AS corr_first_blood,
    ROUND(CORR(totcctimedealt, win), 3) AS corr_cc_time

FROM base
GROUP BY clean_role
ORDER BY clean_role