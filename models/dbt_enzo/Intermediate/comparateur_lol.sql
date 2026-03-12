WITH team_stats AS (
   
    SELECT 
        match_id, 
        teamid, 
        SUM(kills) as total_team_kills
    FROM `dbt_eauvray.stg_raw__all_datas_role_clean` 
    GROUP BY 1, 2
),

champion_comparison AS (
    SELECT
    raw.champion_name,
    raw.vrai_role,
    AVG(SAFE_DIVIDE(raw.kills + raw.assists, t.total_team_kills)) * 100 as kill_participation,
    AVG(SAFE_DIVIDE(raw.damage_to_champs, raw.duration_mins)) as damage_per_min,
    AVG(SAFE_DIVIDE(raw.damage_taken, raw.duration_mins)) as tanking_per_min,
    AVG(raw.team_towerkills + raw.team_dragonkills) as objective_pressure, 
    ROUND(AVG(raw.KDA),2) AS KDA,
    ROUND(AVG(raw.visionscore),2) AS vision_score,
    ROUND(AVG(SAFE_DIVIDE(raw.goldearned, raw.duration_mins)),2) AS gold_per_min,
    ROUND(AVG(raw.team_win)*100,2) AS win_rate


    FROM `dbt_eauvray.stg_raw__all_datas_role_clean` AS raw
    JOIN team_stats AS t 
    ON raw.match_id = t.match_id AND raw.teamid = t.teamid
    GROUP BY 1, 2
)


SELECT 

c.champion_name,
c.vrai_role,
c.nb_games,
c.final_power_score,
c.grade,
cc.win_rate/100 AS win_rate,
cc.KDA,
ROUND(cc.kill_participation, 2) AS kill_participation,
ROUND(cc.damage_per_min, 2) AS damage_per_min,
ROUND(cc.tanking_per_min, 2) AS tanking_per_min,
cc.vision_score,
cc.Gold_per_min,
ROUND(cc.objective_pressure,2) AS objective_pressure


FROM `dbt_eauvray.Comparateur` AS c
LEFT JOIN champion_comparison AS cc
ON cc.champion_name = c.champion_name AND cc.vrai_role = c.vrai_role
ORDER BY final_power_score DESC

