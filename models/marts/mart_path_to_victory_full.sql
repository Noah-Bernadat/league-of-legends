with team as (
    select * from {{ ref('int_team_match') }}
),

players as (
    select * from {{ ref('int_player_match') }}
),

-- Construire le chemin au niveau équipe
team_paths as (
    select
        match_id,
        team_id,
        win,
        score_objectifs,
        game_duration_min,
        game_duration_segment,
        concat(
            case when first_blood  = 1 then 'FB '  else '' end,
            case when first_dragon = 1 then 'FD '  else '' end,
            case when first_tower  = 1 then 'FT '  else '' end,
            case when first_baron  = 1 then 'FBa ' else '' end,
            case when first_herald = 1 then 'FH '  else '' end,
            case when first_inhib  = 1 then 'FI'   else '' end
        ) as chemin
    from team
),

-- Agréger les stats joueurs au niveau équipe
player_team_agg as (
    select
        match_id,
        team_id,
        round(avg(kda), 2)                  as avg_kda,
        round(avg(damage_per_min), 0)        as avg_damage_per_min,
        round(avg(cs_per_min), 1)            as avg_cs_per_min,
        round(avg(vision_per_min), 2)        as avg_vision_per_min,
        round(avg(kill_participation), 1)    as avg_kill_participation,
        round(avg(gold_efficiency), 3)       as avg_gold_efficiency
    from players
    group by match_id, team_id
),

joined as (
    select
        tp.chemin,
        tp.win,
        tp.score_objectifs,
        tp.game_duration_min,
        tp.game_duration_segment,
        pa.avg_kda,
        pa.avg_damage_per_min,
        pa.avg_cs_per_min,
        pa.avg_vision_per_min,
        pa.avg_kill_participation,
        pa.avg_gold_efficiency
    from team_paths tp
    left join player_team_agg pa
        on pa.match_id = tp.match_id
        and pa.team_id = tp.team_id
)

select
    case when chemin = '' then 'Aucun objectif' else trim(chemin) end as chemin,
    score_objectifs,
    count(*)                                    as total_equipes,
    countif(win = 1)                            as victoires,
    round(avg(win) * 100, 1)                   as winrate_pct,
    round(avg(game_duration_min), 1)            as duree_moyenne_min,
    round(avg(avg_kda), 2)                      as kda_moyen_joueurs_equipe,
    round(avg(avg_damage_per_min), 0)           as degats_champions_par_min_moyen,
    round(avg(avg_cs_per_min), 1)              as creeps_par_minute_moyen,
    round(avg(avg_vision_per_min), 2)           as vision_score_par_minute_moyen,
    round(avg(avg_kill_participation), 1)       as participation_kills_pct_moyen,
    round(avg(avg_gold_efficiency), 3)          as ratio_or_depense_sur_gagne_moyen
from joined
group by chemin, score_objectifs
having count(*) >= 100
order by winrate_pct desc, total_equipes desc
