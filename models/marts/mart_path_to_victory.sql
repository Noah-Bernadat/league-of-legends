with t as (
    select * from {{ ref('int_team_match') }}
),

with_path as (
    select
        *,
        concat(
            case when first_blood  = 1 then 'FB '  else '' end,
            case when first_dragon = 1 then 'FD '  else '' end,
            case when first_tower  = 1 then 'FT '  else '' end,
            case when first_baron  = 1 then 'FBa ' else '' end,
            case when first_herald = 1 then 'FH '  else '' end,
            case when first_inhib  = 1 then 'FI'   else '' end
        ) as chemin
    from t
)

select
    case when chemin = '' then 'Aucun objectif' else trim(chemin) end as chemin,
    score_objectifs,
    count(*)                                as total_equipes,
    countif(win = 1)                        as victoires,
    round(avg(win) * 100, 1)               as winrate,
    round(avg(game_duration_min), 1)        as duree_moyenne_min,
    round(avg(dragon_kills), 1)             as dragon_kills_moyen,
    round(avg(tower_kills), 1)              as tower_kills_moyen
from with_path
group by chemin, score_objectifs
having count(*) >= 100
order by winrate desc, total_equipes desc