with t as (
    select * from {{ ref('int_team_match') }}
)

select
    score_objectifs,
    count(*)                                as total_equipes,
    countif(win = 1)                        as victoires,
    round(avg(win) * 100, 1)               as winrate,
    round(avg(dragon_kills), 1)             as dragon_kills_moyen,
    round(avg(baron_kills), 1)              as baron_kills_moyen,
    round(avg(tower_kills), 1)              as tower_kills_moyen,
    round(avg(game_duration_min), 1)        as duree_moyenne_min
from t
group by score_objectifs
order by score_objectifs