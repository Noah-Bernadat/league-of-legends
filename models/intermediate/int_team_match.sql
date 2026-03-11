with teamstats as (
    select * from {{ ref('stg_raw_v2__teamstats') }}
),

matches as (
    select * from {{ ref('stg_raw_v2__matches') }}
),

participants as (
    select participant_id, match_id, team_id
    from {{ ref('stg_raw_v2__participants') }}
),

stats as (
    select participant_id, win
    from {{ ref('stg_raw_v2__stats') }}
),

-- Récupération du résultat (win) depuis les stats joueur → niveau équipe
team_wins as (
    select
        p.match_id,
        p.team_id,
        max(s.win) as win   -- tous les joueurs d'une équipe ont le même win
    from participants p
    inner join stats s on s.participant_id = p.participant_id
    group by p.match_id, p.team_id
),

final as (
    select
        ts.match_id,
        ts.team_id,

        -- Résultat
        tw.win,

        -- Durée du match
        m.game_duration_sec,
        m.game_duration_min,
        m.game_duration_segment,

        -- Objectifs "first" (0 ou 1 par équipe)
        ts.first_blood,
        ts.first_dragon,
        ts.first_baron,
        ts.first_tower,
        ts.first_herald,
        ts.first_inhib,

        -- Compteurs totaux
        ts.dragon_kills,
        ts.baron_kills,
        ts.tower_kills,
        ts.herald_kills,
        ts.inhib_kills,

        -- KPI : score d'objectifs (nb de "first" remportés, de 0 à 6)
        (   ts.first_blood
          + ts.first_dragon
          + ts.first_baron
          + ts.first_tower
          + ts.first_herald
          + ts.first_inhib
        )                                                   as score_objectifs,

        -- Côté (Blue = 100, Red = 200)
        case when ts.team_id = 100 then 'Blue' else 'Red' end as side

    from teamstats ts
    left join matches m  on m.match_id = ts.match_id
    left join team_wins tw on tw.match_id = ts.match_id and tw.team_id = ts.team_id
)

select * from final
