with participants as (
    select * from {{ ref('stg_raw__participants') }}
),

stats as (
    select * from {{ ref('stg_stats') }}
),

results as (
    select
        p.match_id,
        p.team_id,
        max(s.win) as win
        -- max() car tous les joueurs d'une même équipe ont le même win
        -- max(1,1,1,1,1) = 1 → victoire  |  max(0,0,0,0,0) = 0 → défaite

    from participants p
    inner join stats s
        on s.participant_id = p.participant_id

    group by
        p.match_id,
        p.team_id
)

select * from results