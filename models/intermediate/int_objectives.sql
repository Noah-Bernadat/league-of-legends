with teamstats as (
    select * from {{ ref('stg_raw__teamstats') }}
),

team_results as (
    select * from {{ ref('int_team_results') }}
),

joined as (
    select
        ts.match_id,
        ts.team_id,

        -- Objectifs "first"
        ts.first_blood,
        ts.first_tower,
        ts.first_inhib,
        ts.first_baron,
        ts.first_dragon,
        ts.first_herald,

        -- Compteurs
        ts.tower_kills,
        ts.inhib_kills,
        ts.baron_kills,
        ts.dragon_kills,
        ts.herald_kills,

        -- Score total : nb d'objectifs "first" obtenus (0 à 6)
        (  ts.first_blood
         + ts.first_tower
         + ts.first_dragon
         + ts.first_baron
         + ts.first_inhib
         + ts.first_herald)  as score_objectifs,

        -- Le résultat de l'équipe (vient de int_team_results)
        tr.win

    from teamstats ts
    left join team_results tr
        on  tr.match_id = ts.match_id
        and tr.team_id  = ts.team_id
)

select * from joined