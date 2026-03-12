{{
    config(
        materialized = 'table'
    )
}}

with team_match as (
    select * from {{ ref('int_team_match') }}
),

with_opponent as (
    select
        t.*,

        opp.score_objectifs          as opponent_score_objectifs,
        opp.team_kills               as opponent_kills,
        opp.team_deaths              as opponent_deaths,
        opp.team_assists             as opponent_assists,
        opp.team_gold_earned         as opponent_gold_earned,
        opp.team_damage_to_champs    as opponent_damage_to_champs,

        opp.first_blood              as opp_first_blood,
        opp.first_dragon             as opp_first_dragon,
        opp.first_baron              as opp_first_baron,
        opp.first_tower              as opp_first_tower,
        opp.first_herald             as opp_first_herald,
        opp.first_inhib              as opp_first_inhib,

        opp.dragon_kills             as opp_dragon_kills,
        opp.baron_kills              as opp_baron_kills,
        opp.tower_kills              as opp_tower_kills,
        opp.herald_kills             as opp_herald_kills,
        opp.inhib_kills              as opp_inhib_kills

    from team_match t
    inner join team_match opp
        on  opp.match_id = t.match_id
        and opp.team_id != t.team_id
),

final as (
    select
        *,

        score_objectifs - opponent_score_objectifs as objectives_advantage,

        case
            when win = 1 and score_objectifs < opponent_score_objectifs then 1
            else 0
        end as is_comeback,

        case
            when score_objectifs = 6 then 'Domination totale'
            when score_objectifs >= 4 then 'Avantage'
            when score_objectifs >= 2 then 'Leger'
            else 'Aucun'
        end as domination_category,

        first_blood + first_dragon                                   as combo_blood_dragon,
        first_blood + first_tower                                    as combo_blood_tower,
        first_dragon + first_baron                                   as combo_dragon_baron,
        first_dragon + first_herald                                  as combo_dragon_herald,
        first_tower + first_inhib                                    as combo_tower_inhib,
        first_blood + first_dragon + first_tower                     as combo_early_trio,
        first_dragon + first_baron + first_herald                    as combo_monster_trio,
        first_blood + first_dragon + first_baron + first_tower       as combo_four,
        first_blood + first_dragon + first_baron + first_tower + first_herald as combo_five,
        first_blood + first_dragon + first_baron + first_tower + first_herald + first_inhib as combo_all

    from with_opponent
)

select * from final
