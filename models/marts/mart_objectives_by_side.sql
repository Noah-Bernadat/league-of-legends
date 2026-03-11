with t as (
    select * from {{ ref('int_team_match') }}
),

agg as (
    select
        round(avg(case when team_id = 100 then win end)            * 100, 1) as winrate_blue,
        round(avg(case when team_id = 200 then win end)            * 100, 1) as winrate_red,

        round(avg(case when team_id = 100 then first_blood end)    * 100, 1) as first_blood_blue,
        round(avg(case when team_id = 200 then first_blood end)    * 100, 1) as first_blood_red,

        round(avg(case when team_id = 100 then first_dragon end)   * 100, 1) as first_dragon_blue,
        round(avg(case when team_id = 200 then first_dragon end)   * 100, 1) as first_dragon_red,

        round(avg(case when team_id = 100 then first_baron end)    * 100, 1) as first_baron_blue,
        round(avg(case when team_id = 200 then first_baron end)    * 100, 1) as first_baron_red,

        round(avg(case when team_id = 100 then first_tower end)    * 100, 1) as first_tower_blue,
        round(avg(case when team_id = 200 then first_tower end)    * 100, 1) as first_tower_red,

        round(avg(case when team_id = 100 then first_herald end)   * 100, 1) as first_herald_blue,
        round(avg(case when team_id = 200 then first_herald end)   * 100, 1) as first_herald_red,

        round(avg(case when team_id = 100 then score_objectifs end), 2)      as score_moyen_blue,
        round(avg(case when team_id = 200 then score_objectifs end), 2)      as score_moyen_red
    from t
),

final as (
    select 'Winrate global'         as metrique, winrate_blue       as blue_side, winrate_red       as red_side from agg union all
    select 'Taux First Blood (%)',               first_blood_blue,               first_blood_red               from agg union all
    select 'Taux First Dragon (%)',              first_dragon_blue,              first_dragon_red              from agg union all
    select 'Taux First Baron (%)',               first_baron_blue,               first_baron_red               from agg union all
    select 'Taux First Tower (%)',               first_tower_blue,               first_tower_red               from agg union all
    select 'Taux First Herald (%)',              first_herald_blue,              first_herald_red              from agg union all
    select 'Score objectifs moyen',              score_moyen_blue,               score_moyen_red               from agg
)

select * from final