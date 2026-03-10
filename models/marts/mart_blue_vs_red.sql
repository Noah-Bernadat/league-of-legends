with objectives as (
    select * from {{ ref('int_objectives') }}
),

by_side as (
    select
        count(*)                                as total_equipes,
        round(avg(case when team_id = 100 then win end) * 100, 1)           as winrate_blue,
        round(avg(case when team_id = 200 then win end) * 100, 1)           as winrate_red,
        round(avg(case when team_id = 100 then first_blood end) * 100, 1)   as first_blood_blue,
        round(avg(case when team_id = 200 then first_blood end) * 100, 1)   as first_blood_red,
        round(avg(case when team_id = 100 then first_dragon end) * 100, 1)  as first_dragon_blue,
        round(avg(case when team_id = 200 then first_dragon end) * 100, 1)  as first_dragon_red,
        round(avg(case when team_id = 100 then first_baron end) * 100, 1)   as first_baron_blue,
        round(avg(case when team_id = 200 then first_baron end) * 100, 1)   as first_baron_red,
        round(avg(case when team_id = 100 then first_tower end) * 100, 1)   as first_tower_blue,
        round(avg(case when team_id = 200 then first_tower end) * 100, 1)   as first_tower_red,
        round(avg(case when team_id = 100 then first_herald end) * 100, 1)  as first_herald_blue,
        round(avg(case when team_id = 200 then first_herald end) * 100, 1)  as first_herald_red,
        round(avg(case when team_id = 100 then score_objectifs end), 2)     as score_moyen_blue,
        round(avg(case when team_id = 200 then score_objectifs end), 2)     as score_moyen_red
    from objectives
),

unpivoted as (
    select 'Winrate global'         as metrique, winrate_blue      as blue_side, winrate_red      as red_side from by_side
    union all
    select 'Taux First Blood'       as metrique, first_blood_blue  as blue_side, first_blood_red  as red_side from by_side
    union all
    select 'Taux First Dragon'      as metrique, first_dragon_blue as blue_side, first_dragon_red as red_side from by_side
    union all
    select 'Taux First Baron'       as metrique, first_baron_blue  as blue_side, first_baron_red  as red_side from by_side
    union all
    select 'Taux First Tower'       as metrique, first_tower_blue  as blue_side, first_tower_red  as red_side from by_side
    union all
    select 'Taux First Herald'      as metrique, first_herald_blue as blue_side, first_herald_red as red_side from by_side
    union all
    select 'Score objectifs moyen'  as metrique, score_moyen_blue  as blue_side, score_moyen_red  as red_side from by_side
)

select * from unpivoted