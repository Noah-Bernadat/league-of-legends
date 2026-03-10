with objectives as (
    select * from {{ ref('int_objectives') }}
),

agg as (
    select
        count(*) as total_equipes,

        -- Winrate si objectif obtenu
        round(avg(case when first_blood  = 1 then win end) * 100, 1)  as wr_first_blood,
        round(avg(case when first_tower  = 1 then win end) * 100, 1)  as wr_first_tower,
        round(avg(case when first_dragon = 1 then win end) * 100, 1)  as wr_first_dragon,
        round(avg(case when first_baron  = 1 then win end) * 100, 1)  as wr_first_baron,
        round(avg(case when first_inhib  = 1 then win end) * 100, 1)  as wr_first_inhib,
        round(avg(case when first_herald = 1 then win end) * 100, 1)  as wr_first_herald,

        -- Winrate par nb de dragons
        round(avg(case when dragon_kills = 0 then win end) * 100, 1)  as wr_0_dragon,
        round(avg(case when dragon_kills = 1 then win end) * 100, 1)  as wr_1_dragon,
        round(avg(case when dragon_kills = 2 then win end) * 100, 1)  as wr_2_dragon,
        round(avg(case when dragon_kills = 3 then win end) * 100, 1)  as wr_3_dragon,
        round(avg(case when dragon_kills >= 4 then win end) * 100, 1) as wr_4plus_dragon,

        -- Score global d'objectifs vs winrate
        round(avg(case when score_objectifs = 0 then win end) * 100, 1)  as wr_score_0,
        round(avg(case when score_objectifs = 1 then win end) * 100, 1)  as wr_score_1,
        round(avg(case when score_objectifs = 2 then win end) * 100, 1)  as wr_score_2,
        round(avg(case when score_objectifs = 3 then win end) * 100, 1)  as wr_score_3,
        round(avg(case when score_objectifs = 4 then win end) * 100, 1)  as wr_score_4,
        round(avg(case when score_objectifs >= 5 then win end) * 100, 1) as wr_score_5plus

    from objectives
)

select * from agg