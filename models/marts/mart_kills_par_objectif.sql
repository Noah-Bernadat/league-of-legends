with objectives as (
    select * from {{ ref('int_objectives') }}
),

agg as (
    select
        -- Dragons
        round(avg(case when dragon_kills = 0 then win end) * 100, 1)             as wr_dragon_0,
        round(avg(case when dragon_kills between 1 and 2 then win end) * 100, 1) as wr_dragon_moins2,
        round(avg(case when dragon_kills between 3 and 5 then win end) * 100, 1) as wr_dragon_moins5,
        round(avg(case when dragon_kills > 5 then win end) * 100, 1)             as wr_dragon_plus5,

        -- Barons
        round(avg(case when baron_kills = 0 then win end) * 100, 1)              as wr_baron_0,
        round(avg(case when baron_kills between 1 and 2 then win end) * 100, 1)  as wr_baron_moins2,
        round(avg(case when baron_kills between 3 and 5 then win end) * 100, 1)  as wr_baron_moins5,
        round(avg(case when baron_kills > 5 then win end) * 100, 1)              as wr_baron_plus5,

        -- Inhibiteurs
        round(avg(case when inhib_kills = 0 then win end) * 100, 1)              as wr_inhib_0,
        round(avg(case when inhib_kills between 1 and 2 then win end) * 100, 1)  as wr_inhib_moins2,
        round(avg(case when inhib_kills between 3 and 5 then win end) * 100, 1)  as wr_inhib_moins5,
        round(avg(case when inhib_kills > 5 then win end) * 100, 1)              as wr_inhib_plus5,

        -- Tours
        round(avg(case when tower_kills = 0 then win end) * 100, 1)              as wr_tower_0,
        round(avg(case when tower_kills between 1 and 2 then win end) * 100, 1)  as wr_tower_moins2,
        round(avg(case when tower_kills between 3 and 5 then win end) * 100, 1)  as wr_tower_moins5,
        round(avg(case when tower_kills > 5 then win end) * 100, 1)              as wr_tower_plus5

    from objectives
),

pivoted as (
    select 'Dragon'     as type_objectif, wr_dragon_0 as zero_kill, wr_dragon_moins2 as moins_de_2_kills, wr_dragon_moins5 as moins_de_5_kills, wr_dragon_plus5 as plus_de_5_kills from agg
    union all
    select 'Baron'      as type_objectif, wr_baron_0  as zero_kill, wr_baron_moins2  as moins_de_2_kills, wr_baron_moins5  as moins_de_5_kills, wr_baron_plus5  as plus_de_5_kills from agg
    union all
    select 'Inhibiteur' as type_objectif, wr_inhib_0  as zero_kill, wr_inhib_moins2  as moins_de_2_kills, wr_inhib_moins5  as moins_de_5_kills, wr_inhib_plus5  as plus_de_5_kills from agg
    union all
    select 'Tour'       as type_objectif, wr_tower_0  as zero_kill, wr_tower_moins2  as moins_de_2_kills, wr_tower_moins5  as moins_de_5_kills, wr_tower_plus5  as plus_de_5_kills from agg
)

select * from pivoted
order by type_objectif