with objectives as (
    select * from {{ ref('int_objectives') }}
),

matches as (
    select * from {{ ref('stg_matches') }}
),

joined as (
    select
        o.*,
        m.game_duration_segment,
        m.game_duration_min
    from objectives o
    inner join matches m
        on m.match_id = o.match_id
),

agg as (
    select
        round(avg(case when game_duration_segment = 'Court (<25 min)'    and first_blood  = 1 then win end) * 100, 1) as blood_court,
        round(avg(case when game_duration_segment = 'Normal (25-40 min)' and first_blood  = 1 then win end) * 100, 1) as blood_normal,
        round(avg(case when game_duration_segment = 'Long (>40 min)'     and first_blood  = 1 then win end) * 100, 1) as blood_long,

        round(avg(case when game_duration_segment = 'Court (<25 min)'    and first_dragon = 1 then win end) * 100, 1) as dragon_court,
        round(avg(case when game_duration_segment = 'Normal (25-40 min)' and first_dragon = 1 then win end) * 100, 1) as dragon_normal,
        round(avg(case when game_duration_segment = 'Long (>40 min)'     and first_dragon = 1 then win end) * 100, 1) as dragon_long,

        round(avg(case when game_duration_segment = 'Court (<25 min)'    and first_baron  = 1 then win end) * 100, 1) as baron_court,
        round(avg(case when game_duration_segment = 'Normal (25-40 min)' and first_baron  = 1 then win end) * 100, 1) as baron_normal,
        round(avg(case when game_duration_segment = 'Long (>40 min)'     and first_baron  = 1 then win end) * 100, 1) as baron_long,

        round(avg(case when game_duration_segment = 'Court (<25 min)'    and first_tower  = 1 then win end) * 100, 1) as tower_court,
        round(avg(case when game_duration_segment = 'Normal (25-40 min)' and first_tower  = 1 then win end) * 100, 1) as tower_normal,
        round(avg(case when game_duration_segment = 'Long (>40 min)'     and first_tower  = 1 then win end) * 100, 1) as tower_long,

        round(avg(case when game_duration_segment = 'Court (<25 min)'    and first_herald = 1 then win end) * 100, 1) as herald_court,
        round(avg(case when game_duration_segment = 'Normal (25-40 min)' and first_herald = 1 then win end) * 100, 1) as herald_normal,
        round(avg(case when game_duration_segment = 'Long (>40 min)'     and first_herald = 1 then win end) * 100, 1) as herald_long,

        round(avg(case when game_duration_segment = 'Court (<25 min)'    and first_inhib  = 1 then win end) * 100, 1) as inhib_court,
        round(avg(case when game_duration_segment = 'Normal (25-40 min)' and first_inhib  = 1 then win end) * 100, 1) as inhib_normal,
        round(avg(case when game_duration_segment = 'Long (>40 min)'     and first_inhib  = 1 then win end) * 100, 1) as inhib_long

    from joined
),

unpivoted as (
    select 'First Blood'  as metrique, blood_court  as court_25min, blood_normal  as normal_25_40min, blood_long  as long_40min from agg
    union all
    select 'First Dragon' as metrique, dragon_court as court_25min, dragon_normal as normal_25_40min, dragon_long as long_40min from agg
    union all
    select 'First Baron'  as metrique, baron_court  as court_25min, baron_normal  as normal_25_40min, baron_long  as long_40min from agg
    union all
    select 'First Tower'  as metrique, tower_court  as court_25min, tower_normal  as normal_25_40min, tower_long  as long_40min from agg
    union all
    select 'First Herald' as metrique, herald_court as court_25min, herald_normal as normal_25_40min, herald_long as long_40min from agg
    union all
    select 'First Inhib'  as metrique, inhib_court  as court_25min, inhib_normal  as normal_25_40min, inhib_long  as long_40min from agg
)

select * from unpivoted