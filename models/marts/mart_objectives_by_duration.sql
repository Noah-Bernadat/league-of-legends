with t as (
    select * from {{ ref('int_team_match') }}
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
    from t
),

final as (
    select 'First Blood'   as objectif, blood_court  as court_25min, blood_normal  as normal_25_40min, blood_long  as long_40min from agg union all
    select 'First Dragon',              dragon_court,                 dragon_normal,                    dragon_long                from agg union all
    select 'First Baron',               baron_court,                  baron_normal,                     baron_long                 from agg union all
    select 'First Tower',               tower_court,                  tower_normal,                     tower_long                 from agg union all
    select 'First Herald',              herald_court,                 herald_normal,                    herald_long                from agg union all
    select 'First Inhib',               inhib_court,                  inhib_normal,                     inhib_long                 from agg
)

select
    objectif,
    court_25min     as winrate_parties_courtes_moins_25min_pct,
    normal_25_40min as winrate_parties_normales_25_40min_pct,
    long_40min      as winrate_parties_longues_plus_40min_pct
from final