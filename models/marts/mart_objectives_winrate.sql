with t as (
    select * from {{ ref('int_team_match') }}
),

final as (
    select
        'First Blood'   as objectif,
        count(*)        as total_equipes,
        countif(first_blood = 1)                                            as fois_pris,
        round(countif(first_blood = 1) / count(*) * 100, 1)                as taux_prise_pct,
        round(avg(case when first_blood  = 1 then win end) * 100, 1)       as winrate_si_pris,
        round(avg(case when first_blood  = 0 then win end) * 100, 1)       as winrate_si_pas_pris
    from t

    union all

    select
        'First Dragon',
        count(*),
        countif(first_dragon = 1),
        round(countif(first_dragon = 1) / count(*) * 100, 1),
        round(avg(case when first_dragon = 1 then win end) * 100, 1),
        round(avg(case when first_dragon = 0 then win end) * 100, 1)
    from t

    union all

    select
        'First Baron',
        count(*),
        countif(first_baron = 1),
        round(countif(first_baron = 1) / count(*) * 100, 1),
        round(avg(case when first_baron  = 1 then win end) * 100, 1),
        round(avg(case when first_baron  = 0 then win end) * 100, 1)
    from t

    union all

    select
        'First Tower',
        count(*),
        countif(first_tower = 1),
        round(countif(first_tower = 1) / count(*) * 100, 1),
        round(avg(case when first_tower  = 1 then win end) * 100, 1),
        round(avg(case when first_tower  = 0 then win end) * 100, 1)
    from t

    union all

    select
        'First Herald',
        count(*),
        countif(first_herald = 1),
        round(countif(first_herald = 1) / count(*) * 100, 1),
        round(avg(case when first_herald = 1 then win end) * 100, 1),
        round(avg(case when first_herald = 0 then win end) * 100, 1)
    from t

    union all

    select
        'First Inhib',
        count(*),
        countif(first_inhib = 1),
        round(countif(first_inhib = 1) / count(*) * 100, 1),
        round(avg(case when first_inhib  = 1 then win end) * 100, 1),
        round(avg(case when first_inhib  = 0 then win end) * 100, 1)
    from t
)

select * from final
order by winrate_si_pris desc