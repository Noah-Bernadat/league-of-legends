
with t as (
    select * from {{ ref('mart_analyse_equipe') }}
),

final as (

    select
        'First Blood'                                                               as objectif,
        1                                                                           as objectif_order,
        count(*)                                                                    as total_equipes,
        countif(first_blood = 1)                                                    as fois_pris,
        round(countif(first_blood = 1) / count(*) * 100, 1)                        as taux_prise_pct,
        round(avg(case when first_blood = 1 then win end) * 100, 1)                as winrate_si_pris,
        round(avg(case when first_blood = 0 then win end) * 100, 1)                as winrate_si_pas_pris,
        round(
            (avg(case when first_blood = 1 then win end)
           - avg(case when first_blood = 0 then win end)) * 100, 1)                as delta_winrate,
        round(avg(case when first_blood = 1
            then cast(team_gold_earned - opponent_gold_earned as float64)
            end), 0)                                                                as gold_diff_avg_si_pris,
        round(avg(case when first_blood = 1
            then cast(team_kills - opponent_kills as float64)
            end), 1)                                                                as kills_diff_avg_si_pris
    from t

    union all

    select
        'First Dragon',
        2,
        count(*),
        countif(first_dragon = 1),
        round(countif(first_dragon = 1) / count(*) * 100, 1),
        round(avg(case when first_dragon = 1 then win end) * 100, 1),
        round(avg(case when first_dragon = 0 then win end) * 100, 1),
        round(
            (avg(case when first_dragon = 1 then win end)
           - avg(case when first_dragon = 0 then win end)) * 100, 1),
        round(avg(case when first_dragon = 1
            then cast(team_gold_earned - opponent_gold_earned as float64) end), 0),
        round(avg(case when first_dragon = 1
            then cast(team_kills - opponent_kills as float64) end), 1)
    from t

    union all

    select
        'First Baron',
        3,
        count(*),
        countif(first_baron = 1),
        round(countif(first_baron = 1) / count(*) * 100, 1),
        round(avg(case when first_baron = 1 then win end) * 100, 1),
        round(avg(case when first_baron = 0 then win end) * 100, 1),
        round(
            (avg(case when first_baron = 1 then win end)
           - avg(case when first_baron = 0 then win end)) * 100, 1),
        round(avg(case when first_baron = 1
            then cast(team_gold_earned - opponent_gold_earned as float64) end), 0),
        round(avg(case when first_baron = 1
            then cast(team_kills - opponent_kills as float64) end), 1)
    from t

    union all

    select
        'First Tower',
        4,
        count(*),
        countif(first_tower = 1),
        round(countif(first_tower = 1) / count(*) * 100, 1),
        round(avg(case when first_tower = 1 then win end) * 100, 1),
        round(avg(case when first_tower = 0 then win end) * 100, 1),
        round(
            (avg(case when first_tower = 1 then win end)
           - avg(case when first_tower = 0 then win end)) * 100, 1),
        round(avg(case when first_tower = 1
            then cast(team_gold_earned - opponent_gold_earned as float64) end), 0),
        round(avg(case when first_tower = 1
            then cast(team_kills - opponent_kills as float64) end), 1)
    from t

    union all

    select
        'First Herald',
        5,
        count(*),
        countif(first_herald = 1),
        round(countif(first_herald = 1) / count(*) * 100, 1),
        round(avg(case when first_herald = 1 then win end) * 100, 1),
        round(avg(case when first_herald = 0 then win end) * 100, 1),
        round(
            (avg(case when first_herald = 1 then win end)
           - avg(case when first_herald = 0 then win end)) * 100, 1),
        round(avg(case when first_herald = 1
            then cast(team_gold_earned - opponent_gold_earned as float64) end), 0),
        round(avg(case when first_herald = 1
            then cast(team_kills - opponent_kills as float64) end), 1)
    from t

    union all

    select
        'First Inhib',
        6,
        count(*),
        countif(first_inhib = 1),
        round(countif(first_inhib = 1) / count(*) * 100, 1),
        round(avg(case when first_inhib = 1 then win end) * 100, 1),
        round(avg(case when first_inhib = 0 then win end) * 100, 1),
        round(
            (avg(case when first_inhib = 1 then win end)
           - avg(case when first_inhib = 0 then win end)) * 100, 1),
        round(avg(case when first_inhib = 1
            then cast(team_gold_earned - opponent_gold_earned as float64) end), 0),
        round(avg(case when first_inhib = 1
            then cast(team_kills - opponent_kills as float64) end), 1)
    from t

)

select * from final
order by objectif_order
