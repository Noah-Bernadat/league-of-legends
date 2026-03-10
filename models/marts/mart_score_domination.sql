with objectives as (
    select * from {{ ref('int_objectives') }}
),

agg as (
    select
        countif(score_objectifs between 0 and 1)                                    as nb_equipes_score_0a1,
        round(avg(case when score_objectifs between 0 and 1 then win end) * 100, 1) as winrate_score_0a1_pct,
        countif(score_objectifs between 2 and 3)                                    as nb_equipes_score_2a3,
        round(avg(case when score_objectifs between 2 and 3 then win end) * 100, 1) as winrate_score_2a3_pct,
        countif(score_objectifs between 4 and 5)                                    as nb_equipes_score_4a5,
        round(avg(case when score_objectifs between 4 and 5 then win end) * 100, 1) as winrate_score_4a5_pct,
        countif(score_objectifs > 5)                                                as nb_equipes_score_5plus,
        round(avg(case when score_objectifs > 5 then win end) * 100, 1)             as winrate_score_5plus_pct
    from objectives
),

unpivoted as (
    select 'Équipe dominée'      as profil_equipe, '0 à 1 objectif'      as tranche, nb_equipes_score_0a1   as nb_equipes, winrate_score_0a1_pct   as winrate_pct from agg
    union all
    select 'Partie équilibrée'   as profil_equipe, '2 à 3 objectifs'     as tranche, nb_equipes_score_2a3   as nb_equipes, winrate_score_2a3_pct   as winrate_pct from agg
    union all
    select 'Équipe dominante'    as profil_equipe, '4 à 5 objectifs'     as tranche, nb_equipes_score_4a5   as nb_equipes, winrate_score_4a5_pct   as winrate_pct from agg
    union all
    select 'Domination totale'   as profil_equipe, 'Plus de 5 objectifs' as tranche, nb_equipes_score_5plus as nb_equipes, winrate_score_5plus_pct as winrate_pct from agg
)

select * from unpivoted
order by winrate_pct