with p as (
    select * from {{ ref('int_player_match') }}
    where role not in ('UNKNOWN', 'BOT')
      and champion_name is not null
),

-- Stats agrégées par champion × rôle
champ_stats as (
    select
        role,
        champion_name                                       as champion,

        -- Volume
        count(*)                                            as nb_parties,
        countif(win = 1)                                    as nb_victoires,

        -- KPI PRINCIPAL
        round(avg(win) * 100, 1)                            as winrate_pct,

        -- Style de jeu dominant (le plus fréquent)
        -- On le calcule séparément dans une sous-requête
        -- Pour l'instant on prend les KPIs

        -- Performance individuelle moyenne
        round(avg(kda), 2)                                  as kda_moyen,
        round(avg(damage_share), 1)                         as part_degats_equipe_pct,
        round(avg(kill_participation), 1)                   as participation_kills_pct,
        round(avg(cs_per_min), 1)                           as creeps_par_minute,
        round(avg(vision_per_min), 2)                       as vision_par_minute,
        round(avg(gold_share), 1)                           as part_or_equipe_pct,
        round(avg(gold_efficiency), 3)                      as ratio_or_depense_gagne,
        round(avg(damage_per_min), 0)                       as degats_champions_par_min,
        round(avg(champ_level), 1)                          as niveau_moyen,

        -- Contexte équipe quand ce champion est joué
        round(avg(team_score_objectifs), 1)                 as score_objectifs_equipe_moyen,
        round(avg(case when win = 1 then team_score_objectifs end), 1)
                                                            as score_obj_equipe_si_victoire

    from p
    group by role, champion_name
    having count(*) >= 200
),

-- Style de jeu dominant par champion × rôle
styles as (
    select
        role,
        champion_name as champion,
        play_style,
        count(*) as cnt,
        row_number() over (partition by role, champion_name order by count(*) desc) as rn
    from p
    group by role, champion_name, play_style
),

dominant_style as (
    select role, champion, play_style as style_dominant
    from styles
    where rn = 1
),

-- Rang par rôle (top 5 par winrate)
ranked as (
    select
        cs.*,
        ds.style_dominant,
        row_number() over (partition by cs.role order by cs.winrate_pct desc) as rang
    from champ_stats cs
    left join dominant_style ds
        on ds.role = cs.role and ds.champion = cs.champion
)

select
    rang,
    role,
    champion,
    style_dominant,
    nb_parties,
    nb_victoires,
    winrate_pct,
    kda_moyen,
    part_degats_equipe_pct,
    participation_kills_pct,
    creeps_par_minute,
    vision_par_minute,
    part_or_equipe_pct,
    ratio_or_depense_gagne,
    degats_champions_par_min,
    niveau_moyen,
    score_objectifs_equipe_moyen,
    score_obj_equipe_si_victoire
from ranked
where rang <= 5
order by role, rang