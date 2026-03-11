with p as (
    select * from {{ ref('int_player_match') }}
    where role not in ('UNKNOWN', 'BOT')
      and champion_name is not null
),

-- Winrate par champion × rôle (minimum 200 parties)
champ_stats as (
    select
        role,
        champion_name,
        count(*)                                            as nb_parties,
        round(avg(win) * 100, 1)                            as winrate_pct,
        round(avg(kda), 2)                                  as kda_moyen,
        round(avg(damage_share), 1)                         as part_degats_equipe_pct,
        round(avg(kill_participation), 1)                   as participation_kills_pct,
        round(avg(cs_per_min), 1)                           as creeps_par_minute,
        round(avg(vision_per_min), 2)                       as vision_par_minute,
        round(avg(damage_per_min), 0)                       as degats_champions_par_min,
        round(avg(gold_share), 1)                           as part_or_equipe_pct,
        round(avg(case when win = 1 then team_score_objectifs end), 1)
                                                            as objectifs_equipe_si_victoire,
        row_number() over (partition by role order by avg(win) desc) as rang
    from p
    group by role, champion_name
    having count(*) >= 1000
),

-- Style de jeu dominant de chaque champion × rôle
styles as (
    select
        role,
        champion_name,
        play_style,
        row_number() over (partition by role, champion_name order by count(*) desc) as rn
    from p
    group by role, champion_name, play_style
)

select
    cs.role,
    cs.champion_name                                        as champion,
    s.play_style                                            as style_de_jeu,
    cs.nb_parties,
    cs.winrate_pct,
    cs.kda_moyen,
    cs.part_degats_equipe_pct,
    cs.participation_kills_pct,
    cs.creeps_par_minute,
    cs.vision_par_minute,
    cs.degats_champions_par_min,
    cs.part_or_equipe_pct,
    cs.objectifs_equipe_si_victoire
from champ_stats cs
left join styles s
    on s.role = cs.role
    and s.champion_name = cs.champion_name
    and s.rn = 1
where cs.rang = 1
order by cs.winrate_pct desc