with t as (
    select * from {{ ref('int_team_match') }}
),

-- Winrate par objectif (pris vs pas pris)
obj_impact as (
    select 'First Blood'  as objectif, round(avg(case when first_blood  = 1 then win end)*100,1) as wr_si_pris, round(avg(case when first_blood  = 0 then win end)*100,1) as wr_si_pas_pris, corr(first_blood,  win) as corr_val from t union all
    select 'First Dragon',              round(avg(case when first_dragon = 1 then win end)*100,1),              round(avg(case when first_dragon = 0 then win end)*100,1),              corr(first_dragon, win) from t union all
    select 'First Tower',               round(avg(case when first_tower  = 1 then win end)*100,1),              round(avg(case when first_tower  = 0 then win end)*100,1),              corr(first_tower,  win) from t union all
    select 'First Baron',               round(avg(case when first_baron  = 1 then win end)*100,1),              round(avg(case when first_baron  = 0 then win end)*100,1),              corr(first_baron,  win) from t union all
    select 'First Herald',              round(avg(case when first_herald = 1 then win end)*100,1),              round(avg(case when first_herald = 0 then win end)*100,1),              corr(first_herald, win) from t union all
    select 'First Inhib',               round(avg(case when first_inhib  = 1 then win end)*100,1),              round(avg(case when first_inhib  = 0 then win end)*100,1),              corr(first_inhib,  win) from t
),

objectifs_ranked as (
    select
        'OBJECTIF' as categorie,
        objectif as element,
        cast(null as string) as role,
        wr_si_pris as valeur_principale,
        round(wr_si_pris - wr_si_pas_pris, 1) as ecart_winrate_pts,
        round(corr_val, 3) as correlation_victoire,
        cast(null as float64) as kda_moyen,
        cast(null as float64) as part_degats_pct,
        cast(null as float64) as vision_par_min,
        cast(null as float64) as creeps_par_min,
        cast(null as float64) as kill_participation_pct,
        cast(null as int64) as nb_parties,
        cast(null as string) as style_dominant,
        row_number() over (order by wr_si_pris - wr_si_pas_pris desc) as priorite
    from obj_impact
),


-- SECTION 2 : Profil KPI idéal par rôle (moyenne des gagnants)


p as (
    select * from {{ ref('int_player_match') }}
    where role not in ('UNKNOWN', 'BOT')
      and champion_name is not null
),

winner_profile as (
    select
        'PROFIL_IDEAL' as categorie,
        'Profil gagnant' as element,
        role,
        round(avg(win) * 100, 1) as valeur_principale,
        cast(null as float64) as ecart_winrate_pts,
        cast(null as float64) as correlation_victoire,
        round(avg(kda), 2) as kda_moyen,
        round(avg(damage_share), 1) as part_degats_pct,
        round(avg(vision_per_min), 2) as vision_par_min,
        round(avg(cs_per_min), 1) as creeps_par_min,
        round(avg(kill_participation), 1) as kill_participation_pct,
        count(*) as nb_parties,
        cast(null as string) as style_dominant,
        cast(null as int64) as priorite
    from p
    where win = 1
    group by role
),


-- SECTION 3 : Meilleur champion par rôle (winrate top 1, minimum 200 parties)


champ_wr as (
    select
        role,
        champion_name,
        count(*) as nb,
        round(avg(win) * 100, 1) as wr,
        round(avg(kda), 2) as kda,
        round(avg(damage_share), 1) as dmg,
        round(avg(vision_per_min), 2) as vis,
        round(avg(cs_per_min), 1) as cs,
        round(avg(kill_participation), 1) as kp,
        row_number() over (partition by role order by avg(win) desc) as rn
    from p
    group by role, champion_name
    having count(*) >= 200
),

-- Style dominant du top 1
top1_styles as (
    select
        p2.role,
        p2.champion_name,
        p2.play_style,
        count(*) as cnt,
        row_number() over (partition by p2.role, p2.champion_name order by count(*) desc) as srn
    from p p2
    inner join champ_wr cw
        on cw.role = p2.role
        and cw.champion_name = p2.champion_name
        and cw.rn = 1
    group by p2.role, p2.champion_name, p2.play_style
),

best_champs as (
    select
        'CHAMPION_OPTIMAL' as categorie,
        cw.champion_name as element,
        cw.role,
        cw.wr as valeur_principale,
        cast(null as float64) as ecart_winrate_pts,
        cast(null as float64) as correlation_victoire,
        cw.kda as kda_moyen,
        cw.dmg as part_degats_pct,
        cw.vis as vision_par_min,
        cw.cs as creeps_par_min,
        cw.kp as kill_participation_pct,
        cw.nb as nb_parties,
        ts.play_style as style_dominant,
        cast(null as int64) as priorite
    from champ_wr cw
    left join top1_styles ts
        on ts.role = cw.role
        and ts.champion_name = cw.champion_name
        and ts.srn = 1
    where cw.rn = 1
),


-- SECTION 4 : Meilleur combo de 3 objectifs


combo3 as (
    select 'First Blood + First Tower + First Baron'   as combo, countif(first_blood=1 and first_tower=1 and first_baron=1) as n, round(avg(case when first_blood=1 and first_tower=1 and first_baron=1 then win end)*100,1) as wr from t union all
    select 'First Blood + First Tower + First Inhib',             countif(first_blood=1 and first_tower=1 and first_inhib=1),     round(avg(case when first_blood=1 and first_tower=1 and first_inhib=1 then win end)*100,1) from t union all
    select 'First Blood + First Baron + First Inhib',             countif(first_blood=1 and first_baron=1 and first_inhib=1),     round(avg(case when first_blood=1 and first_baron=1 and first_inhib=1 then win end)*100,1) from t union all
    select 'First Dragon + First Tower + First Baron',            countif(first_dragon=1 and first_tower=1 and first_baron=1),    round(avg(case when first_dragon=1 and first_tower=1 and first_baron=1 then win end)*100,1) from t union all
    select 'First Dragon + First Baron + First Inhib',            countif(first_dragon=1 and first_baron=1 and first_inhib=1),    round(avg(case when first_dragon=1 and first_baron=1 and first_inhib=1 then win end)*100,1) from t union all
    select 'First Tower + First Baron + First Inhib',             countif(first_tower=1 and first_baron=1 and first_inhib=1),     round(avg(case when first_tower=1 and first_baron=1 and first_inhib=1 then win end)*100,1) from t union all
    select 'First Dragon + First Tower + First Inhib',            countif(first_dragon=1 and first_tower=1 and first_inhib=1),    round(avg(case when first_dragon=1 and first_tower=1 and first_inhib=1 then win end)*100,1) from t union all
    select 'First Blood + First Dragon + First Tower',            countif(first_blood=1 and first_dragon=1 and first_tower=1),    round(avg(case when first_blood=1 and first_dragon=1 and first_tower=1 then win end)*100,1) from t union all
    select 'First Blood + First Dragon + First Baron',            countif(first_blood=1 and first_dragon=1 and first_baron=1),    round(avg(case when first_blood=1 and first_dragon=1 and first_baron=1 then win end)*100,1) from t union all
    select 'First Blood + First Dragon + First Inhib',            countif(first_blood=1 and first_dragon=1 and first_inhib=1),    round(avg(case when first_blood=1 and first_dragon=1 and first_inhib=1 then win end)*100,1) from t
),

best_combo as (
    select
        'COMBO_3_OBJECTIFS' as categorie,
        combo as element,
        cast(null as string) as role,
        wr as valeur_principale,
        cast(null as float64) as ecart_winrate_pts,
        cast(null as float64) as correlation_victoire,
        cast(null as float64) as kda_moyen,
        cast(null as float64) as part_degats_pct,
        cast(null as float64) as vision_par_min,
        cast(null as float64) as creeps_par_min,
        cast(null as float64) as kill_participation_pct,
        n as nb_parties,
        cast(null as string) as style_dominant,
        row_number() over (order by wr desc) as priorite
    from combo3
    where n >= 100
)

-- ASSEMBLAGE FINAL : toutes les recommandations dans une seule table


select * from objectifs_ranked
union all
select * from winner_profile
union all
select * from best_champs
union all
select * from best_combo
order by
    case categorie
        when 'OBJECTIF'          then 1
        when 'COMBO_3_OBJECTIFS' then 2
        when 'CHAMPION_OPTIMAL'  then 3
        when 'PROFIL_IDEAL'      then 4
    end,
    priorite,
    role