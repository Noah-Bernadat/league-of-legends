with t as (
    select * from {{ ref('int_team_match') }}
),

combos as (

    -- ── PAIRES (15) ──────────────────────────────────────────────────────────
    select 'Blood + Dragon'          as combo, 2 as nb_objectifs, countif(first_blood=1 and first_dragon=1)  as total, round(avg(case when first_blood=1 and first_dragon=1  then win end)*100,1) as winrate from t union all
    select 'Blood + Baron',                    2,                 countif(first_blood=1 and first_baron=1),           round(avg(case when first_blood=1 and first_baron=1   then win end)*100,1) from t union all
    select 'Blood + Tower',                    2,                 countif(first_blood=1 and first_tower=1),           round(avg(case when first_blood=1 and first_tower=1   then win end)*100,1) from t union all
    select 'Blood + Herald',                   2,                 countif(first_blood=1 and first_herald=1),          round(avg(case when first_blood=1 and first_herald=1  then win end)*100,1) from t union all
    select 'Blood + Inhib',                    2,                 countif(first_blood=1 and first_inhib=1),           round(avg(case when first_blood=1 and first_inhib=1   then win end)*100,1) from t union all
    select 'Dragon + Baron',                   2,                 countif(first_dragon=1 and first_baron=1),          round(avg(case when first_dragon=1 and first_baron=1  then win end)*100,1) from t union all
    select 'Dragon + Tower',                   2,                 countif(first_dragon=1 and first_tower=1),          round(avg(case when first_dragon=1 and first_tower=1  then win end)*100,1) from t union all
    select 'Dragon + Herald',                  2,                 countif(first_dragon=1 and first_herald=1),         round(avg(case when first_dragon=1 and first_herald=1 then win end)*100,1) from t union all
    select 'Dragon + Inhib',                   2,                 countif(first_dragon=1 and first_inhib=1),          round(avg(case when first_dragon=1 and first_inhib=1  then win end)*100,1) from t union all
    select 'Baron + Tower',                    2,                 countif(first_baron=1 and first_tower=1),           round(avg(case when first_baron=1 and first_tower=1   then win end)*100,1) from t union all
    select 'Baron + Herald',                   2,                 countif(first_baron=1 and first_herald=1),          round(avg(case when first_baron=1 and first_herald=1  then win end)*100,1) from t union all
    select 'Baron + Inhib',                    2,                 countif(first_baron=1 and first_inhib=1),           round(avg(case when first_baron=1 and first_inhib=1   then win end)*100,1) from t union all
    select 'Tower + Herald',                   2,                 countif(first_tower=1 and first_herald=1),          round(avg(case when first_tower=1 and first_herald=1  then win end)*100,1) from t union all
    select 'Tower + Inhib',                    2,                 countif(first_tower=1 and first_inhib=1),           round(avg(case when first_tower=1 and first_inhib=1   then win end)*100,1) from t union all
    select 'Herald + Inhib',                   2,                 countif(first_herald=1 and first_inhib=1),          round(avg(case when first_herald=1 and first_inhib=1  then win end)*100,1) from t union all

    -- ── TRIPLETS (20) ────────────────────────────────────────────────────────
    select 'Blood + Dragon + Baron',           3,                 countif(first_blood=1 and first_dragon=1 and first_baron=1),   round(avg(case when first_blood=1 and first_dragon=1 and first_baron=1  then win end)*100,1) from t union all
    select 'Blood + Dragon + Tower',           3,                 countif(first_blood=1 and first_dragon=1 and first_tower=1),   round(avg(case when first_blood=1 and first_dragon=1 and first_tower=1  then win end)*100,1) from t union all
    select 'Blood + Dragon + Herald',          3,                 countif(first_blood=1 and first_dragon=1 and first_herald=1),  round(avg(case when first_blood=1 and first_dragon=1 and first_herald=1 then win end)*100,1) from t union all
    select 'Blood + Dragon + Inhib',           3,                 countif(first_blood=1 and first_dragon=1 and first_inhib=1),   round(avg(case when first_blood=1 and first_dragon=1 and first_inhib=1  then win end)*100,1) from t union all
    select 'Blood + Baron + Tower',            3,                 countif(first_blood=1 and first_baron=1 and first_tower=1),    round(avg(case when first_blood=1 and first_baron=1 and first_tower=1   then win end)*100,1) from t union all
    select 'Blood + Baron + Herald',           3,                 countif(first_blood=1 and first_baron=1 and first_herald=1),   round(avg(case when first_blood=1 and first_baron=1 and first_herald=1  then win end)*100,1) from t union all
    select 'Blood + Baron + Inhib',            3,                 countif(first_blood=1 and first_baron=1 and first_inhib=1),    round(avg(case when first_blood=1 and first_baron=1 and first_inhib=1   then win end)*100,1) from t union all
    select 'Blood + Tower + Herald',           3,                 countif(first_blood=1 and first_tower=1 and first_herald=1),   round(avg(case when first_blood=1 and first_tower=1 and first_herald=1  then win end)*100,1) from t union all
    select 'Blood + Tower + Inhib',            3,                 countif(first_blood=1 and first_tower=1 and first_inhib=1),    round(avg(case when first_blood=1 and first_tower=1 and first_inhib=1   then win end)*100,1) from t union all
    select 'Blood + Herald + Inhib',           3,                 countif(first_blood=1 and first_herald=1 and first_inhib=1),   round(avg(case when first_blood=1 and first_herald=1 and first_inhib=1  then win end)*100,1) from t union all
    select 'Dragon + Baron + Tower',           3,                 countif(first_dragon=1 and first_baron=1 and first_tower=1),   round(avg(case when first_dragon=1 and first_baron=1 and first_tower=1  then win end)*100,1) from t union all
    select 'Dragon + Baron + Herald',          3,                 countif(first_dragon=1 and first_baron=1 and first_herald=1),  round(avg(case when first_dragon=1 and first_baron=1 and first_herald=1 then win end)*100,1) from t union all
    select 'Dragon + Baron + Inhib',           3,                 countif(first_dragon=1 and first_baron=1 and first_inhib=1),   round(avg(case when first_dragon=1 and first_baron=1 and first_inhib=1  then win end)*100,1) from t union all
    select 'Dragon + Tower + Herald',          3,                 countif(first_dragon=1 and first_tower=1 and first_herald=1),  round(avg(case when first_dragon=1 and first_tower=1 and first_herald=1 then win end)*100,1) from t union all
    select 'Dragon + Tower + Inhib',           3,                 countif(first_dragon=1 and first_tower=1 and first_inhib=1),   round(avg(case when first_dragon=1 and first_tower=1 and first_inhib=1  then win end)*100,1) from t union all
    select 'Dragon + Herald + Inhib',          3,                 countif(first_dragon=1 and first_herald=1 and first_inhib=1),  round(avg(case when first_dragon=1 and first_herald=1 and first_inhib=1 then win end)*100,1) from t union all
    select 'Baron + Tower + Herald',           3,                 countif(first_baron=1 and first_tower=1 and first_herald=1),   round(avg(case when first_baron=1 and first_tower=1 and first_herald=1  then win end)*100,1) from t union all
    select 'Baron + Tower + Inhib',            3,                 countif(first_baron=1 and first_tower=1 and first_inhib=1),    round(avg(case when first_baron=1 and first_tower=1 and first_inhib=1   then win end)*100,1) from t union all
    select 'Baron + Herald + Inhib',           3,                 countif(first_baron=1 and first_herald=1 and first_inhib=1),   round(avg(case when first_baron=1 and first_herald=1 and first_inhib=1  then win end)*100,1) from t union all
    select 'Tower + Herald + Inhib',           3,                 countif(first_tower=1 and first_herald=1 and first_inhib=1),   round(avg(case when first_tower=1 and first_herald=1 and first_inhib=1  then win end)*100,1) from t
)

select
    combo,
    nb_objectifs,
    total           as equipes_avec_combo,
    winrate
from combos
where total > 0
order by winrate desc, total desc