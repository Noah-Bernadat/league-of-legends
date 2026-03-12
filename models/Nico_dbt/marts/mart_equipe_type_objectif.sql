with joueur as (
    select * from {{ ref('mart_analyse_joueur') }}
),

unpivoted as (

    select *, 'First Blood'  as objectif_name, team_first_blood  as objectif_pris from joueur
    union all
    select *, 'First Dragon' as objectif_name, team_first_dragon as objectif_pris from joueur
    union all
    select *, 'First Baron'  as objectif_name, team_first_baron  as objectif_pris from joueur
    union all
    select *, 'First Tower'  as objectif_name, team_first_tower  as objectif_pris from joueur
    union all
    select *, 'First Herald' as objectif_name, team_first_herald as objectif_pris from joueur
    union all
    select *, 'First Inhib'  as objectif_name, team_first_inhib  as objectif_pris from joueur

)

select * from unpivoted
