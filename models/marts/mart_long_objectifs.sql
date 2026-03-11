{{
    config(
        materialized = 'table'
    )
}}

with team_match as (
    select * from {{ ref('int_team_match') }}
),

unpivoted as (
    select match_id, team_id, side, win, season_id, game_version, game_duration_segment,
           'First Blood'  as objectif_name, first_blood  as objectif_pris, cast(null as int64) as objectif_total_kills
    from team_match
    union all
    select match_id, team_id, side, win, season_id, game_version, game_duration_segment,
           'First Dragon' as objectif_name, first_dragon as objectif_pris, dragon_kills as objectif_total_kills
    from team_match
    union all
    select match_id, team_id, side, win, season_id, game_version, game_duration_segment,
           'First Baron'  as objectif_name, first_baron  as objectif_pris, baron_kills  as objectif_total_kills
    from team_match
    union all
    select match_id, team_id, side, win, season_id, game_version, game_duration_segment,
           'First Tower'  as objectif_name, first_tower  as objectif_pris, tower_kills  as objectif_total_kills
    from team_match
    union all
    select match_id, team_id, side, win, season_id, game_version, game_duration_segment,
           'First Herald' as objectif_name, first_herald as objectif_pris, herald_kills as objectif_total_kills
    from team_match
    union all
    select match_id, team_id, side, win, season_id, game_version, game_duration_segment,
           'First Inhib'  as objectif_name, first_inhib  as objectif_pris, inhib_kills  as objectif_total_kills
    from team_match
)

select * from unpivoted
