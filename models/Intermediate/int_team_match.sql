{{
    config(
        materialized = 'table'
    )
}}

with teamstats as (
    select * from {{ ref('stg_raw__teamstats') }}
),

matches as (
    select * from {{ ref('stg_raw__matches') }}
),

participants as (
    select * from {{ ref('stg_raw__participants') }}
),

stats as (
    select * from {{ ref('stg_raw__statss') }}
),

team_player_agg as (
    select
        p.match_id,
        p.team_id,
        max(s.win)                  as win,
        sum(s.kills)                as team_kills,
        sum(s.deaths)               as team_deaths,
        sum(s.assists)              as team_assists,
        sum(s.gold_earned)          as team_gold_earned,
        sum(s.damage_to_champs)     as team_damage_to_champs
    from participants p
    inner join stats s on s.participant_id = p.participant_id
    group by p.match_id, p.team_id
),

final as (
    select
        ts.match_id,
        ts.team_id,
        case when ts.team_id = 100 then 'Blue' else 'Red' end as side,

        tpa.win,

        m.game_id,
        m.platform_id,
        m.queue_id,
        m.season_id,
        m.game_version,
        m.game_duration_sec,
        m.game_duration_min,
        m.game_duration_segment,

        ts.first_blood,
        ts.first_dragon,
        ts.first_baron,
        ts.first_tower,
        ts.first_herald,
        ts.first_inhib,

        ts.dragon_kills,
        ts.baron_kills,
        ts.tower_kills,
        ts.herald_kills,
        ts.inhib_kills,

        tpa.team_kills,
        tpa.team_deaths,
        tpa.team_assists,
        tpa.team_gold_earned,
        tpa.team_damage_to_champs,

        ts.first_blood + ts.first_dragon + ts.first_baron
            + ts.first_tower + ts.first_herald + ts.first_inhib
            as score_objectifs

    from teamstats ts
    inner join matches m        on m.match_id = ts.match_id
    inner join team_player_agg tpa
        on  tpa.match_id = ts.match_id
        and tpa.team_id  = ts.team_id
)

select * from final
