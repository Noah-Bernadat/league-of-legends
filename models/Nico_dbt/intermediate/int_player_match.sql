{{
    config(
        materialized = 'table'
    )
}}

with participants as (
    select * from {{ ref('stg_raw__participants') }}
),

stats as (
    select * from {{ ref('stg_raw__stats') }}
),

champions as (
    select * from {{ ref('stg_raw__champions') }}
),

matches as (
    select * from {{ ref('stg_raw__matches') }}
),

team_match as (
    select * from {{ ref('int_team_match') }}
),

base as (
    select
        p.participant_id,
        p.match_id,
        p.player_slot,
        p.team_id,
        case when p.team_id = 100 then 'Blue' else 'Red' end    as side,

        p.champion_id,
        c.champion_name,

        p.ss1_id, p.ss1_name,
        p.ss2_id, p.ss2_name,

        p.role,

        s.win,

        m.season_id,
        m.game_version,
        m.game_duration_sec,
        m.game_duration_min,
        m.game_duration_segment,

        tm.first_blood          as team_first_blood,
        tm.first_dragon         as team_first_dragon,
        tm.first_baron          as team_first_baron,
        tm.first_tower          as team_first_tower,
        tm.first_herald         as team_first_herald,
        tm.first_inhib          as team_first_inhib,
        tm.dragon_kills         as team_dragon_kills,
        tm.baron_kills          as team_baron_kills,
        tm.tower_kills          as team_tower_kills,
        tm.herald_kills         as team_herald_kills,
        tm.inhib_kills          as team_inhib_kills,
        tm.team_kills,
        tm.team_deaths,
        tm.team_assists,
        tm.team_gold_earned,
        tm.team_damage_to_champs,
        tm.score_objectifs      as team_score_objectifs,

        s.kills, s.deaths, s.assists,
        s.first_blood,
        s.largest_killing_spree, s.killing_sprees,
        s.largest_multi_kill, s.longest_time_alive_sec,
        s.double_kills, s.triple_kills, s.quadra_kills, s.penta_kills, s.legendary_kills,

        s.damage_to_champs, s.magic_damage_to_champs, s.physical_damage_to_champs, s.true_damage_to_champs,

        s.total_damage_dealt, s.magic_damage_dealt, s.physical_damage_dealt, s.true_damage_dealt,
        s.largest_crit,

        s.damage_to_objectives, s.damage_to_turrets,

        s.damage_self_mitigated,
        s.total_damage_taken, s.magic_damage_taken, s.physical_damage_taken, s.true_damage_taken,

        s.total_heal, s.units_healed,

        s.vision_score, s.pinks_bought, s.wards_bought, s.wards_placed, s.wards_killed,

        s.time_cc_dealt, s.total_cc_time,

        s.minions_killed, s.neutral_minions_killed, s.own_jungle_kills, s.enemy_jungle_kills,

        s.gold_earned, s.gold_spent,

        s.champ_level, s.turret_kills, s.inhib_kills,

        s.item1, s.item2, s.item3, s.item4, s.item5, s.item6, s.trinket

    from participants p
    inner join stats        s   on  s.participant_id = p.participant_id
    left  join champions    c   on  c.champion_id    = p.champion_id
    left  join matches      m   on  m.match_id       = p.match_id
    left  join team_match   tm  on  tm.match_id      = p.match_id
                                and tm.team_id       = p.team_id
),

with_kpis as (
    select
        *,

        round((kills + assists) / nullif(deaths, 0), 2) as kda,

        round(damage_to_champs / nullif(sum(damage_to_champs) over (partition by match_id, team_id), 0) * 100, 1) as damage_share,

        round(gold_earned / nullif(sum(gold_earned) over (partition by match_id, team_id), 0) * 100, 1) as gold_share,

        round((kills + assists) / nullif(sum(kills) over (partition by match_id, team_id), 0) * 100, 1) as kill_participation,

        round((minions_killed + neutral_minions_killed) / nullif(game_duration_min, 0), 1) as cs_per_min,

        round(gold_spent / nullif(gold_earned, 0), 3) as gold_efficiency,

        round(damage_to_champs / nullif(game_duration_min, 0), 0) as damage_per_min,

        round(vision_score / nullif(game_duration_min, 0), 2) as vision_per_min,

        round(total_cc_time / nullif(game_duration_min, 0), 2) as cc_per_min,

        round(total_heal / nullif(game_duration_min, 0), 0) as heal_per_min,

        round(total_damage_taken / nullif(game_duration_min, 0), 0) as damage_taken_per_min,

        round(gold_earned / nullif(game_duration_min, 0), 0) as gold_per_min,

        sum(team_score_objectifs) over (partition by match_id) - team_score_objectifs as opponent_score_objectifs

    from base
),

final as (
    select
        *,

        team_score_objectifs - opponent_score_objectifs as objectives_advantage,

        case when team_score_objectifs >= 4 then 1 else 0 end as team_dominated,

        case
            when ss1_name <= ss2_name then ss1_name || ' + ' || ss2_name
            else ss2_name || ' + ' || ss1_name
        end as spell_combo,

        case
            when vision_per_min > 1.2 and damage_share < 15 then 'Support'
            when neutral_minions_killed / nullif(game_duration_min, 0) > 2 and damage_share < 22 then 'Jungler'
            when damage_share >= 25 then 'Carry'
            when cs_per_min > 7 and damage_share < 22 then 'Farmer'
            when total_damage_taken / nullif(game_duration_min, 0) > 500 and damage_share < 22 then 'Tank'
            else 'Standard'
        end as play_style

    from with_kpis
)

select * from final
