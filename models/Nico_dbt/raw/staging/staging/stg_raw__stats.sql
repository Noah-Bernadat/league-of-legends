with stats1 as (
    select * from {{ source('raw', 'stats1') }}
),

stats2 as (
    select * from {{ source('raw', 'stats2') }}
),

unioned as (
    select * from stats1
    union all
    select * from stats2
),

cleaned as (
    select
        id                          as participant_id,
        win,

        item1, item2, item3, item4, item5, item6,
        trinket,

        kills,
        deaths,
        assists,
        largestkillingspree         as largest_killing_spree,
        killingsprees               as killing_sprees,
        largestmultikill            as largest_multi_kill,
        longesttimespentliving      as longest_time_alive_sec,
        doublekills                 as double_kills,
        triplekills                 as triple_kills,
        quadrakills                 as quadra_kills,
        pentakills                  as penta_kills,
        legendarykills              as legendary_kills,
        firstblood                  as first_blood,

        totdmgdealt                 as total_damage_dealt,
        magicdmgdealt               as magic_damage_dealt,
        physicaldmgdealt            as physical_damage_dealt,
        truedmgdealt                as true_damage_dealt,
        largestcrit                 as largest_crit,

        totdmgtochamp               as damage_to_champs,
        magicdmgtochamp             as magic_damage_to_champs,
        physdmgtochamp              as physical_damage_to_champs,
        truedmgtochamp              as true_damage_to_champs,

        totheal                     as total_heal,
        totunitshealed              as units_healed,

        dmgselfmit                  as damage_self_mitigated,
        totdmgtaken                 as total_damage_taken,
        magicdmgtaken               as magic_damage_taken,
        physdmgtaken                as physical_damage_taken,
        truedmgtaken                as true_damage_taken,

        dmgtoobj                    as damage_to_objectives,
        dmgtoturrets                as damage_to_turrets,
        turretkills                 as turret_kills,
        inhibkills                  as inhib_kills,

        visionscore                 as vision_score,
        pinksbought                 as pinks_bought,
        wardsbought                 as wards_bought,
        wardsplaced                 as wards_placed,
        wardskilled                 as wards_killed,

        timecc                      as time_cc_dealt,
        totcctimedealt              as total_cc_time,

        totminionskilled            as minions_killed,
        neutralminionskilled        as neutral_minions_killed,
        ownjunglekills              as own_jungle_kills,
        enemyjunglekills            as enemy_jungle_kills,

        goldearned                  as gold_earned,
        goldspent                   as gold_spent,

        champlvl                    as champ_level

    from unioned
)

select * from cleaned
