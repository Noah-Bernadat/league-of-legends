with source as (
    select * from {{ source('raw_v2', 'participants') }}
),

cleaned as (
    select
        id                                              as participant_id,
        matchid                                         as match_id,
        player                                          as player_slot,
        championid                                      as champion_id,

        -- Quel côté (team) : slots 1-5 = Blue (100), 6-10 = Red (200)
        case when player <= 5 then 100 else 200 end     as team_id,

        -- Sorts d'invocateur : codes numériques Riot → noms lisibles
        ss1                                             as ss1_id,
        ss2                                             as ss2_id,

        case ss1
            when 4  then 'Flash'
            when 11 then 'Smite'
            when 12 then 'Teleport'
            when 14 then 'Ignite'
            when 7  then 'Heal'
            when 3  then 'Exhaust'
            when 6  then 'Ghost'
            when 1  then 'Cleanse'
            when 21 then 'Barrier'
            when 13 then 'Clarity'
            else         'Other'
        end                                             as ss1_name,

        case ss2
            when 4  then 'Flash'
            when 11 then 'Smite'
            when 12 then 'Teleport'
            when 14 then 'Ignite'
            when 7  then 'Heal'
            when 3  then 'Exhaust'
            when 6  then 'Ghost'
            when 1  then 'Cleanse'
            when 21 then 'Barrier'
            when 13 then 'Clarity'
            else         'Other'
        end                                             as ss2_name,

        -- Rôle standard (5 postes) déduit de role + position
        case
            when role = 'NONE'        and position = 'JUNGLE' then 'JGL'
            when role = 'DUO_CARRY'   and position = 'BOT'    then 'ADC'
            when role = 'DUO_SUPPORT' and position = 'BOT'    then 'SUP'
            when role = 'SOLO'        and position = 'TOP'    then 'TOP'
            when role = 'SOLO'        and position = 'MID'    then 'MID'
            when role = 'DUO'         and position = 'BOT'    then 'BOT'   -- non résolu
            else                                                    'UNKNOWN'
        end                                             as role

    from source
)

select * from cleaned
