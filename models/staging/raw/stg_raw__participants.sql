with source as (
    select * from {{ source('raw_v2', 'participants') }}
),

cleaned as (
    select
        id          as participant_id,
        matchid     as match_id,
        player      as player_slot,

        -- team_id déduit du slot (pas de colonne directe dans la source)
        case when player <= 5 then 100 else 200 end as team_id

    from source
)

select * from cleaned