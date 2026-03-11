with source as (
    select * from {{ source('raw_v2', 'teambans') }}
),

cleaned as (
    select
        matchid         as match_id,
        teamid          as team_id,
        championid      as champion_id,
        banturn         as ban_turn
    from source
)

select * from cleaned
