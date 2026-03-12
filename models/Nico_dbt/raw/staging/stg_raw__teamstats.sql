with source as (
    select * from {{ source('raw', 'teamstats') }}
),

cleaned as (
    select
        matchid         as match_id,
        teamid          as team_id,

        firstblood      as first_blood,
        firsttower      as first_tower,
        firstinhib      as first_inhib,
        firstbaron      as first_baron,
        firstdragon     as first_dragon,
        firstharry      as first_herald,

        towerkills      as tower_kills,
        inhibkills      as inhib_kills,
        baronkills      as baron_kills,
        dragonkills     as dragon_kills,
        harrykills      as herald_kills

    from source
)

select * from cleaned
