with 

source as (

    select * from {{ source('raw', 'teamstats') }}

),

renamed as (

    select
        matchid,
        teamid,
        firstblood,
        firsttower,
        firstinhib,
        firstbaron,
        firstdragon,
        firstharry,
        towerkills,
        inhibkills,
        baronkills,
        dragonkills,
        harrykills,

    from source

)

select * from renamed