with 

source as (

    select * from {{ source('raw', 'match_loser') }}

),

renamed as (

    select
        int64_field_0,
        teamid,
        win,
        firstblood,
        firsttower,
        firstinhibitor,
        firstbaron,
        firstdragon,
        firstriftherald,
        towerkills,
        inhibitorkills,
        baronkills,
        dragonkills,
        vilemawkills,
        riftheraldkills,
        dominionvictoryscore,
        bans,
        gameid

    from source

)

select * from renamed