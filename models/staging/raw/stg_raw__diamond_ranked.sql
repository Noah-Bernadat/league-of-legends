with 

source as (

    select * from {{ source('raw', 'diamond_ranked') }}

),

renamed as (

    select
        gameid,
        bluewins,
        bluewardsplaced,
        bluewardsdestroyed,
        bluefirstblood,
        bluekills,
        bluedeaths,
        blueassists,
        blueelitemonsters,
        bluedragons,
        blueheralds,
        bluetowersdestroyed,
        bluetotalgold,
        blueavglevel,
        bluetotalexperience,
        bluetotalminionskilled,
        bluetotaljungleminionskilled,
        bluegolddiff,
        blueexperiencediff,
        bluecspermin,
        bluegoldpermin,
        redwardsplaced,
        redwardsdestroyed,
        redfirstblood,
        redkills,
        reddeaths,
        redassists,
        redelitemonsters,
        reddragons,
        redheralds,
        redtowersdestroyed,
        redtotalgold,
        redavglevel,
        redtotalexperience,
        redtotalminionskilled,
        redtotaljungleminionskilled,
        redgolddiff,
        redexperiencediff,
        redcspermin,
        redgoldpermin

    from source

)

select * from renamed