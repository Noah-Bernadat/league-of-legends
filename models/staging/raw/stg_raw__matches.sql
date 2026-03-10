with 

source as (

    select * from {{ source('raw', 'matches') }}

),

renamed as (

    select
        id,
        gameid,
        platformid,
        queueid,
        seasonid,
        duration,
        creation,
        version

    from source

)

select * from renamed