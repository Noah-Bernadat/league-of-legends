with 

source as (

    select * from {{ source('raw', 'participants') }}

),

renamed as (

    select
        id,
        matchid,
        player,
        championid,
        ss1,
        ss2,
        role,
        position

    from source

)

select * from renamed