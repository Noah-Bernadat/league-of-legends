with 

source as (

    select * from {{ source('raw_v2', 'teambans') }}

),

renamed as (

    select
        matchid,
        teamid,
        championid,
        banturn,

    from source

)

select * from renamed