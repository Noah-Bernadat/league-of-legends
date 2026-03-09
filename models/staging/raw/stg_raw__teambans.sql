with 

source as (

    select * from {{ source('raw', 'teambans') }}

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