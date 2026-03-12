with 

source as (

    select * from {{ source('raw', 'challenger_match') }}

),

renamed as (

    select
        int64_field_0,
        gameid,
        season,
        role,
        lane,
        accountid

    from source

)

select * from renamed