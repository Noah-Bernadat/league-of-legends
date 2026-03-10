with 

source as (

    select * from {{ source('raw_v2', 'champions') }}

),

renamed as (

    select
        name,
        id,

    from source

)

select * from renamed