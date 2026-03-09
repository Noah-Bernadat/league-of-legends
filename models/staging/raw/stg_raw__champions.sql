with 

source as (

    select * from {{ source('raw', 'champions') }}

),

renamed as (

    select
        name,
        id,

    from source

)

select * from renamed