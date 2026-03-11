with 

source as (

    select * from {{ source('raw', 'all_datas') }}

),

renamed as (

    select *
    from source

)

select * from renamed