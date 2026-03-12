with 

source as (

    select * from {{ source('raw', 'all_datas_role_clean') }}

),

renamed as (

    select

    from source

)

select * from renamed