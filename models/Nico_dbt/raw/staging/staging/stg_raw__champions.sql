with source as (
    select * from {{ source('raw', 'champions') }}
),

cleaned as (
    select
        id      as champion_id,
        name    as champion_name
    from source
)

select * from cleaned
