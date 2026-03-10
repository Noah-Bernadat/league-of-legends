with stats1 as (
    select
        id  as participant_id,
        win
    from {{ source('raw_v2', 'stats1') }}
),

stats2 as (
    select
        id  as participant_id,
        win
    from {{ source('raw_v2', 'stats2') }}
),

unioned as (
    select * from stats1
    union all
    select * from stats2
)

select * from unioned