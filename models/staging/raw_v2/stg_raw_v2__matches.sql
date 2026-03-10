with source as (
    select * from {{ source('raw_v2', 'matches') }}
),

cleaned as (
    select
        id                          as match_id,

        duration                    as game_duration_sec,

        round(duration / 60, 1)       as game_duration_min,

        case
            when duration < 1500  then 'Court (<25 min)'
            when duration < 2400  then 'Normal (25-40 min)'
            else                        'Long (>40 min)'
        end                         as game_duration_segment

    from source
)

select * from cleaned