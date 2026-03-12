{{
    config(
        materialized = 'table'
    )
}}

with player_match as (
    select * from {{ ref('int_player_match') }}
),

final as (
    select
        *,

        case
            when deaths = 0 then 'Perfect'
            when kda >= 5 then 'Excellent'
            when kda >= 3 then 'Bon'
            when kda >= 1.5 then 'Moyen'
            else 'Faible'
        end as kda_category,

        case
            when damage_share >= 30 then 'Hyper carry'
            when damage_share >= 25 then 'Carry'
            when damage_share >= 18 then 'Moyen'
            else 'Faible'
        end as damage_category,

        case
            when cs_per_min >= 8 then 'Excellent'
            when cs_per_min >= 6.5 then 'Bon'
            when cs_per_min >= 5 then 'Moyen'
            else 'Faible'
        end as farm_category,

        case
            when vision_per_min >= 1.5 then 'Excellent'
            when vision_per_min >= 1.0 then 'Bon'
            when vision_per_min >= 0.5 then 'Moyen'
            else 'Faible'
        end as vision_category,

        case
            when cc_per_min >= 1.5 then 'Fort CC'
            when cc_per_min >= 0.8 then 'Moyen CC'
            when cc_per_min >= 0.3 then 'Faible CC'
            else 'Tres faible CC'
        end as cc_category,

        case when win = 1 then 'Victoire' else 'Defaite' end as resultat

    from player_match
    where role not in ('UNKNOWN', 'BOT')
      and champion_name is not null
)

select * from final
