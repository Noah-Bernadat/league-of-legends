WITH base AS (
    SELECT * FROM {{ ref('int_player_match_stats') }}
),

with_brackets AS (
    SELECT
        *,
        CASE
            WHEN deaths BETWEEN 0 AND 3   THEN '0-3 deaths'
            WHEN deaths BETWEEN 4 AND 6   THEN '4-6 deaths'
            WHEN deaths BETWEEN 7 AND 9   THEN '7-9 deaths'
            WHEN deaths >= 10              THEN '10+ deaths'
        END AS death_bracket,
       
        CASE
            WHEN deaths BETWEEN 0 AND 3   THEN 1
            WHEN deaths BETWEEN 4 AND 6   THEN 2
            WHEN deaths BETWEEN 7 AND 9   THEN 3
            WHEN deaths >= 10              THEN 4
        END AS death_bracket_order                              
    FROM base
)

SELECT
    clean_role,
    death_bracket,
    death_bracket_order,
    COUNT(*) AS games,
    ROUND(AVG(win) * 100, 1) AS winrate_pct

FROM with_brackets
GROUP BY clean_role, death_bracket, death_bracket_order
ORDER BY clean_role, death_bracket_order