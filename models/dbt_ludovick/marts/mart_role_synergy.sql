{{ config(materialized='table') }}



WITH team_perf AS (
    SELECT * FROM {{ ref('int_team_role_performance') }}
),


--Nombre de rôles performants vs winrate

team_count AS (
    SELECT
        'team_count'                            AS synergy_type,
        CAST(n_roles_performing AS STRING)       AS category_a,
        CAST(NULL AS STRING)                     AS category_b,
        'all'                                    AS perf_category,
        COUNT(*)                                 AS games,
        ROUND(AVG(win) * 100, 1)                AS winrate_pct
    FROM team_perf
    GROUP BY n_roles_performing
),


--Synergie par paire de rôles

pairs AS (
    SELECT 'Top' AS role_a, 'Jungle' AS role_b, top_perf AS a_perf, jungle_perf AS b_perf, win FROM team_perf
    UNION ALL
    SELECT 'Top', 'Mid', top_perf, mid_perf, win FROM team_perf
    UNION ALL
    SELECT 'Top', 'ADC', top_perf, adc_perf, win FROM team_perf
    UNION ALL
    SELECT 'Top', 'Support', top_perf, support_perf, win FROM team_perf
    UNION ALL
    SELECT 'Jungle', 'Mid', jungle_perf, mid_perf, win FROM team_perf
    UNION ALL
    SELECT 'Jungle', 'ADC', jungle_perf, adc_perf, win FROM team_perf
    UNION ALL
    SELECT 'Jungle', 'Support', jungle_perf, support_perf, win FROM team_perf
    UNION ALL
    SELECT 'Mid', 'ADC', mid_perf, adc_perf, win FROM team_perf
    UNION ALL
    SELECT 'Mid', 'Support', mid_perf, support_perf, win FROM team_perf
    UNION ALL
    SELECT 'ADC', 'Support', adc_perf, support_perf, win FROM team_perf
),

pair_stats AS (
    SELECT
        'pair'                                  AS synergy_type,
        role_a                                  AS category_a,
        role_b                                  AS category_b,
        CASE
            WHEN a_perf AND b_perf       THEN 'both_perform'
            WHEN a_perf AND NOT b_perf   THEN 'only_a'
            WHEN NOT a_perf AND b_perf   THEN 'only_b'
            ELSE 'neither'
        END                                     AS perf_category,
        COUNT(*)                                AS games,
        ROUND(AVG(win) * 100, 1)               AS winrate_pct
    FROM pairs
    GROUP BY role_a, role_b, perf_category
)

SELECT * FROM team_count
UNION ALL
SELECT * FROM pair_stats

ORDER BY synergy_type, category_a, category_b, perf_category