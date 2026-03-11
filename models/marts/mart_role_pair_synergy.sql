WITH base AS (
    SELECT * FROM {{ ref('int_team_role_performance') }}
),



pairs AS (                                                                                                  --générer toutes les paires de rôles avec leurs combinaisons de perf
    
    SELECT 'Top' AS role_a, 'Jungle' AS role_b, top_perf AS a_perf, jungle_perf AS b_perf, win FROM base
    UNION ALL
    SELECT 'Top', 'Mid', top_perf, mid_perf, win FROM base
    UNION ALL
    SELECT 'Top', 'ADC', top_perf, adc_perf, win FROM base
    UNION ALL
    SELECT 'Top', 'Support', top_perf, support_perf, win FROM base
    UNION ALL
    SELECT 'Jungle', 'Mid', jungle_perf, mid_perf, win FROM base
    UNION ALL
    SELECT 'Jungle', 'ADC', jungle_perf, adc_perf, win FROM base
    UNION ALL
    SELECT 'Jungle', 'Support', jungle_perf, support_perf, win FROM base
    UNION ALL
    SELECT 'Mid', 'ADC', mid_perf, adc_perf, win FROM base
    UNION ALL
    SELECT 'Mid', 'Support', mid_perf, support_perf, win FROM base
    UNION ALL
    SELECT 'ADC', 'Support', adc_perf, support_perf, win FROM base
),

categorized AS (                                                    --catégorie : soit les 2 perf, 1 seul ou 0
    SELECT
        role_a,
        role_b,
        CASE
            WHEN a_perf AND b_perf       THEN 'both_perform'
            WHEN a_perf AND NOT b_perf   THEN 'only_a'
            WHEN NOT a_perf AND b_perf   THEN 'only_b'
            ELSE 'neither'
        END AS perf_category,
        win
    FROM pairs
)

SELECT
    role_a,
    role_b,
    perf_category,
    COUNT(*) AS games,
    ROUND(AVG(win) * 100, 1) AS winrate_pct

FROM categorized
GROUP BY role_a, role_b, perf_category
ORDER BY role_a, role_b, perf_category