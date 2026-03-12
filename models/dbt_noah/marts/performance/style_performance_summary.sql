SELECT
    player_style,
    COUNT(*) AS nb_games,
    sum(win) as wins,

    ROUND(AVG(kda), 2) AS avg_kda,
    ROUND(SAFE_DIVIDE(SUM(win), COUNT(*)) * 100, 2) AS winrate_pct,

    ROUND(
        AVG(
            (0.35 * COALESCE(kda, 0)) +
            (0.30 * SAFE_DIVIDE(totdmgtochamp, 10000)) +
            (0.20 * SAFE_DIVIDE(visionscore, 20)) +
            (0.15 * SAFE_DIVIDE(total_farm, 100))
        ),
        2
    ) AS avg_impact_score,

    COUNTIF(kda >= 3) AS games_kda_3_plus,
    ROUND(SAFE_DIVIDE(COUNTIF(kda >= 3), COUNT(*)) * 100, 2) AS pct_kda_3_plus,

    COUNTIF(totdmgtochamp >= 25000) AS games_25k_damage_plus,
    ROUND(SAFE_DIVIDE(COUNTIF(totdmgtochamp >= 25000), COUNT(*)) * 100, 2) AS pct_25k_damage_plus

FROM {{ ref('int_player_styles') }}
GROUP BY player_style
ORDER BY winrate_pct DESC