SELECT
    *,
    ROUND(
        (0.35 * COALESCE(kda, 0)) +
        (0.30 * SAFE_DIVIDE(totdmgtochamp, 10000)) +
        (0.20 * SAFE_DIVIDE(visionscore, 20)) +
        (0.15 * SAFE_DIVIDE(total_farm, 100)),
        2
    ) AS impact_score
FROM {{ ref('int_player_styles') }}