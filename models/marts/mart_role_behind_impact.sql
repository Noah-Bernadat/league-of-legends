WITH base AS (
    SELECT *
    FROM {{ ref('int_player_match_stats') }}
),

with_status AS (
    SELECT
        *,
        CASE WHEN kda < 1 THEN TRUE ELSE FALSE END AS is_behind
    FROM base                                                           --seuil kda <1 : joueur en retard
)

SELECT
    clean_role,
    is_behind,
    COUNT(*) AS games,
    ROUND(AVG(win) * 100, 1) AS winrate_pct,
    ROUND(AVG(kills), 1) AS avg_kills,
    ROUND(AVG(deaths), 1) AS avg_deaths,
    ROUND(AVG(assists), 1) AS avg_assists

FROM with_status
GROUP BY clean_role, is_behind
ORDER BY clean_role, is_behind