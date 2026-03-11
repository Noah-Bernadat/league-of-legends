WITH base AS (
    SELECT * FROM {{ ref('int_team_role_performance') }}
)

SELECT
    n_roles_performing,
    COUNT(*) AS games,
    ROUND(AVG(win) * 100, 1) AS winrate_pct

FROM base
GROUP BY n_roles_performing
ORDER BY n_roles_performing