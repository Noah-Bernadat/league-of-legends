WITH agg AS (
    SELECT
        vrai_role,
        player_style,
        COUNT(*) AS nb
    FROM {{ ref('int_player_styles') }}
    WHERE vrai_role != 'Autres'
    GROUP BY vrai_role, player_style
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY vrai_role ORDER BY nb DESC) AS rn
    FROM agg
)

SELECT
    vrai_role,
    player_style AS dominant_style,
    nb
FROM ranked
WHERE rn = 1
ORDER BY vrai_role