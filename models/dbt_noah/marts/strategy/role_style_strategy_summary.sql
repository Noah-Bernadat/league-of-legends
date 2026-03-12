WITH role_style_counts AS (
    SELECT
        vrai_role,
        player_style,
        COUNT(*) AS nb_players,
        ROUND(
            SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER (PARTITION BY vrai_role)) * 100,
            2
        ) AS pct_in_role
    FROM {{ ref('int_player_styles') }}
    WHERE vrai_role != 'Autres'
    GROUP BY vrai_role, player_style
),

dominant_style_by_role AS (
    SELECT
        vrai_role,
        player_style AS dominant_style,
        nb_players AS dominant_style_nb_players
    FROM (
        SELECT
            vrai_role,
            player_style,
            COUNT(*) AS nb_players,
            ROW_NUMBER() OVER (
                PARTITION BY vrai_role
                ORDER BY COUNT(*) DESC
            ) AS rn
        FROM {{ ref('int_player_styles') }}
        WHERE vrai_role != 'Autres'
        GROUP BY vrai_role, player_style
    )
    WHERE rn = 1
),

farm_win_by_style AS (
    SELECT
        player_style,
        COUNT(*) AS nb_games_style,
        ROUND(AVG(total_farm), 2) AS avg_farm,
        ROUND(AVG(goldearned), 2) AS avg_gold,
        ROUND(SAFE_DIVIDE(SUM(win), COUNT(*)) * 100, 2) AS winrate_pct
    FROM {{ ref('int_player_styles') }}
    GROUP BY player_style
),

vision_by_style AS (
    SELECT
        player_style,
        ROUND(AVG(visionscore), 2) AS avg_vision_score,
        ROUND(AVG(wardsplaced), 2) AS avg_wards_placed,
        ROUND(AVG(wardskilled), 2) AS avg_wards_killed
    FROM {{ ref('int_player_styles') }}
    GROUP BY player_style
)

SELECT
    r.vrai_role,
    r.player_style,
    r.nb_players,
    r.pct_in_role,

    d.dominant_style,
    CASE
        WHEN r.player_style = d.dominant_style THEN 1
        ELSE 0
    END AS is_dominant_style_in_role,

    f.nb_games_style,
    f.avg_farm,
    f.avg_gold,
    f.winrate_pct,

    v.avg_vision_score,
    v.avg_wards_placed,
    v.avg_wards_killed

FROM role_style_counts r
LEFT JOIN dominant_style_by_role d
    ON r.vrai_role = d.vrai_role
LEFT JOIN farm_win_by_style f
    ON r.player_style = f.player_style
LEFT JOIN vision_by_style v
    ON r.player_style = v.player_style
ORDER BY r.vrai_role, r.nb_players DESC