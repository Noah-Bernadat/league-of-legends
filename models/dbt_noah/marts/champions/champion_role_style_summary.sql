WITH champ_role_style AS (
    SELECT
        champion_name,
        vrai_role,
        player_style,
        COUNT(*) AS nb_style_games
    FROM {{ ref('int_player_styles') }}
    WHERE vrai_role != 'Autres'
    GROUP BY champion_name, vrai_role, player_style
),

ranked_styles AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY champion_name, vrai_role
            ORDER BY nb_style_games DESC
        ) AS rn
    FROM champ_role_style
),

dominant_style AS (
    SELECT
        champion_name,
        vrai_role,
        player_style AS dominant_style,
        nb_style_games
    FROM ranked_styles
    WHERE rn = 1
),

champion_profile AS (
    SELECT
        champion_name,
        COUNT(*) AS nb_games_total,
        ROUND(AVG(totdmgtochamp), 2) AS avg_damage,
        ROUND(AVG(totdmgtaken), 2) AS avg_damage_taken,
        ROUND(AVG(totheal), 2) AS avg_heal,
        ROUND(AVG(total_farm), 2) AS avg_farm,
        ROUND(AVG(kda), 2) AS avg_kda_global,
        ROUND(AVG(damage_per_gold), 4) AS avg_damage_per_gold
    FROM {{ ref('int_player_styles') }}
    GROUP BY champion_name
),

champ_style_stats AS (
    SELECT
        champion_name,
        vrai_role,
        player_style,
        COUNT(*) AS nb_games,
        SUM(win) AS wins,
        ROUND(SAFE_DIVIDE(SUM(win), COUNT(*)) * 100, 2) AS winrate_pct,
        ROUND(AVG(kda), 2) AS avg_kda_style
    FROM {{ ref('int_player_styles') }}
    WHERE vrai_role != 'Autres'
    GROUP BY champion_name, vrai_role, player_style
),

filtered_style_stats AS (
    SELECT *
    FROM champ_style_stats
    WHERE nb_games >= 20
)

SELECT
    f.champion_name,
    f.vrai_role,
    d.dominant_style,
    f.player_style,
    f.nb_games,
    f.wins,
    f.winrate_pct,
    f.avg_kda_style,
    c.nb_games_total,
    c.avg_damage,
    c.avg_damage_taken,
    c.avg_heal,
    c.avg_farm,
    c.avg_kda_global,
    c.avg_damage_per_gold,
    CASE
        WHEN f.player_style = d.dominant_style THEN 1
        ELSE 0
    END AS is_dominant_style
FROM filtered_style_stats f
LEFT JOIN dominant_style d
    ON f.champion_name = d.champion_name
   AND f.vrai_role = d.vrai_role
LEFT JOIN champion_profile c
    ON f.champion_name = c.champion_name
ORDER BY f.champion_name, f.vrai_role, f.winrate_pct DESC, f.avg_kda_style DESC