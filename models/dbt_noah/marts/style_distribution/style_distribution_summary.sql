WITH base AS (
    SELECT
        champion_name,
        player_style,
        kda,
        totdmgtochamp,
        goldearned,
        damage_per_gold
    FROM {{ ref('int_player_styles') }}
),

style_distribution AS (
    SELECT
        player_style,
        COUNT(*) AS nb_players,
        ROUND(
            SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER ()) * 100,
            2
        ) AS pct
    FROM base
    GROUP BY player_style
),

style_consistency AS (
    SELECT
        player_style,
        COUNT(*) AS nb_games,
        ROUND(AVG(kda), 2) AS avg_kda,
        ROUND(STDDEV(kda), 2) AS std_kda,
        ROUND(AVG(totdmgtochamp), 2) AS avg_damage,
        ROUND(STDDEV(totdmgtochamp), 2) AS std_damage
    FROM base
    GROUP BY player_style
),

style_efficiency AS (
    SELECT
        player_style,
        ROUND(AVG(damage_per_gold), 4) AS avg_damage_per_gold,
        ROUND(AVG(goldearned), 2) AS avg_gold
    FROM base
    GROUP BY player_style
),

champion_style_counts AS (
    SELECT
        champion_name,
        player_style,
        COUNT(*) AS nb
    FROM base
    GROUP BY champion_name, player_style
),

champion_style_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY champion_name
            ORDER BY nb DESC
        ) AS rn
    FROM champion_style_counts
),

champion_dominant_style AS (
    SELECT
        champion_name,
        player_style
    FROM champion_style_ranked
    WHERE rn = 1
),

dominant_style_summary AS (
    SELECT
        player_style,
        COUNT(*) AS nb_dominant_champions,
        STRING_AGG(champion_name, ', ' ORDER BY champion_name LIMIT 10) AS example_dominant_champions
    FROM champion_dominant_style
    GROUP BY player_style
)

SELECT
    d.player_style,
    d.nb_players,
    d.pct,

    c.nb_games,
    c.avg_kda,
    c.std_kda,
    c.avg_damage,
    c.std_damage,

    e.avg_damage_per_gold,
    e.avg_gold,

    COALESCE(ds.nb_dominant_champions, 0) AS nb_dominant_champions,
    ds.example_dominant_champions

FROM style_distribution d
LEFT JOIN style_consistency c
    ON d.player_style = c.player_style
LEFT JOIN style_efficiency e
    ON d.player_style = e.player_style
LEFT JOIN dominant_style_summary ds
    ON d.player_style = ds.player_style
ORDER BY d.nb_players DESC