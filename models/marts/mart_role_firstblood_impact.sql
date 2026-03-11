WITH base AS (
    SELECT * FROM {{ ref('int_player_match_stats') }}
),

fb_stats AS (                                                       --stats pour players qui prennent le 1st blood
    SELECT
        clean_role,
        COUNT(*) AS total_games,
        SUM(firstblood) AS first_bloods_taken,
        ROUND(AVG(firstblood) * 100, 1) AS first_blood_rate_pct
    FROM base
    GROUP BY clean_role
),

fb_winrate AS (                                                     -- winrate quand c'est le rôle en question qui prend le 1st blood
    SELECT
        clean_role,
        ROUND(AVG(win) * 100, 1) AS winrate_after_fb,
        COUNT(*) AS fb_games
    FROM base
    WHERE firstblood = 1
    GROUP BY clean_role
)

SELECT
    f.clean_role,
    f.total_games,
    f.first_bloods_taken,
    f.first_blood_rate_pct,
    w.winrate_after_fb,
    w.fb_games

FROM fb_stats f
LEFT JOIN fb_winrate w ON f.clean_role = w.clean_role
ORDER BY w.winrate_after_fb DESC