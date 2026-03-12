WITH base AS (
    SELECT *
    FROM {{ ref('int_player_match_stats') }}
),

role_medians AS (
    SELECT
        clean_role,
        PERCENTILE_CONT(kda, 0.5) OVER (PARTITION BY clean_role) AS median_kda          --calcul mediane kda pour définir seuil de per
    FROM base
),

role_median_distinct AS (
    SELECT DISTINCT clean_role, median_kda
    FROM role_medians
),

with_performance AS (
    SELECT
        b.*,
        rm.median_kda AS role_median_kda,
        CASE WHEN b.kda >= rm.median_kda THEN TRUE ELSE FALSE END AS is_performing      --perf ?
    FROM base b
    LEFT JOIN role_median_distinct rm ON b.clean_role = rm.clean_role
),

team_roles AS (                                                                     
    SELECT
        matchid,
        win,
        MAX(CASE WHEN clean_role = 'Top'     THEN is_performing END) AS top_perf,
        MAX(CASE WHEN clean_role = 'Jungle'  THEN is_performing END) AS jungle_perf,
        MAX(CASE WHEN clean_role = 'Mid'     THEN is_performing END) AS mid_perf,
        MAX(CASE WHEN clean_role = 'ADC'     THEN is_performing END) AS adc_perf,
        MAX(CASE WHEN clean_role = 'Support' THEN is_performing END) AS support_perf
    FROM with_performance
    GROUP BY match_id, win                                                               -- pour chaque matchid,win avoir le statut de chaque role et grouper par match&team pour identifier la win
)

SELECT
    match_id,
    win,
    COALESCE(top_perf, FALSE)     AS top_perf,
    COALESCE(jungle_perf, FALSE)  AS jungle_perf,
    COALESCE(mid_perf, FALSE)     AS mid_perf,
    COALESCE(adc_perf, FALSE)     AS adc_perf,
    COALESCE(support_perf, FALSE) AS support_perf,

    (CASE WHEN COALESCE(top_perf, FALSE)     THEN 1 ELSE 0 END
   + CASE WHEN COALESCE(jungle_perf, FALSE)  THEN 1 ELSE 0 END
   + CASE WHEN COALESCE(mid_perf, FALSE)     THEN 1 ELSE 0 END
   + CASE WHEN COALESCE(adc_perf, FALSE)     THEN 1 ELSE 0 END
   + CASE WHEN COALESCE(support_perf, FALSE) THEN 1 ELSE 0 END
    ) AS n_roles_performing                                             --nbr de roles qui perf

FROM team_roles

WHERE top_perf IS NOT NULL                                              --teams où les 5 rôles sont identifiés
  AND jungle_perf IS NOT NULL
  AND mid_perf IS NOT NULL
  AND adc_perf IS NOT NULL
  AND support_perf IS NOT NULL