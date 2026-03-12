SELECT
    *,
    (totminionskilled + neutralminionskilled) AS total_farm,
    SAFE_DIVIDE(kills + assists, NULLIF(deaths, 0)) AS kda,
    SAFE_DIVIDE(totdmgtochamp, NULLIF(goldearned, 0)) AS damage_per_gold,
    CASE
        WHEN totdmgtaken > 30000 THEN 'Tank'
        WHEN totheal > 5000 THEN 'Support'
        WHEN totdmgtochamp > 25000 THEN 'DPS'
        WHEN (totminionskilled + neutralminionskilled) > 250 THEN 'Farmer'
        ELSE 'Polyvalent'
    END AS player_style
FROM {{ ref('int_classification_styles_joueurs') }}