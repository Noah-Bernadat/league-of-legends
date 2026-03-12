WITH stats_union AS (

    SELECT * 
    FROM `projetlol-489709.projet2.stats1`

    UNION ALL

    SELECT * 
    FROM `projetlol-489709.projet2.stats2`

)

SELECT
    -- Identifiants
    p.id,
    p.matchid,
    p.player,
    p.championid,
    c.name AS champion_name,

    -- Infos rôle
    p.role,
    p.position,

    CASE
        WHEN p.position = 'TOP' THEN 'Toplane'
        WHEN p.position = 'JUNGLE' THEN 'Jungle'
        WHEN p.position = 'MID' THEN 'Midlane'
        WHEN p.role = 'DUO_CARRY' AND p.position = 'BOT' THEN 'ADC'
        WHEN p.role = 'DUO_SUPPORT' AND p.position = 'BOT' THEN 'Support'
        ELSE 'Autres'
    END AS vrai_role,

    -- Résultat
    s.win,

    -- Combat
    s.kills,
    s.deaths,
    s.assists,

    -- Dégâts
    s.totdmgtochamp,
    s.totdmgtaken,

    -- Support / sustain
    s.totheal,

    -- Farm
    s.totminionskilled,
    s.neutralminionskilled,

    -- Economie
    s.goldearned,

    -- Vision
    s.visionscore,
    s.wardsplaced,
    s.wardskilled

FROM `projetlol-489709.projet2.participants` p

LEFT JOIN stats_union s
    ON p.id = s.id

LEFT JOIN `projetlol-489709.projet2.champs` c
    ON p.championid = c.id