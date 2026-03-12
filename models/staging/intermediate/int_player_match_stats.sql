WITH all_datas AS (
    SELECT * FROM {{ ref('stg_raw__all_datas') }}
),

-- Participants sert de pont pour récupérer l'id qui lie aux stats
participants AS (
    SELECT
        id AS stats_id,
        matchid,
        player
    FROM {{ ref('stg_raw__participants') }}
),

-- Union des deux tables de stats pour les colonnes manquantes
stats_extra AS (
    SELECT
        id,
        totheal,
        totunitshealed,
        dmgtoobj,
        dmgtoturrets,
        dmgselfmit,
        goldspent,
        totcctimedealt,
        firstblood,
        doublekills,
        triplekills,
        quadrakills,
        pentakills,
        longesttimespentliving,
        magicdmgtochamp,
        physdmgtochamp,
        truedmgtochamp
    FROM {{ ref('stg_raw__stats1') }}

    UNION ALL

    SELECT
        id,
        totheal,
        totunitshealed,
        dmgtoobj,
        dmgtoturrets,
        dmgselfmit,
        goldspent,
        totcctimedealt,
        firstblood,
        doublekills,
        triplekills,
        quadrakills,
        pentakills,
        longesttimespentliving,
        magicdmgtochamp,
        physdmgtochamp,
        truedmgtochamp
    FROM {{ ref('stg_raw__stats2') }}
),

-- Jointure : all_datas → participants (pont) → stats_extra
joined AS (
    SELECT
        a.participant_id,
        a.match_id,
        a.player_slot,
        a.server,
        a.queueid,
        a.seasonid,
        a.patch,
        a.match_datetime,
        a.championid,
        a.champion_name,
        a.role,
        a.position,
        a.teamid,
        a.team_side,

        -- Rôle propre
        CASE
            WHEN a.position = 'JUNGLE'                              THEN 'Jungle'
            WHEN a.position = 'TOP'     AND a.role = 'SOLO'         THEN 'Top'
            WHEN a.position = 'MID'     AND a.role = 'SOLO'         THEN 'Mid'
            WHEN a.position = 'BOT'     AND a.role = 'DUO_CARRY'    THEN 'ADC'
            WHEN a.position = 'BOT'     AND a.role = 'DUO_SUPPORT'  THEN 'Support'
            ELSE 'Autre'
        END AS clean_role,

        -- Résultat
        a.player_win AS win,
        a.team_win,

        -- KDA (depuis all_datas)
        a.kills,
        a.deaths,
        a.assists,
        a.kda,

        -- Dégâts (depuis all_datas)
        a.damage_to_champs AS totdmgtochamp,
        a.damage_taken AS totdmgtaken,

        -- Dégâts détaillés (depuis stats)
        s.magicdmgtochamp,
        s.physdmgtochamp,
        s.truedmgtochamp,

        -- Tanking (depuis stats)
        s.dmgselfmit,

        -- Heal (depuis stats)
        s.totheal,
        s.totunitshealed,

        -- Objectifs (depuis stats)
        s.dmgtoobj,
        s.dmgtoturrets,

        -- Farm (depuis all_datas)
        a.cs_lane AS totminionskilled,
        a.cs_jungle AS neutralminionskilled,
        a.cs_lane + a.cs_jungle AS total_cs,

        -- Vision (depuis all_datas)
        a.visionscore,
        a.wardsplaced,
        a.wardskilled,

        -- Économie
        a.goldearned,
        s.goldspent,
        a.gold_per_min,

        -- CC et divers (depuis stats)
        s.totcctimedealt,
        a.champion_level AS champlvl,
        s.firstblood,
        s.longesttimespentliving,
        s.doublekills,
        s.triplekills,
        s.quadrakills,
        s.pentakills,

        -- Infos équipe (depuis all_datas)
        a.team_firstblood,
        a.team_firsttower,
        a.team_firstinhib,
        a.team_firstbaron,
        a.team_firstdragon,
        a.team_first_herald,
        a.team_towerkills,
        a.team_inhibkills,
        a.team_baronkills,
        a.team_dragonkills,
        a.team_herald_kills,

        -- Bans (depuis all_datas)
        a.name AS banned_champion,
        a.banturn,

        -- Durée
        a.duration_mins,
        ROUND(a.duration_mins * 60) AS duration_seconds

    FROM all_datas a
    -- Pont via participants pour trouver le stats_id
    LEFT JOIN participants p
        ON a.match_id = p.matchid
        AND a.player_slot = p.player
    -- Jointure stats via le stats_id du participant
    LEFT JOIN stats_extra s
        ON p.stats_id = s.id
)

SELECT
    *,

    -- ================================================
    -- STATS NORMALISÉES PAR MINUTE
    -- ================================================
    ROUND(SAFE_DIVIDE(totdmgtochamp, duration_mins), 1)     AS dmg_to_champs_per_min,
    ROUND(SAFE_DIVIDE(totdmgtaken, duration_mins), 1)       AS dmg_taken_per_min,
    ROUND(SAFE_DIVIDE(totheal, duration_mins), 1)           AS heal_per_min,
    ROUND(SAFE_DIVIDE(goldearned, duration_mins), 1)        AS gold_earned_per_min,
    ROUND(SAFE_DIVIDE(total_cs, duration_mins), 1)          AS cs_per_min,
    ROUND(SAFE_DIVIDE(visionscore, duration_mins), 2)       AS vision_per_min,
    ROUND(SAFE_DIVIDE(dmgtoobj, duration_mins), 1)          AS obj_dmg_per_min,
    ROUND(SAFE_DIVIDE(dmgtoturrets, duration_mins), 1)      AS turret_dmg_per_min

FROM joined

-- Exclure les rôles non classifiables (~4%)
WHERE clean_role != 'Autre'