WITH participants AS (
    SELECT * FROM {{ ref('stg_raw__participants') }}
),

stats AS (
    SELECT * FROM {{ ref('stg_raw__stats1') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_raw__stats2') }}
),

matches AS (
    SELECT * FROM {{ ref('stg_raw__matches') }}
),

champions AS (
    SELECT * FROM {{ ref('stg_raw__champions') }}
),

joined AS (
    SELECT
        p.id            AS participant_id,
        p.matchid,
        p.player,
        p.championid,
        ch.name         AS champion_name,
        p.ss1,
        p.ss2,
        p.role,
        p.position,

        CASE
            WHEN p.position = 'JUNGLE'                              THEN 'Jungle'
            WHEN p.position = 'TOP'     AND p.role = 'SOLO'         THEN 'Top'
            WHEN p.position = 'MID'     AND p.role = 'SOLO'         THEN 'Mid'
            WHEN p.position = 'BOT'     AND p.role = 'DUO_CARRY'    THEN 'ADC'
            WHEN p.position = 'BOT'     AND p.role = 'DUO_SUPPORT'  THEN 'Support'
            ELSE 'Autre'
        END AS clean_role,

        
        s.win,
        s.kills,
        s.deaths,
        s.assists,
        ROUND(SAFE_DIVIDE(s.kills + s.assists, NULLIF(s.deaths, 0)), 2) AS kda,
        s.totdmgtochamp,
        s.magicdmgtochamp,
        s.physdmgtochamp,
        s.truedmgtochamp,
        s.totdmgtaken,
        s.dmgselfmit,
        s.totheal,
        s.totunitshealed,
        s.dmgtoobj,
        s.dmgtoturrets,
        s.turretkills,
        s.totminionskilled,
        s.neutralminionskilled,
        s.totminionskilled + s.neutralminionskilled AS total_cs,
        s.visionscore,
        s.wardsplaced,
        s.wardskilled,
        s.pinksbought,
        s.goldearned,
        s.goldspent,
        s.totcctimedealt,
        s.champlvl,
        s.firstblood,
        s.longesttimespentliving,
        s.doublekills,
        s.triplekills,
        s.quadrakills,
        s.pentakills,
        m.duration AS duration_seconds,
        ROUND(m.duration / 60.0, 1) AS duration_minutes

    FROM participants p
    INNER JOIN stats s      ON p.id = s.id
    INNER JOIN matches m    ON p.matchid = m.id
    LEFT JOIN  champions ch ON p.championid = ch.id
)

SELECT
    *,
    ROUND(SAFE_DIVIDE(totdmgtochamp, duration_minutes), 1)          AS dmg_to_champs_per_min,
    ROUND(SAFE_DIVIDE(totdmgtaken, duration_minutes), 1)            AS dmg_taken_per_min,
    ROUND(SAFE_DIVIDE(totheal, duration_minutes), 1)                AS heal_per_min,
    ROUND(SAFE_DIVIDE(goldearned, duration_minutes), 1)             AS gold_per_min,
    ROUND(SAFE_DIVIDE(total_cs, duration_minutes), 1)               AS cs_per_min,
    ROUND(SAFE_DIVIDE(visionscore, duration_minutes), 2)            AS vision_per_min,
    ROUND(SAFE_DIVIDE(dmgtoobj, duration_minutes), 1)               AS obj_dmg_per_min,
    ROUND(SAFE_DIVIDE(dmgtoturrets, duration_minutes), 1)           AS turret_dmg_per_min

FROM joined

WHERE clean_role != 'Autre'