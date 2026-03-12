with 

source as (

    select * from {{ source('raw', 'stats2') }}

),

renamed as (

    select
        id,
        win,
        item1,
        item2,
        item3,
        item4,
        item5,
        item6,
        trinket,
        kills,
        deaths,
        assists,
        largestkillingspree,
        largestmultikill,
        killingsprees,
        longesttimespentliving,
        doublekills,
        triplekills,
        quadrakills,
        pentakills,
        legendarykills,
        totdmgdealt,
        magicdmgdealt,
        physicaldmgdealt,
        truedmgdealt,
        largestcrit,
        totdmgtochamp,
        magicdmgtochamp,
        physdmgtochamp,
        truedmgtochamp,
        totheal,
        totunitshealed,
        dmgselfmit,
        dmgtoobj,
        dmgtoturrets,
        visionscore,
        timecc,
        totdmgtaken,
        magicdmgtaken,
        physdmgtaken,
        truedmgtaken,
        goldearned,
        goldspent,
        turretkills,
        inhibkills,
        totminionskilled,
        neutralminionskilled,
        ownjunglekills,
        enemyjunglekills,
        totcctimedealt,
        champlvl,
        pinksbought,
        wardsbought,
        wardsplaced,
        wardskilled,
        firstblood

    from source

)

select * from renamed