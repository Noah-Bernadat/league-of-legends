with 

source as (

    select * from {{ source('raw', 'all_datas') }}

),

renamed as (

    select
        match_id,
        riot_game_id,
        participant_id,
        player_slot,
        server,
        queueid,
        seasonid,
        patch,
        duration_mins,
        match_datetime,
        championid,
        champion_name,
        role,
        position,
        teamid,
        team_side,
        player_win,
        kills,
        deaths,
        assists,
        kda,
        goldearned,
        gold_per_min,
        damage_to_champs,
        damage_taken,
        cs_lane,
        cs_jungle,
        visionscore,
        wardsplaced,
        wardskilled,
        champion_level,
        team_win,
        team_firstblood,
        team_firsttower,
        team_firstinhib,
        team_firstbaron,
        team_firstdragon,
        team_first_herald,
        team_towerkills,
        team_inhibkills,
        team_baronkills,
        team_dragonkills,
        team_herald_kills,
        name,
        banturn

    from source

)

select * from renamed