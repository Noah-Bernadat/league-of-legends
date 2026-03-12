with equipe as (
    select * from {{ ref('mart_analyse_equipe') }}
),

unpivoted as (
    select
        match_id, team_id, side, win,
        season_id, game_version,
        game_duration_sec, game_duration_min, game_duration_segment,

        score_objectifs, opponent_score_objectifs, objectives_advantage,
        is_comeback, domination_category,

        team_kills, team_deaths, team_assists, team_gold_earned, team_damage_to_champs,
        opponent_kills, opponent_deaths, opponent_assists, opponent_gold_earned, opponent_damage_to_champs,

        combo_blood_dragon, combo_blood_tower, combo_dragon_baron, combo_dragon_herald,
        combo_tower_inhib, combo_early_trio, combo_monster_trio, combo_four, combo_five, combo_all,

        'First Blood'  as objectif_name, first_blood  as objectif_pris, cast(null as int64) as objectif_total_kills
    from equipe
    union all
    select
        match_id, team_id, side, win,
        season_id, game_version,
        game_duration_sec, game_duration_min, game_duration_segment,
        score_objectifs, opponent_score_objectifs, objectives_advantage,
        is_comeback, domination_category,
        team_kills, team_deaths, team_assists, team_gold_earned, team_damage_to_champs,
        opponent_kills, opponent_deaths, opponent_assists, opponent_gold_earned, opponent_damage_to_champs,
        combo_blood_dragon, combo_blood_tower, combo_dragon_baron, combo_dragon_herald,
        combo_tower_inhib, combo_early_trio, combo_monster_trio, combo_four, combo_five, combo_all,
        'First Dragon', first_dragon, dragon_kills
    from equipe
    union all
    select
        match_id, team_id, side, win,
        season_id, game_version,
        game_duration_sec, game_duration_min, game_duration_segment,
        score_objectifs, opponent_score_objectifs, objectives_advantage,
        is_comeback, domination_category,
        team_kills, team_deaths, team_assists, team_gold_earned, team_damage_to_champs,
        opponent_kills, opponent_deaths, opponent_assists, opponent_gold_earned, opponent_damage_to_champs,
        combo_blood_dragon, combo_blood_tower, combo_dragon_baron, combo_dragon_herald,
        combo_tower_inhib, combo_early_trio, combo_monster_trio, combo_four, combo_five, combo_all,
        'First Baron', first_baron, baron_kills
    from equipe
    union all
    select
        match_id, team_id, side, win,
        season_id, game_version,
        game_duration_sec, game_duration_min, game_duration_segment,
        score_objectifs, opponent_score_objectifs, objectives_advantage,
        is_comeback, domination_category,
        team_kills, team_deaths, team_assists, team_gold_earned, team_damage_to_champs,
        opponent_kills, opponent_deaths, opponent_assists, opponent_gold_earned, opponent_damage_to_champs,
        combo_blood_dragon, combo_blood_tower, combo_dragon_baron, combo_dragon_herald,
        combo_tower_inhib, combo_early_trio, combo_monster_trio, combo_four, combo_five, combo_all,
        'First Tower', first_tower, tower_kills
    from equipe
    union all
    select
        match_id, team_id, side, win,
        season_id, game_version,
        game_duration_sec, game_duration_min, game_duration_segment,
        score_objectifs, opponent_score_objectifs, objectives_advantage,
        is_comeback, domination_category,
        team_kills, team_deaths, team_assists, team_gold_earned, team_damage_to_champs,
        opponent_kills, opponent_deaths, opponent_assists, opponent_gold_earned, opponent_damage_to_champs,
        combo_blood_dragon, combo_blood_tower, combo_dragon_baron, combo_dragon_herald,
        combo_tower_inhib, combo_early_trio, combo_monster_trio, combo_four, combo_five, combo_all,
        'First Herald', first_herald, herald_kills
    from equipe
    union all
    select
        match_id, team_id, side, win,
        season_id, game_version,
        game_duration_sec, game_duration_min, game_duration_segment,
        score_objectifs, opponent_score_objectifs, objectives_advantage,
        is_comeback, domination_category,
        team_kills, team_deaths, team_assists, team_gold_earned, team_damage_to_champs,
        opponent_kills, opponent_deaths, opponent_assists, opponent_gold_earned, opponent_damage_to_champs,
        combo_blood_dragon, combo_blood_tower, combo_dragon_baron, combo_dragon_herald,
        combo_tower_inhib, combo_early_trio, combo_monster_trio, combo_four, combo_five, combo_all,
        'First Inhib', first_inhib, inhib_kills
    from equipe
)

select * from unpivoted
