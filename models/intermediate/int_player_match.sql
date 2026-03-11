with participants as (
    select * from {{ ref('stg_raw_v2__participants') }}
),

stats as (
    select * from {{ ref('stg_raw_v2__stats') }}
),

champions as (
    select * from {{ ref('stg_raw_v2__champions') }}
),

matches as (
    select * from {{ ref('stg_raw_v2__matches') }}
),

team_match as (
    select * from {{ ref('int_team_match') }}
),

-- ── Jointure de base ──────────────────────────────────────────────────────────
base as (
    select
        -- Identifiants
        p.participant_id,
        p.match_id,
        p.player_slot,
        p.team_id,
        case when p.team_id = 100 then 'Blue' else 'Red' end    as side,

        -- Champion
        p.champion_id,
        c.champion_name,

        -- Sorts d'invocateur
        p.ss1_id, p.ss1_name,
        p.ss2_id, p.ss2_name,

        -- Rôle
        p.role,

        -- Résultat
        s.win,

        -- Durée du match
        m.game_duration_sec,
        m.game_duration_min,
        m.game_duration_segment,

        -- ── Données équipe (depuis int_team_match) ───────────────────────────
        -- Objectifs "first" de l'équipe du joueur
        tm.first_blood          as team_first_blood,
        tm.first_dragon         as team_first_dragon,
        tm.first_baron          as team_first_baron,
        tm.first_tower          as team_first_tower,
        tm.first_herald         as team_first_herald,
        tm.first_inhib          as team_first_inhib,

        -- Compteurs totaux de l'équipe
        tm.dragon_kills         as team_dragon_kills,
        tm.baron_kills          as team_baron_kills,
        tm.tower_kills          as team_tower_kills,
        tm.herald_kills         as team_herald_kills,
        tm.inhib_kills          as team_inhib_kills,

        -- Score d'objectifs de l'équipe (0 à 6)
        tm.score_objectifs      as team_score_objectifs,

        -- ── Stats joueur ─────────────────────────────────────────────────────
        s.kills,
        s.deaths,
        s.assists,
        s.first_blood,
        s.largest_killing_spree,
        s.largest_multi_kill,
        s.double_kills,
        s.triple_kills,
        s.quadra_kills,
        s.penta_kills,

        -- Dégâts sur champions
        s.damage_to_champs,
        s.magic_damage_to_champs,
        s.physical_damage_to_champs,
        s.true_damage_to_champs,

        -- Dégâts totaux / autres
        s.total_damage_dealt,
        s.damage_to_objectives,
        s.damage_to_turrets,
        s.damage_self_mitigated,
        s.total_damage_taken,

        -- Soins
        s.total_heal,
        s.units_healed,

        -- Vision
        s.vision_score,
        s.pinks_bought,
        s.wards_placed,
        s.wards_killed,

        -- Farm
        s.minions_killed,
        s.neutral_minions_killed,
        s.own_jungle_kills,
        s.enemy_jungle_kills,

        -- Or
        s.gold_earned,
        s.gold_spent,

        -- Niveau
        s.champ_level,

        -- Structures
        s.turret_kills,
        s.inhib_kills

    from participants p
    inner join stats        s   on  s.participant_id = p.participant_id
    left  join champions    c   on  c.champion_id    = p.champion_id
    left  join matches      m   on  m.match_id       = p.match_id
    left  join team_match   tm  on  tm.match_id      = p.match_id
                                and tm.team_id       = p.team_id
),

-- ── KPIs joueur (window functions pour les parts relatives) ──────────────────
with_kpis as (
    select
        *,

        -- KDA (null si 0 mort = perfect game)
        round((kills + assists) / nullif(deaths, 0), 2)
            as kda,

        -- Part des dégâts dans l'équipe (%)
        round(
            damage_to_champs
            / nullif(sum(damage_to_champs) over (partition by match_id, team_id), 0)
            * 100, 1
        )   as damage_share,

        -- Part de l'or dans l'équipe (%)
        round(
            gold_earned
            / nullif(sum(gold_earned) over (partition by match_id, team_id), 0)
            * 100, 1
        )   as gold_share,

        -- Participation aux kills de l'équipe (%)
        round(
            (kills + assists)
            / nullif(sum(kills) over (partition by match_id, team_id), 0)
            * 100, 1
        )   as kill_participation,

        -- CS par minute
        round(
            (minions_killed + neutral_minions_killed)
            / nullif(game_duration_min, 0), 1
        )   as cs_per_min,

        -- Efficacité de l'or (0→1 : proche de 1 = dépense presque tout)
        round(gold_spent / nullif(gold_earned, 0), 3)
            as gold_efficiency,

        -- Dégâts sur champions par minute (normalisé par durée)
        round(damage_to_champs / nullif(game_duration_min, 0), 0)
            as damage_per_min,

        -- Vision score par minute (normalisé par durée)
        round(vision_score / nullif(game_duration_min, 0), 2)
            as vision_per_min,

        -- Score objectifs de l'équipe adverse
        -- = total du match - score de notre équipe
        -- (chaque "first" va à exactement une des deux équipes)
        sum(team_score_objectifs) over (partition by match_id)
            - team_score_objectifs                              as opponent_score_objectifs

    from base
),

-- ── KPIs dérivés + play style ─────────────────────────────────────────────────
final as (
    select
        *,

        -- Avantage d'objectifs de l'équipe vs l'adversaire (-6 à +6)
        -- Positif = équipe domine, négatif = équipe dominée
        team_score_objectifs - opponent_score_objectifs         as objectives_advantage,

        -- Flag : l'équipe a dominé les objectifs (score >= 4 sur 6)
        case when team_score_objectifs >= 4 then 1 else 0 end   as team_dominated,

        -- Style de jeu dominant du joueur
        -- Priorité : Support → Jungler → Carry → Farmer → Tank → Standard
        case
            when vision_per_min > 1.2
                 and damage_share < 15                          then 'Support'
            when neutral_minions_killed
                 / nullif(game_duration_min, 0) > 2
                 and damage_share < 22                          then 'Jungler'
            when damage_share >= 25                             then 'Carry'
            when cs_per_min > 7
                 and damage_share < 22                          then 'Farmer'
            when total_damage_taken
                 / nullif(game_duration_min, 0) > 500
                 and damage_share < 22                          then 'Tank'
            else                                                     'Standard'
        end                                                     as play_style

    from with_kpis
)

select * from final