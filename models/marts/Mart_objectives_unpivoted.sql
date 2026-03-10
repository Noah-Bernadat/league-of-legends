 with objectives as (
      select * from {{ ref('int_objectives') }}
  ),

  agg as (
      select
          -- First Blood
          countif(first_blood = 1)
  as games_first_blood,
          sum(case when first_blood = 1 then win else 0 end)
  as wins_first_blood,
          round(avg(case when first_blood = 1 then win end) * 100, 1)
  as wr_first_blood,

          -- First Tower
          countif(first_tower = 1)
  as games_first_tower,
          sum(case when first_tower = 1 then win else 0 end)
  as wins_first_tower,
          round(avg(case when first_tower = 1 then win end) * 100, 1)
  as wr_first_tower,

          -- First Dragon
          countif(first_dragon = 1)
  as games_first_dragon,
          sum(case when first_dragon = 1 then win else 0 end)
  as wins_first_dragon,
          round(avg(case when first_dragon = 1 then win end) * 100, 1)
  as wr_first_dragon,

          -- First Baron
          countif(first_baron = 1)
  as games_first_baron,
          sum(case when first_baron = 1 then win else 0 end)
  as wins_first_baron,
          round(avg(case when first_baron = 1 then win end) * 100, 1)
  as wr_first_baron,

          -- First Inhib
          countif(first_inhib = 1)
  as games_first_inhib,
          sum(case when first_inhib = 1 then win else 0 end)
  as wins_first_inhib,
          round(avg(case when first_inhib = 1 then win end) * 100, 1)
  as wr_first_inhib,

          -- First Herald
          countif(first_herald = 1)
  as games_first_herald,
          sum(case when first_herald = 1 then win else 0 end)
  as wins_first_herald,
          round(avg(case when first_herald = 1 then win end) * 100, 1)
  as wr_first_herald

      from objectives
  ),

  unpivoted as (
      select 'First Blood'  as objectif, games_first_blood  as
  total_games, wins_first_blood  as games_won, wr_first_blood  as winrate
  from agg
      union all
      select 'First Tower'  as objectif, games_first_tower  as
  total_games, wins_first_tower  as games_won, wr_first_tower  as winrate
  from agg
      union all
      select 'First Dragon' as objectif, games_first_dragon as
  total_games, wins_first_dragon as games_won, wr_first_dragon as winrate
  from agg
      union all
      select 'First Baron'  as objectif, games_first_baron  as
  total_games, wins_first_baron  as games_won, wr_first_baron  as winrate
  from agg
      union all
      select 'First Inhib'  as objectif, games_first_inhib  as
  total_games, wins_first_inhib  as games_won, wr_first_inhib  as winrate
  from agg
      union all
      select 'First Herald' as objectif, games_first_herald as
  total_games, wins_first_herald as games_won, wr_first_herald as winrate
  from agg
  )

  select * from unpivoted
  order by winrate desc