# NOT DEPLOYING A FUNCTION THIS TIME — JUST USING THIS FILE AS A CONVENIENT PLACE TO STORE GOLD LAYER SQL
# CAN BE RUN MANUALLY IN THE BQ CONSOLE OR VIA A SCRIPT TO CREATE THE GOLD LAYER VIEWS

# =============================================================================
# GOLD LAYER: Analytics-Ready Views
# =============================================================================
# Purpose-built views that join Silver tables to answer real FPL questions:
#   • Who are the best value picks?
#   • Which players are in form?
#   • Which teams are strongest defensively?
#   • Who should I captain this week?

# VIEWS reflect the latest Silver data.
# =============================================================================

# Create Gold dataset
f"""
CREATE SCHEMA IF NOT EXISTS `fantasy-premier-league-488117.fpl_gold`
OPTIONS (
    description = "FPL Gold Layer - Analytics-ready views for dashboards and insights"
);

-- -----------------------------------------------------------------------------
-- GOLD VIEW 1: PLAYER PERFORMANCE OVERVIEW
-- The master player view — one row per player
-- Base for most FPL analysis
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `fantasy-premier-league-488117.fpl_gold.player_overview` AS
SELECT
    p.player_id,
    p.full_name,
    p.display_name,
    p.team_id,
    p.team_name,
    p.team_short,
    p.position,
    p.current_price,
    p.total_points,
    p.minutes,
    p.starts,
    p.form,
    p.points_per_game,
    p.ownership_pct,

    -- Goal Contributions
    p.goals_scored,
    p.assists,
    p.goals_scored + p.assists                              AS goal_involvements,
    p.clean_sheets,

    -- Expected vs Actual (over/underperformance)
    p.xg,
    p.xa,
    p.xgi,
    p.goals_scored - p.xg                                   AS goals_vs_xg,
    p.assists - p.xa                                        AS assists_vs_xa,
    (p.goals_scored + p.assists) - p.xgi                    AS gi_vs_xgi,

    -- Value Metrics
    CASE
        WHEN p.current_price > 0
        THEN ROUND(p.total_points / p.current_price, 2)
        ELSE 0
    END                                                     AS points_per_million,

    CASE
        WHEN p.minutes > 0
        THEN ROUND(p.total_points / (p.minutes / 90.0), 2)
        ELSE 0
    END                                                     AS points_per_90,

    -- ICT
    p.ict_index,
    p.influence,
    p.creativity,
    p.threat,

    -- Bonus Magnet
    p.bonus,
    CASE
        WHEN p.starts > 0
        THEN ROUND(p.bonus * 1.0 / p.starts, 2)
        ELSE 0
    END                                                     AS bonus_per_start,

    -- Availability
    p.availability_label,
    p.injury_news,
    p.chance_next_gw,

    -- Transfers (demand indicator)
    p.transfers_in_gw,
    p.transfers_out_gw,
    p.transfers_in_gw - p.transfers_out_gw                  AS net_transfers_gw,

    -- Price Movement
    p.price_change_season,
    p.price_change_gw,

    -- Dream Team
    p.dreamteam_count,
    p.in_dreamteam

FROM `fantasy-premier-league-488117.fpl_silver.players` p;


-- -----------------------------------------------------------------------------
-- GOLD VIEW 2: VALUE PICKS
-- Players offering the best points-per-million — great for budget picks
-- Filtered to players with meaningful game time
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `fantasy-premier-league-488117.fpl_gold.value_picks` AS
SELECT
    player_id,
    full_name,
    display_name,
    team_name,
    position,
    current_price,
    total_points,
    points_per_game,
    form,
    ownership_pct,
    minutes,
    starts,
    goals_scored,
    assists,
    clean_sheets,
    xgi,
    points_per_million,
    points_per_90,
    bonus,
    availability_label,
    net_transfers_gw,

    -- Rank within each position by value
    RANK() OVER (
        PARTITION BY position
        ORDER BY points_per_million DESC
    )                                                       AS value_rank_in_position,

    -- Rank overall
    RANK() OVER (
        ORDER BY points_per_million DESC
    )                                                       AS value_rank_overall

FROM `fantasy-premier-league-488117.fpl_gold.player_overview`
WHERE minutes >= 270                                        -- minimum 3 full matches
  AND availability_label = 'Available';


-- -----------------------------------------------------------------------------
-- GOLD VIEW 3: FORM PLAYERS (Last 5 GW Trend)
-- Who is hot right now? Uses gameweek_live to calculate recent form
-- Rolling window of the most recent 5 completed gameweeks
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `fantasy-premier-league-488117.fpl_gold.form_players` AS
WITH recent_gameweeks AS (
    SELECT gameweek_id
    FROM `fantasy-premier-league-488117.fpl_silver.gameweeks`
    WHERE is_finished = TRUE
    ORDER BY gameweek_id DESC
    LIMIT 5
),
recent_performance AS (
    SELECT
        gl.player_id,
        COUNTIF(gl.played)                                  AS matches_played,
        SUM(gl.total_points)                                AS points_last_5,
        SUM(gl.goals_scored)                                AS goals_last_5,
        SUM(gl.assists)                                     AS assists_last_5,
        SUM(gl.bonus)                                       AS bonus_last_5,
        SUM(gl.clean_sheets)                                AS clean_sheets_last_5,
        ROUND(AVG(gl.total_points), 2)                      AS avg_points_last_5,
        ROUND(SUM(gl.xg), 2)                                AS xg_last_5,
        ROUND(SUM(gl.xa), 2)                                AS xa_last_5,
        ROUND(AVG(gl.ict_index), 2)                         AS avg_ict_last_5,
        SUM(gl.minutes)                                     AS minutes_last_5
    FROM `fantasy-premier-league-488117.fpl_silver.gameweek_live` gl
    INNER JOIN recent_gameweeks rg ON gl.gameweek = rg.gameweek_id
    GROUP BY gl.player_id
)
SELECT
    rp.*,
    p.full_name,
    p.display_name,
    p.team_name,
    p.position,
    p.current_price,
    p.total_points                                          AS total_points_season,
    p.ownership_pct,
    p.availability_label,

    -- Form ranking by position
    RANK() OVER (
        PARTITION BY p.position
        ORDER BY rp.avg_points_last_5 DESC
    )                                                       AS form_rank_in_position

FROM recent_performance rp
INNER JOIN `fantasy-premier-league-488117.fpl_silver.players` p
    ON rp.player_id = p.player_id
WHERE rp.matches_played >= 1                                -- played at least 3 of last 5
ORDER BY rp.avg_points_last_5 DESC;


-- -----------------------------------------------------------------------------
-- GOLD VIEW 4: CAPTAINCY PICKS
-- Who should you captain? Combines form, xG, ICT, bonus, and fixture difficulty
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `fantasy-premier-league-488117.fpl_gold.captaincy_picks` AS
WITH next_fixture AS (
    SELECT
        home_team_id                                        AS team_id,
        away_team                                           AS opponent,
        home_difficulty                                     AS difficulty,
        'HOME'                                              AS venue,
        gameweek
    FROM `fantasy-premier-league-488117.fpl_silver.fixtures`
    WHERE is_finished = FALSE
    UNION ALL
    SELECT
        away_team_id                                        AS team_id,
        home_team                                           AS opponent,
        away_difficulty                                     AS difficulty,
        'AWAY'                                              AS venue,
        gameweek
    FROM `fantasy-premier-league-488117.fpl_silver.fixtures`
    WHERE is_finished = FALSE
),
next_gw AS (
    -- Get only the next gameweek's fixtures
    SELECT *
    FROM next_fixture
    WHERE gameweek = (SELECT MIN(gameweek) FROM next_fixture)
)
SELECT
    p.player_id,
    p.full_name,
    p.display_name,
    p.team_name,
    p.position,
    p.current_price,
    p.form,
    p.points_per_game,
    p.total_points,
    p.ownership_pct,

    -- Next fixture info
    nf.opponent                                             AS next_opponent,
    nf.venue                                                AS next_venue,
    nf.difficulty                                           AS fixture_difficulty,
    nf.gameweek                                             AS next_gameweek,

    -- Key attacking stats
    p.goals_scored,
    p.assists,
    p.xgi,
    p.ict_index,
    p.threat,
    p.bonus,
    p.bonus_per_start,

    -- Captaincy Score (weighted composite)
    ROUND(
        (p.form * 2.0) +                                   -- recent form (heavy weight)
        (p.points_per_game * 1.5) +                         -- consistency
        (p.bonus_per_start * 3.0) +                         -- bonus magnet
        (p.threat * 0.05) +                                 -- attacking threat
        ((5 - nf.difficulty) * 2.0)                         -- easier fixture = higher score
    , 2)                                                    AS captaincy_score

FROM `fantasy-premier-league-488117.fpl_gold.player_overview` p
INNER JOIN next_gw nf ON p.team_id = nf.team_id
WHERE p.availability_label = 'Available'
  AND p.minutes >= 270                                      -- must have meaningful game time
ORDER BY captaincy_score DESC;


-- -----------------------------------------------------------------------------
-- GOLD VIEW 5: TEAM FORM & STRENGTH
-- Team-level aggregations — useful for fixture analysis and defense picks
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `fantasy-premier-league-488117.fpl_gold.team_overview` AS
WITH team_fixture_stats AS (
    SELECT
        home_team_id                                        AS team_id,
        COUNT(*)                                            AS home_played,
        SUM(home_score)                                     AS home_goals_scored,
        SUM(away_score)                                     AS home_goals_conceded,
        SUM(CASE WHEN result = 'HOME_WIN' THEN 1 ELSE 0 END) AS home_wins,
        SUM(CASE WHEN away_score = 0 THEN 1 ELSE 0 END)    AS home_clean_sheets
    FROM `fantasy-premier-league-488117.fpl_silver.fixtures`
    WHERE is_finished = TRUE
    GROUP BY home_team_id
),
team_away_stats AS (
    SELECT
        away_team_id                                        AS team_id,
        COUNT(*)                                            AS away_played,
        SUM(away_score)                                     AS away_goals_scored,
        SUM(home_score)                                     AS away_goals_conceded,
        SUM(CASE WHEN result = 'AWAY_WIN' THEN 1 ELSE 0 END) AS away_wins,
        SUM(CASE WHEN home_score = 0 THEN 1 ELSE 0 END)    AS away_clean_sheets
    FROM `fantasy-premier-league-488117.fpl_silver.fixtures`
    WHERE is_finished = TRUE
    GROUP BY away_team_id
)
SELECT
    t.team_id,
    t.team_name,
    t.team_short,
    t.league_position,
    t.league_points,
    t.matches_played,
    t.wins,
    t.draws,
    t.losses,
    t.form,

    -- Goals
    COALESCE(h.home_goals_scored, 0) + COALESCE(a.away_goals_scored, 0)
                                                            AS total_goals_scored,
    COALESCE(h.home_goals_conceded, 0) + COALESCE(a.away_goals_conceded, 0)
                                                            AS total_goals_conceded,
    (COALESCE(h.home_goals_scored, 0) + COALESCE(a.away_goals_scored, 0))
    - (COALESCE(h.home_goals_conceded, 0) + COALESCE(a.away_goals_conceded, 0))
                                                            AS goal_difference,

    -- Clean Sheets
    COALESCE(h.home_clean_sheets, 0) + COALESCE(a.away_clean_sheets, 0)
                                                            AS total_clean_sheets,

    -- Home vs Away Split
    COALESCE(h.home_goals_scored, 0)                        AS home_goals_scored,
    COALESCE(h.home_goals_conceded, 0)                      AS home_goals_conceded,
    COALESCE(a.away_goals_scored, 0)                        AS away_goals_scored,
    COALESCE(a.away_goals_conceded, 0)                      AS away_goals_conceded,

    -- Averages
    ROUND(
        (COALESCE(h.home_goals_scored, 0) + COALESCE(a.away_goals_scored, 0))
        * 1.0 / NULLIF(t.matches_played, 0), 2
    )                                                       AS avg_goals_scored_per_match,
    ROUND(
        (COALESCE(h.home_goals_conceded, 0) + COALESCE(a.away_goals_conceded, 0))
        * 1.0 / NULLIF(t.matches_played, 0), 2
    )                                                       AS avg_goals_conceded_per_match,

    -- Strength Ratings
    t.overall_strength,
    t.attack_home,
    t.attack_away,
    t.defence_home,
    t.defence_away

FROM `fantasy-premier-league-488117.fpl_silver.teams` t
LEFT JOIN team_fixture_stats h ON t.team_id = h.team_id
LEFT JOIN team_away_stats a ON t.team_id = a.team_id
ORDER BY t.league_position;


-- -----------------------------------------------------------------------------
-- GOLD VIEW 6: GAMEWEEK SUMMARY
-- One row per completed gameweek with top performers resolved to names
-- Great for tracking season trends over time
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `fantasy-premier-league-488117.fpl_gold.gameweek_summary` AS
SELECT
    gw.gameweek_id,
    gw.gameweek_name,
    gw.deadline,
    gw.is_finished,
    gw.avg_score,
    gw.highest_score,
    gw.total_managers,
    gw.total_transfers,

    -- Resolve player IDs to names
    mc.display_name                                         AS most_captained_player,
    ms.display_name                                         AS most_selected_player,
    mt.display_name                                         AS most_transferred_in_player,
    ts.display_name                                         AS top_scorer_player,

    -- Status
    gw.is_current,
    gw.is_next

FROM `fantasy-premier-league-488117.fpl_silver.gameweeks` gw
LEFT JOIN `fantasy-premier-league-488117.fpl_silver.players` mc ON gw.most_captained_player_id = mc.player_id
LEFT JOIN `fantasy-premier-league-488117.fpl_silver.players` ms ON gw.most_selected_player_id = ms.player_id
LEFT JOIN `fantasy-premier-league-488117.fpl_silver.players` mt ON gw.most_transferred_in_player_id = mt.player_id
LEFT JOIN `fantasy-premier-league-488117.fpl_silver.players` ts ON gw.top_scorer_player_id = ts.player_id
ORDER BY gw.gameweek_id;


-- -----------------------------------------------------------------------------
-- GOLD VIEW 7: DIFFERENTIAL PICKS
-- Under-owned players with strong stats — great for climbing the ranks
-- Differentials are typically players owned by < 10% of managers
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `fantasy-premier-league-488117.fpl_gold.differential_picks` AS
SELECT
    player_id,
    full_name,
    display_name,
    team_name,
    position,
    current_price,
    total_points,
    points_per_game,
    form,
    ownership_pct,
    points_per_million,
    points_per_90,
    goals_scored,
    assists,
    xgi,
    ict_index,
    bonus,
    availability_label,
    net_transfers_gw,

    RANK() OVER (
        PARTITION BY position
        ORDER BY points_per_million DESC
    )                                                       AS diff_rank_in_position

FROM `fantasy-premier-league-488117.fpl_gold.player_overview`
WHERE ownership_pct < 10.0                                  -- under 10% ownership
  AND minutes >= 270                                        -- meaningful game time
  AND availability_label = 'Available'
  AND form > 3.0                                            -- decent recent form
ORDER BY points_per_million DESC;


-- -----------------------------------------------------------------------------
-- GOLD VIEW 8: FIXTURE DIFFICULTY TRACKER
-- Upcoming fixtures with difficulty ratings — helps plan transfers
-- Shows the next 5 gameweeks for each team
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW `fantasy-premier-league-488117.fpl_gold.fixture_difficulty_tracker` AS
WITH upcoming AS (
    SELECT
        home_team_id                                        AS team_id,
        home_team                                           AS team_name,
        gameweek,
        CONCAT(away_team_short, ' (H)')                     AS fixture,
        home_difficulty                                     AS difficulty
    FROM `fantasy-premier-league-488117.fpl_silver.fixtures`
    WHERE is_finished = FALSE

    UNION ALL

    SELECT
        away_team_id                                        AS team_id,
        away_team                                           AS team_name,
        gameweek,
        CONCAT(home_team_short, ' (A)')                     AS fixture,
        away_difficulty                                     AS difficulty
    FROM `fantasy-premier-league-488117.fpl_silver.fixtures`
    WHERE is_finished = FALSE
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY gameweek) AS fixture_order
    FROM upcoming
)
SELECT
    team_id,
    team_name,
    gameweek,
    fixture,
    difficulty,
    CASE
        WHEN difficulty <= 2 THEN 'EASY'
        WHEN difficulty = 3 THEN 'MEDIUM'
        WHEN difficulty >= 4 THEN 'HARD'
    END                                                     AS difficulty_label,
    fixture_order
FROM ranked
WHERE fixture_order <= 5                                    -- next 5 fixtures
ORDER BY team_name, gameweek;
"""