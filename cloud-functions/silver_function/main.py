"""
=============================================================================
FPL Silver Cloud Function
=============================================================================
Refreshes Silver BigQuery tables from Bronze layer.
Triggered by: Cloud Scheduler (daily, 5 min after Bronze)
=============================================================================
"""

import functions_framework
from google.cloud import bigquery
import json
import os

BQ_PROJECT = os.environ.get("GCP_PROJECT", "fantasy-premier-league")
bq_client = bigquery.Client()

# =============================================================================
# SILVER SQL QUERIES
# Each query reads from Bronze and rebuilds the Silver table
# =============================================================================
SILVER_QUERIES = {
    "players": f"""
        CREATE OR REPLACE TABLE `{BQ_PROJECT}.fpl_silver.players` AS
        SELECT
            p.id AS player_id, p.first_name,
            p.second_name AS last_name, p.web_name AS display_name,
            CONCAT(p.first_name, ' ', p.second_name) AS full_name,
            p.team AS team_id, t.name AS team_name, t.short_name AS team_short,
            p.element_type AS position_id,
            CASE p.element_type
                WHEN 1 THEN 'GKP' WHEN 2 THEN 'DEF'
                WHEN 3 THEN 'MID' WHEN 4 THEN 'FWD'
            END AS position,
            ROUND(p.now_cost / 10.0, 1) AS current_price,
            ROUND(p.cost_change_start / 10.0, 1) AS price_change_season,
            ROUND(p.cost_change_event / 10.0, 1) AS price_change_gw,
            SAFE_CAST(p.selected_by_percent AS FLOAT64) AS ownership_pct,
            p.transfers_in_event AS transfers_in_gw,
            p.transfers_out_event AS transfers_out_gw,
            p.transfers_in AS transfers_in_total,
            p.transfers_out AS transfers_out_total,
            p.total_points,
            p.event_points AS gameweek_points,
            SAFE_CAST(p.points_per_game AS FLOAT64) AS points_per_game,
            SAFE_CAST(p.form AS FLOAT64) AS form,
            SAFE_CAST(p.value_form AS FLOAT64) AS value_form,
            SAFE_CAST(p.value_season AS FLOAT64) AS value_season,
            p.minutes, p.starts, p.goals_scored, p.assists, p.clean_sheets,
            p.goals_conceded, p.own_goals, p.penalties_saved, p.penalties_missed,
            p.yellow_cards, p.red_cards, p.saves, p.bonus,
            p.bps AS bonus_points_system,
            SAFE_CAST(p.influence AS FLOAT64) AS influence,
            SAFE_CAST(p.creativity AS FLOAT64) AS creativity,
            SAFE_CAST(p.threat AS FLOAT64) AS threat,
            SAFE_CAST(p.ict_index AS FLOAT64) AS ict_index,
            SAFE_CAST(p.expected_goals AS FLOAT64) AS xg,
            SAFE_CAST(p.expected_assists AS FLOAT64) AS xa,
            SAFE_CAST(p.expected_goal_involvements AS FLOAT64) AS xgi,
            SAFE_CAST(p.expected_goals_conceded AS FLOAT64) AS xgc,
            p.status AS availability_status,
            CASE p.status
                WHEN 'a' THEN 'Available' WHEN 'd' THEN 'Doubtful'
                WHEN 'i' THEN 'Injured' WHEN 's' THEN 'Suspended'
                WHEN 'u' THEN 'Unavailable' WHEN 'n' THEN 'Not in Squad'
                ELSE 'Unknown'
            END AS availability_label,
            p.news AS injury_news,
            p.chance_of_playing_next_round AS chance_next_gw,
            p.dreamteam_count, p.in_dreamteam,
            CURRENT_TIMESTAMP() AS loaded_at
        FROM `{BQ_PROJECT}.fpl_bronze.players` p
        LEFT JOIN `{BQ_PROJECT}.fpl_bronze.teams` t ON p.team = t.id
    """,

    "teams": f"""
        CREATE OR REPLACE TABLE `{BQ_PROJECT}.fpl_silver.teams` AS
        SELECT
            id AS team_id, code AS team_code, name AS team_name,
            short_name AS team_short, position AS league_position,
            played AS matches_played, win AS wins, draw AS draws,
            loss AS losses, points AS league_points,
            strength AS overall_strength,
            strength_overall_home AS strength_home,
            strength_overall_away AS strength_away,
            strength_attack_home AS attack_home,
            strength_attack_away AS attack_away,
            strength_defence_home AS defence_home,
            strength_defence_away AS defence_away,
            SAFE_CAST(form AS FLOAT64) AS form,
            CURRENT_TIMESTAMP() AS loaded_at
        FROM `{BQ_PROJECT}.fpl_bronze.teams`
    """,

    "gameweeks": f"""
        CREATE OR REPLACE TABLE `{BQ_PROJECT}.fpl_silver.gameweeks` AS
        SELECT
            id AS gameweek_id, name AS gameweek_name,
            deadline_time AS deadline,
            finished AS is_finished, data_checked AS is_data_final,
            average_entry_score AS avg_score, highest_score,
            ranked_count AS total_managers,
            most_selected AS most_selected_player_id,
            most_captained AS most_captained_player_id,
            most_vice_captained AS most_vc_player_id,
            most_transferred_in AS most_transferred_in_player_id,
            top_element AS top_scorer_player_id,
            transfers_made AS total_transfers,
            is_previous, is_current, is_next,
            CURRENT_TIMESTAMP() AS loaded_at
        FROM `{BQ_PROJECT}.fpl_bronze.gameweeks`
    """,

    "fixtures": f"""
        CREATE OR REPLACE TABLE `{BQ_PROJECT}.fpl_silver.fixtures` AS
        SELECT
            f.id AS fixture_id, f.code AS fixture_code,
            f.event AS gameweek, f.kickoff_time,
            f.team_h AS home_team_id, th.name AS home_team,
            th.short_name AS home_team_short,
            f.team_h_score AS home_score, f.team_h_difficulty AS home_difficulty,
            f.team_a AS away_team_id, ta.name AS away_team,
            ta.short_name AS away_team_short,
            f.team_a_score AS away_score, f.team_a_difficulty AS away_difficulty,
            f.finished AS is_finished, f.started AS is_started,
            f.minutes AS minutes_played,
            CASE
                WHEN f.finished AND f.team_h_score > f.team_a_score THEN 'HOME_WIN'
                WHEN f.finished AND f.team_a_score > f.team_h_score THEN 'AWAY_WIN'
                WHEN f.finished AND f.team_h_score = f.team_a_score THEN 'DRAW'
                ELSE NULL
            END AS result,
            CASE WHEN f.finished THEN f.team_h_score + f.team_a_score ELSE NULL END AS total_goals,
            CURRENT_TIMESTAMP() AS loaded_at
        FROM `{BQ_PROJECT}.fpl_bronze.fixtures` f
        LEFT JOIN `{BQ_PROJECT}.fpl_bronze.teams` th ON f.team_h = th.id
        LEFT JOIN `{BQ_PROJECT}.fpl_bronze.teams` ta ON f.team_a = ta.id
    """,

    "gameweek_live": f"""
        CREATE TABLE IF NOT EXISTS `{BQ_PROJECT}.fpl_silver.gameweek_live` (
            player_id INT64,
            gameweek INT64,
            minutes INT64,
            starts INT64,
            goals_scored INT64,
            assists INT64,
            clean_sheets INT64,
            goals_conceded INT64,
            own_goals INT64,
            penalties_saved INT64,
            penalties_missed INT64,
            yellow_cards INT64,
            red_cards INT64,
            saves INT64,
            bonus INT64,
            bonus_points_system INT64,
            total_points INT64,
            influence FLOAT64,
            creativity FLOAT64,
            threat FLOAT64,
            ict_index FLOAT64,
            xg FLOAT64,
            xa FLOAT64,
            xgi FLOAT64,
            xgc FLOAT64,
            in_dreamteam BOOL,
            played BOOL,
            goal_involvements INT64,
            loaded_at TIMESTAMP
    );
        MERGE INTO `{BQ_PROJECT}.fpl_silver.gameweek_live` AS target
        USING (
            SELECT
                gl.id                                                   AS player_id,
                gl.gameweek,
                gl.minutes,
                gl.starts,
                gl.goals_scored,
                gl.assists,
                gl.clean_sheets,
                gl.goals_conceded,
                gl.own_goals,
                gl.penalties_saved,
                gl.penalties_missed,
                gl.yellow_cards,
                gl.red_cards,
                gl.saves,
                gl.bonus,
                gl.bps                                                  AS bonus_points_system,
                gl.total_points,
                SAFE_CAST(gl.influence AS FLOAT64)                      AS influence,
                SAFE_CAST(gl.creativity AS FLOAT64)                     AS creativity,
                SAFE_CAST(gl.threat AS FLOAT64)                         AS threat,
                SAFE_CAST(gl.ict_index AS FLOAT64)                      AS ict_index,
                SAFE_CAST(gl.expected_goals AS FLOAT64)                 AS xg,
                SAFE_CAST(gl.expected_assists AS FLOAT64)               AS xa,
                SAFE_CAST(gl.expected_goal_involvements AS FLOAT64)     AS xgi,
                SAFE_CAST(gl.expected_goals_conceded AS FLOAT64)        AS xgc,
                gl.in_dreamteam,
                CASE WHEN gl.minutes > 0 THEN TRUE ELSE FALSE END      AS played,
                gl.goals_scored + gl.assists                            AS goal_involvements,
                CURRENT_TIMESTAMP()                                     AS loaded_at
    FROM `{BQ_PROJECT}.fpl_bronze.gameweek_live` gl
    ) AS source
    ON target.player_id = source.player_id AND target.gameweek = source.gameweek
    WHEN MATCHED THEN UPDATE SET
        minutes = source.minutes,
        starts = source.starts,
        goals_scored = source.goals_scored,
        assists = source.assists,
        clean_sheets = source.clean_sheets,
        goals_conceded = source.goals_conceded,
        own_goals = source.own_goals,
        penalties_saved = source.penalties_saved,
        penalties_missed = source.penalties_missed,
        yellow_cards = source.yellow_cards,
        red_cards = source.red_cards,
        saves = source.saves,
        bonus = source.bonus,
        bonus_points_system = source.bonus_points_system,
        total_points = source.total_points,
        influence = source.influence,
        creativity = source.creativity,
        threat = source.threat,
        ict_index = source.ict_index,
        xg = source.xg,
        xa = source.xa,
        xgi = source.xgi,
        xgc = source.xgc,
        in_dreamteam = source.in_dreamteam,
        played = source.played,
        goal_involvements = source.goal_involvements,
        loaded_at = source.loaded_at
    WHEN NOT MATCHED THEN INSERT ROW
    """
}


# =============================================================================
# CLOUD FUNCTION ENTRY POINT
# =============================================================================
@functions_framework.http
def fpl_silver_trigger(request):
    """HTTP entry point — refreshes all Silver tables."""
    results = {}

    for table_name, query in SILVER_QUERIES.items():
        try:
            print(f"🥈 Refreshing Silver: {table_name}...")
            job = bq_client.query(query)
            job.result()  # Wait for completion
            results[table_name] = "success"
            print(f"✅ Silver {table_name} refreshed")
        except Exception as e:
            results[table_name] = f"error: {str(e)}"
            print(f"❌ Silver {table_name} failed: {str(e)}")

    status = "success" if all(v == "success" for v in results.values()) else "partial_failure"
    response = {"status": status, "tables": results}

    status_code = 200 if status == "success" else 207
    return json.dumps(response), status_code, {"Content-Type": "application/json"}
