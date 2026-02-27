-- =============================================================================
-- BRONZE LAYER: Raw data from GCS Landing Zone
-- =============================================================================
-- External tables that read directly from raw JSON files in GCS.
-- No transformations — this is the "source of truth" raw layer.
-- If anything breaks downstream,can always reprocess from Bronze.
-- =============================================================================

-- Create Bronze dataset
CREATE SCHEMA IF NOT EXISTS `fantasy-premier-league-488117.fpl_bronze`
OPTIONS (
    description = "FPL Bronze Layer - Raw API data from GCS landing zone"           
);

-- -----------------------------------------------------------------------------
-- PLAYERS (from bootstrap-static → elements)
-- Raw player data with all 48+ fields as received from the API
-- -----------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE `fantasy-premier-league-488117.fpl_bronze.players`
OPTIONS (
    format = 'JSON',
    uris = ['gs://fpl-bucket-2026/landing/fpl-api/players/*.json']  
);

-- -----------------------------------------------------------------------------
-- TEAMS (from bootstrap-static → teams)
-- Raw team data for all 20 Premier League clubs
-- -----------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE `fantasy-premier-league-488117.fpl_bronze.teams` 
OPTIONS (
    format = 'JSON',
    uris = ['gs://fpl-bucket-2026/landing/fpl-api/teams/*.json']   
);

-- -----------------------------------------------------------------------------
-- GAMEWEEKS (from bootstrap-static → events)
-- Raw gameweek summary data for all 38 gameweeks
-- -----------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE `fantasy-premier-league-488117.fpl_bronze.gameweeks` 
OPTIONS (
    format = 'JSON',
    uris = ['gs://fpl-bucket-2026/landing/fpl-api/gameweeks/*.json'] 
);

-- -----------------------------------------------------------------------------
-- FIXTURES (from /fixtures/ endpoint)
-- Raw match data for all 380 fixtures in the season
-- Note: 'stats' is a complex nested array — we'll flatten it in Silver
-- -----------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE `fantasy-premier-league-488117.fpl_bronze.fixtures` 
OPTIONS (
    format = 'JSON',
    uris = ['gs://fpl-bucket-2026/landing/fpl-api/fixtures/*.json']    
);

-- -----------------------------------------------------------------------------
-- GAMEWEEK LIVE (from /event/{gw}/live/ endpoint)
-- Per-player performance stats for each gameweek (flattened in pipeline)
-- This is the most granular data — one row per player per gameweek
-- -----------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE `fantasy-premier-league-488117.fpl_bronze.gameweek_live` 
OPTIONS (
    format = 'JSON',
    uris = ['gs://fpl-bucket-2026/landing/fpl-api/gameweek_live/*.json']  
);