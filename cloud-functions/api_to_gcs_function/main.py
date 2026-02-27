"""
=============================================================================
FPL Landing (Bronze) Function
=============================================================================
Entry point for Google Cloud Functions.
Extracts data from FPL API and lands in GCS Landing layer.

Triggered by: Cloud Scheduler (daily HTTP call)
=============================================================================
"""

import requests
import datetime
import time
import json
import csv
import io
import functions_framework

from google.cloud import storage, bigquery
# from scripts.log import log_event, get_log_entries, clear_logs
# from scripts.fpl_api import *


# Initialize GCS & BigQuery Clients
storage_client = storage.Client()
bq_client = bigquery.Client()

# Google Cloud Storage (GCS) Configuration
GCS_BUCKET = "fpl-bucket-2026"                          
DATA_SOURCE = "fpl-api"
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/{DATA_SOURCE}/"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/{DATA_SOURCE}/archive/"
CONFIG_FILE_PATH = f"configs/load_config.csv" # path within the bucket
# CONFIG_LOCAL_FILE_PATH = f"fpl-pipeline/configs/load_config.csv" # local path for testing


# BigQuery Configuration
BQ_PROJECT = "fantasy-premier-league-488117"
BQ_DATASET = "temp_dataset"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"


log_entries = []

def log_event(event_type, message, table=None):
    """Log an event and store it in the log list."""
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")

def get_log_entries():
    """Return all collected log entries."""
    return log_entries

def clear_logs():
    """Clear all log entries."""
    log_entries.clear()

##--------------------------------------------------------------------------------------------##
# API EXTRACTION FUNCTIONS
##--------------------------------------------------------------------------------------------##

API_DELAY_SECONDS = 1  # Delay between API calls in seconds
FPL_BASE_URL = "https://fantasy.premierleague.com/api/"  # Base URL for FPL API

##--------------------------------------------------------------------------------------------##
def call_fpl_api(endpoint, params=None):
    """Make a GET request to the FPL API with error handling and rate limiting."""
    url = f"{FPL_BASE_URL}{endpoint}"
    try:
        log_event("INFO", f"Calling FPL API: {url}")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        time.sleep(API_DELAY_SECONDS)
        return response.json()
    except requests.exceptions.RequestException as e:
        log_event("ERROR", f"API call failed for {url}: {str(e)}")
        return None
    
# Cache bootstrap data to avoid redundant API calls
# (players, player stats, teams, and gameweeks all come from the same endpoint)
_bootstrap_cache = None

def get_bootstrap_data():
    """Fetch and cache bootstrap-static data"""
    global _bootstrap_cache
    if _bootstrap_cache is None:
        _bootstrap_cache = call_fpl_api("/bootstrap-static/")
    return _bootstrap_cache
##--------------------------------------------------------------------------------------------##

# function to extract players data
def extract_players():
    """Extract all player data from bootstrap-static endpoint.

    Returns ~700 player records with fields like:
    id, first_name, second_name, team, element_type (position),
    total_points, goals_scored, assists, minutes, now_cost, etc.
    """
    data = get_bootstrap_data()
    if data:
        log_event("INFO", f"Player data extracted successfully")
        return data.get("elements", [])
    return []


# function to extract player positions data
def extract_player_positions():
    """Extract all player position (GKP, DEF, MID, FWD) data from bootstrap-static endpoint.
    """
    data = get_bootstrap_data()
    if data:
        log_event("INFO", f"Player positions data extracted successfully")
        return data.get("element_types", [])
    return []


# function to extract teams data. This is teams from current season, not historical teams
def extract_teams():
    """Extract all team data from bootstrap-static endpoint.

    Returns 20 team records with fields like:
    id, name, short_name, strength, strength_overall_home, etc.
    """
    data = get_bootstrap_data()
    if data:
        log_event("INFO", f"Team data extracted successfully")
        return data.get("teams", [])
    return []


# function to extract gameweeks data
def extract_gameweeks():
    """Extract all gameweek summary data from bootstrap-static endpoint.
    Returns 38 gameweek records
    """
    data = get_bootstrap_data()
    if data:
        log_event("INFO", f"Gameweek data extracted successfully")
        return data.get("events", [])
    return []


# function to extract fixtures data
def extract_fixtures(gameweek=None):
    """Extract fixture data, optionally filtered by gameweek.
    Returns fixture records with fields like
    """
    params = {"event": gameweek} if gameweek else None
    data = call_fpl_api("/fixtures/", params=params)
    return data if data else []


# function to extract live/final player stats for a specific gameweek
def extract_gameweek_live(gameweek):
    """Extract live/final player stats for a specific gameweek.
    Returns player performance records with fields like
    """
    data = call_fpl_api(f"/event/{gameweek}/live/")
    if data:
        log_event("INFO", f"Live player data for gameweek {gameweek} extracted successfully")
        elements = data.get("elements", [])
        for element in elements:
            element["gameweek"] = gameweek
            # Flatten nested 'stats' dict into the top level
            if "stats" in element:
                for key, value in element["stats"].items():
                    element[key] = value
                del element["stats"]
        return elements
    return []


# Mapping of table names to API extraction functions
EXTRACTION_MAP = {
    "players":       lambda _: extract_players(),
    "positions":     lambda _: extract_player_positions(),
    "teams":         lambda _: extract_teams(),
    "gameweeks":     lambda _: extract_gameweeks(),
    "fixtures":      lambda _: extract_fixtures(),
    "gameweek_live": lambda gw: extract_gameweek_live(gw),
}

##--------------------------------------------------------------------------------------------##
##--------------------------------------------------------------------------------------------##
# CONFIG FILE READER
##--------------------------------------------------------------------------------------------##
def read_config_file():
    """
    Read the config CSV file from GCS.

    Expected CSV format:
        datasource,table_name,load_type,is_active
        fpl-api,players,full,1
        fpl-api,teams,full,1
        fpl-api,fixtures,full,1
        fpl-api,gameweeks,full,1
        fpl-api,gameweek_live,incremental,1
    """
    try:
        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(CONFIG_FILE_PATH)
        csv_content = blob.download_as_text()

        reader = csv.DictReader(io.StringIO(csv_content))
        config_rows = list(reader)

        log_event("INFO", f"✅ Config file loaded: {len(config_rows)} entries")
        return config_rows

    except Exception as e:
        log_event("ERROR", f"Failed to read config file: {str(e)}")
        return []
    
##--------------------------------------------------------------------------------------------##
# LOGS AND ARCHIVE MECHANISM
##--------------------------------------------------------------------------------------------##

log_entries = get_log_entries()

def save_logs_to_gcs():
    """Save logs to a JSON file and upload to GCS."""
    log_filename = f"pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    log_filepath = f"logs/fpl_pipeline/{log_filename}"

    json_data = json.dumps(log_entries, indent=4)

    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(log_filepath)
    blob.upload_from_string(json_data, content_type="application/json")

    print(f"✅ Logs saved to gs://{GCS_BUCKET}/{log_filepath}")
    

def move_existing_files_to_archive(table):
    """Move existing landing files to date-partitioned archive folder."""
    blobs = list(storage_client.bucket(GCS_BUCKET).list_blobs(
        prefix=f"landing/{DATA_SOURCE}/{table}/"
    ))
    existing_files = [blob.name for blob in blobs if blob.name.endswith(".json")]

    if not existing_files:
        log_event("INFO", f"No existing files to archive for {table}", table=table)
        return

    for file in existing_files:
        if "/archive/" in file:
            continue

        source_blob = storage_client.bucket(GCS_BUCKET).blob(file)

        try:
            date_part = file.split("_")[-1].split(".")[0]
            day, month, year = date_part[:2], date_part[2:4], date_part[4:]
        except (IndexError, ValueError):
            now = datetime.datetime.now()
            day, month, year = now.strftime("%d"), now.strftime("%m"), now.strftime("%Y")

        archive_path = f"landing/{DATA_SOURCE}/archive/{table}/{year}/{month}/{day}/{file.split('/')[-1]}"

        storage_client.bucket(GCS_BUCKET).copy_blob(
            source_blob, storage_client.bucket(GCS_BUCKET), archive_path
        )
        source_blob.delete()

        log_event("INFO", f"Archived {file} → {archive_path}", table=table)


##--------------------------------------------------------------------------------------------##
# WATERMARK MECHANISM (Gameweek-based)
##--------------------------------------------------------------------------------------------##

def get_latest_watermark(table_name):
    """Get the latest gameweek loaded from BigQuery audit table."""
    query = f"""
        SELECT MAX(load_watermark) AS latest_watermark
        FROM `{BQ_AUDIT_TABLE}`
        WHERE tablename = '{table_name}' AND data_source = '{DATA_SOURCE}'
    """
    try:
        result = bq_client.query(query).result()
        for row in result:
            return int(row.latest_watermark) if row.latest_watermark else 0
    except Exception:
        return 0
    return 0

def get_current_gameweek():
    """Get the current gameweek number from the FPL API."""
    try:
        response = requests.get(f"{FPL_BASE_URL}/bootstrap-static/")
        response.raise_for_status()
        data = response.json()

        for event in data["events"]:
            if event["is_current"]:
                return event["id"]

        # If no current gameweek, find the latest finished one
        finished = [e for e in data["events"] if e["finished"]]
        return finished[-1]["id"] if finished else 1
    except Exception as e:
        return None

def get_latest_finished_gameweek():
    """Get the most recent fully completed gameweek."""
    try:
        response = requests.get(f"{FPL_BASE_URL}/bootstrap-static/")
        response.raise_for_status()
        data = response.json()
        finished = [e for e in data["events"] if e["finished"] and e["data_checked"]]
        return finished[-1]["id"] if finished else 0
    except Exception as e:
        log_event("ERROR", f"Failed to get finished gameweek: {str(e)}")
        return 0

##--------------------------------------------------------------------------------------------##
# LANDING & AUDIT
##--------------------------------------------------------------------------------------------##

def save_to_landing(table, data):
    """Save extracted data as newline-delimited JSON to GCS landing zone."""
    if not data:
        log_event("WARNING", f"No data to save for {table}", table=table)
        return 0

    today = datetime.datetime.today().strftime('%d%m%Y')
    json_file_path = f"landing/{DATA_SOURCE}/{table}/{table}_{today}.json"

    json_lines = "\n".join([json.dumps(record) for record in data])

    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(json_file_path)
    blob.upload_from_string(json_lines, content_type="application/json")

    record_count = len(data)
    log_event("SUCCESS", f"✅ {record_count} records saved to gs://{GCS_BUCKET}/{json_file_path}", table=table)
    return record_count


def write_audit_log(table, load_type, record_count, watermark_value=None):
    """Write an audit entry to BigQuery."""
    try:
        audit_record = [{
            "data_source": DATA_SOURCE,
            "tablename": table,
            "load_type": load_type,
            "record_count": record_count,
            "load_watermark": str(watermark_value) if watermark_value else None,
            "load_timestamp": datetime.datetime.now().isoformat(),
            "status": "SUCCESS"
        }]

        table_ref = bq_client.dataset(BQ_DATASET, project=BQ_PROJECT).table("audit_log")
        errors = bq_client.insert_rows_json(table_ref, audit_record)

        if errors:
            log_event("ERROR", f"BigQuery audit insert errors: {errors}", table=table)
        else:
            log_event("SUCCESS", f"✅ Audit log updated for {table}", table=table)

    except Exception as e:
        log_event("ERROR", f"Failed to write audit log for {table}: {str(e)}", table=table)


##--------------------------------------------------------------------------------------------##
# MAIN EXTRACTION ORCHESTRATOR
##--------------------------------------------------------------------------------------------##

def extract_and_save_to_landing(table, load_type, current_gameweek):
    """
    Main extraction function.

    For 'full' loads: pulls all available data from the endpoint.
    For 'incremental' loads: uses gameweek as watermark to only pull new data.
    """
    try:
        log_event("INFO", f"Starting extraction for: {table} ({load_type})", table=table)

        if load_type.lower() == "incremental":
            last_watermark = get_latest_watermark(table)
            log_event("INFO", f"Last loaded gameweek for {table}: {last_watermark}", table=table)

            all_data = []
            for gw in range(last_watermark + 1, current_gameweek + 1):
                log_event("INFO", f"Extracting {table} for gameweek {gw}", table=table)
                extractor = EXTRACTION_MAP.get(table)
                if extractor:
                    gw_data = extractor(gw)
                    all_data.extend(gw_data)

            if not all_data:
                log_event("INFO", f"No new data for {table} since gameweek {last_watermark}", table=table)
                return

            record_count = save_to_landing(table, all_data)

            # only advance watermark for FINISHED gameweeks
            # this ensures we don't miss late updates to live data, 
            # and only move forward once we know the data is final
            latest_finished_gw = get_latest_finished_gameweek()
            write_audit_log(table, load_type, record_count, watermark_value=latest_finished_gw)

        else:
            extractor = EXTRACTION_MAP.get(table)
            if extractor:
                data = extractor(current_gameweek)
            else:
                log_event("ERROR", f"No extractor found for table: {table}", table=table)
                return

            record_count = save_to_landing(table, data)
            write_audit_log(table, load_type, record_count, watermark_value=current_gameweek)

        log_event("SUCCESS", f"✅ Completed extraction for {table}", table=table)

    except Exception as e:
        log_event("ERROR", f"Error processing {table}: {str(e)}", table=table)


# =============================================================================
# MAIN PIPELINE (callable from Cloud Function or locally)
# =============================================================================
def run_pipeline():
    """Main pipeline function."""
    global _bootstrap_cache
    log_entries.clear()
    _bootstrap_cache = None

    log_event("INFO", "========== FPL PIPELINE STARTED ==========")

    # Step 1: Get the current gameweek
    current_gw = get_current_gameweek()
    if current_gw is None:
        log_event("ERROR", "Could not determine current gameweek. Exiting.")
        save_logs_to_gcs()
        return {"status": "error", "message": "Could not determine current gameweek"}

    log_event("INFO", f"Current gameweek: {current_gw}")

    # Step 2: Read config file
    config_rows = read_config_file()

    # Step 3: Process each active table
    for row in config_rows:
        if row["is_active"] == "1" and row["datasource"] == DATA_SOURCE:
            table = row["table_name"]
            load_type = row["load_type"]
            log_event("INFO", f"Processing: {table} ({load_type})")
            # Archive existing files before new load
            move_existing_files_to_archive(table)
            # Extract and save (raw API data)
            extract_and_save_to_landing(table, load_type, current_gw)

    log_event("INFO", "========== FPL PIPELINE COMPLETED ==========")
    save_logs_to_gcs()
    # save_logs_to_bigquery()
    _bootstrap_cache = None

    print("\n🏁 Pipeline run complete!")
    return {"status": "success", "gameweek": current_gw, "tables_processed": len(config_rows)}

# =============================================================================
# CLOUD FUNCTION ENTRY POINT
# =============================================================================
@functions_framework.http
def fpl_api_trigger(request):
    """HTTP entry point for Cloud Functions / Cloud Scheduler."""
    try:
        result = run_pipeline()
        return json.dumps(result), 200, {"Content-Type": "application/json"}
    except Exception as e:
        error_msg = f"Pipeline failed: {str(e)}"
        print(f"❌ {error_msg}")
        return json.dumps({"status": "error", "message": error_msg}), 500


# =============================================================================
# LOCAL EXECUTION
# =============================================================================
if __name__ == "__main__":
    result = run_pipeline()
    print(f"\n🏁 Pipeline result: {result}")