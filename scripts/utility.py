import requests
import datetime
import json
import csv
import io

from google.cloud import storage, bigquery
from scripts.log import log_event, get_log_entries, clear_logs
from scripts.fpl_api import *


# Initialize GCS & BigQuery Clients
storage_client = storage.Client()
bq_client = bigquery.Client()

# Google Cloud Storage (GCS) Configuration
GCS_BUCKET = "fpl-bucket-2026"                          
DATA_SOURCE = "fpl-api"
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/{DATA_SOURCE}/"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/{DATA_SOURCE}/archive/"
CONFIG_FILE_PATH = f"configs/load_config.csv" # path within the bucket


# BigQuery Configuration
BQ_PROJECT = "fantasy-premier-league-488117"
BQ_DATASET = "temp_dataset"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"



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