from scripts.log import log_event, get_log_entries, clear_logs
from scripts.fpl_api import *
from scripts.utility import *


##--------------------------------------------------------------------------------------------##
# PIPELINE EXECUTION
##--------------------------------------------------------------------------------------------##

if __name__ == "__main__":
    log_event("INFO", "========== FPL PIPELINE STARTED ==========")

    # Step 1: Get the current gameweek
    current_gw = get_current_gameweek()
    if current_gw is None:
        log_event("ERROR", "Could not determine current gameweek. Exiting.")
        save_logs_to_gcs()
        exit(1)
    log_event("INFO", f"Current gameweek: {current_gw}")

    # Step 2: Read config file
    config_rows = read_config_file()

    # Step 3: Process each active table
    for row in config_rows:
        if row["is_active"] == "1" and row["datasource"] == "fpl-api":
            table = row["table_name"]
            load_type = row["load_type"]

            log_event("INFO", f"Processing: {table} ({load_type})")

            # Archive existing files before new load
            move_existing_files_to_archive(table)

            # Extract and save (raw API data)
            extract_and_save_to_landing(table, load_type, current_gw)

    # Step 4: Save pipeline logs
    log_event("INFO", "========== FPL PIPELINE COMPLETED ==========")
    save_logs_to_gcs()

    # Clear bootstrap cache
    global _bootstrap_cache
    _bootstrap_cache = None

    print("\n🏁 Pipeline run complete!")