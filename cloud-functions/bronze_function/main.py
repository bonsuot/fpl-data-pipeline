"""
 =============================================================================
 BRONZE LAYER: Raw data from GCS Landing Zone
 =============================================================================
 External tables that read directly from raw JSON files in GCS.
 No transformations — this is the "source of truth" raw layer.
 If anything breaks downstream,can always reprocess from Bronze.
 =============================================================================
"""
import functions_framework
from google.cloud import bigquery
import json
import os

GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET", "fpl-bucket-2026")
GCS_PROJECT_ID = os.environ.get("GCS_PROJECT_ID", "fantasy-premier-league-488117") 
bq_client = bigquery.Client()

BRONZE_QUERIES = {
    "players": f""" 
                CREATE OR REPLACE EXTERNAL TABLE `{GCS_PROJECT_ID}.fpl_bronze.players`
        OPTIONS (
                format = 'JSON',
                uris = ['gs://{GCS_BUCKET_NAME}/landing/fpl-api/players/*.json']  
                 )        
   """,

    "teams": f""" 
        CREATE OR REPLACE EXTERNAL TABLE `{GCS_PROJECT_ID}.fpl_bronze.teams`
        OPTIONS (
                format = 'JSON',
                uris = ['gs://{GCS_BUCKET_NAME}/landing/fpl-api/teams/*.json']
                )
        """,

    "gameweeks": f"""  
        CREATE OR REPLACE EXTERNAL TABLE `{GCS_PROJECT_ID}.fpl_bronze.gameweeks`
        OPTIONS (
                format = 'JSON',
                uris = ['gs://{GCS_BUCKET_NAME}/landing/fpl-api/gameweeks/*.json']
                )
        """,

    "fixtures": f"""
        CREATE OR REPLACE EXTERNAL TABLE `{GCS_PROJECT_ID}.fpl_bronze.fixtures`
        OPTIONS (
                format = 'JSON',
                uris = ['gs://{GCS_BUCKET_NAME}/landing/fpl-api/fixtures/*.json']
                )
        """,

    "gameweek_live": f"""  
        CREATE OR REPLACE EXTERNAL TABLE `{GCS_PROJECT_ID}.fpl_bronze.gameweek_live`
        OPTIONS (     
                format = 'JSON',
                uris = ['gs://{GCS_BUCKET_NAME}/landing/fpl-api/gameweek_live/*.json']
                )
        """,

        "positions": f"""  
        CREATE OR REPLACE EXTERNAL TABLE `{GCS_PROJECT_ID}.fpl_bronze.positions`
        OPTIONS (     
                format = 'JSON',
                uris = ['gs://{GCS_BUCKET_NAME}/landing/fpl-api/positions/*.json']
                )
        """
}

# =============================================================================
# CLOUD FUNCTION ENTRY POINT
# =============================================================================
@functions_framework.http
def fpl_bronze_trigger(request):
    """HTTP entry point — refreshes all Bronze tables."""
    results = {}

    for table_name, query in BRONZE_QUERIES.items():
        try:
            print(f"🥈 Refreshing Bronze: {table_name}...")
            job = bq_client.query(query)
            job.result()  # Wait for completion
            results[table_name] = "success"
            print(f"✅ Bronze {table_name} refreshed")
        except Exception as e:
            results[table_name] = f"error: {str(e)}"
            print(f"❌ Bronze {table_name} failed: {str(e)}")

    status = "success" if all(v == "success" for v in results.values()) else "partial_failure"
    response = {"status": status, "tables": results}

    status_code = 200 if status == "success" else 207
    return json.dumps(response), status_code, {"Content-Type": "application/json"}




