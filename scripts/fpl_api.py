
import requests
import time
import datetime
import json

from scripts.log import log_event

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
