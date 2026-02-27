# ⚽ FPL Data Pipeline

A fully automated, cloud-native data pipeline that extracts Fantasy Premier League data daily, transforms it through a medallion architecture, and serves analytics-ready insights through a live dashboard.

Built with Python and Google Cloud Platform.

![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![GCP](https://img.shields.io/badge/Google%20Cloud-Platform-4285F4?logo=google-cloud&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-Data%20Warehouse-669DF6?logo=google-cloud&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)

---

## Architecture

```
                          MEDALLION ARCHITECTURE
                          
  ┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
  │  FPL API │───▶│    BRONZE    │───▶│    SILVER    │───▶│     GOLD     │
  │          │    │    (GCS)     │    │  (BigQuery)  │    │  (BigQuery)  │
  └──────────┘    └──────────────┘    └──────────────┘    └──────────────┘
    Python           Raw JSON           Cleaned &           Analytics
    Requests         Landing Zone       Standardized        Views
                     External Tables    Tables              
                                                                │
                                                                ▼
                                                       ┌──────────────┐
                                                       │  DASHBOARD   │
                                                       │ (Streamlit)  │
                                                       └──────────────┘
                                                         Plotly Charts
                                                         Live Insights

  ┌───────────────────────────────────────────────────────────────────┐
  │                      ORCHESTRATION                                │
  │  Cloud Scheduler → Cloud Functions (Bronze + Silver) → Daily 6AM │
  └───────────────────────────────────────────────────────────────────┘
```

## What It Does

Every morning at 6:00 AM UTC, the pipeline automatically:

1. **Extracts** player stats, team data, fixtures, and gameweek performance from the official FPL API
2. **Lands** raw JSON files in Google Cloud Storage (Bronze layer)
3. **Cleans & standardizes** the data in BigQuery — casting types, mapping IDs to names, converting prices (Silver layer)
4. **Serves** 8 analytics views optimized for FPL decision-making (Gold layer)
5. **Displays** insights through a live Streamlit dashboard (coming soon!!!)

All within GCP's free tier.

---

## Gold Layer Analytics

| View | What It Answers |
|------|----------------|
| 🏅 **Player Overview** | Master view — points, price, xG, value metrics for every player |
| 💰 **Value Picks** | Best points-per-million at each position |
| 🔥 **Form Players** | Who's hot over the last 5 gameweeks |
| 🏆 **Captaincy Picks** | Weighted score combining form + fixture difficulty + threat + bonus |
| 🏟️ **Team Overview** | Goals scored/conceded, clean sheets, home vs away splits |
| 📈 **Gameweek Summary** | Season trends with top performer names resolved |
| 🎯 **Differentials** | Under-owned gems (< 10% ownership) with strong underlying stats |
| 📅 **Fixture Difficulty** | Next 5 gameweeks per team, color-coded easy to hard |

---

## Repo Structure

```
fpl-data-pipeline/
├── cloud-functions/
│   ├── api_to_gcs_function/       ← Landing: FPL API → GCS
│   │   ├── main.py
│   │   └── requirements.txt
│   └── bronze_function/           ← Bronze: GCS → BigQuery (External Tables)
│   │   ├── main.py
│   │   └── requirements.txt
│   └── silver_function/           ← Silver: Refresh tables from Bronze → BigQuery
│       ├── main.py
│       └── requirements.txt
├── sql/
│   ├── bronze/                        ← bronze sql script for big query
│   ├── silver/                        ← silver script for big query
│   └── gold/                          ← Analytics views script for big query
├── scripts/
│   ├── fpl_api.py                     ← API extraction script
│   ├── log.py                         ← Shared logging module
│   └── utility.py                     ← Pipeline utility functions
├──pipeline.py                         ← Local execution script
├── config/
│   └── fpl_load_config.csv            ← Config driving the pipeline
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Extraction** | Python, Requests | Pull data from FPL API |
| **Bronze Storage** | Google Cloud Storage | Raw JSON landing zone |
| **Silver + Gold** | BigQuery | Cleaned tables + analytics views |
| **Orchestration** | Cloud Functions + Cloud Scheduler | Serverless daily automation |
| **Dashboard** | Streamlit + Plotly | Interactive analytics UI |
| **Source Control** | GitHub | Version control |
| **Data Source** | Official FPL API | Free, no key required |
| **Cost** | GCP Free Tier | $0/month |

---

## Key Engineering Decisions

**Config-driven extraction** — A CSV file controls which endpoints to pull, the load type, and active status. Adding a new data source is as simple as adding a row.

**Gameweek-based watermarking** — Incremental loads for gameweek live data use the gameweek number as a watermark. The watermark only advances when FPL confirms data is both `finished` and `data_checked`, preventing partial gameweek data from being missed.

**Pure Python, no Spark** — The data volumes (~700 players, 20 teams, 380 fixtures) don't justify distributed computing. Removing Spark simplified the codebase, eliminated infrastructure costs, and made the pipeline portable.

**SQL transformations in BigQuery** — Keeps extraction (Python), transformation (SQL), and presentation (Streamlit) cleanly separated and independently testable.

**Season-end archiving** — (TBD) Automatically detects when GW38 is complete and archives the full season's data to both GCS and a BigQuery `fpl_archive` dataset for historical cross-season analysis.

---

## Pipeline Schedule

```
6:00 AM UTC  │  Cloud Scheduler triggers API Function
             │  → Python extracts FPL API → JSON lands in GCS
             │  → Logs written to GCS
             │
6:01 AM UTC  │  Cloud Scheduler triggers Bronze Function
             │  → BigQuery SQL creates 5 Bronze tables
             │  → Audit log written to BigQuery
             │
6:02 AM UTC  │  Cloud Scheduler triggers Silver Function
             │  → BigQuery SQL rebuilds 5 Silver tables
             │  → Gold views auto-reflect latest data
             │
  Anytime    │  Dashboard reads from Gold views (always fresh)
```

---

## Getting Started

### Prerequisites

- Python 3.10+
- Google Cloud account with a project
- `gcloud` CLI installed

### Quick Start

```bash
# Clone the repo
git clone https://github.com/bonsuot/fpl-data-pipeline.git
cd fpl-data-pipeline

# Install dependencies
pip install -r requirements.txt

# Authenticate with GCP
gcloud auth application-default login

# Run the pipeline locally
python pipeline.py
```

### Full Setup

1. **Create GCS bucket** and upload config
2. **Run Bronze SQL** in BigQuery Console to create external tables
3. **Run the Python pipeline** to populate Bronze
4. **Run Silver SQL** to build cleaned tables
5. **Run Gold SQL** to create analytics views
6. **Deploy Cloud Functions** for automation
7. **Create Cloud Scheduler jobs** for daily runs
8. **Deploy dashboard** to Streamlit Community Cloud

---

## Data Source

This project uses the [official Fantasy Premier League API](https://fantasy.premierleague.com/api/bootstrap-static/), which is free and publicly accessible with no API key required.

| Endpoint | Data |
|----------|------|
| `/bootstrap-static/` | All players, teams, gameweeks, positions |
| `/fixtures/` | All 380 matches with scores and stats |
| `/event/{gw}/live/` | Per-player stats for each gameweek |
| `/element-summary/{id}/` | Player history and past seasons |

---

## License

This project is for educational and personal use. Fantasy Premier League data belongs to the Premier League.
