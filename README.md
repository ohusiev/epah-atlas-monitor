# EPAH Atlas Monitor

This is a local ethical scraper, persistence layer  (with a database backend and optional JSON upload override), and Streamlit dashboard for monitoring and descriptive analytics of projects from the European Energy Poverty Advisory Hub (EPAH) Atlas. **Used only in research and educational purposes, not affiliated with or endorsed by the EPAH or the European Commission.**

## What This Project Does

This repository combines three pieces:

1. A Stage 1 scraper that discovers project pages from the EPAH Atlas listing.
2. A Stage 2 scraper that visits each project page and extracts detailed fields.
3. A Streamlit dashboard that reads the processed dataset from a local SQLite database and explores it with filters and charts.

The dashboard also triggers the pipeline once on startup, so opening the app can refresh local data if new or stale records need processing.

> Developed with an application of Claude and Codex agentic assistance

## Stack

- Python 3.11+
- Streamlit
- pandas
- Plotly
- requests
- Beautiful Soup
- SQLite

## Quick Start

### With `uv`

```powershell
uv sync
uv run streamlit run app.py
```

If you want the `uv` cache inside the repository:

```powershell
$env:UV_CACHE_DIR = ".uv-cache"
uv run streamlit run app.py
```

### Without `uv`

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m streamlit run app.py
```

## Running The Pipeline Manually

Run the scraper and database update without launching the dashboard:

```powershell
uv run python orchestrator.py
```

What happens during a pipeline run:

1. The database is created if it does not exist.
2. Stage 1 discovers project links from the EPAH Atlas list pages.
3. Newly found or stale projects are selected for Stage 2.
4. Stage 2 parses project detail pages and upserts results into the database.
5. Raw snapshots and logs are written locally for traceability.

## Data Outputs

The project writes data to:

- `data/epah_pipeline.db`: main SQLite database used by the dashboard
- `data/raw/epah_list_atlas_projects_*.json`: Stage 1 discovery snapshots
- `data/raw/epah_details_atlas_projects_*.jsonl`: Stage 2 detail snapshots
- `logs/atlas.parser.log`: scraper logs
- `logs/atlas.orchestrator.log`: pipeline logs

## Dashboard Behavior

The Streamlit app in `app.py`:

- runs the pipeline once when the app starts
- loads records from the database when available
- falls back to the latest local JSON or JSONL snapshot if needed
- supports optional JSON/JSONL upload as an override
- provides filters, descriptive charts, co-occurrence heatmaps, project detail views, and a data quality report

## Project Layout

```text
app.py             Streamlit dashboard
config.py          Shared paths, request settings, logging setup
db.py              SQLite schema and persistence helpers
etl.py             JSON/JSONL loading and dataframe normalization
orchestrator.py    Pipeline controller for Stage 1 and Stage 2
parser.py          Scrapers for project discovery and detail extraction
data/              Local database and raw snapshots
logs/              Runtime logs
```

## Notes

- The active database used by the pipeline and dashboard is SQLite at `data/epah_pipeline.db`.
- Some older comments or historical files in the repo still reference earlier designs; this README reflects the current runtime path.
- Network access is required to refresh EPAH Atlas data.

## Typical Workflow

1. Start the dashboard with `uv run streamlit run app.py`.
2. Let the startup pipeline refresh any pending project data.
3. Explore the latest dataset in the UI.
4. Inspect `logs/` or `data/raw/` if you need debugging or raw snapshots.
