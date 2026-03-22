# 1. Create virtual env and install deps
uv sync

# 2. Run the app
uv run streamlit run app.py

$env:UV_CACHE_DIR = ".uv-cache"
uv run python -m streamlit run app.py

without uv 

.\.venv\Scripts\python.exe -m streamlit run app.py

your-repo/
├── parser.py          ← unchanged (yours)
├── config.py          ← unchanged (yours)
├── db.py              ← new: SQLite persistence layer
├── orchestrator.py    ← new: pipeline entry point
├── app.py             ← dashboard (existing)
├── etl.py             ← ETL for dashboard (existing)
└── data/
    ├── epah_pipeline.db
    └── raw/
        ├── epah_list_atlas_projects_{ts}.json
        └── epah_details_atlas_projects_{ts}.jsonl

Key design decisions
Why I bypass runStageTwo() in the orchestrator: Your existing runStageTwo() selects projects via a file glob or the LOCAL_OUTPUT_PATH global — it has no concept of DB state. Instead, the orchestrator calls Stage2Scraper.parse_links_file() directly per DB-selected project, using a temp file as a minimal adapter. This keeps parser.py untouched while gaining full DB-driven control.
Temp file adapter pattern: parse_links_file() expects a JSON file path. Rather than refactoring the parser, the orchestrator writes a single-item temp JSON, calls the method, then deletes it. Clean and non-invasive.
JSONL for Stage 2 snapshots: As your spec suggested — one line per project, appendable, easier to debug than a large JSON array.
Per-project failure isolation: If one project fails in Stage 2, needs_stage2 stays 1 and the rest continue. The next run retries only failures.

Run it
bashuv run python orchestrator.py