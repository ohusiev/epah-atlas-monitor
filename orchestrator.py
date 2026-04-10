"""
orchestrator.py — Pipeline entry point for the EPAH Atlas scraper.

Workflow
--------
1.  init_db()              — ensure DB + tables exist
2.  Stage 1 check          — skip if last successful run is recent enough
3.  runStageOne()          — discover projects, save snapshot
4.  upsert_projects()      — sync discoveries into project_index
5.  get_projects_needing_stage2() — DB-driven selection (not file glob)
6.  Stage2Scraper per URL  — parse only selected projects
7.  mark_stage2_done/failed per project
8.  finish_run()           — log outcome in pipeline_runs
"""
from __future__ import annotations

import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

try:
    from .config import RAW_DIR, setup_logging
    from .db import (
        finish_run,
        get_latest_successful_run,
        get_projects_needing_stage2,
        init_db,
        mark_stage2_done,
        mark_stage2_failed,
        start_run,
        upsert_project_details,
        upsert_projects,
        validate_db,
    )
    from .parser import Stage2Scraper, build_session, fetch_page_response, runStageOne
except ImportError:
    from config import RAW_DIR, setup_logging
    from db import (
        finish_run,
        get_latest_successful_run,
        get_projects_needing_stage2,
        init_db,
        mark_stage2_done,
        mark_stage2_failed,
        start_run,
        upsert_project_details,
        upsert_projects,
        validate_db,
    )
    from parser import Stage2Scraper, build_session, fetch_page_response, runStageOne

LOGGER = setup_logging("atlas.orchestrator")

# ── Configuration ──────────────────────────────────────────────────────────────

DB_PATH = Path("data") / "epah_pipeline.db"

# Re-run Stage 1 if the last successful run is older than this
STAGE1_MAX_AGE_HOURS: int = 24 * 14 # 2 weeks

# Re-parse Stage 2 details if older than this (None = never re-parse)
STAGE2_STALE_AFTER_DAYS: int | None = None # int= 24 * 1000 # effectively disable automatic stale re-parsing for now; or change to  trigger manually as needed

# ── Helpers ────────────────────────────────────────────────────────────────────

def _stage1_is_fresh() -> bool:
    """Return True if a recent successful Stage-1 run exists in the DB."""
    latest = get_latest_successful_run(DB_PATH, stage=1)
    if not latest:
        return False
    finished = datetime.fromisoformat(latest["finished_at"])
    age = datetime.now(timezone.utc) - finished
    return age < timedelta(hours=STAGE1_MAX_AGE_HOURS)


def _save_stage2_snapshot(projects: list[dict[str, Any]]) -> Path:
    """Persist Stage-2 results as both a timestamped and rolling JSONL snapshot."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    timestamped_out = RAW_DIR / f"epah_details_atlas_projects_{ts}.jsonl"
    rolling_out = RAW_DIR / "epah_details_atlas_projects.jsonl"

    with timestamped_out.open("w", encoding="utf-8") as fh:
        for record in projects:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    rolling_mode = "a" if rolling_out.exists() else "w" # a = append if exists, else w = write new
    with rolling_out.open(rolling_mode, encoding="utf-8") as fh:
        for record in projects:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    LOGGER.info("Stage 2 timestamped snapshot saved: %s (%d records)", timestamped_out, len(projects))
    LOGGER.info(
        "Stage 2 rolling snapshot %s: %s (%d records)",
        "appended" if rolling_mode == "a" else "created",
        rolling_out,
        len(projects),
    )
    return timestamped_out


# ── Stage 1 orchestration ──────────────────────────────────────────────────────

def run_stage1() -> list[dict[str, Any]]:
    if _stage1_is_fresh():
        LOGGER.info(
            "Stage 1 skipped — last successful run is within %d hours (i.e. %d days) .",
            STAGE1_MAX_AGE_HOURS,STAGE1_MAX_AGE_HOURS/24
        )
        return []

    LOGGER.info("Starting Stage 1 (discovery).")
    run_id = start_run(DB_PATH, stage=1)
    try:
        # runStageOne() handles its own HTTP + snapshot saving; returns list[dict]
        projects = runStageOne()

        counts = upsert_projects(DB_PATH, projects)
        LOGGER.info(
            "project_index upserted — inserted: %d, updated: %d",
            counts["inserted"],
            counts["updated"],
        )
        finish_run(DB_PATH, run_id, success=True)
        LOGGER.info("Stage 1 completed successfully.")
        return projects

    except Exception as exc:
        finish_run(DB_PATH, run_id, success=False)
        LOGGER.exception("Stage 1 failed: %s", exc)
        return []


# ── Stage 2 orchestration ──────────────────────────────────────────────────────

def run_stage2() -> list[dict[str, Any]]:
    targets = get_projects_needing_stage2(
        DB_PATH,
        stale_after_days=STAGE2_STALE_AFTER_DAYS,
    )

    if not targets:
        LOGGER.info("Stage 2 skipped — no projects require parsing.")
        return []

    LOGGER.info("Stage 2 starting — %d projects selected.", len(targets))
    run_id = start_run(DB_PATH, stage=2)
    scraper = Stage2Scraper()
    session = build_session()
    parsed: list[dict[str, Any]] = []

    for target in targets:
        atlas_id = target["atlas_id"]
        project_url = target["project_url"]

        try:
            # Call the scraper directly per URL, bypassing the file-glob path
            # that runStageTwo() uses internally.  We pass a minimal single-item
            # JSON file via a temp file so parse_links_file() is reused as-is.
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False, encoding="utf-8"
            ) as tmp:
                json.dump([{"project_url": project_url, "atlas_id": atlas_id}], tmp)
                tmp_path = Path(tmp.name)

            results = scraper.parse_links_file(tmp_path, session=session)
            tmp_path.unlink(missing_ok=True)

            if results:
                parsed.extend(results)
                counts = upsert_project_details(DB_PATH, results)
                LOGGER.info(
                    "Stage 2 parsed: %s — db inserted=%d updated=%d",
                    project_url, counts["inserted"], counts["updated"],
                )
                mark_stage2_done(DB_PATH, atlas_id)
            else:
                mark_stage2_failed(DB_PATH, atlas_id)
                LOGGER.warning("Stage 2 returned no data for: %s", project_url)

        except Exception as exc:
            mark_stage2_failed(DB_PATH, atlas_id)
            LOGGER.exception("Stage 2 failed for %s: %s", project_url, exc)

    if parsed:
        _save_stage2_snapshot(parsed)

    finish_run(DB_PATH, run_id, success=True)
    LOGGER.info("Stage 2 completed — %d projects parsed.", len(parsed))
    return parsed


# ── Entry point ────────────────────────────────────────────────────────────────

def run_pipeline() -> None:
    """A Simple two-stage controller"""
    LOGGER.info("Pipeline starting. DB path: %s", DB_PATH)

    # Step 1 — initialise or validate DB
    if not validate_db(DB_PATH):
        LOGGER.info("Initialising database.")
        init_db(DB_PATH)
    else:
        LOGGER.info("Database validated.")

    # Step 2+3 — Stage 1: discovery
    # Dev note: we return the Stage 1 discoveries here to allow skipping Stage 2 if Stage 1 is fresh.
    temp = run_stage1()
    if len(temp) == 0:
        LOGGER.info("Stage 1 is fresh, Stage 2 skipped.")
        return None
    # Step 4+5 — Stage 2: detail parsing (DB-driven selection)
    run_stage2()

    LOGGER.info("Pipeline finished.")


if __name__ == "__main__":
    run_pipeline()
