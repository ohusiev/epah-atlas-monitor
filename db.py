"""
db.py — SQLite persistence layer for the EPAH pipeline.

Tables
------
project_index   : one row per project; identity + current pipeline state
pipeline_runs   : one row per stage run; audit history
"""
from __future__ import annotations

import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

# ── Schema ─────────────────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS project_index (
    atlas_id              TEXT PRIMARY KEY,
    project_url           TEXT NOT NULL UNIQUE,
    project_name          TEXT,
    first_seen_at         TEXT NOT NULL,
    last_seen_at          TEXT NOT NULL,
    last_stage1_seen_at   TEXT,
    last_stage2_parsed_at TEXT,
    needs_stage2          INTEGER NOT NULL DEFAULT 1   -- 1 = pending, 0 = done
);

CREATE TABLE IF NOT EXISTS project_details (
    atlas_id                TEXT PRIMARY KEY
                            REFERENCES project_index(atlas_id) ON DELETE CASCADE,
    project_title           TEXT,
    project_scope           TEXT,
    project_body            TEXT,
    countries_impacted      TEXT,
    geographical_scale      TEXT,
    energy_poverty_phase    TEXT,
    intervention_type       TEXT,
    professionals_involved  TEXT,
    partners_involved       TEXT,
    type_of_funding         TEXT,
    website                 TEXT,
    parsed_at               TEXT,
    updated_at              TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id      TEXT PRIMARY KEY,
    stage       INTEGER NOT NULL,   -- 1 or 2
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    status      TEXT NOT NULL DEFAULT 'running'  -- running | success | failed
);
"""

# ── Connection helper ──────────────────────────────────────────────────────────

@contextmanager
def _connect(db_path: Path) -> Generator[sqlite3.Connection, None, None]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Init ───────────────────────────────────────────────────────────────────────

def init_db(db_path: Path) -> None:
    """Create DB and tables if they do not yet exist."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with _connect(db_path) as conn:
        conn.executescript(_DDL)


def validate_db(db_path: Path) -> bool:
    """Return True if both required tables exist."""
    if not db_path.exists():
        return False
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN "
            "('project_index','project_details','pipeline_runs');"
        ).fetchall()
    return len(rows) == 3


# ── project_index ──────────────────────────────────────────────────────────────

def upsert_projects(db_path: Path, projects: list[dict[str, Any]]) -> dict[str, int]:
    """
    Upsert Stage-1 discovery records into project_index.

    Match priority: atlas_id → project_url.
    Returns counts: {"inserted": N, "updated": N}.
    """
    now = _now()
    inserted = updated = 0

    with _connect(db_path) as conn:
        for p in projects:
            atlas_id = str(p.get("atlas_id") or "")
            project_url = p.get("project_url", "")
            project_name = p.get("project_name") or p.get("project_title")

            if not atlas_id and not project_url:
                continue

            # Derive atlas_id from URL if missing
            if not atlas_id and project_url:
                from urllib.parse import urlparse
                atlas_id = urlparse(project_url).path.rstrip("/").split("/")[-1]

            existing = conn.execute(
                "SELECT atlas_id FROM project_index WHERE atlas_id = ? OR project_url = ?",
                (atlas_id, project_url),
            ).fetchone()

            if existing:
                conn.execute(
                    """UPDATE project_index
                       SET last_seen_at = ?,
                           last_stage1_seen_at = ?,
                           project_name = COALESCE(?, project_name)
                       WHERE atlas_id = ?""",
                    (now, now, project_name, existing["atlas_id"]),
                )
                updated += 1
            else:
                conn.execute(
                    """INSERT INTO project_index
                       (atlas_id, project_url, project_name,
                        first_seen_at, last_seen_at, last_stage1_seen_at,
                        needs_stage2)
                       VALUES (?, ?, ?, ?, ?, ?, 1)""",
                    (atlas_id, project_url, project_name, now, now, now),
                )
                inserted += 1

    return {"inserted": inserted, "updated": updated}


def get_projects_needing_stage2(
    db_path: Path,
    *,
    stale_after_days: int | None = None,
) -> list[dict[str, Any]]:
    """
    Return rows where:
      - needs_stage2 = 1, OR
      - last_stage2_parsed_at IS NULL, OR
      - last_stage2_parsed_at is older than stale_after_days (if set)
    """
    with _connect(db_path) as conn:
        base = """
            SELECT atlas_id, project_url, project_name
            FROM project_index
            WHERE needs_stage2 = 1
               OR last_stage2_parsed_at IS NULL
        """
        params: list[Any] = []
        if stale_after_days is not None:
            base += " OR last_stage2_parsed_at < ?"
            from datetime import timedelta
            cutoff = (
                datetime.now(timezone.utc)
                - timedelta(days=stale_after_days)
            ).isoformat()
            params.append(cutoff)

        rows = conn.execute(base, params).fetchall()
    return [dict(r) for r in rows]


def mark_stage2_done(db_path: Path, atlas_id: str) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """UPDATE project_index
               SET last_stage2_parsed_at = ?,
                   needs_stage2 = 0
               WHERE atlas_id = ?""",
            (_now(), atlas_id),
        )


def mark_stage2_failed(db_path: Path, atlas_id: str) -> None:
    """Keep needs_stage2 = 1 so the next run retries."""
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE project_index SET needs_stage2 = 1 WHERE atlas_id = ?",
            (atlas_id,),
        )


# ── project_details ───────────────────────────────────────────────────────────

_DETAIL_FIELDS = (
    "project_title", "project_scope", "project_body",
    "countries_impacted", "geographical_scale", "energy_poverty_phase",
    "intervention_type", "professionals_involved", "partners_involved",
    "type_of_funding", "website", "parsed_at",
)


def upsert_project_details(db_path: Path, details: list[dict[str, Any]]) -> dict[str, int]:
    """
    Upsert Stage-2 parsed detail records into project_details.

    Requires atlas_id to be present on each record. Records without a
    matching atlas_id in project_index are skipped with a warning.
    Returns counts: {"inserted": N, "updated": N}.
    """
    now = _now()
    inserted = updated = 0

    with _connect(db_path) as conn:
        for d in details:
            atlas_id = str(d.get("atlas_id") or "")
            if not atlas_id:
                continue

            # Guard: only insert if the parent row exists
            parent = conn.execute(
                "SELECT atlas_id FROM project_index WHERE atlas_id = ?",
                (atlas_id,),
            ).fetchone()
            if not parent:
                continue

            existing = conn.execute(
                "SELECT atlas_id FROM project_details WHERE atlas_id = ?",
                (atlas_id,),
            ).fetchone()

            row = {f: d.get(f) for f in _DETAIL_FIELDS}
            row["updated_at"] = now

            if existing:
                set_clause = ", ".join(f"{f} = ?" for f in row)
                conn.execute(
                    f"UPDATE project_details SET {set_clause} WHERE atlas_id = ?",
                    (*row.values(), atlas_id),
                )
                updated += 1
            else:
                fields = ("atlas_id", *row.keys())
                placeholders = ", ".join("?" * len(fields))
                conn.execute(
                    f"INSERT INTO project_details ({', '.join(fields)}) VALUES ({placeholders})",
                    (atlas_id, *row.values()),
                )
                inserted += 1

    return {"inserted": inserted, "updated": updated}


def get_all_project_details(db_path: Path) -> list[dict[str, Any]]:
    """Return a joined view of project_index + project_details for all projects."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            """SELECT i.atlas_id, i.project_url, i.project_name,
                      i.first_seen_at, i.last_seen_at, i.last_stage2_parsed_at,
                      d.project_title, d.project_scope, d.project_body,
                      d.countries_impacted, d.geographical_scale,
                      d.energy_poverty_phase, d.intervention_type,
                      d.professionals_involved, d.partners_involved,
                      d.type_of_funding, d.website, d.parsed_at
               FROM project_index i
               LEFT JOIN project_details d USING (atlas_id)
               ORDER BY i.first_seen_at DESC"""
        ).fetchall()
    return [dict(r) for r in rows]


# ── pipeline_runs ──────────────────────────────────────────────────────────────

def start_run(db_path: Path, stage: int) -> str:
    """Insert a new pipeline_run row and return its run_id."""
    run_id = str(uuid.uuid4())
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT INTO pipeline_runs (run_id, stage, started_at, status) VALUES (?,?,?,'running')",
            (run_id, stage, _now()),
        )
    return run_id


def finish_run(db_path: Path, run_id: str, *, success: bool) -> None:
    status = "success" if success else "failed"
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE pipeline_runs SET finished_at = ?, status = ? WHERE run_id = ?",
            (_now(), status, run_id),
        )


def get_latest_successful_run(db_path: Path, stage: int) -> dict[str, Any] | None:
    """Return the most recent successful run for a given stage, or None."""
    with _connect(db_path) as conn:
        row = conn.execute(
            """SELECT * FROM pipeline_runs
               WHERE stage = ? AND status = 'success'
               ORDER BY finished_at DESC
               LIMIT 1""",
            (stage,),
        ).fetchone()
    return dict(row) if row else None


# ── Pipeline status queries ───────────────────────────────────────────────────

def get_pipeline_status(db_path: Path, stage1_max_age_hours: int = 24) -> dict[str, Any]:
    """
    Return a summary dict for display in the dashboard:
      - last_stage1_run: finished_at of latest successful Stage 1 run
      - last_stage2_run: finished_at of latest successful Stage 2 run
      - next_stage1_due: estimated next Stage 1 run time
      - projects_added_since_last_run: count of projects first_seen after last Stage 1 run
      - new_projects: list of {atlas_id, project_name, project_url} for those projects
    """
    from datetime import timedelta

    result: dict[str, Any] = {
        "last_stage1_run": None,
        "last_stage2_run": None,
        "next_stage1_due": None,
        "projects_added_since_last_run": 0,
        "new_projects": [],
    }

    last_s1 = get_latest_successful_run(db_path, stage=1)
    last_s2 = get_latest_successful_run(db_path, stage=2)

    if last_s1:
        finished = last_s1["finished_at"]
        result["last_stage1_run"] = finished
        result["next_stage1_due"] = (
            datetime.fromisoformat(finished) + timedelta(hours=stage1_max_age_hours)
        ).isoformat()

    if last_s2:
        result["last_stage2_run"] = last_s2["finished_at"]

    # Projects first seen after the last Stage 1 run started
    if last_s1:
        cutoff = last_s1["started_at"]
        with _connect(db_path) as conn:
            rows = conn.execute(
                """SELECT i.atlas_id, i.project_name, i.project_url, i.first_seen_at,
                          d.project_title
                   FROM project_index i
                   LEFT JOIN project_details d USING (atlas_id)
                   WHERE i.first_seen_at >= ?
                   ORDER BY i.first_seen_at DESC""",
                (cutoff,),
            ).fetchall()
        result["new_projects"] = [dict(r) for r in rows]
        result["projects_added_since_last_run"] = len(result["new_projects"])

    return result


# ── Utility ────────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
