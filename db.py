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
            "('project_index','pipeline_runs');"
        ).fetchall()
    return len(rows) == 2


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


# ── Utility ────────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
