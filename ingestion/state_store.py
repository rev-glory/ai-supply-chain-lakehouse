import os
import sqlite3
from datetime import UTC, datetime

DATABASE_PATH = 'data/state/state.db'

# Helper Method
def _now():
    return datetime.now(UTC).isoformat()

# Date value checker
def _require_aware(value):
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")

# Initialize database and tables
def connect():
    os.makedirs(os.path.dirname(DATABASE_PATH),exist_ok=True)
    with sqlite3.connect(DATABASE_PATH) as conn :
        conn.execute("""
            CREATE TABLE IF NOT EXISTS watermarks (
                entity          TEXT PRIMARY KEY,
                last_modified   TEXT NOT NULL,
                updated_at      TEXT NOT NULL    
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id      TEXT PRIMARY KEY,
                entity      TEXT NOT NULL,
                started_at  TEXT NOT NULL,
                finished_at TEXT,
                status      TEXT NOT NULL,
                rows_in     INTEGER,
                rows_out    INTEGER
            )
        """)

# Fetch the last modified watermark for an entity
def get_watermark(entity):
    with sqlite3.connect(DATABASE_PATH) as conn:
        result = conn.execute("""
            SELECT last_modified FROM watermarks WHERE entity = ? 
        """, (entity,)).fetchone()
    return datetime.fromisoformat(result[0]) if result else None

# Set the watermark for the entity on successful run
def set_watermark(entity, value):
    _require_aware(value)
    current = get_watermark(entity)

    if current is not None and current >= value:
        return
    
    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.execute("""
            INSERT INTO watermarks (entity, last_modified, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(entity) DO UPDATE SET
                last_modified = excluded.last_modified,
                updated_at    = excluded.updated_at
        """, (entity, value.isoformat(), _now()))

# Create a new run record and store as running in database
def start_run(run_id, entity):
    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.execute("""
            INSERT INTO runs(run_id, entity, started_at, status) VALUES (? , ? , ?, ?) 
        """, (run_id, entity, _now(), "running"))

# Mark run as success or failed
def finish_run(run_id, status, rows_in, rows_out):
    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.execute("""
            UPDATE runs 
            SET status = ?, rows_in = ?, rows_out = ?, finished_at = ?
            WHERE run_id = ? 
        """, (status, rows_in, rows_out, _now(), run_id))