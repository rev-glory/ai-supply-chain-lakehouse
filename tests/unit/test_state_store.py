import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from ingestion import state_store


@pytest.fixture
def db(tmp_path, monkeypatch):
    """Point the state store at a temporary database for each test."""
    monkeypatch.setattr(state_store, "DATABASE_PATH", str(tmp_path / "state.db"))
    state_store.connect()
    return tmp_path / "state.db"

def utc(*args):
    return datetime(*args, tzinfo=UTC)

def test_fresh_database_has_no_watermark(db):
    assert state_store.get_watermark("hf_models") is None

def test_set_then_get_returns_same_value(db):
    value = utc(2026, 11, 20, 10, 15)
    state_store.set_watermark("hf_models", value)
    assert state_store.get_watermark("hf_models") == value

def test_watermark_never_moves_backward(db):
    newer = utc(2026, 11, 20, 10, 15)
    older = newer - timedelta(hours=1)
    state_store.set_watermark("hf_models", newer)
    state_store.set_watermark("hf_models", older)
    assert state_store.get_watermark("hf_models") == newer

def test_watermark_moves_forward(db):
    first = utc(2026, 11, 20, 10, 15)
    second = first + timedelta(minutes=5)
    state_store.set_watermark("hf_models", first)
    state_store.set_watermark("hf_models", second)
    assert state_store.get_watermark("hf_models") == second

def test_naive_datetime_is_rejected(db):
    with pytest.raises(ValueError):
        state_store.set_watermark("hf_models", datetime(2026, 11, 20, 10, 15))

def test_entities_are_independent(db):
    models_value = utc(2026, 11, 20)
    state_store.set_watermark("hf_models", models_value)
    assert state_store.get_watermark("hf_datasets") is None

def test_run_is_recorded_with_status_and_counts(db):
    state_store.start_run("run-1", "hf_models")
    state_store.finish_run("run-1", "success", rows_in=20, rows_out=40)

    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT entity, status, rows_in, rows_out, finished_at "
            "FROM runs WHERE run_id = ?", ("run-1",)
        ).fetchone()

    assert row[0] == "hf_models"
    assert row[1] == "success"
    assert row[2] == 20
    assert row[3] == 40
    assert row[4] is not None

def test_started_run_has_running_status(db):
    state_store.start_run("run-2", "hf_models")

    with sqlite3.connect(db) as conn:
        status = conn.execute(
            "SELECT status FROM runs WHERE run_id = ?", ("run-2",)
        ).fetchone()[0]

    assert status == "running"

# pytest tests/unit -v
# ruff check .