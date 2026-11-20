"""SQLite schema and transaction helper, persisted on a Docker volume."""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def db_session(db_path: str):
    conn = sqlite3.connect(db_path, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=15000")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: str, worker_urls: dict[str, str]):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with db_session(db_path) as db:
        db.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS workers (
                worker_id TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                last_seen REAL NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS objects (
                object_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                size INTEGER NOT NULL,
                content_type TEXT NOT NULL,
                checksum TEXT NOT NULL,
                created_at TEXT NOT NULL,
                modified_at TEXT NOT NULL,
                custom_metadata TEXT NOT NULL DEFAULT '{}'
            );
            CREATE TABLE IF NOT EXISTS chunks (
                chunk_id TEXT PRIMARY KEY,
                object_id TEXT NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
                chunk_index INTEGER NOT NULL,
                size INTEGER NOT NULL,
                checksum TEXT NOT NULL,
                UNIQUE(object_id, chunk_index)
            );
            CREATE TABLE IF NOT EXISTS replicas (
                chunk_id TEXT NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
                worker_id TEXT NOT NULL REFERENCES workers(worker_id),
                PRIMARY KEY(chunk_id, worker_id)
            );
            CREATE TABLE IF NOT EXISTS pending_deletions (
                chunk_id TEXT NOT NULL,
                worker_id TEXT NOT NULL REFERENCES workers(worker_id),
                PRIMARY KEY(chunk_id, worker_id)
            );
            CREATE INDEX IF NOT EXISTS idx_chunks_object ON chunks(object_id);
            CREATE INDEX IF NOT EXISTS idx_replicas_worker ON replicas(worker_id);
        """)
        for worker_id, url in worker_urls.items():
            db.execute("INSERT INTO workers(worker_id,url) VALUES(?,?) "
                       "ON CONFLICT(worker_id) DO UPDATE SET url=excluded.url",
                       (worker_id, url.rstrip("/")))
