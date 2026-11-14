from __future__ import annotations

import sqlite3

from app.config import DATA_DIR, DB_FILENAME

DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / DB_FILENAME


def connect() -> sqlite3.Connection:
    """Open a connection in autocommit mode.

    Services issue explicit ``BEGIN IMMEDIATE`` for read-check-write paths so
    that lease acquisition and fenced commits are serialized by SQLite itself.
    """
    conn = sqlite3.connect(DB_PATH, timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 10000")
    return conn
