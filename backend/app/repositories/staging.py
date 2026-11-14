from __future__ import annotations

import json
from typing import Any

import sqlite3


def save_staging(
    conn: sqlite3.Connection,
    *,
    batch_id: int,
    projection_rows: list[dict[str, Any]],
    applied_log: list[dict[str, Any]],
    counts: dict[str, int],
    staged_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO batch_staging(batch_id, payload_json, staged_at)
        VALUES (?,?,?)
        ON CONFLICT(batch_id) DO UPDATE SET
            payload_json=excluded.payload_json, staged_at=excluded.staged_at
        """,
        (
            batch_id,
            json.dumps(
                {
                    "projection_rows": projection_rows,
                    "applied_log": applied_log,
                    "counts": counts,
                },
                ensure_ascii=False,
            ),
            staged_at,
        ),
    )


def load_staging(conn: sqlite3.Connection, batch_id: int) -> dict[str, Any] | None:
    r = conn.execute(
        "SELECT * FROM batch_staging WHERE batch_id=?", (batch_id,)
    ).fetchone()
    if r is None:
        return None
    data = json.loads(r["payload_json"])
    data["staged_at"] = r["staged_at"]
    return data


def delete_staging(conn: sqlite3.Connection, batch_id: int) -> None:
    conn.execute("DELETE FROM batch_staging WHERE batch_id=?", (batch_id,))
