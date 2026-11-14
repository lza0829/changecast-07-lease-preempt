from __future__ import annotations

from typing import Any

import sqlite3


def insert_batch(
    conn: sqlite3.Connection,
    *,
    batch_no: str,
    stream_id: int,
    status: str,
    counts: dict[str, int],
    note: str | None,
    created_at: str,
    lease_token: str | None = None,
    lease_generation: int | None = None,
    started_at: str | None = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO project_batches(
            batch_no, stream_id, status, source_events, applied_upsert, applied_delete,
            skipped_duplicate, projected_alive, projected_deleted, max_seq, note, created_at,
            lease_token, lease_generation, started_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            batch_no,
            stream_id,
            status,
            counts["source_events"],
            counts["applied_upsert"],
            counts["applied_delete"],
            counts["skipped_duplicate"],
            counts["projected_alive"],
            counts["projected_deleted"],
            counts["max_seq"],
            note,
            created_at,
            lease_token,
            lease_generation,
            started_at,
        ),
    )
    return int(cur.lastrowid)


def complete_batch(
    conn: sqlite3.Connection,
    *,
    batch_id: int,
    counts: dict[str, int],
    finished_at: str,
) -> None:
    conn.execute(
        """
        UPDATE project_batches
           SET status='completed',
               source_events=?, applied_upsert=?, applied_delete=?, skipped_duplicate=?,
               projected_alive=?, projected_deleted=?, max_seq=?,
               finished_at=?
         WHERE id=?
        """,
        (
            counts["source_events"],
            counts["applied_upsert"],
            counts["applied_delete"],
            counts["skipped_duplicate"],
            counts["projected_alive"],
            counts["projected_deleted"],
            counts["max_seq"],
            finished_at,
            batch_id,
        ),
    )


def void_batch(
    conn: sqlite3.Connection,
    *,
    batch_id: int,
    status: str,
    fence_reason: str,
    finished_at: str,
    supersede_fenced: bool = False,
) -> None:
    """Fence a batch: it may no longer commit projection or watermark.

    ``supersede_fenced`` lets a reclaim/release close a batch that already
    self-fenced on a late commit, appending the new reason instead of losing it.
    """
    allowed = "('pending','fenced')" if supersede_fenced else "('pending')"
    row = conn.execute(
        "SELECT fence_reason FROM project_batches WHERE id=?", (batch_id,)
    ).fetchone()
    if row and row["fence_reason"] and supersede_fenced:
        if fence_reason not in row["fence_reason"]:
            fence_reason = f"{row['fence_reason']}；{fence_reason}"
    conn.execute(
        f"""
        UPDATE project_batches
           SET status=?, fence_reason=?, finished_at=?
         WHERE id=? AND status IN {allowed}
        """,
        (status, fence_reason, finished_at, batch_id),
    )


def list_batches(conn: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM project_batches ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_batch(conn: sqlite3.Connection, batch_id: int) -> dict[str, Any] | None:
    r = conn.execute("SELECT * FROM project_batches WHERE id=?", (batch_id,)).fetchone()
    return dict(r) if r else None


def get_batch_by_no(conn: sqlite3.Connection, batch_no: str) -> dict[str, Any] | None:
    r = conn.execute(
        "SELECT * FROM project_batches WHERE batch_no=?", (batch_no,)
    ).fetchone()
    return dict(r) if r else None


def get_latest_completed(conn: sqlite3.Connection, stream_id: int) -> dict[str, Any] | None:
    r = conn.execute(
        """
        SELECT * FROM project_batches
         WHERE stream_id=? AND status='completed'
         ORDER BY id DESC LIMIT 1
        """,
        (stream_id,),
    ).fetchone()
    return dict(r) if r else None


def count_by_status(conn: sqlite3.Connection, stream_id: int) -> dict[str, int]:
    rows = conn.execute(
        "SELECT status, COUNT(*) c FROM project_batches WHERE stream_id=? GROUP BY status",
        (stream_id,),
    ).fetchall()
    return {r["status"]: int(r["c"]) for r in rows}


def next_batch_no(conn: sqlite3.Connection, date_prefix: str) -> str:
    total = conn.execute("SELECT COUNT(*) c FROM project_batches").fetchone()["c"]
    return f"PB-{date_prefix}-{int(total) + 1:04d}"


def insert_applied_event(
    conn: sqlite3.Connection,
    *,
    batch_id: int,
    stream_id: int,
    event: dict[str, Any],
    created_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO applied_events(
            batch_id, stream_id, event_id, entity_key, op, seq, event_time, result, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            batch_id,
            stream_id,
            event["event_id"],
            event["entity_key"],
            event["op"],
            event["seq"],
            event["event_time"],
            event["result"],
            created_at,
        ),
    )


def list_applied_events(
    conn: sqlite3.Connection,
    *,
    batch_id: int | None = None,
    stream_id: int | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM applied_events WHERE 1=1"
    params: list[Any] = []
    if batch_id is not None:
        sql += " AND batch_id=?"
        params.append(batch_id)
    if stream_id is not None:
        sql += " AND stream_id=?"
        params.append(stream_id)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    return [dict(r) for r in conn.execute(sql, params).fetchall()]
