from __future__ import annotations

import sqlite3
from typing import Any


def _row(r: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(r) if r else None


def get_lease_row(conn: sqlite3.Connection, stream_id: int) -> dict[str, Any] | None:
    return _row(
        conn.execute("SELECT * FROM stream_leases WHERE stream_id=?", (stream_id,)).fetchone()
    )


def list_lease_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [
        dict(r)
        for r in conn.execute(
            "SELECT * FROM stream_leases ORDER BY stream_id"
        ).fetchall()
    ]


def upsert_lease(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    lease_token: str,
    owner: str,
    batch_no: str | None,
    generation: int,
    state: str,
    ttl_seconds: int,
    acquired_at: str,
    expires_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO stream_leases(
            stream_id, lease_token, owner, batch_no, generation, state, ttl_seconds,
            acquired_at, last_heartbeat_at, expires_at, ended_at, end_reason
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(stream_id) DO UPDATE SET
            lease_token=excluded.lease_token,
            owner=excluded.owner,
            batch_no=excluded.batch_no,
            generation=excluded.generation,
            state=excluded.state,
            ttl_seconds=excluded.ttl_seconds,
            acquired_at=excluded.acquired_at,
            last_heartbeat_at=excluded.last_heartbeat_at,
            expires_at=excluded.expires_at,
            ended_at=excluded.ended_at,
            end_reason=excluded.end_reason
        """,
        (
            stream_id,
            lease_token,
            owner,
            batch_no,
            generation,
            state,
            ttl_seconds,
            acquired_at,
            acquired_at,
            expires_at,
            None,
            None,
        ),
    )


def update_lease_expiry(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    expires_at: str,
    heartbeat_at: str,
    ttl_seconds: int,
) -> None:
    conn.execute(
        """
        UPDATE stream_leases
           SET expires_at=?, last_heartbeat_at=?, ttl_seconds=?
         WHERE stream_id=?
        """,
        (expires_at, heartbeat_at, ttl_seconds, stream_id),
    )


def end_lease(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    state: str,
    ended_at: str,
    end_reason: str,
) -> None:
    conn.execute(
        """
        UPDATE stream_leases
           SET state=?, ended_at=?, end_reason=?
         WHERE stream_id=?
        """,
        (state, ended_at, end_reason, stream_id),
    )


def insert_lease_event(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    kind: str,
    batch_no: str | None,
    lease_token: str | None,
    generation: int | None,
    actor: str | None,
    reason: str | None,
    detail: str | None,
    created_at: str,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO lease_events(
            stream_id, kind, batch_no, lease_token, generation, actor, reason, detail, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            stream_id,
            kind,
            batch_no,
            lease_token,
            generation,
            actor,
            reason,
            detail,
            created_at,
        ),
    )
    return int(cur.lastrowid)


def list_lease_events(
    conn: sqlite3.Connection, *, stream_id: int | None = None, limit: int = 200
) -> list[dict[str, Any]]:
    if stream_id is None:
        rows = conn.execute(
            "SELECT * FROM lease_events ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM lease_events WHERE stream_id=? ORDER BY id DESC LIMIT ?",
            (stream_id, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def count_events(conn: sqlite3.Connection, stream_id: int) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT kind, COUNT(*) c
          FROM lease_events
         WHERE stream_id=?
         GROUP BY kind
        """,
        (stream_id,),
    ).fetchall()
    return {r["kind"]: int(r["c"]) for r in rows}


def lease_events_for_verification(conn: sqlite3.Connection) -> dict[int, dict[str, int]]:
    out: dict[int, dict[str, int]] = {}
    for r in conn.execute(
        "SELECT stream_id, kind, COUNT(*) c FROM lease_events GROUP BY stream_id, kind"
    ).fetchall():
        out.setdefault(int(r["stream_id"]), {})[str(r["kind"])] = int(r["c"])
    return out
