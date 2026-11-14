from __future__ import annotations

import json
from typing import Any

from app.config import FIXTURES_DIR
from app.db import connect
from app.engines.project import apply_events
from app.repositories import batches as batch_repo
from app.repositories import projections as proj_repo
from app.repositories import staging as staging_repo
from app.repositories import streams as stream_repo
from app.services import lease_service
from app.times import now_iso


def _load_fixture(name: str) -> list[dict[str, Any]]:
    path = FIXTURES_DIR / name
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"fixture {name} must be a JSON array")
    return data


def dashboard_stats() -> dict[str, Any]:
    conn = connect()
    try:
        st = lease_service.get_settings(conn)
        return {
            "stream_count": conn.execute("SELECT COUNT(*) c FROM project_streams").fetchone()["c"],
            "enabled_streams": conn.execute(
                "SELECT COUNT(*) c FROM project_streams WHERE enabled=1"
            ).fetchone()["c"],
            "batch_count": conn.execute("SELECT COUNT(*) c FROM project_batches").fetchone()["c"],
            "pending_batches": conn.execute(
                "SELECT COUNT(*) c FROM project_batches WHERE status='pending'"
            ).fetchone()["c"],
            "preempted_batches": conn.execute(
                "SELECT COUNT(*) c FROM project_batches WHERE status='preempted'"
            ).fetchone()["c"],
            "failed_batches": conn.execute(
                "SELECT COUNT(*) c FROM project_batches WHERE status='failed'"
            ).fetchone()["c"],
            "fenced_batches": conn.execute(
                "SELECT COUNT(*) c FROM project_batches WHERE status='fenced'"
            ).fetchone()["c"],
            "projection_alive": conn.execute(
                "SELECT COUNT(*) c FROM projection_rows WHERE deleted=0"
            ).fetchone()["c"],
            "projection_deleted": conn.execute(
                "SELECT COUNT(*) c FROM projection_rows WHERE deleted=1"
            ).fetchone()["c"],
            "ooo_buffered": conn.execute("SELECT COUNT(*) c FROM ooo_buffer").fetchone()["c"],
            "apply_order": st.get("apply_order", "by_seq"),
            "lease_preempt_policy": lease_service.preempt_policy(conn),
            "lease_default_ttl": lease_service.default_ttl(conn),
            "lease_held_streams": sum(
                1 for row in lease_service.list_leases(conn) if row["held"]
            ),
            "lease_expired_streams": sum(
                1 for row in lease_service.list_leases(conn) if row["expired"]
            ),
        }
    finally:
        conn.close()


def list_streams() -> list[dict[str, Any]]:
    conn = connect()
    try:
        return stream_repo.list_streams(conn)
    finally:
        conn.close()


def list_batches(limit: int = 50) -> list[dict[str, Any]]:
    conn = connect()
    try:
        rows = batch_repo.list_batches(conn, limit=limit)
        streams = {s["id"]: s for s in stream_repo.list_streams(conn)}
        for r in rows:
            s = streams.get(r["stream_id"])
            r["stream_code"] = s["code"] if s else None
            r["stream_title"] = s["title"] if s else None
        return rows
    finally:
        conn.close()


def list_projections(stream_id: int | None = None, include_deleted: bool = True) -> list[dict[str, Any]]:
    conn = connect()
    try:
        rows = proj_repo.list_projections(
            conn, stream_id=stream_id, include_deleted=include_deleted
        )
        streams = {s["id"]: s for s in stream_repo.list_streams(conn)}
        for r in rows:
            s = streams.get(r["stream_id"])
            r["stream_code"] = s["code"] if s else None
            r["stream_title"] = s["title"] if s else None
        return rows
    finally:
        conn.close()


def list_events(batch_id: int | None = None, stream_id: int | None = None) -> list[dict[str, Any]]:
    conn = connect()
    try:
        return batch_repo.list_applied_events(conn, batch_id=batch_id, stream_id=stream_id)
    finally:
        conn.close()


def list_ooo_buffer(stream_id: int | None = None) -> list[dict[str, Any]]:
    conn = connect()
    try:
        return proj_repo.list_ooo_buffer(conn, stream_id=stream_id)
    finally:
        conn.close()


def run_project(
    stream_id: int,
    note: str | None = None,
    *,
    owner: str = "project-worker",
    ttl_seconds: int | None = None,
    actor: str = "api",
) -> dict[str, Any]:
    """Acquire the stream lease, stage a projection, then commit it fenced.

    The projection is computed and staged before the commit so that a run
    between acquisition and commit is visible in ``batch_staging``; only the
    token-fenced commit may replace projection rows and move the watermark.
    """
    conn = connect()
    try:
        stream = stream_repo.get_stream(conn, stream_id)
        if not stream:
            raise lease_service.LeaseError(
                "STREAM_NOT_FOUND", "投影流不存在", status_code=404
            )
        if not stream["enabled"]:
            raise lease_service.LeaseError("STREAM_DISABLED", "投影流已停用", status_code=409)

        events = _load_fixture(stream["fixture"])
        result = apply_events(events, known_event_ids=set())

        lease_row, batch = lease_service.begin_run(
            conn,
            stream_id=stream_id,
            owner=owner,
            ttl_seconds=ttl_seconds,
            actor=actor,
            batch_prefix=now_iso()[:10].replace("-", ""),
        )
        conn.execute("BEGIN IMMEDIATE")
        try:
            staging_repo.save_staging(
                conn,
                batch_id=batch["id"],
                projection_rows=result["projection"],
                applied_log=result["applied_log"],
                counts=result["counts"],
                staged_at=now_iso(),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        committed = lease_service.commit_run(
            conn,
            stream_id=stream_id,
            batch_id=batch["id"],
            lease_token=lease_row["lease_token"],
            projection_rows=result["projection"],
            applied_log=result["applied_log"],
            counts=result["counts"],
            note=note,
            actor=actor,
        )
        conn.execute("DELETE FROM batch_staging WHERE batch_id=?", (batch["id"],))
        conn.commit()

        committed["stream_code"] = stream["code"]
        committed["stream_title"] = stream["title"]
        committed["apply_order"] = "by_seq"
        committed["counts"] = result["counts"]
        committed["lease_token"] = lease_row["lease_token"]
        committed["lease_generation"] = lease_row["generation"]
        committed["projection_preview"] = proj_repo.list_projections(
            conn, stream_id=stream_id
        )[:20]
        committed["event_preview"] = batch_repo.list_applied_events(
            conn, batch_id=batch["id"], limit=20
        )
        return committed
    finally:
        conn.close()
