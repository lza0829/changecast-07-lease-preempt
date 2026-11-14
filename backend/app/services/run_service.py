from __future__ import annotations

from typing import Any

from app.engines.project import apply_events
from app.repositories import batches as batch_repo
from app.repositories import projections as proj_repo
from app.repositories import staging as staging_repo
from app.repositories import streams as stream_repo
from app.services import lease_service
from app.services.lease_service import LeaseError
from app.services.project_service import _load_fixture
from app.times import now_iso


def _stream_or_404(conn, stream_id: int) -> dict[str, Any]:
    stream = stream_repo.get_stream(conn, stream_id)
    if not stream:
        raise LeaseError("STREAM_NOT_FOUND", "投影流不存在", status_code=404)
    return stream


def start_run(
    conn,
    *,
    stream_id: int,
    owner: str,
    ttl_seconds: int | None,
    note: str | None,
    actor: str,
) -> dict[str, Any]:
    """Acquire the lease and leave a *pending* batch behind (crash demo)."""
    stream = _stream_or_404(conn, stream_id)
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
        conn.execute("UPDATE project_batches SET note=? WHERE id=?", (note, batch["id"]))
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "batch": _batch_view(conn, batch["id"]),
        "lease": lease_service.describe(conn, stream_id),
        "lease_token": lease_row["lease_token"],
        "generation": lease_row["generation"],
        "staged_projection_rows": len(result["projection"]),
        "counts": result["counts"],
    }


def commit_staged_run(
    conn,
    *,
    batch_id: int,
    lease_token: str,
    actor: str,
) -> dict[str, Any]:
    batch = batch_repo.get_batch(conn, batch_id)
    if batch is None:
        raise LeaseError("BATCH_NOT_FOUND", "批次不存在", status_code=404)
    staged = staging_repo.load_staging(conn, batch_id)
    if staged is None:
        raise LeaseError(
            "BATCH_NOT_STAGED",
            "该批次没有暂存投影（可能已提交或并非由暂存式开跑产生）",
            status_code=409,
        )
    committed = lease_service.commit_run(
        conn,
        stream_id=batch["stream_id"],
        batch_id=batch_id,
        lease_token=lease_token,
        projection_rows=staged["projection_rows"],
        applied_log=staged["applied_log"],
        counts=staged["counts"],
        note=None,
        actor=actor,
    )
    conn.execute("DELETE FROM batch_staging WHERE batch_id=?", (batch_id,))
    conn.commit()
    committed["counts"] = staged["counts"]
    committed["projection_preview"] = proj_repo.list_projections(
        conn, stream_id=batch["stream_id"]
    )[:20]
    committed["event_preview"] = batch_repo.list_applied_events(
        conn, batch_id=batch_id, limit=20
    )
    return committed


def _batch_view(conn, batch_id: int) -> dict[str, Any]:
    batch = batch_repo.get_batch(conn, batch_id)
    if batch is None:
        raise LeaseError("BATCH_NOT_FOUND", "批次不存在", status_code=404)
    stream = stream_repo.get_stream(conn, batch["stream_id"])
    batch["stream_code"] = stream["code"] if stream else None
    batch["stream_title"] = stream["title"] if stream else None
    staged = staging_repo.load_staging(conn, batch_id)
    batch["staged"] = staged is not None
    batch["staged_counts"] = staged["counts"] if staged else None
    return batch


def run_detail(conn, batch_id: int) -> dict[str, Any]:
    batch = _batch_view(conn, batch_id)
    batch["lease"] = next(
        (r for r in lease_service.list_leases(conn) if r["stream_id"] == batch["stream_id"]),
        None,
    )
    return batch
