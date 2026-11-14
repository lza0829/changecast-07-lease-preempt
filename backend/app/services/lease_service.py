"""Stream-level projection leases with expiry, heartbeat and fencing.

A projection run must hold the stream lease to touch projection rows or the
stream watermark. The lease carries a token that doubles as a fencing token:
once the lease is preempted, released or reclaimed, the old holder's commit is
rejected and the projection is left untouched.
"""

from __future__ import annotations

import time
from typing import Any

import sqlite3

from app.repositories import batches as batch_repo
from app.repositories import leases as lease_repo
from app.repositories import projections as proj_repo
from app.repositories import streams as stream_repo
from app.times import add_seconds, iso, now_dt, now_iso, parse_iso

POLICY_ALLOW = "allow_preempt"
POLICY_REFUSE = "refuse_preempt"
POLICIES = (POLICY_ALLOW, POLICY_REFUSE)

MIN_TTL_SECONDS = 2
MAX_TTL_SECONDS = 3600
MAX_WORK_MS = 20000

EVENT_KINDS = (
    "acquired",
    "rejected",
    "heartbeat",
    "preempt",
    "reclaim",
    "released",
    "completed",
    "fenced_commit",
)


class LeaseError(Exception):
    """A lease rule was violated. ``code`` is stable for API consumers."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status_code: int = 409,
        detail: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.detail = detail or {}

    def as_detail(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, **self.detail}


def _clamp(value: int, low: int, high: int) -> int:
    return max(low, min(high, int(value)))


def get_settings(conn: sqlite3.Connection) -> dict[str, str]:
    rows = conn.execute("SELECT key, value FROM settings").fetchall()
    return {r["key"]: r["value"] for r in rows}


def set_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO settings(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def preempt_policy(conn: sqlite3.Connection) -> str:
    policy = get_settings(conn).get("lease_preempt_policy", POLICY_REFUSE)
    return policy if policy in POLICIES else POLICY_REFUSE


def default_ttl(conn: sqlite3.Connection) -> int:
    raw = get_settings(conn).get("lease_default_ttl", "120")
    try:
        return _clamp(int(raw), MIN_TTL_SECONDS, MAX_TTL_SECONDS)
    except ValueError:
        return 120


def ttl_for(conn: sqlite3.Connection, requested: int | None) -> int:
    if requested is None:
        return default_ttl(conn)
    return _clamp(int(requested), MIN_TTL_SECONDS, MAX_TTL_SECONDS)


def _lease_state(row: dict[str, Any] | None, at=None) -> str:
    if row is None:
        return "free"
    at = at or now_dt()
    if row["state"] == "active":
        return "expired" if parse_iso(row["expires_at"]) <= at else "active"
    return str(row["state"])


def _counters(conn: sqlite3.Connection, stream_id: int) -> dict[str, int]:
    raw = lease_repo.count_events(conn, stream_id)
    return {
        "acquire_count": raw.get("acquired", 0),
        "reject_count": raw.get("rejected", 0),
        "heartbeat_count": raw.get("heartbeat", 0),
        "preempt_count": raw.get("preempt", 0),
        "reclaim_count": raw.get("reclaim", 0),
        "release_count": raw.get("released", 0),
        "complete_count": raw.get("completed", 0),
        "fence_count": raw.get("fenced_commit", 0),
    }


def _serialize(row: dict[str, Any] | None, *, policy: str, ttl: int) -> dict[str, Any]:
    at = now_dt()
    state = _lease_state(row, at)
    out: dict[str, Any] = {
        "state": state,
        "held": state == "active",
        "expired": state == "expired",
        "preempt_policy": policy,
        "default_ttl_seconds": ttl,
        "now": iso(at),
    }
    if row is None:
        out.update(
            lease_token=None,
            owner=None,
            batch_no=None,
            generation=None,
            ttl_seconds=None,
            acquired_at=None,
            last_heartbeat_at=None,
            expires_at=None,
            remaining_seconds=None,
            ended_at=None,
            end_reason=None,
        )
        return out
    remaining = 0.0
    if row["state"] == "active":
        remaining = max(0.0, (parse_iso(row["expires_at"]) - at).total_seconds())
    out.update(
        lease_token=row["lease_token"],
        owner=row["owner"],
        batch_no=row["batch_no"],
        generation=row["generation"],
        ttl_seconds=row["ttl_seconds"],
        acquired_at=row["acquired_at"],
        last_heartbeat_at=row["last_heartbeat_at"],
        expires_at=row["expires_at"],
        remaining_seconds=round(remaining, 3),
        ended_at=row["ended_at"],
        end_reason=row["end_reason"],
    )
    return out


def describe(conn: sqlite3.Connection, stream_id: int) -> dict[str, Any]:
    stream = stream_repo.get_stream(conn, stream_id)
    if not stream:
        raise LeaseError("STREAM_NOT_FOUND", "投影流不存在", status_code=404)
    row = lease_repo.get_lease_row(conn, stream_id)
    data = _serialize(row, policy=preempt_policy(conn), ttl=default_ttl(conn))
    data["stream_id"] = stream_id
    data["stream_code"] = stream["code"]
    data["stream_title"] = stream["title"]
    data["watermark_seq"] = stream["watermark_seq"]
    data["counters"] = _counters(conn, stream_id)
    latest = batch_repo.get_latest_completed(conn, stream_id)
    data["last_completed_batch"] = (
        {
            "batch_no": latest["batch_no"],
            "max_seq": latest["max_seq"],
            "projected_alive": latest["projected_alive"],
            "projected_deleted": latest["projected_deleted"],
            "finished_at": latest["finished_at"],
        }
        if latest
        else None
    )
    return data


def list_leases(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    policy = preempt_policy(conn)
    ttl = default_ttl(conn)
    rows = {r["stream_id"]: r for r in lease_repo.list_lease_rows(conn)}
    out: list[dict[str, Any]] = []
    for stream in stream_repo.list_streams(conn):
        data = _serialize(rows.get(stream["id"]), policy=policy, ttl=ttl)
        data.update(
            stream_id=stream["id"],
            stream_code=stream["code"],
            stream_title=stream["title"],
            watermark_seq=stream["watermark_seq"],
            counters=_counters(conn, stream["id"]),
        )
        latest = batch_repo.get_latest_completed(conn, stream["id"])
        data["last_completed_batch_no"] = latest["batch_no"] if latest else None
        out.append(data)
    return out


def list_lease_events(
    conn: sqlite3.Connection, *, stream_id: int | None = None, limit: int = 200
) -> list[dict[str, Any]]:
    return lease_repo.list_lease_events(conn, stream_id=stream_id, limit=limit)


def _void_pending_batches(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    status: str,
    fence_reason: str,
    finished_at: str,
    only_batch_no: str | None = None,
    include_fenced: bool = False,
) -> list[str]:
    states = "('pending','fenced')" if include_fenced else "('pending')"
    sql = f"SELECT id, batch_no FROM project_batches WHERE stream_id=? AND status IN {states}"
    params: list[Any] = [stream_id]
    if only_batch_no:
        sql += " AND batch_no=?"
        params.append(only_batch_no)
    voided: list[str] = []
    for r in conn.execute(sql, params).fetchall():
        batch_repo.void_batch(
            conn,
            batch_id=r["id"],
            status=status,
            fence_reason=fence_reason,
            finished_at=finished_at,
            supersede_fenced=include_fenced,
        )
        voided.append(r["batch_no"])
    return voided


def _log(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    kind: str,
    batch_no: str | None = None,
    lease_token: str | None = None,
    generation: int | None = None,
    actor: str | None = None,
    reason: str | None = None,
    detail: str | None = None,
) -> None:
    lease_repo.insert_lease_event(
        conn,
        stream_id=stream_id,
        kind=kind,
        batch_no=batch_no,
        lease_token=lease_token,
        generation=generation,
        actor=actor,
        reason=reason,
        detail=detail,
        created_at=now_iso(),
    )


def _new_token(stream_id: int, generation: int) -> str:
    return f"L{stream_id}-g{generation}-{time.time_ns()}"


def _acquire_locked(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    owner: str,
    ttl_seconds: int,
    actor: str,
    pending_batch_no: str | None,
) -> dict[str, Any]:
    """Must be called inside ``BEGIN IMMEDIATE``; the caller commits."""
    stream = stream_repo.get_stream(conn, stream_id)
    if not stream:
        raise LeaseError("STREAM_NOT_FOUND", "投影流不存在", status_code=404)
    if not stream["enabled"]:
        raise LeaseError("STREAM_DISABLED", "投影流已停用", status_code=409)

    at = now_dt()
    row = lease_repo.get_lease_row(conn, stream_id)
    policy = preempt_policy(conn)

    if row is not None and row["state"] == "active":
        if parse_iso(row["expires_at"]) > at:
            _log(
                conn,
                stream_id=stream_id,
                kind="rejected",
                # The refused run never creates a batch; log the lease it hit.
                batch_no=row["batch_no"],
                lease_token=row["lease_token"],
                generation=row["generation"],
                actor=actor,
                reason="lease_held",
                detail=(
                    f"租约仍被 {row['owner']} 持有，到期于 {row['expires_at']}；"
                    f"本次开跑未建批次"
                ),
            )
            raise LeaseError(
                "LEASE_HELD",
                "同流租约仍在有效期内，拒绝本次开跑",
                detail={"lease": _serialize(row, policy=policy, ttl=default_ttl(conn))},
            )

        # Expired while still marked active.
        if policy == POLICY_REFUSE:
            _log(
                conn,
                stream_id=stream_id,
                kind="rejected",
                batch_no=row["batch_no"],
                lease_token=row["lease_token"],
                generation=row["generation"],
                actor=actor,
                reason="expired_refuse_preempt",
                detail="租约已过期，但策略禁止抢占；需显式释放或回收（本次开跑未建批次）",
            )
            raise LeaseError(
                "LEASE_EXPIRED_REFUSED",
                "租约已过期，但当前策略禁止抢占；请先显式释放或回收旧批",
                detail={"lease": _serialize(row, policy=policy, ttl=default_ttl(conn))},
            )

        # allow_preempt: invalidate the old batch and its lease generation.
        voided = _void_pending_batches(
            conn,
            stream_id=stream_id,
            status="preempted",
            fence_reason="租约过期后被新开跑抢占，旧批不得再提交投影或水位",
            only_batch_no=row["batch_no"],
            finished_at=iso(at),
            include_fenced=True,
        )
        _log(
            conn,
            stream_id=stream_id,
            kind="preempt",
            batch_no=row["batch_no"],
            lease_token=row["lease_token"],
            generation=row["generation"],
            actor=actor,
            reason="lease_expired_preempted",
            detail=(
                f"旧租约 {row['lease_token']} 过期于 {row['expires_at']}，"
                f"作废旧批 {', '.join(voided) if voided else row['batch_no'] or '(未开批次)'}"
            ),
        )
        lease_repo.end_lease(
            conn,
            stream_id=stream_id,
            state="preempted",
            ended_at=iso(at),
            end_reason="租约过期被抢占",
        )

    generation = int(row["generation"]) + 1 if row else 1
    token = _new_token(stream_id, generation)
    expires_at = iso(add_seconds(at, ttl_seconds))
    lease_repo.upsert_lease(
        conn,
        stream_id=stream_id,
        lease_token=token,
        owner=owner,
        batch_no=pending_batch_no,
        generation=generation,
        state="active",
        ttl_seconds=ttl_seconds,
        acquired_at=iso(at),
        expires_at=expires_at,
    )
    _log(
        conn,
        stream_id=stream_id,
        kind="acquired",
        batch_no=pending_batch_no,
        lease_token=token,
        generation=generation,
        actor=actor,
        reason="acquired",
        detail=f"领取租约 {ttl_seconds}s，到期于 {expires_at}",
    )
    return lease_repo.get_lease_row(conn, stream_id) or {}


def acquire(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    owner: str,
    ttl_seconds: int | None,
    actor: str,
) -> dict[str, Any]:
    ttl = ttl_for(conn, ttl_seconds)
    conn.execute("BEGIN IMMEDIATE")
    try:
        _acquire_locked(
            conn,
            stream_id=stream_id,
            owner=owner,
            ttl_seconds=ttl,
            actor=actor,
            pending_batch_no=None,
        )
        conn.commit()
    except LeaseError:
        # The rejection event was written before the raise; keep it.
        conn.commit()
        raise
    except Exception:
        conn.rollback()
        raise
    return describe(conn, stream_id)


def heartbeat(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    lease_token: str,
    ttl_seconds: int | None,
    actor: str,
) -> dict[str, Any]:
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = lease_repo.get_lease_row(conn, stream_id)
        at = now_dt()
        if row is None or row["state"] != "active":
            raise LeaseError("LEASE_NOT_HELD", "该投影流当前没有有效租约")
        if row["lease_token"] != lease_token:
            raise LeaseError(
                "LEASE_TOKEN_MISMATCH",
                "租约令牌不匹配，当前租约已被他人持有或已换代",
                detail={"lease": _serialize(row, policy=preempt_policy(conn), ttl=default_ttl(conn))},
            )
        if parse_iso(row["expires_at"]) <= at:
            _log(
                conn,
                stream_id=stream_id,
                kind="rejected",
                batch_no=row["batch_no"],
                lease_token=lease_token,
                generation=row["generation"],
                actor=actor,
                reason="heartbeat_on_expired",
                detail="租约已过期，心跳续租被拒",
            )
            conn.commit()
            raise LeaseError(
                "LEASE_EXPIRED",
                "租约已过期，无法续租；请按策略抢占或回收",
                detail={"lease": _serialize(row, policy=preempt_policy(conn), ttl=default_ttl(conn))},
            )
        ttl = ttl_for(conn, ttl_seconds if ttl_seconds is not None else row["ttl_seconds"])
        expires_at = iso(add_seconds(at, ttl))
        lease_repo.update_lease_expiry(
            conn,
            stream_id=stream_id,
            expires_at=expires_at,
            heartbeat_at=iso(at),
            ttl_seconds=ttl,
        )
        _log(
            conn,
            stream_id=stream_id,
            kind="heartbeat",
            batch_no=row["batch_no"],
            lease_token=lease_token,
            generation=row["generation"],
            actor=actor,
            reason="heartbeat",
            detail=f"续租至 {expires_at}",
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    fresh = lease_repo.get_lease_row(conn, stream_id) or {}
    return describe(conn, stream_id) if fresh else _serialize(
        fresh, policy=preempt_policy(conn), ttl=default_ttl(conn)
    )


def release(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    lease_token: str | None,
    reason: str | None,
    actor: str,
) -> dict[str, Any]:
    """Explicitly give up a lease; any pending batch is marked failed."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        stream = stream_repo.get_stream(conn, stream_id)
        if not stream:
            raise LeaseError("STREAM_NOT_FOUND", "投影流不存在", status_code=404)
        row = lease_repo.get_lease_row(conn, stream_id)
        at = now_dt()
        if row is None or row["state"] != "active":
            raise LeaseError("LEASE_NOT_HELD", "该投影流当前没有有效租约可释放")
        if lease_token and row["lease_token"] != lease_token:
            raise LeaseError("LEASE_TOKEN_MISMATCH", "租约令牌不匹配，未释放")
        voided = _void_pending_batches(
            conn,
            stream_id=stream_id,
            status="failed",
            fence_reason="显式释放租约，旧批标失败，不得再提交投影或水位",
            only_batch_no=row["batch_no"],
            finished_at=iso(at),
            include_fenced=True,
        )
        lease_repo.end_lease(
            conn,
            stream_id=stream_id,
            state="released",
            ended_at=iso(at),
            end_reason=reason or "显式释放",
        )
        _log(
            conn,
            stream_id=stream_id,
            kind="released",
            batch_no=row["batch_no"],
            lease_token=row["lease_token"],
            generation=row["generation"],
            actor=actor,
            reason=reason or "explicit_release",
            detail=(
                f"释放租约 {row['lease_token']}"
                + (f"，旧批 {', '.join(voided)} 标失败" if voided else "")
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return describe(conn, stream_id)


def reclaim(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    reason: str | None,
    actor: str,
) -> dict[str, Any]:
    """Reclaim an expired lease, marking its pending batch failed."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        stream = stream_repo.get_stream(conn, stream_id)
        if not stream:
            raise LeaseError("STREAM_NOT_FOUND", "投影流不存在", status_code=404)
        row = lease_repo.get_lease_row(conn, stream_id)
        at = now_dt()
        if row is None or row["state"] != "active":
            raise LeaseError("LEASE_NOT_HELD", "该投影流当前没有有效租约可回收")
        if parse_iso(row["expires_at"]) > at:
            raise LeaseError(
                "LEASE_NOT_EXPIRED",
                "租约尚未过期，不能回收；请在过期后重试或等待其提交",
                detail={"lease": _serialize(row, policy=preempt_policy(conn), ttl=default_ttl(conn))},
            )
        voided = _void_pending_batches(
            conn,
            stream_id=stream_id,
            status="failed",
            fence_reason="回收过期租约，旧批标失败，不得再提交投影或水位",
            only_batch_no=row["batch_no"],
            finished_at=iso(at),
            include_fenced=True,
        )
        lease_repo.end_lease(
            conn,
            stream_id=stream_id,
            state="reclaimed",
            ended_at=iso(at),
            end_reason=reason or "回收过期租约",
        )
        _log(
            conn,
            stream_id=stream_id,
            kind="reclaim",
            batch_no=row["batch_no"],
            lease_token=row["lease_token"],
            generation=row["generation"],
            actor=actor,
            reason=reason or "expired_reclaim",
            detail=(
                f"回收过期租约 {row['lease_token']}"
                + (f"，旧批 {', '.join(voided)} 标失败" if voided else "")
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return describe(conn, stream_id)


def begin_run(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    owner: str,
    ttl_seconds: int | None,
    actor: str,
    batch_prefix: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Acquire the lease and materialize a pending batch in one transaction."""
    ttl = ttl_for(conn, ttl_seconds)
    conn.execute("BEGIN IMMEDIATE")
    try:
        stream = stream_repo.get_stream(conn, stream_id)
        if not stream:
            raise LeaseError("STREAM_NOT_FOUND", "投影流不存在", status_code=404)
        batch_no = batch_repo.next_batch_no(conn, batch_prefix)
        lease_row = _acquire_locked(
            conn,
            stream_id=stream_id,
            owner=owner,
            ttl_seconds=ttl,
            actor=actor,
            pending_batch_no=batch_no,
        )
        batch_id = batch_repo.insert_batch(
            conn,
            batch_no=batch_no,
            stream_id=stream_id,
            status="pending",
            counts={
                "source_events": 0,
                "applied_upsert": 0,
                "applied_delete": 0,
                "skipped_duplicate": 0,
                "projected_alive": 0,
                "projected_deleted": 0,
                "max_seq": 0,
            },
            note=None,
            created_at=iso(now_dt()),
            lease_token=lease_row["lease_token"],
            lease_generation=lease_row["generation"],
            started_at=lease_row["acquired_at"],
        )
        # Keep batch_no on the lease row for observability.
        conn.execute(
            "UPDATE stream_leases SET batch_no=? WHERE stream_id=?", (batch_no, stream_id)
        )
        conn.commit()
    except LeaseError:
        # Rejection accounting (e.g. rejected log) must survive the refusal.
        conn.commit()
        raise
    except Exception:
        conn.rollback()
        raise
    return lease_row, batch_repo.get_batch(conn, batch_id) or {}


def commit_run(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    batch_id: int,
    lease_token: str,
    projection_rows: list[dict[str, Any]],
    applied_log: list[dict[str, Any]],
    counts: dict[str, int],
    note: str | None,
    actor: str,
) -> dict[str, Any]:
    """Commit a run only if the lease token still fences this batch.

    Raises :class:`LeaseError` with code ``FENCED`` when the batch was preempted,
    reclaimed, released or its lease expired. The projection is not touched.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        at = now_dt()
        batch = batch_repo.get_batch(conn, batch_id)
        if batch is None:
            raise LeaseError("BATCH_NOT_FOUND", "批次不存在", status_code=404)
        lease_row = lease_repo.get_lease_row(conn, stream_id)

        def fence(reason: str, status: str = "fenced") -> LeaseError:
            batch_repo.void_batch(
                conn,
                batch_id=batch_id,
                status=status,
                fence_reason=reason,
                finished_at=iso(at),
            )
            _log(
                conn,
                stream_id=stream_id,
                kind="fenced_commit",
                batch_no=batch["batch_no"],
                lease_token=lease_token,
                generation=batch.get("lease_generation"),
                actor=actor,
                reason="fenced_commit",
                detail=reason,
            )
            conn.commit()
            return LeaseError(
                "FENCED",
                "旧批提交被租约栅栏挡下，投影与水位未改动",
                detail={
                    "batch_no": batch["batch_no"],
                    "batch_status": status,
                    "fence_reason": reason,
                },
            )

        if batch["status"] != "pending":
            raise fence(
                f"批次状态已是 {batch['status']}，提交被拒",
                status=batch["status"],
            )
        if lease_row is None or lease_row["state"] != "active":
            raise fence("租约已结束，提交被拒")
        if lease_row["lease_token"] != lease_token:
            raise fence("租约令牌已换代，提交被拒")
        if parse_iso(lease_row["expires_at"]) <= at:
            raise fence("租约已过期，提交被拒")

        proj_repo.replace_stream_projection(
            conn, stream_id=stream_id, rows=projection_rows, updated_at=iso(at)
        )
        for ev in applied_log:
            batch_repo.insert_applied_event(
                conn, batch_id=batch_id, stream_id=stream_id, event=ev, created_at=iso(at)
            )
        batch_repo.complete_batch(
            conn, batch_id=batch_id, counts=counts, finished_at=iso(at)
        )
        if note is not None:
            conn.execute("UPDATE project_batches SET note=? WHERE id=?", (note, batch_id))
        stream_repo.set_watermark(conn, stream_id, counts["max_seq"])
        lease_repo.end_lease(
            conn,
            stream_id=stream_id,
            state="released",
            ended_at=iso(at),
            end_reason="投影提交完成",
        )
        _log(
            conn,
            stream_id=stream_id,
            kind="completed",
            batch_no=batch["batch_no"],
            lease_token=lease_token,
            generation=lease_row["generation"],
            actor=actor,
            reason="projection_committed",
            detail=(
                f"提交投影：存活 {counts['projected_alive']} / 已删 "
                f"{counts['projected_deleted']}，水位 seq={counts['max_seq']}"
            ),
        )
        conn.commit()
    except LeaseError:
        raise
    except Exception:
        conn.rollback()
        raise
    return batch_repo.get_batch(conn, batch_id) or {}


def audit(conn: sqlite3.Connection) -> dict[str, Any]:
    """Cross-check lease bookkeeping against batches, projection and watermark."""
    checks: list[dict[str, Any]] = []
    streams = stream_repo.list_streams(conn)

    for stream in streams:
        sid = stream["id"]
        latest = batch_repo.get_latest_completed(conn, sid)
        proj_rows = proj_repo.list_projections(conn, stream_id=sid)
        alive = sum(1 for r in proj_rows if not r["deleted"])
        deleted = sum(1 for r in proj_rows if r["deleted"])

        if latest:
            checks.append(
                {
                    "stream_id": sid,
                    "stream_code": stream["code"],
                    "check": "projection_counts_match_latest_batch",
                    "ok": alive == latest["projected_alive"]
                    and deleted == latest["projected_deleted"],
                    "detail": (
                        f"投影 存活{alive}/已删{deleted} vs 批次 "
                        f"{latest['batch_no']} 存活{latest['projected_alive']}/"
                        f"已删{latest['projected_deleted']}"
                    ),
                }
            )
            checks.append(
                {
                    "stream_id": sid,
                    "stream_code": stream["code"],
                    "check": "watermark_matches_latest_batch",
                    "ok": int(stream["watermark_seq"]) == int(latest["max_seq"]),
                    "detail": (
                        f"水位 {stream['watermark_seq']} vs 批次 {latest['batch_no']} "
                        f"max_seq {latest['max_seq']}"
                    ),
                }
            )
        else:
            checks.append(
                {
                    "stream_id": sid,
                    "stream_code": stream["code"],
                    "check": "watermark_matches_latest_batch",
                    "ok": int(stream["watermark_seq"]) == 0 and alive + deleted == 0,
                    "detail": "无完成批次时水位与投影应为空",
                }
            )

        backing = conn.execute(
            """
            SELECT COUNT(*) c FROM projection_rows p
             WHERE p.stream_id=?
               AND NOT EXISTS (
                   SELECT 1 FROM applied_events e
                    WHERE e.stream_id=p.stream_id AND e.event_id=p.applied_event_id
               )
            """,
            (sid,),
        ).fetchone()["c"]
        checks.append(
            {
                "stream_id": sid,
                "stream_code": stream["code"],
                "check": "projection_rows_backed_by_events",
                "ok": int(backing) == 0,
                "detail": f"{backing} 行投影找不到对应事件留痕",
            }
        )

        bad_events = conn.execute(
            """
            SELECT COUNT(*) c FROM applied_events e
              JOIN project_batches b ON b.id=e.batch_id
             WHERE e.stream_id=? AND b.status!='completed'
            """,
            (sid,),
        ).fetchone()["c"]
        checks.append(
            {
                "stream_id": sid,
                "stream_code": stream["code"],
                "check": "applied_events_only_from_completed_batches",
                "ok": int(bad_events) == 0,
                "detail": f"{bad_events} 条留痕挂在未完成批次上",
            }
        )

        missing_fence = conn.execute(
            """
            SELECT COUNT(*) c FROM project_batches
             WHERE stream_id=? AND status IN ('preempted','failed','fenced')
               AND (fence_reason IS NULL OR fence_reason='')
            """,
            (sid,),
        ).fetchone()["c"]
        checks.append(
            {
                "stream_id": sid,
                "stream_code": stream["code"],
                "check": "voided_batches_have_fence_reason",
                "ok": int(missing_fence) == 0,
                "detail": f"{missing_fence} 个作废批次缺少栅栏原因",
            }
        )

        preempt_events = conn.execute(
            "SELECT COUNT(*) c FROM lease_events WHERE stream_id=? AND kind='preempt'",
            (sid,),
        ).fetchone()["c"]
        preempted_batches = conn.execute(
            "SELECT COUNT(*) c FROM project_batches WHERE stream_id=? AND status='preempted'",
            (sid,),
        ).fetchone()["c"]
        checks.append(
            {
                "stream_id": sid,
                "stream_code": stream["code"],
                "check": "preempt_events_cover_preempted_batches",
                "ok": int(preempt_events) >= int(preempted_batches),
                "detail": f"抢占事件 {preempt_events} vs 被抢占批次 {preempted_batches}",
            }
        )

        lease_row = lease_repo.get_lease_row(conn, sid)
        state = _lease_state(lease_row)
        pending = conn.execute(
            "SELECT COUNT(*) c FROM project_batches WHERE stream_id=? AND status='pending'",
            (sid,),
        ).fetchone()["c"]
        # A held lease may have no pending batch (lease-only hold for demos),
        # but a pending batch must never outlive its lease.
        orphan_pending = conn.execute(
            """
            SELECT COUNT(*) c FROM project_batches b
             WHERE b.stream_id=? AND b.status='pending'
               AND NOT EXISTS (
                   SELECT 1 FROM stream_leases l
                    WHERE l.stream_id=b.stream_id AND l.state='active'
                      AND l.lease_token=b.lease_token
               )
            """,
            (sid,),
        ).fetchone()["c"]
        checks.append(
            {
                "stream_id": sid,
                "stream_code": stream["code"],
                "check": "pending_batches_hold_live_lease",
                "ok": int(orphan_pending) == 0,
                "detail": (
                    f"租约状态 {state}，pending 批次 {pending}，无主 pending {orphan_pending}"
                ),
            }
        )

    return {
        "ok": all(c["ok"] for c in checks),
        "checked_at": now_iso(),
        "checks": checks,
    }
