from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.db import connect
from app.services import lease_service as leases
from app.services.lease_service import LeaseError

router = APIRouter(tags=["leases"])


class AcquireBody(BaseModel):
    stream_id: int
    owner: str = "lease-holder"
    ttl_seconds: int | None = Field(default=None, ge=2, le=3600)
    actor: str = "api"


class HeartbeatBody(BaseModel):
    stream_id: int
    lease_token: str
    ttl_seconds: int | None = Field(default=None, ge=2, le=3600)
    actor: str = "api"


class ReleaseBody(BaseModel):
    stream_id: int
    lease_token: str | None = None
    reason: str | None = None
    actor: str = "api"


class ReclaimBody(BaseModel):
    stream_id: int
    reason: str | None = None
    actor: str = "api"


class PolicyBody(BaseModel):
    policy: str | None = None
    default_ttl: int | None = Field(default=None, ge=2, le=3600)


def _lease_call(fn, *args, **kwargs):
    conn = connect()
    try:
        return fn(conn, *args, **kwargs)
    except LeaseError as e:
        raise HTTPException(e.status_code, e.as_detail()) from e
    finally:
        conn.close()


@router.get("/leases")
def list_leases():
    conn = connect()
    try:
        rows = leases.list_leases(conn)
        return {
            "policy": leases.preempt_policy(conn),
            "default_ttl_seconds": leases.default_ttl(conn),
            "held_streams": sum(1 for r in rows if r["held"]),
            "expired_streams": sum(1 for r in rows if r["expired"]),
            "leases": rows,
        }
    finally:
        conn.close()


@router.get("/leases/events")
def list_lease_events(stream_id: int | None = None, limit: int = 200):
    conn = connect()
    try:
        return leases.list_lease_events(conn, stream_id=stream_id, limit=limit)
    finally:
        conn.close()


@router.get("/leases/audit")
def audit_leases():
    conn = connect()
    try:
        return leases.audit(conn)
    finally:
        conn.close()


@router.get("/leases/{stream_id}")
def get_lease(stream_id: int):
    conn = connect()
    try:
        try:
            return leases.describe(conn, stream_id)
        except LeaseError as e:
            raise HTTPException(e.status_code, e.as_detail()) from e
    finally:
        conn.close()


@router.post("/leases/acquire")
def acquire_lease(body: AcquireBody):
    return _lease_call(
        leases.acquire,
        stream_id=body.stream_id,
        owner=body.owner,
        ttl_seconds=body.ttl_seconds,
        actor=body.actor,
    )


@router.post("/leases/heartbeat")
def heartbeat_lease(body: HeartbeatBody):
    return _lease_call(
        leases.heartbeat,
        stream_id=body.stream_id,
        lease_token=body.lease_token,
        ttl_seconds=body.ttl_seconds,
        actor=body.actor,
    )


@router.post("/leases/release")
def release_lease(body: ReleaseBody):
    return _lease_call(
        leases.release,
        stream_id=body.stream_id,
        lease_token=body.lease_token,
        reason=body.reason,
        actor=body.actor,
    )


@router.post("/leases/reclaim")
def reclaim_lease(body: ReclaimBody):
    return _lease_call(
        leases.reclaim,
        stream_id=body.stream_id,
        reason=body.reason,
        actor=body.actor,
    )


@router.put("/leases/policy")
def set_policy(body: PolicyBody):
    conn = connect()
    try:
        if body.policy is not None:
            if body.policy not in leases.POLICIES:
                raise HTTPException(400, {"code": "BAD_POLICY", "message": "未知抢占策略"})
            leases.set_setting(conn, "lease_preempt_policy", body.policy)
        if body.default_ttl is not None:
            leases.set_setting(conn, "lease_default_ttl", str(int(body.default_ttl)))
        conn.commit()
        return {
            "policy": leases.preempt_policy(conn),
            "default_ttl_seconds": leases.default_ttl(conn),
        }
    finally:
        conn.close()
