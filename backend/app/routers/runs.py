from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.db import connect
from app.services import run_service as runs
from app.services.lease_service import LeaseError

router = APIRouter(tags=["runs"])


class StartBody(BaseModel):
    stream_id: int
    owner: str = "project-worker"
    ttl_seconds: int | None = Field(default=None, ge=2, le=3600)
    note: str | None = None
    actor: str = "run-api"


class CommitBody(BaseModel):
    batch_id: int
    lease_token: str
    actor: str = "run-api"


@router.post("/runs/start")
def start_run(body: StartBody):
    """Acquire the lease and stage a projection but do not commit.

    Models a worker that crashes after acquiring the lease, leaving a pending
    batch holding the stream. The lease still expires and can be preempted or
    reclaimed; its late commit will be fenced.
    """
    conn = connect()
    try:
        return runs.start_run(
            conn,
            stream_id=body.stream_id,
            owner=body.owner,
            ttl_seconds=body.ttl_seconds,
            note=body.note,
            actor=body.actor,
        )
    except LeaseError as e:
        raise HTTPException(e.status_code, e.as_detail()) from e
    finally:
        conn.close()


@router.post("/runs/commit")
def commit_run(body: CommitBody):
    conn = connect()
    try:
        return runs.commit_staged_run(
            conn, batch_id=body.batch_id, lease_token=body.lease_token, actor=body.actor
        )
    except LeaseError as e:
        raise HTTPException(e.status_code, e.as_detail()) from e
    finally:
        conn.close()


@router.get("/runs/{batch_id}")
def run_detail(batch_id: int):
    conn = connect()
    try:
        try:
            return runs.run_detail(conn, batch_id)
        except LeaseError as e:
            raise HTTPException(e.status_code, e.as_detail()) from e
    finally:
        conn.close()
