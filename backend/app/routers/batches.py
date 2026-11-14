from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services import project_service as svc
from app.services.lease_service import LeaseError

router = APIRouter(tags=["batches"])


class RunBody(BaseModel):
    stream_id: int
    note: str | None = None
    owner: str = "project-worker"
    ttl_seconds: int | None = Field(default=None, ge=2, le=3600)


@router.get("/batches")
def list_batches(limit: int = 50):
    return svc.list_batches(limit=limit)


@router.post("/project")
def run_project(body: RunBody):
    try:
        return svc.run_project(
            body.stream_id,
            body.note,
            owner=body.owner or "project-worker",
            ttl_seconds=body.ttl_seconds,
            actor="project-api",
        )
    except LeaseError as e:
        raise HTTPException(e.status_code, e.as_detail()) from e
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
