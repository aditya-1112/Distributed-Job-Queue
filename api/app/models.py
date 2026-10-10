from psycopg.types import string
from pydantic import BaseModel
from typing import Any
from uuid import UUID
from datetime import datetime


class JobCreateRequest(BaseModel):
    job_type: str
    payload: dict[str, Any]
    priority: str = "NORMAL"
    timeout_seconds: int = 30

class JobCreateResponse(BaseModel):
    job_id: UUID
    status: str

class JobDetailResponse(BaseModel):
    job_id: UUID
    job_type: str
    payload: dict[str, Any]
    status: str
    result: dict[str, Any] | None = None
    error: str | None = None
    created_at: datetime
    updated_at: datetime
    worker_id: str | None
    attempt_count: int
    max_attempts: int
    priority: str