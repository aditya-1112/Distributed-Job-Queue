from fastapi import Response
from app.jobs import cancel_job
from typing import Optional
from fastapi import FastAPI, HTTPException, Header
from app.db import pool
from app.models import JobCreateRequest, JobCreateResponse, JobDetailResponse
from app.jobs import create_job, get_job, list_jobs, list_dlq_entries, requeue_from_dlq
from app.auth import require_scope
from fastapi import Depends
from prometheus_client import make_asgi_app, generate_latest
from app.redis_client import redis_client
from shared.metrics import queue_depth, active_workers
import time
from app.monitoring import get_system_stats, list_workers
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Distributed Job Queue API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Authorization", "Content-Type", "Idempotency-Key"],
)


PRIORITY_QUEUES = {
    "HIGH": "job_queue:high",
    "NORMAL": "job_queue:normal",
    "LOW": "job_queue:low",
}

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

@app.middleware("http")
async def refresh_gauges(request, call_next):
    if request.url.path == "/metrics":
        for priority, key in PRIORITY_QUEUES.items():
            queue_depth.labels(priority=priority).set(redis_client.llen(key))

        worker_keys = redis_client.keys("worker:*")
        live_count = 0
        now = time.time()
        for wk in worker_keys:
            last_hb = redis_client.hget(wk, "last_heartbeat")
            if last_hb and (now - float(last_hb)) < 15:
                live_count += 1
        active_workers.set(live_count)

    return await call_next(request)


@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/ready")
def ready(response: Response):
    checks = {}
    try:
        with pool.connection() as conn:
            conn.execute("SELECT 1;")
        checks["postgres"] = "ok"
    except Exception as e:
        checks["postgres"] = f"error: {e}"
    try:
        redis_client.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"error: {e}"
    all_ok = all(v == "ok" for v in checks.values())
    response.status_code = 200 if all_ok else 503

    return {"status": "ready" if all_ok else "not_ready", "checks": checks}

        

@app.get("/db-check")
def db_check():
    with pool.connection() as conn:
        result = conn.execute("SELECT 1;").fetchone()
    # pyrefly: ignore [unsupported-operation]
    return {"db_result": result[0]}

#Add a new job
@app.post("/jobs", response_model=JobCreateResponse)
def submit_job(
    request: JobCreateRequest, 
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    scopes: list[str] = Depends(require_scope("submit"))
):
    try:
        job_id, was_created = create_job(request.job_type, request.payload, request.priority, idempotency_key)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if was_created:
        status = "PENDING"
    else:
        existing_job = get_job(str(job_id))
        status = existing_job["status"] if existing_job else "UNKNOWN"

    return JobCreateResponse(job_id=job_id, status=status)

#Get status of a job
@app.get("/jobs/{job_id}", response_model=JobDetailResponse)
def get_job_status(job_id: str, scopes: list[str] = Depends(require_scope("view"))):
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

#List jobs with their details
@app.get("/jobs", response_model=list[JobDetailResponse])
def list_jobs_enpoint(
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    scopes: list[str] = Depends(require_scope("view"))
):
    return list_jobs(status, limit, offset)


@app.get("/dlq")
def get_dlq(
    resolved: bool = False, 
    limit: int = 50, 
    offset: int = 0,
    scopes: list[str] = Depends(require_scope("admin"))
):
    return list_dlq_entries(resolved, limit, offset)


@app.post("/dlq/{dlq_id}/requeue")
def requeue_dlq_entry(
    dlq_id: str,
    scopes: list[str] = Depends(require_scope("admin"))
):
    try:    
        new_job_id = requeue_from_dlq(dlq_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"new_job_id": new_job_id, "status": "PENDING"}

@app.delete("/jobs/{job_id}")
def cancel_job_endpoint(job_id: str, scopes: list[str] = Depends(require_scope("admin"))):
    try:
        return cancel_job(job_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/stats")
def stats(scopes: list[str] = Depends(require_scope("view"))):
    return get_system_stats()


@app.get("/workers")
def workers(scopes: list[str] = Depends(require_scope("admin"))):
    return list_workers()