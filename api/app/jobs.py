from worker.worker import DELAYED_JOBS_KEY
import uuid
import psycopg
from app.redis_client import redis_client
from psycopg.types.json import Jsonb
from app.db import pool
from shared.logging_config import get_logger, log_event
from shared.metrics import jobs_submitted_total

logger = get_logger("api")

from shared import (
    VALID_TRANSITIONS,
    ConcurrentModification,
    InvalidTransition,
    transition_job,
)

JOB_QUEUE_KEY = "job_queue"

PRIORITY_QUEUES = {
    "HIGH": "job_queue:high",
    "NORMAL": "job_queue:normal",
    "LOW": "job_queue:low",
}

#Returns (job_id, was_newly_created)
def create_job(job_type: str, payload: dict, priority: str = "NORMAL", timeout_seconds: int | None = None, idempotency_key: str | None = None) -> tuple[uuid.UUID, bool]:
    if priority not in PRIORITY_QUEUES:
        raise ValueError(f"Invalid priority: {priority}")
    if timeout_seconds is None:
        timeout_seconds = 30
    job_id = uuid.uuid4()
    with pool.connection() as conn:
        if idempotency_key is not None:
            with conn.transaction() as tx:
                conn.execute(
                    """
                    INSERT INTO jobs (job_id, job_type, payload, status, priority, timeout_seconds)
                    VALUES (%s, %s, %s, 'PENDING', %s, %s)
                    """,
                    (job_id, job_type, Jsonb(payload), priority, timeout_seconds),
                )

                claim_result = conn.execute(
                    """
                    INSERT INTO idempotency_keys (idempotency_key, job_id)
                    VALUES (%s, %s)
                    ON CONFLICT (idempotency_key) DO NOTHING
                    RETURNING job_id
                    """,
                    (idempotency_key, job_id),
                ).fetchone()

                if claim_result is None:
                    raise psycopg.Rollback()  # undoes the jobs INSERT above too

            if claim_result is None:
                existing = conn.execute(
                    "SELECT job_id FROM idempotency_keys WHERE idempotency_key = %s",
                    (idempotency_key,),
                ).fetchone()
                log_event(logger, "job_duplicate", job_id=str(job_id), job_type=job_type,
                    priority=priority, idempotency_key=idempotency_key)
                return existing[0], False

            redis_client.lpush(PRIORITY_QUEUES[priority], str(job_id))
            log_event(logger, "job_submitted", job_id=str(job_id), job_type=job_type,
                priority=priority, idempotency_key=idempotency_key)
            jobs_submitted_total.labels(priority=priority).inc()
            return job_id, True

        # No idempotency key given — original simple path, unchanged
        conn.execute(
            """
            INSERT INTO jobs (job_id, job_type, payload, status, priority, timeout_seconds)
            VALUES (%s, %s, %s, 'PENDING', %s, %s)
            """,
            (job_id, job_type, Jsonb(payload), priority, timeout_seconds),
        )

    redis_client.lpush(PRIORITY_QUEUES[priority], str(job_id))
    log_event(logger, "job_submitted", job_id=str(job_id), job_type=job_type, priority=priority)
    jobs_submitted_total.labels(priority=priority).inc()
    return job_id, True

def get_job(job_id: str) -> dict | None:
    with pool.connection() as conn:
        row = conn.execute(
            """
            SELECT job_id, job_type, payload, status, result, created_at, updated_at, worker_id, attempt_count, max_attempts, priority
            FROM jobs WHERE job_id = %s
            """,
            (job_id,)
        ).fetchone()
        
        if row is None:
            return None
        
        columns = ["job_id", "job_type", "payload", "status", "result", "created_at", "updated_at", "worker_id", "attempt_count", "max_attempts", "priority"]
        return dict(zip(columns, row))

def list_jobs(status: str | None = None, limit: int = 50, offset: int = 0) -> list[dict]:
    columns = ["job_id", "job_type", "status", "payload", "result", "error", "created_at", "updated_at", "worker_id", "attempt_count", "max_attempts", "priority"]
    col_list = ", ".join(columns)

    with pool.connection() as conn:
        if status is not None:
            rows = conn.execute(
                # pyrefly: ignore [bad-argument-type]
                f"""
                SELECT {col_list} FROM jobs
                WHERE status = %s
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (status, limit, offset),
            ).fetchall()
        else:
            rows = conn.execute(
                # pyrefly: ignore [bad-argument-type]
                f"""
                SELECT {col_list} FROM jobs
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            ).fetchall()

    return [dict(zip(columns, row)) for row in rows]

def list_dlq_entries(resolved: bool = False, limit: int = 50, offset: int = 0) -> list[dict]:
    columns = ["dlq_id", "job_id", "job_type", "payload", "failure_reason",
               "attempt_count", "moved_to_dlq_at", "resolved", "resolution"]
    col_list = ", ".join(columns)
    with pool.connection() as conn:
        rows = conn.execute(
            # pyre-ignore [bad-argument-type]
            f"""
            SELECT {col_list} FROM dead_letter_queue
            WHERE resolved = %s
            ORDER BY moved_to_dlq_at DESC
            LIMIT %s OFFSET %s
            """,
            (resolved, limit, offset),
        ).fetchall()
    return [dict(zip(columns, row)) for row in rows]


def requeue_from_dlq(dlq_id: str) -> uuid.UUID:
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT job_type, payload, resolved, priority FROM dead_letter_queue WHERE dlq_id = %s",
            (dlq_id,),
        ).fetchone()

        if row is None:
            raise ValueError("DLQ entry not found")

        job_type, payload, resolved, priority = row
        if resolved:
            raise ValueError("DLQ entry already resolved")

        new_job_id = uuid.uuid4()
        conn.execute(
            """
            INSERT INTO jobs (job_id, job_type, payload, status, priority)
            VALUES (%s, %s, %s, 'PENDING', %s)
            """,
            (new_job_id, job_type, Jsonb(payload), priority),
        )

        conn.execute(
            "UPDATE dead_letter_queue SET resolved = true, resolution = %s WHERE dlq_id = %s",
            (f"requeued as job {new_job_id}", dlq_id),
        )
        
    queue_key = f"job_queue:{priority.lower()}"

    redis_client.lpush(queue_key, str(new_job_id))
    return new_job_id

def cancel_job(job_id: str) -> dict:
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT status, priority FROM jobs WHERE job_id = %s", (job_id,)
        ).fetchone()

        if row is None:
            raise ValueError("Job not found")

        status, priority = row

        if status == "PENDING":
            transition_job(job_id, "PENDING", "CANCELLED", conn)
            redis_client.lrem(PRIORITY_QUEUES[priority], 0, job_id)
            return {"job_id": job_id, "result": "CANCELLED", "detail": "Removed from queue before execution."}

        elif status == "RETRYING":
            transition_job(job_id, "RETRYING", "CANCELLED", conn)
            redis_client.zrem(DELAYED_JOBS_KEY, job_id)
            return {"job_id": job_id, "result": "CANCELLED", "detail": "Removed from retry backoff before re-execution."}

        elif status == "RUNNING":
            raise ValueError(f"Job is running, cannot cancel.")

        else:
            raise ValueError(f"Job is already in a terminal state ({status}), cannot cancel.")
