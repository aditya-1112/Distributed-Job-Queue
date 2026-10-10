import time
from app.db import pool
from app.redis_client import redis_client
from app.jobs import PRIORITY_QUEUES

HEARTBEAT_STALE_SECONDS = 15
JOB_STATUSES = ["PENDING", "RUNNING", "RETRYING", "COMPLETED", "FAILED", "CANCELLED"]


def list_workers() -> list[dict]:
    now = time.time()
    workers = []
    for key in redis_client.scan_iter("worker:*"):
        h = redis_client.hgetall(key)
        if not h:
            continue  # key vanished between SCAN and read
        age = now - float(h.get("last_heartbeat", 0))
        workers.append({
            "worker_id": key.removeprefix("worker:"),
            "status": "HEALTHY" if age < HEARTBEAT_STALE_SECONDS else "DEAD",
            "current_job": h.get("current_job") or None,
            "hostname": h.get("hostname"),
            "last_heartbeat_age_seconds": round(age, 1),
        })
    workers.sort(key=lambda w: w["worker_id"])
    return workers


def get_system_stats() -> dict:
    with pool.connection() as conn:
        rows = conn.execute("SELECT status, count(*) FROM jobs GROUP BY status").fetchall()
        dlq_open = conn.execute(
            "SELECT count(*) FROM dead_letter_queue WHERE resolved = false"
        ).fetchone()[0]

    jobs_by_status = {s: 0 for s in JOB_STATUSES}
    jobs_by_status.update({status: count for status, count in rows})

    workers = list_workers()
    return {
        "jobs_by_status": jobs_by_status,
        "queue_depth": {p: redis_client.llen(k) for p, k in PRIORITY_QUEUES.items()},
        "delayed_jobs": redis_client.zcard("delayed_jobs"),
        "dlq_unresolved": dlq_open,
        "active_workers": sum(1 for w in workers if w["status"] == "HEALTHY"),
    }