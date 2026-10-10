from shared.metrics import worker_failures_total
from datetime import timezone
from datetime import datetime
import os
import time
import psycopg
import redis
import socket
import uuid
import threading
import random
import signal
from psycopg.types.json import Jsonb
from shared import ConcurrentModification, InvalidTransition, transition_job
from shared.logging_config import get_logger, log_event
import logging
from shared.metrics import (
    jobs_completed_total, jobs_failed_total, jobs_retried_total,
    job_processing_seconds, job_queue_wait_seconds,
)
from prometheus_client import start_http_server

METRICS_PORT = int(os.environ.get("METRICS_PORT", 9100))

logger = get_logger("worker")

def wlog(event: str, level: int = logging.INFO, **fields):
    log_event(logger, event, level, worker_id = WORKER_ID, **fields )       

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN",
    "postgresql://jobqueue:jobqueue@localhost:5432/jobqueue",
)
JOB_QUEUE_KEY = "job_queue"

WORKER_ID = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"

PROCESSING_KEY = f"processing:{WORKER_ID}"

HEARTBEAT_INTERVAL_SECONDS = 5

WORKER_HASH_KEY = f"worker:{WORKER_ID}"

LEASE_DURATION_SECONDS = 30   # how long a claim is valid without renewal
LEASE_RENEWAL_INTERVAL_SECONDS = 10  # renew well before it expires
JOB_LEASES_KEY = "job_leases"

BASE_BACKOFF_SECONDS = 1
MAX_BACKOFF_SECONDS = 60
DELAYED_JOBS_KEY = "delayed_jobs"

current_job_id = None
current_job_deadline = None

redis_client = redis.from_url(REDIS_URL, decode_responses=True)

PRIORITY_QUEUES_ORDERED = ["job_queue:high", "job_queue:normal", "job_queue:low"]

shutdown_requested = False

def handle_shutdown_signal(signum, frame):
    global shutdown_requested
    print(f"[{WORKER_ID}] Shutdown signal received ({signal.Signals(signum).name}). "
          f"Will stop claiming new jobs and exit after current job finishes.")
    shutdown_requested = True

signal.signal(signal.SIGTERM, handle_shutdown_signal)
signal.signal(signal.SIGINT, handle_shutdown_signal)

def claim_next_job():
    for queue_key in PRIORITY_QUEUES_ORDERED:
        job_id = redis_client.lmove(queue_key, PROCESSING_KEY, "RIGHT", "LEFT")
        if job_id is not None:
            return job_id
    result = redis_client.blmove(
        PRIORITY_QUEUES_ORDERED[-1], PROCESSING_KEY, timeout = 5, src = "RIGHT", dest = "LEFT"
    )
    return result


def fetch_job(job_id: str, conn):
    row = conn.execute(
        "SELECT job_id, job_type, payload, timeout_seconds, attempt_count, created_at FROM jobs WHERE job_id = %s",
        (job_id,)
    ).fetchone()
    return row

def mark_running(job_id: str, conn):
    transition_job(job_id, "PENDING", "RUNNING", conn, worker_id = WORKER_ID)

def mark_completed(job_id: str, result: dict, conn):
    transition_job(job_id, "RUNNING", "COMPLETED", conn, result=Jsonb(result))

def mark_failed(job_id: str, error_message: str, new_attempt_count: int, conn):
    transition_job(
        job_id, "RUNNING", "FAILED", conn, 
        error=error_message, attempt_count=new_attempt_count
    )

def mark_retrying(job_id: str, error_message: str, new_attempt_count: int, conn):
    transition_job(
        job_id, "RUNNING", "RETRYING", conn,
        error=error_message, attempt_count=new_attempt_count,
    )

def execute_job(job_type: str, payload: dict, job_id: str) -> dict:
    if payload.get("simulate_crash"):
        print(f"[{WORKER_ID}] Simulating a hard crash NOW (no cleanup will run)")
        os._exit(1)  
    if payload.get("simulate_failure"):
        raise RuntimeError("Simulated job failure for testing")
    if "sleep_seconds" in payload:
        time.sleep(payload["sleep_seconds"])
        return {"message": "done after long sleep"}
    print(f"[{WORKER_ID}] Executing job_type={job_type} payload={payload}")
    time.sleep(7)
    return {"message": "done"}

def compute_backoff_delay(attempt_number: int) -> float:
    exponential_delay = min(BASE_BACKOFF_SECONDS * (2 ** attempt_number), MAX_BACKOFF_SECONDS)
    return random.uniform(0, exponential_delay)

def move_to_dlq(job_id: str, job_type: str, payload: dict, failure_reason: str, attempt_count: int, conn):
    conn.execute(
        """
        INSERT INTO dead_letter_queue (job_id, job_type, payload, failure_reason, attempt_count)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (job_id, job_type, Jsonb(payload), failure_reason, attempt_count),
    )

def handle_job_failure(job_id: str, job_type: str, payload:dict, error: Exception, conn):
    error_message = f"{type(error).__name__}: {error}"
    row = conn.execute(
        "SELECT attempt_count, max_attempts FROM jobs where job_id = %s", (job_id,)
    ).fetchone()
    current_attempts, max_attempts = row
    new_attempt_count = current_attempts + 1
    
    if new_attempt_count >= max_attempts:
        mark_failed(job_id, error_message, new_attempt_count, conn)
        move_to_dlq(job_id, job_type, payload, error_message, new_attempt_count, conn)
        
        print(f"[{WORKER_ID}] Job {job_id} FAILED permanently after {new_attempt_count} attempts: {error_message}")
        jobs_failed_total.labels(job_type=job_type).inc()
        wlog("job_failed", logging.ERROR, job_id = job_id, attempts = new_attempt_count, error = error_message)
    else:
        mark_retrying(job_id, error_message, new_attempt_count, conn)
        delay = compute_backoff_delay(new_attempt_count)
        ready_at = time.time() + delay
        redis_client.zadd(DELAYED_JOBS_KEY, {job_id: ready_at})
        
        print(f"[{WORKER_ID}] Job {job_id} failed (attempt {new_attempt_count}/{max_attempts}), will retry in {delay:.1f}s: {error_message}")
        jobs_retried_total.labels(job_type=job_type).inc()
        wlog("job_retrying", logging.WARNING, job_id=job_id, attempt=new_attempt_count, delay_seconds=delay, error=error_message)


def register_worker(conn):
    conn.execute(
        """
        INSERT INTO workers (worker_id, hostname, registered_at, last_seen_at)
        VALUES (%s, %s, now(), now())
        ON CONFLICT(worker_id) DO UPDATE SET last_seen_at = now()
        """,
        (WORKER_ID, socket.gethostname()),
    )



def heartbeat_loop():
    timeout_logged_for = None
    while True:
        redis_client.hset(WORKER_HASH_KEY, mapping={
            "status":"ALIVE",
            "last_heartbeat":time.time(),
            "current_job":current_job_id or "",
            "hostname":socket.gethostname(),
        })
        redis_client.expire(WORKER_HASH_KEY, 3600)
        if current_job_id and current_job_deadline:
            if time.time() < current_job_deadline:
                new_expiry = time.time() + LEASE_DURATION_SECONDS
                redis_client.zadd(JOB_LEASES_KEY, {current_job_id: min(new_expiry, current_job_deadline)})
            else:
                if timeout_logged_for != current_job_id:
                    wlog("job_timeout", logging.WARNING, job_id = current_job_id)
                    timeout_logged_for = current_job_id
        wlog("worker_heartbeat", logging.DEBUG, current_job_id=current_job_id or "None")
        time.sleep(HEARTBEAT_INTERVAL_SECONDS)

def is_worker_likely_dead(worker_id: str, staleness_threshold_seconds: float = 10.0) -> bool:
    worker_hash = redis_client.hgetall(f"worker:{worker_id}")

    if not worker_hash:
        return True

    last_heartbeat = float(worker_hash.get("last_heartbeat", 0))
    return (time.time() - last_heartbeat) > staleness_threshold_seconds

RECOVERY_SWEEP_INTERVAL_SECONDS = 10

JOB_OWNER_KEY = "job_owner"  # hash: job_id -> worker_id currently holding it

RECLAIM_SCRIPT = """
local current_score = redis.call('ZSCORE', KEYS[1], ARGV[1])

if current_score == false then
    return "ALREADY_GONE"
end

if tonumber(current_score) > tonumber(ARGV[2]) then
    return "NOT_EXPIRED"
end

redis.call('LREM', 'processing:' .. ARGV[3], 1, ARGV[1])
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])

return "RECLAIMED"
"""

reclaim_job_script = redis_client.register_script(RECLAIM_SCRIPT)

#handles failed workers by reclaiming their jobs and adding them back to the queue
def recovery_sweep_loop():
    sweep_conn = psycopg.connect(POSTGRES_DSN, autocommit=True)
    while True:
        time.sleep(RECOVERY_SWEEP_INTERVAL_SECONDS)
        now = time.time()

        expired_job_ids = redis_client.zrangebyscore(JOB_LEASES_KEY, "-inf", now)

        for job_id in expired_job_ids:
            old_worker_id = redis_client.hget(JOB_OWNER_KEY, job_id)
            if old_worker_id is None:
                continue

            result = reclaim_job_script(
                keys=[JOB_LEASES_KEY, JOB_OWNER_KEY],
                args=[job_id, now, old_worker_id],
            )

            if result == "RECLAIMED":
                if is_worker_likely_dead(old_worker_id):
                    wlog("worker_failed", logging.WARNING, failed_worker_id=old_worker_id, detected_by=WORKER_ID)
                    worker_failures_total.inc()
                try:
                    transition_job(job_id, "RUNNING", "PENDING", sweep_conn, error="Recovered: lease expired (worker crash or job timeout)")
                except ConcurrentModification:
                    # Job's Postgres status wasn't RUNNING when we got here — 
                    # e.g. it may have actually completed just before the lease
                    # check ran. Don't requeue in Redis if Postgres disagrees.
                    print(f"[{WORKER_ID}] Skipping requeue of {job_id}: Postgres state already moved on")
                    continue

                row = sweep_conn.execute("SELECT priority FROM jobs WHERE job_id = %s", (job_id,)).fetchone()
                priority = row[0] if row else "NORMAL"
                queue_key = f"job_queue:{priority.lower()}"
                redis_client.lpush(queue_key, job_id)
                print(f"[{WORKER_ID}] Recovered abandoned job {job_id} from {old_worker_id}, requeued")
                wlog("job_recovered", logging.WARNING, job_id=job_id, from_worker=old_worker_id)  


DELAY_SWEEP_INTERVAL_SECONDS = 2
#handles failed jobs by promoting them back to the queue after backoff period
def delay_sweep_loop():
    sweep_conn = psycopg.connect(POSTGRES_DSN, autocommit=True)
    while True:
        time.sleep(DELAY_SWEEP_INTERVAL_SECONDS)
        now = time.time()
        ready_job_ids = redis_client.zrangebyscore(DELAYED_JOBS_KEY, "-inf", now)

        for job_id in ready_job_ids:
            removed = redis_client.zrem(DELAYED_JOBS_KEY, job_id)
            if removed == 0:
                continue
            try:
                transition_job(job_id, "RETRYING", "PENDING", sweep_conn)
            except ConcurrentModification:
                print(f"[{WORKER_ID}] Skipping promotion of {job_id}: not in RETRYING anymore")
                continue
            
            # Query job's priority
            row = sweep_conn.execute("SELECT priority FROM jobs WHERE job_id = %s", (job_id,)).fetchone()
            priority = row[0] if row else "NORMAL"
            queue_key = f"job_queue:{priority.lower()}"
            redis_client.lpush(queue_key, job_id)
            print(f"[{WORKER_ID}] Promoted delayed job {job_id} back to queue after backoff")
            wlog("job_promoted", job_id = job_id)

def deregister_worker(conn):
    redis_client.delete(WORKER_HASH_KEY)
    conn.execute(
        "UPDATE workers SET last_seen_at = now() WHERE worker_id = %s",
        (WORKER_ID,),
    )
    print(f"[{WORKER_ID}] Deregistered worker.")


def run():
    global current_job_id, current_job_deadline
    print(f"[{WORKER_ID}] Worker started, waiting for jobs...")
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as conn:
        register_worker(conn)
        wlog("worker_registered", hostname = socket.gethostname())
        
        heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        heartbeat_thread.start()

        recovery_thread = threading.Thread(target=recovery_sweep_loop, daemon=True)
        recovery_thread.start()

        delay_thread = threading.Thread(target=delay_sweep_loop, daemon=True)
        delay_thread.start()

        start_http_server(METRICS_PORT)
        wlog("worker_registered", hostname=socket.gethostname(), metrics_port=METRICS_PORT)

        while not shutdown_requested:
            job_id = claim_next_job()
            

            if job_id is None:
                continue  # blocking wait timed out with nothing — loop and recheck all tiers

            wlog("job_claimed", job_id = job_id)

            current_job_id = job_id
            
            
            lease_expires_at = time.time() + LEASE_DURATION_SECONDS
            redis_client.zadd(JOB_LEASES_KEY, {job_id: lease_expires_at})
            redis_client.hset(JOB_OWNER_KEY, job_id, WORKER_ID)

            print(f"[{WORKER_ID}] Claimed job {job_id}, lease expires at {lease_expires_at}")

            row = fetch_job(job_id, conn)
            if row is None:
                print(f"[{WORKER_ID}] Job {job_id} not found in DB, skipping")
                redis_client.lrem(PROCESSING_KEY, 1, job_id)
                current_job_id = None
                continue
            
            _, job_type, payload, timeout_seconds, attempt_count, created_at = row
            current_job_deadline = time.time() + timeout_seconds
            job_lease_expires_at = time.time() + timeout_seconds

            wait_seconds = (datetime.now(timezone.utc) - created_at).total_seconds()
            job_queue_wait_seconds.observe(wait_seconds)
            
            redis_client.zadd(JOB_LEASES_KEY, {job_id: job_lease_expires_at})
            redis_client.hset(JOB_OWNER_KEY, job_id, WORKER_ID)
            
            mark_running(job_id, conn)
            wlog("job_started", job_id = job_id, attempt = attempt_count)
           
            try:
                started = time.monotonic()
                with job_processing_seconds.time():
                    result = execute_job(job_type, payload, job_id)
                try:
                    mark_completed(job_id, result, conn)
                    print(f"[{WORKER_ID}] Completed and ACKed job {job_id}")
                    jobs_completed_total.labels(job_type=job_type).inc()
                    wlog("job_completed", job_id = job_id, duration_seconds = round(time.monotonic() - started, 3))
                except ConcurrentModification:
                    print(f"[{WORKER_ID}] Job {job_id} completed locally, but it was already "
                        f"reclaimed by another worker (likely due to timeout/lease expiry). "
                        f"Discarding this result — the job has already been picked up elsewhere.")
                    wlog("job_result_discarded", logging.WARNING, job_id=job_id, reason="Discarded as job was reclaimed by another worker")
            except Exception as e:
                try:
                    handle_job_failure(job_id, job_type, payload, e, conn)
                except ConcurrentModification:
                    print(f"[{WORKER_ID}] Job {job_id} failed locally, but it was already "
                        f"reclaimed by another worker. Discarding this failure — already handled elsewhere.")
                    wlog("job_failure_discarded", logging.WARNING,job_id=job_id, reason="Discarded as job was reclaimed by another worker")
            # ACK: explicitly remove from the processing list
            redis_client.lrem(PROCESSING_KEY, 1, job_id)
            redis_client.zrem(JOB_LEASES_KEY, job_id)
            redis_client.hdel(JOB_OWNER_KEY, job_id)
            current_job_id = None
            current_job_deadline = None
        
        print(f"[{WORKER_ID}] Shutdown: no job in progress, exiting cleanly.")
        wlog("worker_shutdown")
        deregister_worker(conn)
           

if __name__ == "__main__":
    run()
