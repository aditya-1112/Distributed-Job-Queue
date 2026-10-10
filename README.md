# Distributed Job Queue

A job processing platform built with Python, FastAPI, Redis and PostgreSQL. Clients submit jobs over HTTP and get a `job_id` back immediately. Independent worker processes execute the jobs asynchronously.

**Delivery guarantee: at-least-once.** A job is never lost, and the bookkeeping is never double-counted. Under failures, though, the underlying work *can* run more than once, so job handlers should be idempotent (see Known Limitations).

> Status: backend core (Phases 1-20) complete. Dashboard, Docker Compose, load tests, scaling experiments and formal failure experiments are still to come. No benchmark numbers are claimed here until they are measured.

## Architecture

```text
Client --HTTP--> API (FastAPI) --write--> PostgreSQL (source of truth)
                     |
                     +--LPUSH--> Redis priority queues (high / normal / low)
                                        |
                       +----------------+----------------+
                       v                v                v
                    Worker 1         Worker 2         Worker N
                       |                |                |
                       +--- status/results ---> PostgreSQL
                       +--- leases, heartbeats, delays --> Redis
```

**Split of responsibilities**
- **PostgreSQL** is the durable source of truth: jobs, DLQ, idempotency keys, API keys, worker registry.
- **Redis** is fast, rebuildable coordination state: queues, leases, heartbeats, retry delays.
- **Rule:** always write to Postgres *before* making work visible in Redis.

### Components
| Component | Responsibility |
|---|---|
| API | Validates and authenticates requests, persists jobs, enqueues, serves status, DLQ and `/metrics`, `/health`, `/ready` |
| Worker | Claims a job, executes it, records the result, renews its lease. Runs background threads: heartbeat, recovery sweep, delayed-job sweep, metrics server |
| `shared/` | State machine, structured logging, metric definitions (used by API and worker) |

## Job lifecycle

```text
PENDING --claim--> RUNNING --success--> COMPLETED
   |                  |
   |                  +--failure, attempts left--> RETRYING --backoff elapsed--> PENDING
   |                  +--failure, exhausted------> FAILED (+ DLQ entry)
   |                  +--lease expired (crash/timeout)--> PENDING (recovered)
   +--cancel--> CANCELLED   (also from RETRYING)
```

All status changes go through one guarded function, `transition_job`. It checks that the edge is legal and runs `UPDATE ... WHERE status = <expected>`. If zero rows match, someone else changed the job first and `ConcurrentModification` is raised. A DB `CHECK` constraint backstops invalid status values.

## Reliability mechanisms

| Problem | Mechanism |
|---|---|
| Two workers get the same job | Atomic Redis `LMOVE`/`BLMOVE`; Postgres conditional update as a second layer |
| Worker dies after taking a job | Job moves into `processing:<worker_id>` (ACK = removal), so the reference survives the crash |
| Detecting a dead or stuck claim | Per-job **lease** in a Redis sorted set (score = expiry), renewed by the heartbeat thread |
| Recovery | Every worker runs a sweep; an atomic Lua script verifies expiry and releases the claim. Postgres state is reset `RUNNING -> PENDING`, then the job is requeued |
| Job throws | Caught locally; retried up to `max_attempts` |
| Retry storms | Exponential backoff with full jitter, held in a `delayed_jobs` sorted set and promoted by a sweep |
| Permanent failure | Dead letter queue table; manual requeue creates a *new* job and keeps the audit trail |
| Hung job | Lease duration = job `timeout_seconds`; renewals stop at the deadline, so the existing recovery path handles it |
| Duplicate submission | `Idempotency-Key` header; unique constraint plus `INSERT ... ON CONFLICT` in one transaction with the job insert |
| Deploys / restarts | Graceful shutdown: stop claiming, finish the current job, deregister |
| Priorities | Three queues checked in strict order; blocking fallback with a 5s timeout |

### Redis keys
`job_queue:{high,normal,low}` (lists), `processing:<worker_id>` (list), `job_leases` (zset), `job_owner` (hash), `delayed_jobs` (zset), `worker:<worker_id>` (hash).

## API

| Endpoint | Scope | Notes |
|---|---|---|
| `POST /jobs` | submit | Optional `Idempotency-Key`, `priority`, `timeout_seconds` |
| `GET /jobs`, `GET /jobs/{id}` | view | Filter by status, paginate |
| `DELETE /jobs/{id}` | admin | `PENDING` / `RETRYING` only |
| `GET /dlq`, `POST /dlq/{id}/requeue` | admin | |
| `GET /health` | none | Liveness only, no dependency checks |
| `GET /ready` | none | Checks Postgres and Redis; 503 if either fails |
| `GET /metrics` | none | Prometheus format |

Auth is `Authorization: Bearer <key>`. Only SHA-256 hashes of keys are stored. Scopes are `submit`, `view` and `admin`. Missing or invalid key returns 401; insufficient scope returns 403.

## Observability
- **Logs:** one JSON object per line on stdout. Event vocabulary includes `job_submitted`, `job_claimed`, `job_started`, `job_completed`, `job_failed`, `job_retrying`, `job_timeout`, `job_recovered`, `worker_registered`, `worker_failed`. Payloads and secrets are never logged.
- **Metrics:** counters, histograms and gauges for submissions, completions, failures, retries, processing time, queue wait, queue depth, active workers. Worker metrics are per process, each worker serves its own `/metrics` port, and a Prometheus server must scrape every target and aggregate.

## Key design decisions and tradeoffs
- **Redis lists + `BLMOVE`** instead of Streams or a broker. Chosen to build ack and reclaim mechanics by hand; Streams (`XAUTOCLAIM`) would be the idiomatic production choice.
- **Decentralized recovery** (every worker sweeps) instead of a dedicated reaper, so there is no extra single point of failure. Races are resolved atomically by Lua and `ZREM`.
- **Lease + heartbeat are separate.** The heartbeat says the process is alive; the lease says this worker still owns this job.
- **Strict priority** was chosen over weighted fairness for simplicity. Low-priority starvation is possible under sustained high-priority load.
- **Raw SQL (psycopg)** instead of an ORM, to keep transactions and queries visible.
- **API keys over JWT** for immediate revocation at this scale.

## Known limitations (deliberate and documented)
- **At-least-once execution.** If a lease expires while a worker is still running, another worker can execute the job too. Whichever result reaches Postgres first is recorded. Handlers must be idempotent for real side effects.
- **No forced job interruption.** Timeouts detect and recover; they do not kill stuck code. A truly stuck worker thread stays stuck. Fixing this needs process-per-job.
- **Cancellation of `RUNNING` jobs is not supported.**
- **Two-write gap on submit.** A crash between the Postgres insert and the Redis push leaves a `PENDING` job that is not queued. An orphan sweep is not yet built.
- **Priority claim race.** A high-priority job arriving mid-scan can lose to a low-priority claim for one iteration. The idle fallback blocks on the low queue only, so a HIGH job can wait up to 5s when all workers are idle.
- **Worker failure is inferred, not known.** `worker_failed` is logged when a reclaimed job's previous owner has a stale heartbeat (>15s).
- Idempotency keys are retained forever; `KEYS` is used for worker counting in `/metrics` (use `SCAN` at scale); `/ready` has no explicit timeout.

## Running locally
```powershell
docker compose up -d                      # Redis + Postgres
# apply schema (jobs, workers, dead_letter_queue, idempotency_keys, api_keys + later ALTERs)
cd api;    uvicorn app.main:app --reload --port 8000
cd worker; $env:METRICS_PORT=9101; python worker.py
python api/scripts/create_api_key.py "admin" "submit,view,admin"
```
Simulate a crash with `{"simulate_crash": true}` (calls `os._exit`); Ctrl+C triggers graceful shutdown instead.


## Results
_To be filled in from real experiments 