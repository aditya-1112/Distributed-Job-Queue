from prometheus_client import Counter, Gauge, Histogram

jobs_submitted_total = Counter("jobs_submitted_total", "Total jobs submitted", ["priority"])
jobs_completed_total = Counter("jobs_completed_total", "Total jobs completed successfully", ["job_type"])
jobs_failed_total = Counter("jobs_failed_total", "Total jobs permanently failed", ["job_type"])
jobs_retried_total = Counter("jobs_retried_total", "Total retry attempts", ["job_type"])
worker_failures_total = Counter("worker_failures_total", "Total worker failures detected")

job_processing_seconds = Histogram(
    "job_processing_seconds", "Time spent actually executing a job",
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 120),
)
job_queue_wait_seconds = Histogram(
    "job_queue_wait_seconds", "Time a job spent waiting before a worker claimed it",
    buckets=(0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60),
)

queue_depth = Gauge("queue_depth", "Current number of jobs waiting in queue", ["priority"])
active_workers = Gauge("active_workers", "Number of workers with a recent heartbeat")