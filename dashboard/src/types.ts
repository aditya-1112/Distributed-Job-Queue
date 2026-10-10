export interface Stats {
    jobs_by_status: Record<string, number>;
    queue_depth: Record<string, number>;
    delayed_jobs: number;
    dlq_unresolved: number;
    active_workers: number;
}

export interface Worker {
    worker_id: string;
    status: "HEALTHY" | "DEAD";
    current_job: string | null;
    hostname: string | null;
    last_heartbeat_age_seconds: number;
}

export interface Job {
    job_id: string;
    job_type: string;
    status: string;
    priority: string;
    worker_id: string | null;
    attempt_count: number;
    max_attempts: number;
    error: string | null;
    created_at: string;
    updated_at: string;
}