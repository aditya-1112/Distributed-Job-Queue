CREATE TABLE idempotency_keys (
    idempotency_key TEXT PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(job_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);