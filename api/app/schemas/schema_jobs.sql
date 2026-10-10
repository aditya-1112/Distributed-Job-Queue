CREATE TABLE jobs (
    job_id UUID PRIMARY KEY,
    job_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    result JSONB,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE jobs ADD COLUMN priority TEXT NOT NULL DEFAULT 'NORMAL';
ALTER TABLE jobs ADD CONSTRAINT valid_priority CHECK (priority IN ('HIGH', 'NORMAL', 'LOW'));

ALTER TABLE jobs ADD COLUMN timeout_seconds INT NOT NULL DEFAULT 30