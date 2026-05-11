CREATE TABLE IF NOT EXISTS jobs (
    id BIGSERIAL PRIMARY KEY,

    task_name TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,     --    payload is parameter of each tasks

    status TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'succeeded', 'dead')),

    priority INTEGER NOT NULL DEFAULT 0,

    run_at TIMESTAMPTZ NOT NULL DEFAULT now(),    --  what time the task is start running

    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,

    locked_by TEXT,
    locked_at TIMESTAMPTZ,

    last_error TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_jobs_ready
ON jobs (priority DESC, run_at ASC, id ASC)
WHERE status = 'queued';

CREATE INDEX IF NOT EXISTS idx_jobs_running_locked
ON jobs (locked_at)
WHERE status = 'running';

CREATE TABLE IF NOT EXISTS job_attempts (                -- table for tasks logs
    id BIGSERIAL PRIMARY KEY,

    job_id BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,

    worker_id TEXT NOT NULL,

    status TEXT NOT NULL
        CHECK (status IN ('running', 'succeeded', 'failed')),

    error TEXT,

    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_job_attempts_job_id
ON job_attempts (job_id);

CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY,

    hostname TEXT NOT NULL,

    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    started_at TIMESTAMPTZ NOT NULL DEFAULT now()
);