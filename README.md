# Durable Job Queue and Scheduler

A durable background job queue built with **Python, PostgreSQL, and Redis**.

This project implements a simplified version of a production-style job queue system. It supports durable job storage, delayed jobs, priority scheduling, concurrent workers, retries with exponential backoff, dead-letter handling, Redis-based rate limiting, worker heartbeats, and stuck-job recovery.

## Features

* Durable job storage using PostgreSQL
* Enqueue jobs with JSON payloads
* Delayed jobs using `run_at`
* Priority scheduling
* Multiple concurrent workers
* Safe job claiming with PostgreSQL row-level locking
* `FOR UPDATE SKIP LOCKED` to prevent duplicate job execution
* Retry handling with exponential backoff
* Dead-letter handling after max retry attempts
* Job attempt history tracking
* Worker heartbeat tracking
* Stuck job recovery for abandoned running jobs
* Redis-based fixed-window rate limiting
* Pytest coverage for core queue behavior

## Tech Stack

* Python
* PostgreSQL
* Redis
* psycopg
* Typer
* pytest

## Project Structure

```text
Durable-Job-Queue-and-Scheduler/
  app/
    __init__.py
    cli.py
    config.py
    db.py
    queue.py
    rate_limit.py
    tasks.py
    worker.py

  tests/
    conftest.py
    test_queue_core.py
    test_redis_rate_limit.py

  schema.sql
  requirements.txt
  Dockerfile
  docker-compose.yml
  README.md
  .env.example
  .gitignore
```



## Core Design

### Concurrent Job Claiming

Workers claim jobs using PostgreSQL row-level locking:

```sql
WITH picked AS (
    SELECT id
    FROM jobs
    WHERE status = 'queued'
      AND run_at <= now()
    ORDER BY priority DESC, run_at ASC, id ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs
SET status = 'running',
    locked_by = %s,
    locked_at = now(),
    updated_at = now()
WHERE id IN (SELECT id FROM picked)
RETURNING *;
```

`FOR UPDATE SKIP LOCKED` allows multiple workers to safely poll the queue at the same time. If one worker locks a job, other workers skip it and claim another available job instead. This prevents duplicate execution.

### Retry and Dead-Letter Handling

When a job fails, the worker increments `attempts`.

If the job still has retry attempts remaining, it is moved back to `queued` with a future `run_at` based on exponential backoff.

If the job reaches `max_attempts`, it is moved to `dead`.

Example retry behavior:

```text
1st failure -> retry later
2nd failure -> retry later
3rd failure -> dead
```

### Delayed Jobs

Delayed jobs are supported through the `run_at` column.

Workers only claim jobs where:

```sql
run_at <= now()
```

This means jobs scheduled for the future stay in the queue until they are ready.

### Priority Scheduling

Jobs are claimed in this order:

```sql
ORDER BY priority DESC, run_at ASC, id ASC
```

Higher priority jobs run first. If priorities are the same, the earlier `run_at` job runs first.

### Worker Heartbeats and Stuck Job Recovery

Each worker registers itself in the `workers` table and periodically updates `last_heartbeat_at`.

If a job stays in `running` for too long, it is treated as abandoned and moved back to `queued`.

This handles cases where a worker crashes while processing a job.

### Redis Rate Limiting

Redis is used for fixed-window rate limiting.

Example:

```text
send_email: 5 executions per minute
```

The worker increments a Redis key for each task execution:

```text
rate_limit:send_email:<minute_bucket>
```

If the task exceeds its limit, the job is requeued with a delayed `run_at`.

## Getting Started

### 1. Install and Open Docker Desktop
https://www.docker.com/products/docker-desktop/

### 2. Clone repo & Go to directory

```bash
git clone https://github.com/FanWu56/Durable-Job-Queue-and-Scheduler
cd Durable-Job-Queue-and-Scheduler
```

### 3. Start PostgreSQL + Redis + app

```bash
docker compose up -d
```

### 4. Create PostgreSQL databases

```bash
docker compose exec postgres psql -U postgres -c "CREATE DATABASE jobqueue_test;"
```

### 5. Initialize database tables

```bash
docker compose run --rm app python -m app.cli init-db
```

### 6. Check database connection

```bash
docker compose run --rm app python -m app.cli health
```

### 7. Enqueue a job

```bash
docker compose run --rm app python -m app.cli enqueue send_email --payload "{\"to\":\"test@example.com\"}"
```

### 8. Run a worker

```bash
docker compose run --rm app python -m app.cli worker --once
```

### 9. View jobs

```bash
docker compose run --rm app python -m app.cli jobs
```

### 10. Run test

```bash
docker compose run --rm app pytest -q
```








## Testing

Example result:

```text
12 passed
```

The test suite covers:

* Enqueueing jobs
* Claiming jobs
* Priority ordering
* Delayed jobs
* Marking jobs as succeeded
* Retry behavior
* Dead-letter behavior
* Job attempt history
* Concurrent worker safety
* Rate-limited job requeueing
* Redis rate limit behavior
