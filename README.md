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
sqlproj/
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
  README.md
  .env.example
  .gitignore
```

## Database Design

The project uses three main tables:

### `jobs`

Stores the durable job queue.

Important columns:

* `task_name`: name of the task to execute
* `payload`: JSONB payload for the task
* `status`: `queued`, `running`, `succeeded`, or `dead`
* `priority`: higher priority jobs are claimed first
* `run_at`: delayed jobs are only claimable after this time
* `attempts`: number of failed attempts
* `max_attempts`: maximum retries before dead-letter handling
* `locked_by`: worker that claimed the job
* `locked_at`: when the job was claimed
* `last_error`: latest failure message

### `job_attempts`

Tracks every execution attempt for debugging and retry history.

Important columns:

* `job_id`
* `worker_id`
* `status`
* `error`
* `started_at`
* `finished_at`

### `workers`

Tracks worker liveness.

Important columns:

* `id`
* `hostname`
* `last_heartbeat_at`
* `started_at`

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

## Setup

### 1. Create a virtual environment

```bash
python -m venv .venv
```

On Windows:

```bash
.venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Create PostgreSQL databases

Create a development database:

```bash
createdb -U postgres jobqueue
```

Create a test database:

```bash
createdb -U postgres jobqueue_test
```

If `createdb` is not available, create the databases manually using pgAdmin.

### 4. Create `.env`

Create a `.env` file:

```env
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/jobqueue
TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/jobqueue_test
REDIS_URL=redis://localhost:6379/0
```

Update the username and password if your PostgreSQL setup is different.

### 5. Initialize database tables

```bash
python app/cli.py init-db
```

### 6. Check database connection

```bash
python app/cli.py health
```

## Usage

### Enqueue a job

```bash
python app/cli.py enqueue send_email --payload "{""to"":""test@example.com""}"
```

### Enqueue a delayed job

```bash
python app/cli.py enqueue send_email --payload "{""name"":""delayed""}" --delay 30
```

### Enqueue a priority job

```bash
python app/cli.py enqueue send_email --payload "{""name"":""high""}" --priority 10
```

### Show recent jobs

```bash
python app/cli.py jobs
```

### Start a worker

```bash
python app/cli.py worker
```

### Run one worker iteration

```bash
python app/cli.py worker --once
```

### Start multiple workers

Open two terminals.

Terminal 1:

```bash
python app/cli.py worker --worker-id worker-1
```

Terminal 2:

```bash
python app/cli.py worker --worker-id worker-2
```

### Show worker heartbeats

```bash
python app/cli.py workers
```

### Show job attempts

```bash
python app/cli.py attempts 1
```

### Reset stuck running jobs manually

```bash
python app/cli.py reset-running
```

## Example Output

### Successful job execution

```text
Worker started: worker-1
Claimed job #1: send_email
Sending email with payload: {'to': 'test@example.com'}
Job #1 succeeded.
```

### Retry and dead-letter behavior

```text
Claimed job #5: unstable_task
Running unstable task with payload: {'fail_rate': 1}
Job #5 failed: unstable_task failed randomly. status=queued attempts=1/3 next_run_at=...
```

After max attempts:

```text
Job #5 failed: unstable_task failed randomly. status=dead attempts=3/3
```

### Redis rate limiting

```text
[rate-limit] task=send_email count=1 limit=5
Job #10 succeeded.

[rate-limit] task=send_email count=6 limit=5
Job #15 rate limited. Requeued for later.
```

## Testing

Run all tests:

```bash
pytest -q
```

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

The most important concurrency test verifies that multiple workers do not claim the same job more than once.

## Key Tests

### Concurrent workers do not duplicate jobs

The test creates 100 jobs and starts multiple worker loops in parallel. Each worker claims jobs using `FOR UPDATE SKIP LOCKED`.

The test verifies:

```text
number of claimed jobs == 100
number of unique claimed job IDs == 100
all jobs are marked succeeded
```

This confirms that the queue prevents duplicate execution across concurrent workers.

## Design Decisions

### Why PostgreSQL?

PostgreSQL provides durable storage and transactional row-level locking. This makes it a good fit for a reliable job queue where jobs should not be lost if the process crashes.

### Why Redis?

Redis is used for fast rate limiting. The job state remains durable in PostgreSQL, while Redis handles lightweight per-minute counters.

### Why `FOR UPDATE SKIP LOCKED`?

Without locking, two workers could read the same queued job at the same time and both execute it. `FOR UPDATE SKIP LOCKED` allows workers to safely claim different jobs concurrently.

### Why store job attempts separately?

The `jobs` table stores the current state of the job. The `job_attempts` table stores execution history, which is useful for debugging, retries, and failure analysis.

## Future Improvements

* Add Docker Compose for easier setup
* Add a small web dashboard
* Add structured logging
* Add metrics for queue depth and job latency
* Add task-specific retry policies
* Add cron-style recurring jobs
* Add graceful worker shutdown
* Add database migrations with Alembic

## Resume Summary

Durable Job Queue and Scheduler — PostgreSQL, Redis, Python

* Built a durable background job queue supporting delayed jobs, priority scheduling, retries with exponential backoff, dead-letter handling, Redis-based rate limiting, and worker heartbeats.
* Implemented concurrent job claiming using PostgreSQL row-level locking and `FOR UPDATE SKIP LOCKED` to prevent duplicate execution across multiple workers.
* Designed normalized job, attempt, and worker state tables with indexes to support efficient polling, retry tracking, liveness detection, and stuck-job recovery.
* Added pytest coverage for enqueueing, delayed execution, priority ordering, retries, dead-letter behavior, Redis rate limiting, and concurrent worker safety.
