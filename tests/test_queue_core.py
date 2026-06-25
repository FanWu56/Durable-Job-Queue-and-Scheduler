from concurrent.futures import ThreadPoolExecutor

import pytest

from app.db import get_conn
from app.job_queue import enqueue_job, get_job_attempts
from app.job_worker import (
    claim_next_job,
    finish_job_attempt,
    mark_job_failed,
    mark_job_succeeded,
    requeue_rate_limited_job,
    start_job_attempt,
)


def fetch_job(job_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM jobs
                WHERE id = %s;
                """,
                (job_id,),
            )
            return cur.fetchone()


def test_enqueue_job_creates_queued_job():
    job = enqueue_job(
        task_name="send_email",
        payload={"to": "test@example.com"},
        priority=3,
    )

    stored = fetch_job(job["id"])

    assert stored["task_name"] == "send_email"
    assert stored["payload"] == {"to": "test@example.com"}
    assert stored["priority"] == 3
    assert stored["status"] == "queued"
    assert stored["attempts"] == 0


def test_claim_next_job_marks_job_running():
    job = enqueue_job(task_name="send_email", payload={"n": 1})

    claimed = claim_next_job(worker_id="worker-test")

    assert claimed is not None
    assert claimed["id"] == job["id"]

    stored = fetch_job(job["id"])

    assert stored["status"] == "running"
    assert stored["locked_by"] == "worker-test"
    assert stored["locked_at"] is not None


def test_priority_jobs_are_claimed_first():
    low = enqueue_job(task_name="send_email", payload={"name": "low"}, priority=1)
    high = enqueue_job(task_name="send_email", payload={"name": "high"}, priority=10)
    medium = enqueue_job(task_name="send_email", payload={"name": "medium"}, priority=5)

    first = claim_next_job(worker_id="worker-test")
    mark_job_succeeded(first["id"])

    second = claim_next_job(worker_id="worker-test")
    mark_job_succeeded(second["id"])

    third = claim_next_job(worker_id="worker-test")
    mark_job_succeeded(third["id"])

    assert first["id"] == high["id"]
    assert second["id"] == medium["id"]
    assert third["id"] == low["id"]


def test_delayed_job_is_not_claimed_before_run_at():
    enqueue_job(
        task_name="send_email",
        payload={"name": "delayed"},
        delay_seconds=3600,
    )

    claimed = claim_next_job(worker_id="worker-test")

    assert claimed is None


def test_mark_job_succeeded_updates_status():
    job = enqueue_job(task_name="send_email", payload={})
    claimed = claim_next_job(worker_id="worker-test")

    mark_job_succeeded(claimed["id"])

    stored = fetch_job(job["id"])

    assert stored["status"] == "succeeded"
    assert stored["locked_by"] is None
    assert stored["locked_at"] is None


def test_failed_job_requeues_before_max_attempts():
    job = enqueue_job(
        task_name="unstable_task",
        payload={"fail_rate": 1},
        max_attempts=3,
    )

    claimed = claim_next_job(worker_id="worker-test")

    updated = mark_job_failed(
        job_id=claimed["id"],
        error="boom",
        attempts_before_failure=claimed["attempts"],
    )

    assert updated["status"] == "queued"
    assert updated["attempts"] == 1
    assert updated["max_attempts"] == 3

    stored = fetch_job(job["id"])

    assert stored["status"] == "queued"
    assert stored["attempts"] == 1
    assert stored["last_error"] == "boom"
    assert stored["run_at"] is not None


def test_failed_job_goes_dead_after_max_attempts():
    job = enqueue_job(
        task_name="unstable_task",
        payload={"fail_rate": 1},
        max_attempts=1,
    )

    claimed = claim_next_job(worker_id="worker-test")

    updated = mark_job_failed(
        job_id=claimed["id"],
        error="final failure",
        attempts_before_failure=claimed["attempts"],
    )

    assert updated["status"] == "dead"
    assert updated["attempts"] == 1
    assert updated["max_attempts"] == 1

    stored = fetch_job(job["id"])

    assert stored["status"] == "dead"
    assert stored["last_error"] == "final failure"


def test_job_attempt_history_records_success():
    job = enqueue_job(task_name="send_email", payload={})
    claimed = claim_next_job(worker_id="worker-test")

    attempt = start_job_attempt(
        job_id=claimed["id"],
        worker_id="worker-test",
    )

    finish_job_attempt(
        attempt_id=attempt["id"],
        status="succeeded",
    )

    attempts = get_job_attempts(job["id"])

    assert len(attempts) == 1
    assert attempts[0]["job_id"] == job["id"]
    assert attempts[0]["worker_id"] == "worker-test"
    assert attempts[0]["status"] == "succeeded"
    assert attempts[0]["error"] is None
    assert attempts[0]["finished_at"] is not None


def test_job_attempt_history_records_failure():
    job = enqueue_job(task_name="unstable_task", payload={"fail_rate": 1})
    claimed = claim_next_job(worker_id="worker-test")

    attempt = start_job_attempt(
        job_id=claimed["id"],
        worker_id="worker-test",
    )

    finish_job_attempt(
        attempt_id=attempt["id"],
        status="failed",
        error="unstable_task failed randomly",
    )

    attempts = get_job_attempts(job["id"])

    assert len(attempts) == 1
    assert attempts[0]["status"] == "failed"
    assert attempts[0]["error"] == "unstable_task failed randomly"


def test_concurrent_workers_do_not_claim_duplicate_jobs():
    total_jobs = 100

    for i in range(total_jobs):
        enqueue_job(
            task_name="send_email",
            payload={"n": i},
        )

    claimed_job_ids = []

    def worker_loop(worker_id: str):
        local_claimed = []

        while True:
            job = claim_next_job(worker_id=worker_id)

            if job is None:
                break

            local_claimed.append(job["id"])
            mark_job_succeeded(job["id"])

        return local_claimed

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(worker_loop, f"worker-{i}")
            for i in range(5)
        ]

        for future in futures:
            claimed_job_ids.extend(future.result())

    assert len(claimed_job_ids) == total_jobs
    assert len(set(claimed_job_ids)) == total_jobs

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS count
                FROM jobs
                WHERE status = 'succeeded';
                """
            )
            row = cur.fetchone()

    assert row["count"] == total_jobs


def test_rate_limited_job_can_be_requeued():
    job = enqueue_job(task_name="send_email", payload={"n": 1})
    claimed = claim_next_job(worker_id="worker-test")

    requeue_rate_limited_job(
        job_id=claimed["id"],
        delay_seconds=60,
    )

    stored = fetch_job(job["id"])

    assert stored["status"] == "queued"
    assert stored["locked_by"] is None
    assert stored["locked_at"] is None
    assert stored["run_at"] is not None