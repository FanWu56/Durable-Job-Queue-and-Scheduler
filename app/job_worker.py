import socket
import uuid
import time
from .rate_limit import is_rate_limited

##################  Below is source code python file
from . import db, tasks

def make_worker_id() -> str:
    """
    Assign each worker a unique id
    """
    hostname = socket.gethostname()
    uu_id = uuid.uuid4().hex[:10]
    return f"{hostname}{uu_id}"

worker_id = make_worker_id()

def claim_next_job(worker_id):
    """
    Claim one ready job safely.

    FOR UPDATE SKIP LOCKED prevents multiple workers from claiming
    the same job at the same time.
    """
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH picked AS(
                    SELECT id FROM jobs
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
                    RETURNING
                        id,
                        task_name,
                        payload,
                        status,
                        priority,
                        attempts,
                        max_attempts,
                        run_at;
                """, (worker_id,)
            )
            return cur.fetchone()


def mark_job_succeeded(job_id):
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET status = 'succeeded',
                    locked_by = NULL,
                    locked_at = NULL,
                    updated_at = now()
                WHERE id = %s
                """, (job_id,)
            )


def calculate_backoff_seconds(attempts_before_failure: int) -> int:
    """
    Exponential backoff:
    1st failure: 10 seconds
    2nd failure: 20 seconds
    3rd failure: 40 seconds
    max: 300 seconds
    """
    return min(300, 10 * (2 ** attempts_before_failure))


def mark_job_failed(job_id : int, error: str, attempts_before_failure : int):
    """
    Retry the job with backoff until max_attempts is reached.
    If max_attempts is reached, move it to dead-letter state.
    """
    delay_seconds = calculate_backoff_seconds(attempts_before_failure)

    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET attempts = attempts + 1,
                    status = CASE
                        WHEN attempts + 1 >= max_attempts THEN 'dead'
                        ELSE 'queued'
                    END,

                    run_at = CASE
                        WHEN attempts + 1 >= max_attempts THEN run_at  -- If max attempts is reached, stop updating run_at
                        ELSE now() + (%s * interval '1 second')
                    END,
                    
                    last_error = %s,
                    locked_by = NULL,
                    locked_at = NULL,
                    updated_at = now()
                WHERE id = %s
                RETURNING id, status, attempts, max_attempts, run_at, last_error;
                """,
                (delay_seconds, error, job_id)
            )
            return cur.fetchone()


def run_worker(
    worker_id: str | None = None,
    poll_interval: float = 1,
    once : bool = False
):
    """
    If enter "once" as True, so the function run only one round then finish. 
    If not enter "once" or set it False, so the function run forever unless user end it manually.  
    """
    if worker_id == None:
        worker_id = make_worker_id()
    register_worker(worker_id)
    print(f"Worker started: {worker_id}")

    while True:
            heartbeat_worker(worker_id)
            recover_stuck_jobs(timeout_seconds=300)

            job = claim_next_job(worker_id)

            if job == None:
                print("No ready job found")
                if once:           #### if once == True, function end here
                    return
                time.sleep(poll_interval)
                continue
            
            
            
            job_id = job["id"]
            task_name = job["task_name"]
            payload = job["payload"]
            print(f"Claimed job #{job_id}: {task_name}")

            if is_rate_limited(task_name):
                requeue_rate_limited_job(job_id, delay_seconds=60)
                print(f"Job #{job_id} rate limited. Requeued for 60 seconds later.")

                if once:
                    return

                continue

            attempt = start_job_attempt(job_id, worker_id)

            try:
                tasks.execute_task(task_name, payload)
                mark_job_succeeded(job_id)
                finish_job_attempt(attempt["id"], "succeeded", None)
                print(f"Job #{job_id} succeeded.")
            except Exception as exc:
                updated_job = mark_job_failed(job_id, str(exc), job["attempts"])
                finish_job_attempt(attempt["id"], "failed", str(exc))
                print(
                    f"Job #{job_id} failed: {exc}. "
                    f"status={updated_job['status']} "
                    f"attempts={updated_job['attempts']}/{updated_job['max_attempts']} "
                    f"next_run_at={updated_job['run_at']}"
    )
            if once:
                return
            

def start_job_attempt(job_id : int, worker_id : str):
    """
    Add job attempt logs when worker start
    """
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO job_attempts(
                job_id,
                worker_id,
                status,
                started_at
                )
                VALUES(%s, %s, 'running', now())
                RETURNING id, job_id, worker_id, status, started_at
                """,
                (job_id, worker_id)
            )
            return cur.fetchone()
        

def finish_job_attempt(attempt_id : int, status : str, error : str | None = None):
    """
    Add job attempt logs when worker finish
    """
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE job_attempts
                SET status = %s,
                    error = %s,
                    finished_at = now()
                WHERE id = %s
                RETURNING id, job_id, worker_id, status, error, finished_at
                """,
                (status, error, attempt_id)
            )
            return cur.fetchone()
        

def register_worker(worker_id: str):
    hostname = socket.gethostname()

    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO workers (
                    id,
                    hostname,
                    last_heartbeat_at,
                    started_at
                )
                VALUES (%s, %s, now(), now())
                ON CONFLICT (id)
                DO UPDATE SET
                    hostname = EXCLUDED.hostname,
                    last_heartbeat_at = now();
                """,
                (worker_id, hostname),
            )


def heartbeat_worker(worker_id: str):
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE workers
                SET last_heartbeat_at = now()
                WHERE id = %s;
                """,
                (worker_id,),
            )


def recover_stuck_jobs(timeout_seconds: int = 300):
    """
    Move jobs stuck in running state back to queued.
    """
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET status = 'queued',
                    locked_by = NULL,
                    locked_at = NULL,
                    updated_at = now()
                WHERE status = 'running'
                  AND locked_at < now() - (%s * interval '1 second')
                RETURNING id, task_name, locked_by, locked_at;
                """,
                (timeout_seconds,),
            )

            return cur.fetchall()
        

def requeue_rate_limited_job(job_id: int, delay_seconds: int = 10):
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET status = 'queued',
                    run_at = now() + (%s * interval '1 second'),
                    locked_by = NULL,
                    locked_at = NULL,
                    updated_at = now()
                WHERE id = %s;
                """,
                (delay_seconds, job_id),
            )



            


        
        
    
    


