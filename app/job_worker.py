import socket
import uuid
import time

##################  Below is source code python file
import db
import tasks

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


def mark_job_failed(job_id : int, err_reason: str, attempts_before_failure : int):
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
                (delay_seconds, err_reason, job_id)
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
    print(f"Worker started: {worker_id}")

    while True:
            job = claim_next_job(worker_id)

            if job == None:
                print("No already job found")
                if once:           #### if once == True, function end here
                    return
                time.sleep(poll_interval)
                continue
            
            
            
            job_id = job["id"]
            task_name = job["task_name"]
            payload = job["payload"]

            try:
                tasks.execute_task(task_name, payload)
                mark_job_succeeded(job_id)
                print(f"Job #{job_id} succeeded.")
            except Exception as exc:
                updated_job = mark_job_failed(job_id, str(exc), job["attempts"])
                print(
                    f"Job #{job_id} failed: {exc}. "
                    f"status={updated_job['status']} "
                    f"attempts={updated_job['attempts']}/{updated_job['max_attempts']} "
                    f"next_run_at={updated_job['run_at']}"
    )
            if once:
                return

            


        
        
    
    


