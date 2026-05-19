from datetime import datetime, timedelta, timezone
from typing import Any
from psycopg.types.json import Jsonb

from db import get_conn

def enqueue_jobs(
        task_name : str,
        payload: dict[str : Any] | None = None,
        priority: int = 0,
        delay_seconds: int = 0,
        max_attempts: int = 3,
):

    """
    Insert a new job into the jobs table.
    """
    if payload == None:
        payload ={}

    run_at = datetime.now() + timedelta(seconds = delay_seconds)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
            INSERT INTO jobs(
                task_name,
                payload,
                priority,
                run_at,
                max_attempts
            )
            VALUES(%s, %s, %s, %s, %s)
            RETURNING id, task_name, payload, status, priority, run_at, created_at;
            """,

            (
                task_name,
                Jsonb(payload),
                priority,
                run_at,
                max_attempts
            )
            )
            

            return cur.fetchone()
        
def get_recent_jobs(limits : int = 20):
    """
    get recent jobs for debugging
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    task_name,
                    status,
                    priority,
                    attempts,
                    max_attempts,
                    run_at,
                    created_at
                FROM jobs
                ORDER BY id DESC
                LIMIT %s
                """,
                (limits,)
            )
            return cur.fetchall()


def get_job_attempts(job_id : int):
    """
    get job attempts logs
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM job_attempts
                WHERE job_id = %s
                ORDER BY id ASC
                """, (job_id,)
            )
            return cur.fetchall()

