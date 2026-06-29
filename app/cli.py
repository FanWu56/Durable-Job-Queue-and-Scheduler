import typer
import json

from app import job_queue
from app import db
from app import job_worker

app = typer.Typer()


@app.command()
def init_db():
    """
    Create database tables from schema.sql.
    """
    db.init_db()
    typer.echo("Database initialized.")


@app.command()
def health():
    """
    Check PostgreSQL connection.
    """
    result = db.check_db()
    typer.echo(f"PostgreSQL OK: {result['current_time']}")

@app.command()
def enqueue(
    task_name : str,
    payload = typer.Option("{}", "--payload"),
    priority = typer.Option(0, "--priority"),
    delay_seconds : int = typer.Option(0, "--delay_seconds"),
    max_attempts = typer.Option(3, "--max_attempts")
):
    """
    add a new job into jobs queue 
    """

    try:
        phrased_payload = json.loads(payload)
    except json.JSONDecodeError:
        raise typer.BadParameter("payload must be valid json")
    
    if not isinstance(phrased_payload, dict):
        raise typer.BadParameter("payload must be valid json object")
    
    returned_jobs =job_queue.enqueue_job(
        task_name,
        phrased_payload,
        priority,
        delay_seconds,
        max_attempts
    )

    typer.echo(
        f"task name = {returned_jobs['task_name']}\n"
        f"payload = {returned_jobs['payload']}\n"
        f"priority = {returned_jobs['priority']}\n"
        f"run at = {returned_jobs['run_at']}\n"
    )

@app.command()
def jobs(limits: int = 20):
    displayed_jobs = job_queue.get_recent_jobs()

    if not displayed_jobs:
        typer.echo("No jobs find")

    for a_single_job in displayed_jobs:
        typer.echo(
            f"#{a_single_job['id']} "
            f"{a_single_job['task_name']} "
            f"status={a_single_job['status']} "
            f"priority={a_single_job['priority']} "
            f"attempts={a_single_job['attempts']}/{a_single_job['max_attempts']} "
            f"run_at={a_single_job['run_at']}"
        )


@app.command()
def worker(
    worker_id: str | None = typer.Option(None, "--worker_id"),
    poll_interval: float | None = typer.Option(1, "--poll_interval"),
    once: bool | None = typer.Option(False, "--once")
):
    job_worker.run_worker(worker_id, poll_interval, once)


@app.command()
def attempts(job_id : int):
    """
    Show execution attempts for a job.
    """
    row = job_queue.get_job_attempts(job_id)

    if row == None:
        typer.echo(f"job_id #: {job_id} not exist")
        return
    
    for i in row:
        typer.echo(
            f"attempt #{i['id']}\n "
            f"job=#{i['job_id']}\n "
            f"worker={i['worker_id']}\n "
            f"status={i['status']}\n "
            f"started_at={i['started_at']}\n "
            f"finished_at={i['finished_at']}\n "
            f"error={i['error']}"
    )
        
@app.command("workers")
def workers():
    """
    Show registered workers.
    """
    rows = job_queue.get_workers()

    if not rows:
        typer.echo("No workers found.")
        return

    for row in rows:
        typer.echo(
            f"{row['id']} "
            f"hostname={row['hostname']} "
            f"last_heartbeat_at={row['last_heartbeat_at']} "
            f"started_at={row['started_at']}"
        )



if __name__ == "__main__":
    app()
