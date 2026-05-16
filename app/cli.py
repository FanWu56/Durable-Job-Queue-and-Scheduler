import typer
import json

import job_queue
import db

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
    
    returned_jobs =job_queue.enqueue_jobs(
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




if __name__ == "__main__":
    app()
