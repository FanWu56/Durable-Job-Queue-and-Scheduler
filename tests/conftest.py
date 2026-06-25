import os

import pytest
from dotenv import load_dotenv

load_dotenv()

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")

if not TEST_DATABASE_URL:
    raise RuntimeError(
        "Please set TEST_DATABASE_URL before running tests. "
        "Example: set TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/jobqueue_test"
    )

os.environ["DATABASE_URL"] = TEST_DATABASE_URL
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/1")

from app.db import get_conn, init_db  # noqa: E402


@pytest.fixture(autouse=True)
def clean_database():
    """
    Reset database before every test.

    This keeps tests independent.
    """
    init_db()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                TRUNCATE TABLE job_attempts, jobs, workers
                RESTART IDENTITY CASCADE;
                """
            )

    yield

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                TRUNCATE TABLE job_attempts, jobs, workers
                RESTART IDENTITY CASCADE;
                """
            )