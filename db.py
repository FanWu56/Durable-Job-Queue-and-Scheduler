from contextlib import contextmanager
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from app.config import DATABASE_URL


@contextmanager
def get_conn():
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)

    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


####     connect database    
def init_db(schema_path: str | Path = "schema.sql"):
    schema_path = Path(schema_path).resolve()
    schema_sql = schema_path.read_text(encoding="utf-8")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)


def check_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT now() AS current_time;")
            return cur.fetchone()