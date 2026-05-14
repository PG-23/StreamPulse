"""
StreamPulse Pipeline DAG

Orchestrates the full StreamPulse pipeline on a 5-minute schedule:
  1. Load raw JSON batch files from disk into PostgreSQL
  2. Score new messages with TextBlob sentiment analysis
  3. Rebuild dbt models (staging → intermediate → marts)
  4. Run dbt schema and data quality tests
"""

import json
import logging
import os
import shutil
from contextlib import contextmanager
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textblob import TextBlob

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "twitch_chat"),
    "user": os.getenv("POSTGRES_USER", "pipeline"),
    "password": os.getenv("POSTGRES_PASSWORD", "pipeline_password"),
}

RAW_PATH = "/opt/airflow/data/raw"
PROCESSED_PATH = "/opt/airflow/data/processed"
DBT_DIR = "/opt/airflow/dbt_transforms"
SENTIMENT_BATCH_SIZE = 500

POLARITY_POSITIVE_THRESHOLD = 0.1
POLARITY_NEGATIVE_THRESHOLD = -0.1

INSERT_SQL = """
    INSERT INTO raw_messages
        (timestamp, channel, username, message, display_name,
         user_id, subscriber, turbo, emotes, badges, color, message_id)
    VALUES
        (%(timestamp)s, %(channel)s, %(username)s, %(message)s,
         %(display_name)s, %(user_id)s, %(subscriber)s, %(turbo)s,
         %(emotes)s, %(badges)s, %(color)s, %(message_id)s)
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@contextmanager
def get_connection():
    """
    Context manager that yields an open psycopg2 connection.

    Ensures the connection is always closed on exit, even if an exception
    is raised during task execution.

    Yields:
        An open psycopg2 connection object.
    """
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    try:
        yield conn
    finally:
        conn.close()


def classify_sentiment(polarity: float) -> str:
    """
    Convert a TextBlob polarity score to a human-readable label.

    Args:
        polarity: Float in range [-1.0, 1.0].

    Returns:
        One of 'positive', 'negative', or 'neutral'.
    """
    if polarity > POLARITY_POSITIVE_THRESHOLD:
        return "positive"
    elif polarity < POLARITY_NEGATIVE_THRESHOLD:
        return "negative"
    return "neutral"


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def load_json_to_postgres(**ctx) -> None:
    """
    Airflow task: load raw JSON batch files into the raw_messages table.

    Scans RAW_PATH for JSON files, inserts each into PostgreSQL within its
    own transaction, and moves successfully loaded files to PROCESSED_PATH.
    Failed files are logged and skipped without halting the task.
    """
    if not os.path.exists(RAW_PATH):
        log.warning("Raw data directory not found: %s", RAW_PATH)
        return

    files = sorted(f for f in os.listdir(RAW_PATH) if f.endswith(".json"))
    if not files:
        log.info("No JSON files found to process.")
        return

    log.info("Found %d file(s) to load.", len(files))
    os.makedirs(PROCESSED_PATH, exist_ok=True)
    total_loaded = 0
    total_failed = 0

    with get_connection() as conn:
        cur = conn.cursor()

        for fname in files:
            fpath = os.path.join(RAW_PATH, fname)
            try:
                with open(fpath, "r") as f:
                    messages = json.load(f)
                for msg in messages:
                    cur.execute(INSERT_SQL, msg)
                conn.commit()
                total_loaded += len(messages)
                log.info("Loaded %d messages from %s.", len(messages), fname)
                shutil.move(fpath, os.path.join(PROCESSED_PATH, fname))
            except Exception as e:
                conn.rollback()
                total_failed += 1
                log.error("Failed to load %s: %s. Skipping.", fname, e)

        cur.close()

    log.info(
        "Load complete. %d messages loaded. %d file(s) failed.",
        total_loaded,
        total_failed,
    )


def run_sentiment_analysis(**ctx) -> None:
    """
    Airflow task: score unscored messages with TextBlob sentiment.

    Adds sentiment columns if they do not exist, then processes all rows
    where sentiment_polarity IS NULL in batches of SENTIMENT_BATCH_SIZE.
    Messages that fail scoring default to neutral rather than being skipped.
    """
    with get_connection() as conn:
        cur = conn.cursor()

        for col, dtype in [
            ("sentiment_polarity", "FLOAT"),
            ("sentiment_subjectivity", "FLOAT"),
            ("sentiment_label", "VARCHAR(20)"),
        ]:
            cur.execute(f"""
                DO $$ BEGIN
                    ALTER TABLE raw_messages ADD COLUMN {col} {dtype};
                EXCEPTION WHEN duplicate_column THEN NULL;
                END $$;
            """)
        conn.commit()

        cur.execute(
            "SELECT COUNT(*) FROM raw_messages WHERE sentiment_polarity IS NULL"
        )
        total = cur.fetchone()[0]

        if total == 0:
            log.info("No unscored messages found.")
            cur.close()
            return

        log.info("Scoring %d messages...", total)
        scored = 0

        while scored < total:
            cur.execute("""
                SELECT id, message FROM raw_messages
                WHERE sentiment_polarity IS NULL
                ORDER BY id LIMIT %s
            """, (SENTIMENT_BATCH_SIZE,))
            rows = cur.fetchall()
            if not rows:
                break

            for msg_id, message in rows:
                try:
                    blob = TextBlob(message)
                    pol = round(blob.sentiment.polarity, 4)
                    sub = round(blob.sentiment.subjectivity, 4)
                    label = classify_sentiment(pol)
                except Exception as e:
                    log.warning("Failed to score message %d: %s. Defaulting to neutral.", msg_id, e)
                    pol, sub, label = 0.0, 0.0, "neutral"

                cur.execute("""
                    UPDATE raw_messages
                    SET sentiment_polarity=%s, sentiment_subjectivity=%s,
                        sentiment_label=%s
                    WHERE id=%s
                """, (pol, sub, label, msg_id))

            conn.commit()
            scored += len(rows)
            log.info("Progress: %d/%d messages scored.", scored, total)

        cur.close()
        log.info("Sentiment scoring complete. %d messages processed.", scored)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "streampulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="streampulse_pipeline",
    default_args=default_args,
    description="Load Twitch chat data, score sentiment, and rebuild dbt models",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["streampulse"],
) as dag:

    task_load = PythonOperator(
        task_id="load_json_to_postgres",
        python_callable=load_json_to_postgres,
    )

    task_sentiment = PythonOperator(
        task_id="run_sentiment_analysis",
        python_callable=run_sentiment_analysis,
    )

    task_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir .",
    )

    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir .",
    )

    task_load >> task_sentiment >> task_dbt_run >> task_dbt_test