"""
StreamPulse Pipeline DAG
Orchestrates the full pipeline:
  1. Load raw JSON files into PostgreSQL
  2. Run sentiment analysis on new messages
  3. Rebuild dbt models
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
import os
import shutil
import psycopg2
from textblob import TextBlob

# -- Config --
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


# =====================
# Task 1: Load JSON files into PostgreSQL
# =====================
def load_json_to_postgres(**ctx):
    """Read JSON batch files and insert into raw_messages table."""
    if not os.path.exists(RAW_PATH):
        print("No raw data directory found.")
        return

    files = sorted([f for f in os.listdir(RAW_PATH) if f.endswith(".json")])
    if not files:
        print("No JSON files to process.")
        return

    print(f"Found {len(files)} file(s) to load.")
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    os.makedirs(PROCESSED_PATH, exist_ok=True)

    total = 0
    insert_sql = """
        INSERT INTO raw_messages
            (timestamp, channel, username, message, display_name,
             user_id, subscriber, turbo, emotes, badges, color, message_id)
        VALUES
            (%(timestamp)s, %(channel)s, %(username)s, %(message)s,
             %(display_name)s, %(user_id)s, %(subscriber)s, %(turbo)s,
             %(emotes)s, %(badges)s, %(color)s, %(message_id)s)
    """

    for fname in files:
        fpath = os.path.join(RAW_PATH, fname)
        try:
            with open(fpath, "r") as f:
                messages = json.load(f)
            for msg in messages:
                cur.execute(insert_sql, msg)
            conn.commit()
            total += len(messages)
            print(f"  Loaded {len(messages)} messages from {fname}")
            shutil.move(fpath, os.path.join(PROCESSED_PATH, fname))
        except Exception as e:
            conn.rollback()
            print(f"  ERROR loading {fname}: {e}")

    cur.close()
    conn.close()
    print(f"Loaded {total} total messages.")


# =====================
# Task 2: Run sentiment analysis
# =====================
def run_sentiment_analysis(**ctx):
    """Score unscored messages with TextBlob sentiment."""
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()

    # Ensure sentiment columns exist
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

    # Count unscored
    cur.execute(
        "SELECT COUNT(*) FROM raw_messages WHERE sentiment_polarity IS NULL"
    )
    total = cur.fetchone()[0]
    if total == 0:
        print("No unscored messages.")
        cur.close()
        conn.close()
        return

    print(f"Scoring {total} messages...")
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
                label = (
                    "positive" if pol > 0.1
                    else "negative" if pol < -0.1
                    else "neutral"
                )
                cur.execute("""
                    UPDATE raw_messages
                    SET sentiment_polarity=%s, sentiment_subjectivity=%s,
                        sentiment_label=%s
                    WHERE id=%s
                """, (pol, sub, label, msg_id))
            except Exception:
                cur.execute("""
                    UPDATE raw_messages
                    SET sentiment_polarity=0, sentiment_subjectivity=0,
                        sentiment_label='neutral'
                    WHERE id=%s
                """, (msg_id,))

        conn.commit()
        scored += len(rows)
        print(f"  Scored {scored}/{total}")

    cur.close()
    conn.close()
    print(f"Done. Scored {scored} messages.")


# =====================
# DAG Definition
# =====================
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
    description="Load Twitch chat data, score sentiment, rebuild dbt models",
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

    # Pipeline order
    task_load >> task_sentiment >> task_dbt_run >> task_dbt_test