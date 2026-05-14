"""
JSON to PostgreSQL Loader

Reads raw JSON batch files from data/raw/, inserts them into the
raw_messages PostgreSQL table, and moves successfully processed files
to data/processed/ to prevent re-loading on subsequent runs.

Usage:
    python load_to_postgres.py
"""

import json
import logging
import os
import shutil
from contextlib import contextmanager

import psycopg2

from config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    RAW_DATA_PATH,
)

__all__ = ["get_connection", "load_file", "main"]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROCESSED_PATH = os.path.join(os.path.dirname(RAW_DATA_PATH), "processed")

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
# Database
# ---------------------------------------------------------------------------

@contextmanager
def get_connection():
    """
    Context manager that yields an open psycopg2 connection.

    Ensures the connection is always closed on exit, even if an exception
    is raised during processing.

    Yields:
        An open psycopg2 connection object.
    """
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load_file(filepath: str, cursor) -> int:
    """
    Load a single JSON batch file into the raw_messages table.

    Reads the file, iterates over each message dict, and executes a
    parameterized INSERT for each row. The caller is responsible for
    committing or rolling back the transaction.

    Args:
        filepath: Absolute path to the JSON batch file.
        cursor: An open psycopg2 cursor.

    Returns:
        The number of messages inserted.
    """
    with open(filepath, "r") as f:
        messages = json.load(f)

    for msg in messages:
        cursor.execute(INSERT_SQL, msg)

    return len(messages)


def main() -> None:
    """
    Load all unprocessed JSON batch files into PostgreSQL.

    Scans RAW_DATA_PATH for JSON files, loads each into raw_messages within
    its own transaction, and moves successfully loaded files to PROCESSED_PATH.
    Files that fail to load are logged and skipped without halting the run,
    allowing partial recovery on the next execution.
    """
    if not os.path.exists(RAW_DATA_PATH):
        log.warning("Raw data directory not found: %s", RAW_DATA_PATH)
        return

    json_files = sorted(f for f in os.listdir(RAW_DATA_PATH) if f.endswith(".json"))

    if not json_files:
        log.info("No JSON files found to process. Exiting.")
        return

    log.info("Found %d file(s) to load.", len(json_files))
    os.makedirs(PROCESSED_PATH, exist_ok=True)

    total_loaded = 0
    total_failed = 0

    with get_connection() as conn:
        cursor = conn.cursor()

        for filename in json_files:
            filepath = os.path.join(RAW_DATA_PATH, filename)
            try:
                count = load_file(filepath, cursor)
                conn.commit()
                total_loaded += count
                log.info("Loaded %d messages from %s.", count, filename)
                shutil.move(filepath, os.path.join(PROCESSED_PATH, filename))
            except Exception as e:
                conn.rollback()
                total_failed += 1
                log.error("Failed to load %s: %s. Skipping.", filename, e)

        cursor.close()

    log.info(
        "Run complete. %d messages loaded across %d file(s). %d file(s) failed.",
        total_loaded,
        len(json_files) - total_failed,
        total_failed,
    )


if __name__ == "__main__":
    main()