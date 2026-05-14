"""
Sentiment Analysis

Reads unscored messages from PostgreSQL, scores them using TextBlob,
and writes polarity, subjectivity, and sentiment label back to the database.

Usage:
    python sentiment_analysis.py
"""

import logging
from contextlib import contextmanager

import psycopg2
from textblob import TextBlob

from config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)

__all__ = ["get_connection", "classify_sentiment", "score_messages"]

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

BATCH_SIZE = 500
POLARITY_POSITIVE_THRESHOLD = 0.1
POLARITY_NEGATIVE_THRESHOLD = -0.1


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


def ensure_sentiment_columns(cursor) -> None:
    """
    Add sentiment columns to raw_messages if they do not already exist.

    Uses a PL/pgSQL block to safely handle the duplicate_column exception,
    making this function idempotent and safe to call on every run.

    Args:
        cursor: An open psycopg2 cursor.
    """
    columns = {
        "sentiment_polarity": "FLOAT",
        "sentiment_subjectivity": "FLOAT",
        "sentiment_label": "VARCHAR(20)",
    }
    for col, dtype in columns.items():
        cursor.execute(f"""
            DO $$
            BEGIN
                ALTER TABLE raw_messages ADD COLUMN {col} {dtype};
            EXCEPTION
                WHEN duplicate_column THEN NULL;
            END $$;
        """)


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def classify_sentiment(polarity: float) -> str:
    """
    Convert a TextBlob polarity score into a human-readable label.

    Args:
        polarity: A float in the range [-1.0, 1.0] where -1 is most negative
                  and +1 is most positive.

    Returns:
        One of 'positive', 'negative', or 'neutral'.
    """
    if polarity > POLARITY_POSITIVE_THRESHOLD:
        return "positive"
    elif polarity < POLARITY_NEGATIVE_THRESHOLD:
        return "negative"
    return "neutral"


def score_messages() -> None:
    """
    Fetch all unscored messages from PostgreSQL and apply sentiment scoring.

    Processes messages in batches of BATCH_SIZE to avoid loading the full
    dataset into memory. Each message is scored for polarity and subjectivity
    using TextBlob. Failed messages default to neutral sentiment rather than
    being skipped, ensuring no rows are left unscored after a run.
    """
    with get_connection() as conn:
        cursor = conn.cursor()

        ensure_sentiment_columns(cursor)
        conn.commit()

        cursor.execute(
            "SELECT COUNT(*) FROM raw_messages WHERE sentiment_polarity IS NULL"
        )
        total = cursor.fetchone()[0]

        if total == 0:
            log.info("No unscored messages found. Exiting.")
            cursor.close()
            return

        log.info("Found %d unscored messages. Starting scoring...", total)

        scored = 0

        while scored < total:
            cursor.execute("""
                SELECT id, message
                FROM raw_messages
                WHERE sentiment_polarity IS NULL
                ORDER BY id
                LIMIT %s
            """, (BATCH_SIZE,))

            rows = cursor.fetchall()
            if not rows:
                break

            for msg_id, message in rows:
                try:
                    blob = TextBlob(message)
                    polarity = round(blob.sentiment.polarity, 4)
                    subjectivity = round(blob.sentiment.subjectivity, 4)
                    label = classify_sentiment(polarity)
                except Exception as e:
                    log.warning("Failed to score message %d: %s. Defaulting to neutral.", msg_id, e)
                    polarity, subjectivity, label = 0.0, 0.0, "neutral"

                cursor.execute("""
                    UPDATE raw_messages
                    SET sentiment_polarity = %s,
                        sentiment_subjectivity = %s,
                        sentiment_label = %s
                    WHERE id = %s
                """, (polarity, subjectivity, label, msg_id))

            conn.commit()
            scored += len(rows)
            log.info("Progress: %d/%d messages scored.", scored, total)

        cursor.close()
        log.info("Scoring complete. %d messages processed.", scored)


if __name__ == "__main__":
    score_messages()