"""
Sentiment Analysis
Reads unscored messages from PostgreSQL, scores them using TextBlob,
and writes sentiment scores back to the database.
"""

import psycopg2
from textblob import TextBlob
from config import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
    POSTGRES_USER, POSTGRES_PASSWORD,
)

BATCH_SIZE = 500


def get_connection():
    """Create a PostgreSQL connection."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def ensure_sentiment_columns(cursor):
    """Add sentiment columns to raw_messages if they don't exist."""
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


def classify_sentiment(polarity):
    """Convert polarity score to a human-readable label."""
    if polarity > 0.1:
        return "positive"
    elif polarity < -0.1:
        return "negative"
    else:
        return "neutral"


def score_messages():
    """Fetch unscored messages, analyze sentiment, and update the database."""
    conn = get_connection()
    cursor = conn.cursor()

    # Ensure columns exist
    ensure_sentiment_columns(cursor)
    conn.commit()

    # Count unscored messages
    cursor.execute(
        "SELECT COUNT(*) FROM raw_messages WHERE sentiment_polarity IS NULL"
    )
    total = cursor.fetchone()[0]

    if total == 0:
        print("No unscored messages found.")
        cursor.close()
        conn.close()
        return

    print(f"Found {total} unscored messages. Scoring...")

    scored = 0
    offset = 0

    while offset < total:
        # Fetch a batch of unscored messages
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

        # Score each message
        for msg_id, message in rows:
            try:
                blob = TextBlob(message)
                polarity = round(blob.sentiment.polarity, 4)
                subjectivity = round(blob.sentiment.subjectivity, 4)
                label = classify_sentiment(polarity)

                cursor.execute("""
                    UPDATE raw_messages
                    SET sentiment_polarity = %s,
                        sentiment_subjectivity = %s,
                        sentiment_label = %s
                    WHERE id = %s
                """, (polarity, subjectivity, label, msg_id))

            except Exception as e:
                print(f"  Error scoring message {msg_id}: {e}")
                cursor.execute("""
                    UPDATE raw_messages
                    SET sentiment_polarity = 0,
                        sentiment_subjectivity = 0,
                        sentiment_label = 'neutral'
                    WHERE id = %s
                """, (msg_id,))

        conn.commit()
        scored += len(rows)
        print(f"  Scored {scored}/{total} messages")

    cursor.close()
    conn.close()
    print(f"\nDone. Scored {scored} messages total.")


if __name__ == "__main__":
    score_messages()