"""
JSON to PostgreSQL Loader
Reads raw JSON batch files from data/raw/ and loads them into PostgreSQL.
Moves processed files to data/processed/ to avoid re-loading.
"""

import json
import os
import shutil
import psycopg2
from config import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
    POSTGRES_USER, POSTGRES_PASSWORD, RAW_DATA_PATH,
)

PROCESSED_PATH = os.path.join(os.path.dirname(RAW_DATA_PATH), "processed")


def get_connection():
    """Create a PostgreSQL connection."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def load_file(filepath, cursor):
    """Load a single JSON batch file into the raw_messages table."""
    with open(filepath, "r") as f:
        messages = json.load(f)

    insert_sql = """
        INSERT INTO raw_messages
            (timestamp, channel, username, message, display_name,
             user_id, subscriber, turbo, emotes, badges, color, message_id)
        VALUES
            (%(timestamp)s, %(channel)s, %(username)s, %(message)s,
             %(display_name)s, %(user_id)s, %(subscriber)s, %(turbo)s,
             %(emotes)s, %(badges)s, %(color)s, %(message_id)s)
    """

    for msg in messages:
        cursor.execute(insert_sql, msg)

    return len(messages)


def main():
    """Load all unprocessed JSON files into PostgreSQL."""
    if not os.path.exists(RAW_DATA_PATH):
        print("No raw data directory found.")
        return

    json_files = sorted([
        f for f in os.listdir(RAW_DATA_PATH) if f.endswith(".json")
    ])

    if not json_files:
        print("No JSON files to process.")
        return

    print(f"Found {len(json_files)} file(s) to load.")

    conn = get_connection()
    cursor = conn.cursor()
    os.makedirs(PROCESSED_PATH, exist_ok=True)

    total_loaded = 0

    for filename in json_files:
        filepath = os.path.join(RAW_DATA_PATH, filename)
        try:
            count = load_file(filepath, cursor)
            conn.commit()
            total_loaded += count
            print(f"  Loaded {count} messages from {filename}")

            # Move to processed folder
            shutil.move(filepath, os.path.join(PROCESSED_PATH, filename))

        except Exception as e:
            conn.rollback()
            print(f"  ERROR loading {filename}: {e}")

    cursor.close()
    conn.close()
    print(f"\nDone. Loaded {total_loaded} total messages.")


if __name__ == "__main__":
    main()