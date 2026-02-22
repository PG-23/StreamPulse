import os
from dotenv import load_dotenv

load_dotenv()

# Twitch
TWITCH_TOKEN = os.getenv("TWITCH_TOKEN")
TWITCH_NICKNAME = os.getenv("TWITCH_NICKNAME")
TWITCH_CHANNELS = os.getenv("TWITCH_CHANNELS", "#xqc").split(",")

# PostgreSQL
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB = os.getenv("POSTGRES_DB", "twitch_chat")
POSTGRES_USER = os.getenv("POSTGRES_USER", "pipeline")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pipeline_password")

# Ingestion
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))
BATCH_INTERVAL_SECONDS = int(os.getenv("BATCH_INTERVAL_SECONDS", 60))
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "./data/raw")