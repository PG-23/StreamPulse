"""
Twitch Chat Listener

Connects to Twitch IRC via raw sockets, collects chat messages with full
metadata (badges, emotes, subscriber status), and writes them to disk as
batched JSON files for downstream ingestion into PostgreSQL.

Usage:
    python twitch_chat_listener.py
"""

import json
import logging
import os
import re
import socket
import time
from datetime import datetime, timezone

from config import (
    BATCH_INTERVAL_SECONDS,
    BATCH_SIZE,
    RAW_DATA_PATH,
    TWITCH_CHANNELS,
    TWITCH_NICKNAME,
    TWITCH_TOKEN,
)

__all__ = ["connect_to_twitch", "parse_message", "flush_batch", "listen"]

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

IRC_SERVER = "irc.chat.twitch.tv"
IRC_PORT = 6667
MAX_RECONNECT_ATTEMPTS = 5
RECONNECT_DELAY_SECONDS = 5

# Regex to parse IRC messages with Twitch tags
MSG_PATTERN = re.compile(
    r"^@(?P<tags>\S+) :(?P<user>\w+)!\w+@\w+\.tmi\.twitch\.tv "
    r"PRIVMSG #(?P<channel>\w+) :(?P<message>.+)$"
)

# Fallback pattern for messages without tags
MSG_PATTERN_SIMPLE = re.compile(
    r"^:(?P<user>\w+)!\w+@\w+\.tmi\.twitch\.tv "
    r"PRIVMSG #(?P<channel>\w+) :(?P<message>.+)$"
)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def connect_to_twitch() -> socket.socket | None:
    """
    Establish an authenticated connection to the Twitch IRC server.

    Requests the twitch.tv/tags capability for rich message metadata,
    authenticates with the provided OAuth token, and joins all configured
    channels.

    Returns:
        An open socket ready for message listening, or None if authentication
        fails.
    """
    sock = socket.socket()
    sock.settimeout(10)
    sock.connect((IRC_SERVER, IRC_PORT))

    sock.send("CAP REQ :twitch.tv/tags twitch.tv/commands\r\n".encode("utf-8"))
    sock.send(f"PASS {TWITCH_TOKEN}\r\n".encode("utf-8"))
    sock.send(f"NICK {TWITCH_NICKNAME}\r\n".encode("utf-8"))

    log.info("Waiting for server response...")
    time.sleep(2)

    try:
        response = sock.recv(4096).decode("utf-8", errors="ignore")
        log.debug("Server response: %s", response)

        if "Login authentication failed" in response:
            log.error(
                "Authentication failed. Verify that TWITCH_TOKEN is in the "
                "format 'oauth:your_token_here' and TWITCH_NICKNAME matches "
                "your lowercase Twitch username."
            )
            sock.close()
            return None

        if "Welcome" not in response:
            log.warning("Did not receive welcome message. Check server response.")

    except socket.timeout:
        log.warning("No response from server within timeout period.")

    for channel in TWITCH_CHANNELS:
        channel = channel.strip()
        sock.send(f"JOIN {channel}\r\n".encode("utf-8"))
        log.info("Joined channel: %s", channel)

    sock.settimeout(300)
    return sock


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_tags(tag_string: str) -> dict[str, str]:
    """
    Parse a Twitch IRC tag string into a key-value dictionary.

    Args:
        tag_string: Raw semicolon-delimited tag string from the IRC message.

    Returns:
        Dictionary of tag names to their string values.
    """
    tags = {}
    for tag in tag_string.split(";"):
        if "=" in tag:
            key, val = tag.split("=", 1)
            tags[key] = val
    return tags


def parse_message(raw_line: str) -> dict | None:
    """
    Parse a raw IRC line into a structured message dictionary.

    Attempts to match the full tagged pattern first for rich metadata,
    falling back to a simpler pattern if tags are absent.

    Args:
        raw_line: A single raw line received from the IRC socket.

    Returns:
        A dictionary containing message fields, or None if the line is
        not a PRIVMSG (e.g. JOIN, PART, NOTICE).
    """
    match = MSG_PATTERN.match(raw_line)
    if match:
        tags = parse_tags(match.group("tags"))
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "channel": match.group("channel"),
            "username": match.group("user"),
            "message": match.group("message").strip(),
            "display_name": tags.get("display-name", ""),
            "user_id": tags.get("user-id", ""),
            "subscriber": tags.get("subscriber", "0") == "1",
            "turbo": tags.get("turbo", "0") == "1",
            "emotes": tags.get("emotes", ""),
            "badges": tags.get("badges", ""),
            "color": tags.get("color", ""),
            "message_id": tags.get("id", ""),
        }

    match = MSG_PATTERN_SIMPLE.match(raw_line)
    if match:
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "channel": match.group("channel"),
            "username": match.group("user"),
            "message": match.group("message").strip(),
            "display_name": "",
            "user_id": "",
            "subscriber": False,
            "turbo": False,
            "emotes": "",
            "badges": "",
            "color": "",
            "message_id": "",
        }

    return None


# ---------------------------------------------------------------------------
# Batching
# ---------------------------------------------------------------------------

def flush_batch(batch: list[dict]) -> None:
    """
    Write a batch of parsed messages to a timestamped JSON file on disk.

    Files are written to RAW_DATA_PATH and named using a UTC timestamp
    to avoid collisions when the listener is running continuously.

    Args:
        batch: List of parsed message dictionaries to write.
    """
    if not batch:
        return

    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(RAW_DATA_PATH, f"chat_batch_{ts}.json")

    with open(filepath, "w") as f:
        json.dump(batch, f, indent=2)

    log.info("Flushed %d messages to %s", len(batch), filepath)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def listen() -> None:
    """
    Connect to Twitch IRC and listen for chat messages indefinitely.

    Messages are accumulated into batches and flushed to disk when either
    BATCH_SIZE messages have been collected or BATCH_INTERVAL_SECONDS has
    elapsed, whichever comes first. Automatically attempts reconnection up
    to MAX_RECONNECT_ATTEMPTS times on connection loss before exiting.

    Handles KeyboardInterrupt gracefully by flushing any remaining messages
    before closing the socket.
    """
    sock = connect_to_twitch()
    if sock is None:
        return

    log.info("Listening for messages...")

    buffer = ""
    batch: list[dict] = []
    last_flush = time.time()
    reconnect_attempts = 0

    try:
        while True:
            try:
                data = sock.recv(4096).decode("utf-8", errors="ignore")
            except socket.timeout:
                log.warning("No data received for 5 minutes. Connection may be stale.")
                continue

            if not data:
                if reconnect_attempts >= MAX_RECONNECT_ATTEMPTS:
                    log.error(
                        "Connection lost. Max reconnection attempts (%d) reached. Exiting.",
                        MAX_RECONNECT_ATTEMPTS,
                    )
                    break

                reconnect_attempts += 1
                log.warning(
                    "Connection lost. Reconnecting in %ds (attempt %d/%d)...",
                    RECONNECT_DELAY_SECONDS,
                    reconnect_attempts,
                    MAX_RECONNECT_ATTEMPTS,
                )
                time.sleep(RECONNECT_DELAY_SECONDS)
                sock = connect_to_twitch()
                if sock is None:
                    break
                continue

            reconnect_attempts = 0  # Reset on successful data receipt
            buffer += data
            lines = buffer.split("\r\n")
            buffer = lines.pop()

            for line in lines:
                if line.startswith("PING"):
                    sock.send("PONG :tmi.twitch.tv\r\n".encode("utf-8"))
                    continue

                parsed = parse_message(line)
                if parsed:
                    batch.append(parsed)
                    log.info(
                        "[#%s] %s: %s",
                        parsed["channel"],
                        parsed["username"],
                        parsed["message"],
                    )

            now = time.time()
            if len(batch) >= BATCH_SIZE or (now - last_flush) >= BATCH_INTERVAL_SECONDS:
                flush_batch(batch)
                batch = []
                last_flush = now

    except KeyboardInterrupt:
        log.info("Listener stopped by user.")
    finally:
        flush_batch(batch)
        sock.close()
        log.info("Socket closed. Exiting.")


if __name__ == "__main__":
    listen()