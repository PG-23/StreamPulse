"""
Twitch Chat Listener
Connects to Twitch IRC via websocket, collects chat messages,
and writes them to disk as batched JSON files.
"""

import json
import os
import time
import socket
import re
from datetime import datetime, timezone
from config import (
    TWITCH_TOKEN, TWITCH_NICKNAME, TWITCH_CHANNELS,
    BATCH_SIZE, BATCH_INTERVAL_SECONDS, RAW_DATA_PATH,
)

# Twitch IRC server details
IRC_SERVER = "irc.chat.twitch.tv"
IRC_PORT = 6667

# Regex to parse IRC messages with tags
MSG_PATTERN = re.compile(
    r"^@(?P<tags>\S+) :(?P<user>\w+)!\w+@\w+\.tmi\.twitch\.tv "
    r"PRIVMSG #(?P<channel>\w+) :(?P<message>.+)$"
)

# Simpler fallback pattern without tags
MSG_PATTERN_SIMPLE = re.compile(
    r"^:(?P<user>\w+)!\w+@\w+\.tmi\.twitch\.tv "
    r"PRIVMSG #(?P<channel>\w+) :(?P<message>.+)$"
)


def connect_to_twitch():
    """Establish connection to Twitch IRC server."""
    sock = socket.socket()
    sock.settimeout(10)  # 10 second timeout for initial connection
    sock.connect((IRC_SERVER, IRC_PORT))

    # Request tags capability for richer metadata
    sock.send("CAP REQ :twitch.tv/tags twitch.tv/commands\r\n".encode("utf-8"))
    sock.send(f"PASS {TWITCH_TOKEN}\r\n".encode("utf-8"))
    sock.send(f"NICK {TWITCH_NICKNAME}\r\n".encode("utf-8"))

    # Wait for and print server response to check auth
    print("Waiting for server response...")
    time.sleep(2)

    try:
        response = sock.recv(4096).decode("utf-8", errors="ignore")
        print(f"Server response:\n{response}")

        if "Login authentication failed" in response:
            print("\nERROR: Authentication failed!")
            print("Check that your .env has:")
            print("  - TWITCH_TOKEN in format: oauth:your_token_here")
            print("  - TWITCH_NICKNAME is your lowercase Twitch username")
            sock.close()
            return None

        if "Welcome" not in response:
            print("\nWARNING: Did not receive welcome message.")
            print("Full response above may contain clues.")

    except socket.timeout:
        print("WARNING: No response from server within timeout.")

    # Join channels
    for channel in TWITCH_CHANNELS:
        channel = channel.strip()
        sock.send(f"JOIN {channel}\r\n".encode("utf-8"))
        print(f"Joined {channel}")

    # Set to blocking with longer timeout for message listening
    sock.settimeout(300)

    return sock


def parse_tags(tag_string):
    """Parse IRC tags into a dictionary."""
    tags = {}
    for tag in tag_string.split(";"):
        if "=" in tag:
            key, val = tag.split("=", 1)
            tags[key] = val
    return tags


def parse_message(raw_line):
    """Parse a raw IRC line into a structured message dict."""
    # Try tagged pattern first
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

    # Fallback to simple pattern
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


def flush_batch(batch):
    """Write a batch of messages to a JSON file on disk."""
    if not batch:
        return

    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(RAW_DATA_PATH, f"chat_batch_{ts}.json")

    with open(filepath, "w") as f:
        json.dump(batch, f, indent=2)

    print(f"Flushed {len(batch)} messages to {filepath}")


def listen():
    """Main loop: connect, listen, batch, and flush."""
    sock = connect_to_twitch()
    if sock is None:
        return

    print("\nListening for messages...\n")

    buffer = ""
    batch = []
    last_flush = time.time()

    try:
        while True:
            try:
                data = sock.recv(4096).decode("utf-8", errors="ignore")
            except socket.timeout:
                print("No data received for 5 minutes. Connection may be stale.")
                continue

            if not data:
                print("Connection lost. Reconnecting in 5 seconds...")
                time.sleep(5)
                sock = connect_to_twitch()
                if sock is None:
                    return
                continue

            buffer += data
            lines = buffer.split("\r\n")
            buffer = lines.pop()  # Keep incomplete line in buffer

            for line in lines:
                # Respond to PINGs to stay connected
                if line.startswith("PING"):
                    sock.send("PONG :tmi.twitch.tv\r\n".encode("utf-8"))
                    continue

                parsed = parse_message(line)
                if parsed:
                    batch.append(parsed)
                    print(f"[#{parsed['channel']}] {parsed['username']}: {parsed['message']}")

            # Flush batch if size or time threshold reached
            now = time.time()
            if len(batch) >= BATCH_SIZE or (now - last_flush) >= BATCH_INTERVAL_SECONDS:
                flush_batch(batch)
                batch = []
                last_flush = now

    except KeyboardInterrupt:
        print("\nStopping listener...")
        flush_batch(batch)  # Flush remaining messages
        sock.close()
        print("Done.")


if __name__ == "__main__":
    listen()