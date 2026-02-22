-- StreamPulse Database Schema

-- Raw messages table: stores every chat message as-is
CREATE TABLE IF NOT EXISTS raw_messages (
    id              SERIAL PRIMARY KEY,
    timestamp       TIMESTAMPTZ NOT NULL,
    channel         VARCHAR(100) NOT NULL,
    username        VARCHAR(100) NOT NULL,
    message         TEXT NOT NULL,
    display_name    VARCHAR(100),
    user_id         VARCHAR(50),
    subscriber      BOOLEAN DEFAULT FALSE,
    turbo           BOOLEAN DEFAULT FALSE,
    emotes          TEXT,
    badges          TEXT,
    color           VARCHAR(20),
    message_id      VARCHAR(100),
    loaded_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Index for common query patterns
CREATE INDEX IF NOT EXISTS idx_raw_messages_timestamp ON raw_messages(timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_messages_channel ON raw_messages(channel);
CREATE INDEX IF NOT EXISTS idx_raw_messages_username ON raw_messages(username);
CREATE INDEX IF NOT EXISTS idx_raw_messages_channel_timestamp ON raw_messages(channel, timestamp);