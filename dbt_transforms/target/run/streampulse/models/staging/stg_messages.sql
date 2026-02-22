
  create view "twitch_chat"."public"."stg_messages__dbt_tmp"
    
    
  as (
    -- Staging model: clean and standardize raw messages

with source as (
    select * from "twitch_chat"."public"."raw_messages"
),

cleaned as (
    select
        id,
        timestamp as message_at,
        lower(trim(channel)) as channel,
        lower(trim(username)) as username,
        trim(message) as message_text,
        coalesce(nullif(trim(display_name), ''), username) as display_name,
        nullif(trim(user_id), '') as user_id,
        subscriber as is_subscriber,
        turbo as is_turbo,
        nullif(trim(emotes), '') as emotes_raw,
        nullif(trim(badges), '') as badges_raw,
        nullif(trim(color), '') as user_color,
        nullif(trim(message_id), '') as message_id,
        length(trim(message)) as message_length,

        -- Sentiment fields
        sentiment_polarity,
        sentiment_subjectivity,
        sentiment_label,

        loaded_at
    from source
    where trim(message) != ''
)

select * from cleaned
  );