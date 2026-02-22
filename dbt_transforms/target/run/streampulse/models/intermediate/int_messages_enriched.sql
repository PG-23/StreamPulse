
  create view "twitch_chat"."public"."int_messages_enriched__dbt_tmp"
    
    
  as (
    -- Intermediate model: enrich messages with derived fields

with messages as (
    select * from "twitch_chat"."public"."stg_messages"
),

enriched as (
    select
        *,

        -- Time bucketing for aggregations
        date_trunc('minute', message_at) as minute_bucket,
        date_trunc('hour', message_at) as hour_bucket,
        date_trunc('day', message_at) as day_bucket,

        -- Message characteristics
        case
            when message_length < 10 then 'short'
            when message_length < 50 then 'medium'
            else 'long'
        end as message_size,

        -- Detect if message is all caps (possible excitement/yelling)
        case
            when message_text = upper(message_text)
                and message_length > 3
            then true
            else false
        end as is_all_caps,

        -- Detect if message contains emotes
        case
            when emotes_raw is not null and emotes_raw != ''
            then true
            else false
        end as has_emotes,

        -- Simple word count
        array_length(
            string_to_array(trim(message_text), ' '), 1
        ) as word_count,

        -- Sentiment is scored (true/false)
        case
            when sentiment_polarity is not null
            then true
            else false
        end as is_sentiment_scored

    from messages
)

select * from enriched
  );