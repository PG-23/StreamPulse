
  
    

  create  table "twitch_chat"."public"."chat_activity__dbt_tmp"
  
  
    as
  
  (
    -- Mart: per-minute chat activity and sentiment metrics by channel

with enriched as (
    select * from "twitch_chat"."public"."int_messages_enriched"
),

per_minute as (
    select
        minute_bucket,
        channel,

        -- Volume metrics
        count(*) as message_count,
        count(distinct username) as unique_chatters,

        -- Message characteristics
        round(avg(message_length), 1) as avg_message_length,
        round(avg(word_count), 1) as avg_word_count,

        -- Behavior signals
        count(*) filter (where is_all_caps) as all_caps_count,
        count(*) filter (where has_emotes) as emote_message_count,
        count(*) filter (where is_subscriber) as subscriber_message_count,

        -- Sentiment metrics
        round(avg(sentiment_polarity)::numeric, 4) as avg_sentiment,
        round(avg(sentiment_subjectivity)::numeric, 4) as avg_subjectivity,
        count(*) filter (where sentiment_label = 'positive') as positive_count,
        count(*) filter (where sentiment_label = 'negative') as negative_count,
        count(*) filter (where sentiment_label = 'neutral') as neutral_count,

        -- Ratios
        round(
            count(*) filter (where is_all_caps)::numeric / nullif(count(*), 0), 3
        ) as all_caps_ratio,
        round(
            count(*) filter (where has_emotes)::numeric / nullif(count(*), 0), 3
        ) as emote_ratio,
        round(
            count(*) filter (where is_subscriber)::numeric / nullif(count(*), 0), 3
        ) as subscriber_ratio,
        round(
            count(*) filter (where sentiment_label = 'positive')::numeric
            / nullif(count(*) filter (where is_sentiment_scored), 0), 3
        ) as positive_ratio,
        round(
            count(*) filter (where sentiment_label = 'negative')::numeric
            / nullif(count(*) filter (where is_sentiment_scored), 0), 3
        ) as negative_ratio

    from enriched
    group by minute_bucket, channel
)

select * from per_minute
order by minute_bucket, channel
  );
  