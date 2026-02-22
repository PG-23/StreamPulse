
  
    

  create  table "twitch_chat"."public"."channel_summary__dbt_tmp"
  
  
    as
  
  (
    -- Mart: overall summary statistics per channel

with enriched as (
    select * from "twitch_chat"."public"."int_messages_enriched"
),

summary as (
    select
        channel,
        count(*) as total_messages,
        count(distinct username) as unique_chatters,
        min(message_at) as first_message_at,
        max(message_at) as last_message_at,

        -- Average activity
        round(avg(message_length), 1) as avg_message_length,
        round(avg(word_count), 1) as avg_word_count,

        -- Sentiment overview
        round(avg(sentiment_polarity)::numeric, 4) as avg_sentiment,
        round(avg(sentiment_subjectivity)::numeric, 4) as avg_subjectivity,
        count(*) filter (where sentiment_label = 'positive') as positive_count,
        count(*) filter (where sentiment_label = 'negative') as negative_count,
        count(*) filter (where sentiment_label = 'neutral') as neutral_count,

        -- Peak activity
        (
            select minute_bucket
            from "twitch_chat"."public"."int_messages_enriched" sub
            where sub.channel = enriched.channel
            group by minute_bucket
            order by count(*) desc
            limit 1
        ) as peak_minute,

        -- Behavior breakdown
        round(
            count(*) filter (where is_all_caps)::numeric / nullif(count(*), 0), 3
        ) as all_caps_ratio,
        round(
            count(*) filter (where has_emotes)::numeric / nullif(count(*), 0), 3
        ) as emote_ratio,
        round(
            count(*) filter (where is_subscriber)::numeric / nullif(count(*), 0), 3
        ) as subscriber_ratio,

        -- Top chatter
        (
            select username
            from "twitch_chat"."public"."int_messages_enriched" sub
            where sub.channel = enriched.channel
            group by username
            order by count(*) desc
            limit 1
        ) as top_chatter

    from enriched
    group by channel
)

select * from summary
  );
  