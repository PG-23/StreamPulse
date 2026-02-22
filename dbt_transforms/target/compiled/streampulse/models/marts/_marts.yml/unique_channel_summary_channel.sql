
    
    

select
    channel as unique_field,
    count(*) as n_records

from "twitch_chat"."public"."channel_summary"
where channel is not null
group by channel
having count(*) > 1


