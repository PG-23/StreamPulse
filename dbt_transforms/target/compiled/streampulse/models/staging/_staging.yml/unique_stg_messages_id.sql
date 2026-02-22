
    
    

select
    id as unique_field,
    count(*) as n_records

from "twitch_chat"."public"."stg_messages"
where id is not null
group by id
having count(*) > 1


