
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    channel as unique_field,
    count(*) as n_records

from "twitch_chat"."public"."channel_summary"
where channel is not null
group by channel
having count(*) > 1



  
  
      
    ) dbt_internal_test