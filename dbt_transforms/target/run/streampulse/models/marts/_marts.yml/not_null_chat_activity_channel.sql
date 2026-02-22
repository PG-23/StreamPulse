
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select channel
from "twitch_chat"."public"."chat_activity"
where channel is null



  
  
      
    ) dbt_internal_test