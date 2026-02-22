
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select channel
from "twitch_chat"."public"."stg_messages"
where channel is null



  
  
      
    ) dbt_internal_test