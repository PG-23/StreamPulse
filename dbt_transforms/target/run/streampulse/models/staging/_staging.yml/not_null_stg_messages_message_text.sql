
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select message_text
from "twitch_chat"."public"."stg_messages"
where message_text is null



  
  
      
    ) dbt_internal_test