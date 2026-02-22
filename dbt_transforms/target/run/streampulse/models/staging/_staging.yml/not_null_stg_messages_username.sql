
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select username
from "twitch_chat"."public"."stg_messages"
where username is null



  
  
      
    ) dbt_internal_test