
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select article_id
from "guardian_dw"."analytics_analytics"."fact_articles"
where article_id is null



  
  
      
    ) dbt_internal_test