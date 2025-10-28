
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select fact_keyword_id
from "guardian_dw"."analytics_analytics"."fact_article_keywords"
where fact_keyword_id is null



  
  
      
    ) dbt_internal_test