
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select publication_id
from "guardian_dw"."analytics_analytics"."dim_publications"
where publication_id is null



  
  
      
    ) dbt_internal_test