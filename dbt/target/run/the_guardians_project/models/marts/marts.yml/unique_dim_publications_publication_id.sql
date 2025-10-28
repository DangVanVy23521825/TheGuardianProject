
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    publication_id as unique_field,
    count(*) as n_records

from "guardian_dw"."analytics_analytics"."dim_publications"
where publication_id is not null
group by publication_id
having count(*) > 1



  
  
      
    ) dbt_internal_test