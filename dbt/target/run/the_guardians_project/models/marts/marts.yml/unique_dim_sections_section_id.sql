
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    section_id as unique_field,
    count(*) as n_records

from "guardian_dw"."analytics_analytics"."dim_sections"
where section_id is not null
group by section_id
having count(*) > 1



  
  
      
    ) dbt_internal_test