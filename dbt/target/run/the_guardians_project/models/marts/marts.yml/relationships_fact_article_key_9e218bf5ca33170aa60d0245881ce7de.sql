
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select section_id as from_field
    from "guardian_dw"."analytics_analytics"."fact_article_keywords"
    where section_id is not null
),

parent as (
    select section_id as to_field
    from "guardian_dw"."analytics_analytics"."dim_sections"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test