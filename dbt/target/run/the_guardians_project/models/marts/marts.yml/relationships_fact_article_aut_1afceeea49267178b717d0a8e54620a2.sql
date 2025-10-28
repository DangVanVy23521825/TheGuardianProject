
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select author_key as from_field
    from "guardian_dw"."analytics_analytics"."fact_article_authors"
    where author_key is not null
),

parent as (
    select author_id as to_field
    from "guardian_dw"."analytics_analytics"."dim_authors"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test