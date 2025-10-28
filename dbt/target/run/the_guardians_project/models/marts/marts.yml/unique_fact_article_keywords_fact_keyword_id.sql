
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    fact_keyword_id as unique_field,
    count(*) as n_records

from "guardian_dw"."analytics_analytics"."fact_article_keywords"
where fact_keyword_id is not null
group by fact_keyword_id
having count(*) > 1



  
  
      
    ) dbt_internal_test