
    
    

select
    article_id as unique_field,
    count(*) as n_records

from "guardian_dw"."analytics_analytics"."fact_articles"
where article_id is not null
group by article_id
having count(*) > 1


