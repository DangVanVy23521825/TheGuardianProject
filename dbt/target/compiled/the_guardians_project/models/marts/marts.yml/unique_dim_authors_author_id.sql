
    
    

select
    author_id as unique_field,
    count(*) as n_records

from "guardian_dw"."analytics_analytics"."dim_authors"
where author_id is not null
group by author_id
having count(*) > 1


