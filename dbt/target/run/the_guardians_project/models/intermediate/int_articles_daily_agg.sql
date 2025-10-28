
  create view "guardian_dw"."analytics_intermediate"."int_articles_daily_agg__dbt_tmp"
    
    
  as (
    

with enriched as (
    select * from "guardian_dw"."analytics_intermediate"."int_articles_enriched"
)

select
    publication_date,
    section_key,
    section_name,
    publication_name,
    count(*) as article_count,
    avg(wordcount) as avg_wordcount,
    avg(content_length) as avg_body_length,
    sum(case when is_long_read then 1 else 0 end) as long_read_count,
    sum(case when has_thumbnail then 1 else 0 end) as with_thumbnail_count
from enriched
group by 1,2,3,4
  );