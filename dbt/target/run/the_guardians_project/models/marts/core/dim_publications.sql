
  
    

  create  table "guardian_dw"."analytics_analytics"."dim_publications__dbt_tmp"
  
  
    as
  
  (
    

with

publications as (
    select * from "guardian_dw"."analytics_staging"."stg_publications"
),

-- Use intermediate enriched articles
articles_enriched as (
    select
        article_id,
        publication_name,
        wordcount,
        publication_date
    from "guardian_dw"."analytics_intermediate"."int_articles_enriched"
),

publication_metrics as (
    select
        publication_name,
        count(*) as total_articles,
        avg(wordcount) as avg_wordcount,
        min(publication_date) as first_article_date,
        max(publication_date) as last_article_date,
        count(distinct publication_date) as days_active
    from articles_enriched
    where publication_name is not null
    group by publication_name
)

select
    p.publication_id,
    p.publication_name,
    coalesce(pm.total_articles, 0) as total_articles,
    pm.avg_wordcount,
    pm.first_article_date,
    pm.last_article_date,
    coalesce(pm.days_active, 0) as days_active,
    current_timestamp as dw_updated_at
from publications p
left join publication_metrics pm on p.publication_name = pm.publication_name
  );
  