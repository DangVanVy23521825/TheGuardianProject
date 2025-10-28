
  
    

  create  table "guardian_dw"."analytics_analytics"."dim_sections__dbt_tmp"
  
  
    as
  
  (
    

with

sections as (
    select * from "guardian_dw"."analytics_staging"."stg_sections"
),

-- Use intermediate enriched articles
articles_enriched as (
    select
        article_id,
        section_key,
        section_name,
        wordcount,
        publication_date
    from "guardian_dw"."analytics_intermediate"."int_articles_enriched"
),

section_metrics as (
    select
        section_key,
        count(*) as total_articles,
        avg(wordcount) as avg_wordcount,
        min(publication_date) as first_article_date,
        max(publication_date) as last_article_date,
        count(distinct publication_date) as days_active
    from articles_enriched
    where section_key is not null
    group by section_key
)

select
    s.section_id,
    s.section_key,
    s.section_name,
    s.pillar_name,
    coalesce(sm.total_articles, 0) as total_articles,
    sm.avg_wordcount,
    sm.first_article_date,
    sm.last_article_date,
    coalesce(sm.days_active, 0) as days_active,
    case
        when sm.total_articles >= 100 then 'High Volume'
        when sm.total_articles >= 50 then 'Medium Volume'
        when sm.total_articles >= 10 then 'Low Volume'
        else 'Minimal'
    end as section_activity_level,
    current_timestamp as dw_updated_at
from sections s
left join section_metrics sm on s.section_key = sm.section_key
  );
  