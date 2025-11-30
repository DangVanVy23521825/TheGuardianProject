
  
    

  create  table "guardian_dw"."analytics_analytics"."fact_articles__dbt_tmp"
  
  
    as
  
  (
    

with
articles as (
    select article_id, publication_date, wordcount, content_length, is_long_read, has_thumbnail
    from "guardian_dw"."analytics_intermediate"."int_articles_enriched"
),
article_authors as (
    select article_id, count(distinct author_id) as author_count
    from "guardian_dw"."analytics_staging"."stg_article_authors"
    group by article_id
),
article_keywords as (
    select article_id, count(distinct keyword) as keyword_count
    from "guardian_dw"."analytics_staging"."stg_article_keywords"
    group by article_id
),
sections as (
    select section_id, section_key from "guardian_dw"."analytics_staging"."stg_sections"
),
publications as (
    select publication_id, publication_name from "guardian_dw"."analytics_staging"."stg_publications"
),
articles_staging as (
    select article_id, section_id, publication_id
    from "guardian_dw"."analytics_staging"."stg_articles"
),
dates as (
    select date_id, date_day from "guardian_dw"."analytics_analytics"."dim_date"
)

select
    row_number() over () as fact_article_id,
    a.article_id,
    dd.date_id as date_id,
    ast.section_id,
    ast.publication_id,
    coalesce(aa.author_count, 0) as author_count,
    coalesce(ak.keyword_count, 0) as keyword_count,
    a.wordcount,
    a.content_length,
    a.is_long_read,
    case when a.wordcount > 2000 then true else false end as is_feature_length,
    a.has_thumbnail,
    current_timestamp as dw_loaded_at
from articles a
left join articles_staging ast on a.article_id = ast.article_id
left join article_authors aa on a.article_id = aa.article_id
left join article_keywords ak on a.article_id = ak.article_id
left join dates dd on date_trunc('day', a.publication_date) = dd.date_day
  );
  