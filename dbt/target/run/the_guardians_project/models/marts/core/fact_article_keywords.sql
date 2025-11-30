
  
    

  create  table "guardian_dw"."analytics_analytics"."fact_article_keywords__dbt_tmp"
  
  
    as
  
  (
    

with
article_keywords as (
    select article_id, lower(trim(keyword)) as keyword
    from "guardian_dw"."analytics_staging"."stg_article_keywords"
),
articles_enriched as (
    select article_id, section_key, publication_name, publication_date, wordcount,
           is_long_read, has_thumbnail, content_length
    from "guardian_dw"."analytics_intermediate"."int_articles_enriched"
),
sections as (
    select section_id, section_key from "guardian_dw"."analytics_analytics"."dim_sections"
),
publications as (
    select publication_id, publication_name from "guardian_dw"."analytics_analytics"."dim_publications"
),
keywords_dim as (
    select keyword_id, lower(trim(keyword_name)) as keyword_name
    from "guardian_dw"."analytics_analytics"."dim_keywords"
),
dates as (
    select date_id, date_day from "guardian_dw"."analytics_analytics"."dim_date"
),
joined as (
    select
        kd.keyword_id,
        dd.date_id as date_key,
        s.section_id,
        p.publication_id,
        ae.wordcount,
        ae.is_long_read,
        ae.has_thumbnail,
        ae.content_length
    from article_keywords ak
    join articles_enriched ae on ak.article_id = ae.article_id
    left join sections s on ae.section_key = s.section_key
    left join publications p on ae.publication_name = p.publication_name
    left join keywords_dim kd on ak.keyword = kd.keyword_name
    left join dates dd on date_trunc('day', ae.publication_date) = dd.date_day
),
aggregated as (
    select
        keyword_id,
        date_key,
        section_id,
        publication_id,
        count(*) as total_articles,
        avg(wordcount) as avg_wordcount,
        sum(case when is_long_read then 1 else 0 end) as long_read_count,
        sum(case when has_thumbnail then 1 else 0 end) as with_thumbnail_count,
        avg(content_length) as avg_body_length
    from joined
    group by 1,2,3,4
)

select
    row_number() over () as fact_keyword_id,
    keyword_id,
    date_key,
    section_id,
    publication_id,
    total_articles,
    avg_wordcount,
    long_read_count,
    with_thumbnail_count,
    avg_body_length,
    current_timestamp as dw_loaded_at
from aggregated
  );
  