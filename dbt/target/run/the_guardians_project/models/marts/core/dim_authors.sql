
  
    

  create  table "guardian_dw"."analytics_analytics"."dim_authors__dbt_tmp"
  
  
    as
  
  (
    

with

authors as (
    select * from "guardian_dw"."analytics_staging"."stg_authors"
),

-- Use intermediate author activity
author_activity as (
    select * from "guardian_dw"."analytics_intermediate"."int_author_activity"
),

articles_enriched as (
    select
        article_id,
        publication_date,
        wordcount
    from "guardian_dw"."analytics_intermediate"."int_articles_enriched"
),

article_authors as (
    select * from "guardian_dw"."analytics_staging"."stg_article_authors"
),

author_metrics as (
    select
        aa.author_id,
        count(distinct aa.article_id) as total_articles_written,
        sum(ae.wordcount) as total_wordcount_written,
        avg(ae.wordcount) as avg_wordcount_per_article,
        min(ae.publication_date) as first_article_date,
        max(ae.publication_date) as last_article_date,
        count(distinct ae.publication_date) as days_active
    from article_authors aa
    left join articles_enriched ae on aa.article_id = ae.article_id
    group by aa.author_id
)

select
    au.author_id,
    au.author_name,
    coalesce(am.total_articles_written, 0) as total_articles_written,
    coalesce(am.total_wordcount_written, 0) as total_wordcount_written,
    am.avg_wordcount_per_article,
    am.first_article_date,
    am.last_article_date,
    coalesce(am.days_active, 0) as days_active,
    case
        when am.total_articles_written >= 50 then 'Prolific'
        when am.total_articles_written >= 20 then 'Active'
        when am.total_articles_written >= 5 then 'Regular'
        else 'Occasional'
    end as author_tier,
    current_timestamp as dw_updated_at
from authors au
left join author_metrics am on au.author_id = am.author_id
  );
  