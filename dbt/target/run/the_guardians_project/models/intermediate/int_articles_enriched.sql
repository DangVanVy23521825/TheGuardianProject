
  create view "guardian_dw"."analytics_intermediate"."int_articles_enriched__dbt_tmp"
    
    
  as (
    

with articles as (
    select * from "guardian_dw"."analytics_staging"."stg_articles"
),
sections as (
    select * from "guardian_dw"."analytics_staging"."stg_sections"
),
publications as (
    select * from "guardian_dw"."analytics_staging"."stg_publications"
)

select
    a.article_id,
    a.title,
    a.headline,
    a.trail_text,
    a.publication_date::date as publication_date,
    a.wordcount,
    a.body,
    s.section_name,
    s.section_key,
    p.publication_name,
    a.has_thumbnail,
    case when a.wordcount > 1000 then true else false end as is_long_read,
    length(a.body) as content_length
from articles a
left join sections s on a.section_id = s.section_id
left join publications p on a.publication_id = p.publication_id
  );