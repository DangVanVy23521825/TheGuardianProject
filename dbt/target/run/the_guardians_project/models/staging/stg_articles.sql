
  create view "guardian_dw"."analytics_staging"."stg_articles__dbt_tmp"
    
    
  as (
    

with

source as (
    select * from "guardian_dw"."dw"."articles"
),

renamed as (
    select
        article_id,
        type AS article_type,
        section_id,
        publication_id,
        publication_date,
        title,
        headline,
        trail_text,
        body,
        wordcount,
        thumbnail_url,
        web_url,
        has_thumbnail,
        is_live_blog,
        topic_country,
        ingested_at,
        source_system,
        raw_s3_key,
        processed_by
    from source
)

select * from renamed
  );