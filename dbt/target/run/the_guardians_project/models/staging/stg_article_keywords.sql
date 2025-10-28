
  create view "guardian_dw"."analytics_staging"."stg_article_keywords__dbt_tmp"
    
    
  as (
    

with

source as (
    select * from "guardian_dw"."dw"."article_keywords"
),

renamed as (
    select
        article_id,
        LOWER(TRIM(keyword)) AS keyword
    from source
)

select * from renamed
  );