
  create view "guardian_dw"."analytics_staging"."stg_article_authors__dbt_tmp"
    
    
  as (
    

with

source as (
    select * from "guardian_dw"."dw"."article_authors"
),

renamed as (
    select
        article_id,
        author_id,
        ord
    from source
)

select * from renamed
  );