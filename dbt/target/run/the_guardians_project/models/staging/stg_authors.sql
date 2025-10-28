
  create view "guardian_dw"."analytics_staging"."stg_authors__dbt_tmp"
    
    
  as (
    

with

source as (
    select * from "guardian_dw"."dw"."authors"
),

renamed as (
    select
        author_id,
        TRIM(LOWER(author_name)) AS author_name
    from source
)

select * from renamed
  );