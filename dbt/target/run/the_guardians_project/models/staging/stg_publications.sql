
  create view "guardian_dw"."analytics_staging"."stg_publications__dbt_tmp"
    
    
  as (
    

with

source as (
    select * from "guardian_dw"."dw"."publications"
),

renamed as (
    select
        publication_id,
        publication_name
    from source
)

select * from renamed
  );