
  create view "guardian_dw"."analytics_staging"."stg_sections__dbt_tmp"
    
    
  as (
    

with

source as (
    select * from "guardian_dw"."dw"."sections"
),

renamed as (
    select
        section_id,
        section_key,
        section_name,
        pillar_name
    from source
)

select * from renamed
  );