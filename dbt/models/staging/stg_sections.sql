{{ config(materialized='view') }}

with

source as (
    select * from {{ source('guardian_dw', 'sections') }}
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