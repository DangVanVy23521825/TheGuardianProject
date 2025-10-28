{{ config(materialized='view') }}

with

source as (
    select * from {{ source('guardian_dw', 'article_keywords') }}
),

renamed as (
    select
        article_id,
        LOWER(TRIM(keyword)) AS keyword
    from source
)

select * from renamed