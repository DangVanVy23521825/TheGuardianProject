{{ config(materialized='view') }}

with

source as (
    select * from {{ source('guardian_dw', 'article_authors') }}
),

renamed as (
    select
        article_id,
        author_id,
        ord
    from source
)

select * from renamed