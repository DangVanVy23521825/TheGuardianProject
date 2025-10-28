{{ config(materialized='view') }}

with

source as (
    select * from {{ source('guardian_dw', 'authors') }}
),

renamed as (
    select
        author_id,
        TRIM(LOWER(author_name)) AS author_name
    from source
)

select * from renamed