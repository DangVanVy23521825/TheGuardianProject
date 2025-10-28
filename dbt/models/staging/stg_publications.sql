{{ config(materialized='view') }}

with

source as (
    select * from {{ source('guardian_dw', 'publications') }}
),

renamed as (
    select
        publication_id,
        publication_name
    from source
)

select * from renamed