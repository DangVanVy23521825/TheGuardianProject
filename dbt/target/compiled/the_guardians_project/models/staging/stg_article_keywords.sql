

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