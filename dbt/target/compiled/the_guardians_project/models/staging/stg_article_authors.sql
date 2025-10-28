

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