

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