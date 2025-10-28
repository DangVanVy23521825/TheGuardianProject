

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