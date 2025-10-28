
    
    

with child as (
    select publication_id as from_field
    from "guardian_dw"."analytics_analytics"."fact_articles"
    where publication_id is not null
),

parent as (
    select publication_id as to_field
    from "guardian_dw"."analytics_analytics"."dim_publications"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


