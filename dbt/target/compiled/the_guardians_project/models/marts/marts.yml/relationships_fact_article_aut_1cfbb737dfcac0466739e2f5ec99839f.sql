
    
    

with child as (
    select article_id as from_field
    from "guardian_dw"."analytics_analytics"."fact_article_authors"
    where article_id is not null
),

parent as (
    select article_id as to_field
    from "guardian_dw"."analytics_analytics"."fact_articles"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


