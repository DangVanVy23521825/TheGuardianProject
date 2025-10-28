
    
    

with child as (
    select date_key as from_field
    from "guardian_dw"."analytics_analytics"."fact_author_daily_stats"
    where date_key is not null
),

parent as (
    select date_day as to_field
    from "guardian_dw"."analytics_analytics"."dim_date"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


