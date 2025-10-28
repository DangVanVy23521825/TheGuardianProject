
    
    

select
    section_id as unique_field,
    count(*) as n_records

from "guardian_dw"."analytics_analytics"."dim_sections"
where section_id is not null
group by section_id
having count(*) > 1


