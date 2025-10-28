

select
    d::date as date_day,
    extract(year from d) as year,
    extract(month from d) as month,
    extract(day from d) as day,
    to_char(d, 'Day') as weekday_name,
    extract(dow from d) as weekday_number
from generate_series('2020-01-01'::date, current_date, interval '1 day') d