{{ config(materialized='table') }}

-- Reuse intermediate date dimension
with date_base as (
    select * from {{ ref('int_date_dim') }}
),

enhanced_date as (
    select
        row_number() over (order by date_day) as date_id,
        date_day,
        year,
        month,
        day,
        weekday_name,
        weekday_number,
        extract(quarter from date_day) as quarter,
        extract(week from date_day) as week_of_year,
        to_char(date_day, 'Mon') as month_name,
        to_char(date_day, 'YYYY-MM') as year_month,
        concat(year, '-Q', extract(quarter from date_day)) as year_quarter,
        case
            when weekday_number in (0, 6) then true
            else false
        end as is_weekend,
        case
            when month in (12, 1, 2) then 'Winter'
            when month in (3, 4, 5) then 'Spring'
            when month in (6, 7, 8) then 'Summer'
            else 'Autumn'
        end as season
    from date_base
)

select * from enhanced_date