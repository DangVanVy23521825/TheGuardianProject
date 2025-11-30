{{ config(materialized='table') }}

with
article_authors as (
    select * from {{ ref('stg_article_authors') }}
),
articles as (
    select article_id, publication_date, section_key
    from {{ ref('int_articles_enriched') }}
),
sections as (
    select section_id, section_key
    from {{ ref('stg_sections') }}
),
dates as (
    select date_id, date_day from {{ ref('dim_date') }}
)

select
    row_number() over () as fact_article_author_id,
    aa.article_id,
    aa.author_id,
    s.section_id,
    dd.date_id,
    aa.ord as author_order,
    case when aa.ord = 0 then true else false end as is_primary_author,
    current_timestamp as dw_loaded_at
from article_authors aa
join articles a on aa.article_id = a.article_id
left join sections s on a.section_key = s.section_key
left join dates dd on date_trunc('day', a.publication_date) = dd.date_day