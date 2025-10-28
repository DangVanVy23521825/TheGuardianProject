{{ config(materialized='view') }}

with keywords as (
    select * from {{ ref('stg_article_keywords') }}
),
articles as (
    select article_id, publication_date, section_id from {{ ref('stg_articles') }}
)
select
    lower(k.keyword) as keyword,
    date_trunc('day', a.publication_date) as pub_day,
    count(distinct a.article_id) as article_count
from keywords k
join articles a on k.article_id = a.article_id
group by 1,2