{{ config(materialized='view') }}

with article_authors as (
    select * from {{ ref('stg_article_authors') }}
),
articles as (
    select article_id, publication_date from {{ ref('stg_articles') }}
),
joined as (
    select
        aa.author_id,
        a.publication_date,
        count(*) as articles_written
    from article_authors aa
    join articles a on a.article_id = aa.article_id
    group by 1,2
)
select * from joined