

with
article_authors as (
    select * from "guardian_dw"."analytics_staging"."stg_article_authors"
),
articles as (
    select article_id, publication_date, section_key, wordcount
    from "guardian_dw"."analytics_intermediate"."int_articles_enriched"
),
sections as (
    select section_id, section_key from "guardian_dw"."analytics_staging"."stg_sections"
),
dates as (
    select date_id, date_day from "guardian_dw"."analytics_analytics"."dim_date"
),
enriched as (
    select
        aa.author_id,
        dd.date_id,
        s.section_id,
        count(distinct a.article_id) as articles_written,
        sum(a.wordcount) as total_wordcount,
        avg(a.wordcount) as avg_wordcount,
        count(distinct s.section_id) as sections_covered,
        sum(case when a.wordcount > 1000 then 1 else 0 end) as long_reads_written,
        sum(case when a.wordcount > 2000 then 1 else 0 end) as feature_length_written,
        min(a.wordcount) as min_wordcount,
        max(a.wordcount) as max_wordcount
    from article_authors aa
    join articles a on aa.article_id = a.article_id
    left join sections s on a.section_key = s.section_key
    left join dates dd on date_trunc('day', a.publication_date) = dd.date_day
    group by aa.author_id, dd.date_id, s.section_id
)

select
    row_number() over () as fact_author_daily_id,
    author_id,
    date_id,
    section_id,
    articles_written,
    total_wordcount,
    avg_wordcount,
    min_wordcount,
    max_wordcount,
    sections_covered,
    long_reads_written,
    feature_length_written,
    case
        when articles_written >= 5 then 'Very Active'
        when articles_written >= 3 then 'Active'
        when articles_written >= 2 then 'Moderate'
        else 'Light'
    end as daily_activity_level,
    current_timestamp as dw_loaded_at
from enriched