
  
    

  create  table "guardian_dw"."analytics_analytics"."dim_keywords__dbt_tmp"
  
  
    as
  
  (
    

with

-- Lấy dữ liệu đã xử lý (tính số bài viết mỗi ngày cho từng keyword)
keywords_popularity as (
    select * from "guardian_dw"."analytics_intermediate"."int_keywords_popularity"
),

-- Tính toán độ phổ biến tổng thể
keyword_metrics as (
    select
        keyword,
        count(distinct pub_day) as active_days,
        sum(article_count) as total_articles,
        avg(article_count) as avg_articles_per_day,
        min(pub_day) as first_seen_date,
        max(pub_day) as last_seen_date
    from keywords_popularity
    group by keyword
),

-- Gán thêm nhãn mức độ phổ biến
classified as (
    select
        lower(trim(keyword)) as keyword_name,
        total_articles,
        avg_articles_per_day,
        active_days,
        first_seen_date,
        last_seen_date,
        case
            when total_articles >= 1000 then 'Very Popular'
            when total_articles >= 200 then 'Popular'
            when total_articles >= 50 then 'Common'
            when total_articles >= 10 then 'Rare'
            else 'Occasional'
        end as keyword_popularity_level
    from keyword_metrics
)

-- Dimension chính
select
    row_number() over (order by keyword_name) as keyword_id,
    keyword_name,
    total_articles,
    avg_articles_per_day,
    active_days,
    first_seen_date,
    last_seen_date,
    keyword_popularity_level,
    current_timestamp as dw_updated_at
from classified
  );
  