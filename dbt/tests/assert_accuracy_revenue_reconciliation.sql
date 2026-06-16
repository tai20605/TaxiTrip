{{ config(
    tags = ['transform']
) }}

with staging_total as (
    select
        round(sum(price), 4) as staging_revenue,
        count(*) as staging_count
    from {{ ref('stg_rideshare_events') }}
    where price is not null
),

fact_total as (
    select
        round(sum(price), 4) as fact_revenue,
        count(*) as fact_count
    from {{ ref('fact_trips') }}
    where price is not null
)

select
    s.staging_revenue,
    f.fact_revenue,
    s.staging_count,
    f.fact_count,
    abs(s.staging_revenue - f.fact_revenue) as revenue_discrepancy,
    round(abs(s.staging_revenue - f.fact_revenue) / nullif(s.staging_revenue, 0) * 100, 4) as discrepancy_pct
from staging_total s
cross join fact_total f
-- Fail if revenue discrepancy > 0.01% (1 basis point)
where abs(s.staging_revenue - f.fact_revenue) / nullif(s.staging_revenue, 0) > 0.0001
