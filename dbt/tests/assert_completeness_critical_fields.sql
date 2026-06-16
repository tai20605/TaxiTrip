with total as (
    select count(*) as total_rows from {{ ref('stg_rideshare_events') }}
),

nulls as (
    select 'price' as column_name, countif(price is null) as null_count from {{ ref('stg_rideshare_events') }}
    union all
    select 'distance_miles' as column_name, countif(distance_miles is null) as null_count from {{ ref('stg_rideshare_events') }}
    union all
    select 'cab_provider' as column_name, countif(cab_provider is null) as null_count from {{ ref('stg_rideshare_events') }}
    union all
    select 'cab_tier' as column_name, countif(cab_tier is null) as null_count from {{ ref('stg_rideshare_events') }}
    union all
    select 'source_neighborhood' as column_name, countif(source_neighborhood is null) as null_count from {{ ref('stg_rideshare_events') }}
    union all
    select 'destination_neighborhood' as column_name, countif(destination_neighborhood is null) as null_count from {{ ref('stg_rideshare_events') }}
    union all
    select 'surge_multiplier' as column_name, countif(surge_multiplier is null) as null_count from {{ ref('stg_rideshare_events') }}
)

select
    n.column_name,
    n.null_count,
    t.total_rows,
    round(n.null_count * 100.0 / t.total_rows, 2) as null_pct,
    round((t.total_rows - n.null_count) * 100.0 / t.total_rows, 2) as completeness_rate_pct
from nulls n
cross join total t
where n.null_count * 100.0 / t.total_rows > 5.0