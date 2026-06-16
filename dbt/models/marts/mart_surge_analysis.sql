with fact_trips as (
    select * from {{ ref('fact_trips') }}
),
dim_locations as (
    select * from {{ ref('dim_locations') }}
),
dim_cabs as (
    select * from {{ ref('dim_cabs') }}
)

select
    l.neighborhood as source_neighborhood,
    c.cab_provider,
    c.cab_tier,
    count(t.trip_id) as total_trips,
    count(case when t.surge_multiplier > 1.0 then 1 end) as surge_trips,
    round(count(case when t.surge_multiplier > 1.0 then 1 end) * 100.0 / count(t.trip_id), 2) as surge_percentage,
    max(t.surge_multiplier) as max_surge_multiplier,
    avg(t.price) as avg_price,
    avg(t.surge_multiplier) as avg_surge_multiplier
from fact_trips t
join dim_locations l on t.source_location_key = l.location_key
join dim_cabs c on t.cab_key = c.cab_key
group by 1, 2, 3
