with fact_trips as (
    select * from {{ ref('fact_trips') }}
),
dim_datetime as (
    select * from {{ ref('dim_datetime') }}
),
dim_cabs as (
    select * from {{ ref('dim_cabs') }}
)

select
    d.year,
    d.month,
    d.day,
    d.hour,
    d.day_of_week,
    d.is_weekend,
    c.cab_provider,
    c.cab_tier,
    count(t.trip_id) as total_trips,
    avg(t.price) as avg_price,
    avg(t.distance_miles) as avg_distance_miles,
    avg(t.surge_multiplier) as avg_surge_multiplier
from fact_trips t
join dim_datetime d on t.datetime_key = d.datetime_key
join dim_cabs c on t.cab_key = c.cab_key
group by 1, 2, 3, 4, 5, 6, 7, 8
