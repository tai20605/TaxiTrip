with fact_trips as (
    select * from {{ ref('fact_trips') }}
),
dim_weather as (
    select * from {{ ref('dim_weather') }}
),
dim_cabs as (
    select * from {{ ref('dim_cabs') }}
)

select
    w.weather_short_summary,
    c.cab_provider,
    c.cab_tier,
    count(t.trip_id) as total_trips,
    avg(t.temperature) as avg_temperature,
    avg(t.precip_probability) as avg_precip_probability,
    avg(t.price) as avg_price,
    avg(t.surge_multiplier) as avg_surge_multiplier
from fact_trips t
join dim_weather w on t.weather_key = w.weather_key
join dim_cabs c on t.cab_key = c.cab_key
group by 1, 2, 3
