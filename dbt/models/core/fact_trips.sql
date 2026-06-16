with trips as (
    select * from {{ ref('stg_rideshare_events') }}
)

select
    trip_id,
    
    -- Dimension Keys (Foreign Keys matching MD5 hashes in dim tables)
    md5(source_neighborhood) as source_location_key,
    md5(destination_neighborhood) as destination_location_key,
    md5(concat(coalesce(cab_provider, ''), '_', coalesce(product_id, ''), '_', coalesce(cab_tier, ''))) as cab_key,
    md5(concat(coalesce(weather_short_summary, ''), '_', coalesce(weather_long_summary, ''), '_', coalesce(weather_icon, ''))) as weather_key,
    md5(trip_datetime) as datetime_key,
    
    -- Trip Measures
    price,
    distance_miles,
    surge_multiplier,
    
    -- Environmental metrics specific to trip dispatch time
    temperature,
    apparent_temperature,
    precip_intensity,
    precip_probability,
    humidity,
    wind_speed,
    wind_gust,
    visibility,
    visibility_1,
    uv_index,
    ozone,
    
    -- Kafka / Ingestion metadata
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    ingestion_time
from trips
