with raw_events as (
    select 
        `partition`,
        `offset`,
        kafka_timestamp,
        ingestion_time,
        safe.parse_json(value) as json_value_parsed
    from {{ source('gcs_bronze', 'external_rideshare_events') }}
),

parsed_fields as (
    select
        -- ─── THÔNG TIN CUỐC XE CỐT LÕI (TRIP ATTRIBUTES) ───
        json_value(json_value_parsed, "$.id") as trip_id,
        safe_cast(json_value(json_value_parsed, "$.timestamp") as float64) as trip_timestamp,
        safe_cast(json_value(json_value_parsed, "$.hour") as int64) as trip_hour,
        safe_cast(json_value(json_value_parsed, "$.day") as int64) as trip_day,
        safe_cast(json_value(json_value_parsed, "$.month") as int64) as trip_month,
        json_value(json_value_parsed, "$.datetime") as trip_datetime,
        json_value(json_value_parsed, "$.timezone") as timezone,
        json_value(json_value_parsed, "$.source") as source_neighborhood,
        json_value(json_value_parsed, "$.destination") as destination_neighborhood,
        json_value(json_value_parsed, "$.cab_type") as cab_provider, 
        json_value(json_value_parsed, "$.product_id") as product_id,
        json_value(json_value_parsed, "$.name") as cab_tier, 
        safe_cast(json_value(json_value_parsed, "$.price") as float64) as price,
        safe_cast(json_value(json_value_parsed, "$.distance") as float64) as distance_miles,
        safe_cast(json_value(json_value_parsed, "$.surge_multiplier") as float64) as surge_multiplier,
        
        -- ─── TỌA ĐỘ VỊ TRÍ (COORDINATES) ───
        safe_cast(json_value(json_value_parsed, "$.latitude") as float64) as latitude,
        safe_cast(json_value(json_value_parsed, "$.longitude") as float64) as longitude,
        
        -- ─── ĐẦY ĐỦ CÁC TRƯỜNG THỜI TIẾT (ALL 39 WEATHER FIELDS FROM FEATURE FILE) ───
        safe_cast(json_value(json_value_parsed, "$.temperature") as float64) as temperature,
        safe_cast(json_value(json_value_parsed, "$.apparentTemperature") as float64) as apparent_temperature,
        json_value(json_value_parsed, "$.short_summary") as weather_short_summary,
        json_value(json_value_parsed, "$.long_summary") as weather_long_summary,
        safe_cast(json_value(json_value_parsed, "$.precipIntensity") as float64) as precip_intensity,
        safe_cast(json_value(json_value_parsed, "$.precipProbability") as float64) as precip_probability,
        safe_cast(json_value(json_value_parsed, "$.humidity") as float64) as humidity,
        safe_cast(json_value(json_value_parsed, "$.windSpeed") as float64) as wind_speed,
        safe_cast(json_value(json_value_parsed, "$.windGust") as float64) as wind_gust,
        safe_cast(json_value(json_value_parsed, "$.windGustTime") as float64) as wind_gust_time,
        safe_cast(json_value(json_value_parsed, "$.visibility") as float64) as visibility,
        safe_cast(json_value(json_value_parsed, '$."visibility.1"') as float64) as visibility_1,
        json_value(json_value_parsed, "$.icon") as weather_icon,
        safe_cast(json_value(json_value_parsed, "$.dewPoint") as float64) as dew_point,
        safe_cast(json_value(json_value_parsed, "$.pressure") as float64) as pressure,
        safe_cast(json_value(json_value_parsed, "$.windBearing") as float64) as wind_bearing,
        safe_cast(json_value(json_value_parsed, "$.cloudCover") as float64) as cloud_cover,
        safe_cast(json_value(json_value_parsed, "$.uvIndex") as float64) as uv_index,
        safe_cast(json_value(json_value_parsed, "$.uvIndexTime") as float64) as uv_index_time,
        safe_cast(json_value(json_value_parsed, "$.ozone") as float64) as ozone,
        
        -- Các mốc nhiệt độ cao/thấp chi tiết
        safe_cast(json_value(json_value_parsed, "$.temperatureHigh") as float64) as temp_high,
        safe_cast(json_value(json_value_parsed, "$.temperatureHighTime") as float64) as temp_high_time,
        safe_cast(json_value(json_value_parsed, "$.temperatureLow") as float64) as temp_low,
        safe_cast(json_value(json_value_parsed, "$.temperatureLowTime") as float64) as temp_low_time,
        safe_cast(json_value(json_value_parsed, "$.temperatureMin") as float64) as temp_min,
        safe_cast(json_value(json_value_parsed, "$.temperatureMinTime") as float64) as temp_min_time,
        safe_cast(json_value(json_value_parsed, "$.temperatureMax") as float64) as temp_max,
        safe_cast(json_value(json_value_parsed, "$.temperatureMaxTime") as float64) as temp_max_time,
        
        -- Cảm giác nhiệt độ thực tế (Apparent Temperature)
        safe_cast(json_value(json_value_parsed, "$.apparentTemperatureHigh") as float64) as apparent_temp_high,
        safe_cast(json_value(json_value_parsed, "$.apparentTemperatureHighTime") as float64) as apparent_temp_high_time,
        safe_cast(json_value(json_value_parsed, "$.apparentTemperatureLow") as float64) as apparent_temp_low,
        safe_cast(json_value(json_value_parsed, "$.apparentTemperatureLowTime") as float64) as apparent_temp_low_time,
        safe_cast(json_value(json_value_parsed, "$.apparentTemperatureMin") as float64) as apparent_temp_min,
        safe_cast(json_value(json_value_parsed, "$.apparentTemperatureMinTime") as float64) as apparent_temp_min_time,
        safe_cast(json_value(json_value_parsed, "$.apparentTemperatureMax") as float64) as apparent_temp_max,
        safe_cast(json_value(json_value_parsed, "$.apparentTemperatureMaxTime") as float64) as apparent_temp_max_time,
        
        -- Thiên văn học (Astronomy)
        safe_cast(json_value(json_value_parsed, "$.sunriseTime") as float64) as sunrise_time,
        safe_cast(json_value(json_value_parsed, "$.sunsetTime") as float64) as sunset_time,
        safe_cast(json_value(json_value_parsed, "$.moonPhase") as float64) as moon_phase,
        safe_cast(json_value(json_value_parsed, "$.precipIntensityMax") as float64) as precip_intensity_max,
        
        -- ─── METADATA HỆ THỐNG (SYSTEM METADATA) ───
        `partition` as kafka_partition,
        `offset` as kafka_offset,
        kafka_timestamp,
        ingestion_time
    from raw_events
    where json_value(json_value_parsed, "$.id") is not null
),

deduped as (
    select 
        *,
        row_number() over (partition by trip_id order by kafka_timestamp desc) as rn
    from parsed_fields
)

select * except(rn)
from deduped
where rn = 1