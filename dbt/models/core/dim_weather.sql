with unique_weather_source as (
    select
        -- Sử dụng các trường chính để tạo key băm ổn định
        md5(concat(coalesce(cast(temperature as string), ''), weather_short_summary)) as weather_key,
        temperature,
        apparent_temperature,
        humidity,
        precip_probability,
        weather_short_summary,
        weather_long_summary,
        -- Sắp xếp thứ tự ưu tiên lấy bản ghi đầu tiên xuất hiện
        row_number() over (
            partition by md5(concat(coalesce(cast(temperature as string), ''), weather_short_summary)) 
            order by temperature desc
        ) as rn
    from {{ ref('stg_rideshare_events') }}
    where temperature is not null
)

select 
    weather_key,
    temperature,
    apparent_temperature,
    humidity,
    precip_probability,
    weather_short_summary,
    weather_long_summary
from unique_weather_source
where rn = 1