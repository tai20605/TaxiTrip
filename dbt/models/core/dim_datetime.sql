with unique_datetimes as (
    select distinct
        trip_datetime
    from {{ ref('stg_rideshare_events') }}
)

select
    md5(trip_datetime) as datetime_key,
    parse_datetime('%Y-%m-%d %H:%M:%S', trip_datetime) as trip_datetime,
    extract(hour from parse_datetime('%Y-%m-%d %H:%M:%S', trip_datetime)) as hour,
    extract(day from parse_datetime('%Y-%m-%d %H:%M:%S', trip_datetime)) as day,
    extract(month from parse_datetime('%Y-%m-%d %H:%M:%S', trip_datetime)) as month,
    extract(year from parse_datetime('%Y-%m-%d %H:%M:%S', trip_datetime)) as year,
    format_datetime('%A', parse_datetime('%Y-%m-%d %H:%M:%S', trip_datetime)) as day_of_week,
    case 
        when format_datetime('%A', parse_datetime('%Y-%m-%d %H:%M:%S', trip_datetime)) in ('Saturday', 'Sunday') then true
        else false
    end as is_weekend
from unique_datetimes
