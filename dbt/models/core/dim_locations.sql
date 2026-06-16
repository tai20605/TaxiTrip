with unioned_locations as (
    select 
        source_neighborhood as neighborhood, 
        latitude, 
        longitude 
    from {{ ref('stg_rideshare_events') }}
    
    union all
    
    select 
        destination_neighborhood as neighborhood, 
        latitude, 
        longitude 
    from {{ ref('stg_rideshare_events') }}
)

select
    -- Generate stable deterministic key using MD5
    md5(neighborhood) as location_key,
    neighborhood,
    avg(latitude) as latitude,
    avg(longitude) as longitude
from unioned_locations
group by neighborhood
