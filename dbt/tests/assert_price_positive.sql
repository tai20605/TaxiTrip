select
    trip_id,
    price,
    cab_provider,
    cab_tier,
    ingestion_time
from {{ ref('stg_rideshare_events') }}
where price is null
   or price <= 0
