with unique_cabs as (
    select distinct
        cab_provider,
        product_id,
        cab_tier
    from {{ ref('stg_rideshare_events') }}
)

select
    -- Stable MD5 key representing unique cab configuration
    md5(concat(coalesce(cab_provider, ''), '_', coalesce(product_id, ''), '_', coalesce(cab_tier, ''))) as cab_key,
    cab_provider,
    product_id,
    cab_tier
from unique_cabs
