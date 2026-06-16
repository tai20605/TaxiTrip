-- TEST: assert_no_duplicate_trip_ids
-- Severity: ERROR (critical – uniqueness of fact grain)
-- Metric: Uniqueness Rate on fact_trips.trip_id
--
-- Alert level: CRITICAL → duplicate trips corrupt all aggregated metrics.
-- Returns only trip_ids that appear more than once.

select
    trip_id,
    count(*) as duplicate_count
from {{ ref('fact_trips') }}
group by trip_id
having count(*) > 1
