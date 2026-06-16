select
    trip_id,
    kafka_timestamp,
    ingestion_time,
    timestamp_diff(ingestion_time, kafka_timestamp, minute) as lag_minutes
from {{ ref('fact_trips') }}
where ingestion_time is not null
  and kafka_timestamp is not null
  and timestamp_diff(ingestion_time, kafka_timestamp, minute) > 120 