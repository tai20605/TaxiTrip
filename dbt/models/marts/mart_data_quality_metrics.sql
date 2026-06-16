-- mart_data_quality_metrics.sql
-- Purpose: Compute all 5 data quality dimensions as a single monitoring view.
{{
    config(
        materialized='incremental',
        unique_key='measured_at',
        on_schema_change='sync_all_columns'
    )
}}
with stg as (
    select * from {{ ref('stg_rideshare_events') }}
),

fact as (
    select * from {{ ref('fact_trips') }}
),

-- ── 1. COMPLETENESS ──────────────────────────────────────────────────────────
completeness as (
    select
        count(*) as total_rows,
        round(countif(trip_id              is not null) * 100.0 / count(*), 2) as completeness_trip_id,
        round(countif(price                is not null) * 100.0 / count(*), 2) as completeness_price,
        round(countif(distance_miles       is not null) * 100.0 / count(*), 2) as completeness_distance,
        round(countif(cab_provider         is not null) * 100.0 / count(*), 2) as completeness_cab_provider,
        round(countif(cab_tier             is not null) * 100.0 / count(*), 2) as completeness_cab_tier,
        round(countif(source_neighborhood  is not null) * 100.0 / count(*), 2) as completeness_source_nbhd,
        round(countif(surge_multiplier     is not null) * 100.0 / count(*), 2) as completeness_surge,
        
        round(
            (
                countif(trip_id              is not null) +
                countif(price                is not null) +
                countif(distance_miles       is not null) +
                countif(cab_provider         is not null) +
                countif(cab_tier             is not null) +
                countif(source_neighborhood  is not null) +
                countif(surge_multiplier     is not null)
            ) * 100.0 / (count(*) * 7), 2
        ) as overall_completeness_rate
    from stg
),

-- ── 2. UNIQUENESS ────────────────────────────────────────────────────────────
uniqueness as (
    select
        count(*) as fact_total_rows,
        count(*) - count(distinct trip_id) as duplicate_count,
        round(count(distinct trip_id) * 100.0 / count(*), 2) as uniqueness_rate
    from fact
),

-- ── 3. VALIDITY ──────────────────────────────────────────────────────────────
validity as (
    select
        round(countif(price > 0 or price is null) * 100.0 / count(*), 2) as validity_price_rate,
        round(countif(distance_miles >= 0) * 100.0 / count(*), 2) as validity_distance_rate,
        round(countif(surge_multiplier >= 1.0) * 100.0 / count(*), 2) as validity_surge_rate,
        round(countif(cab_provider in ('Uber', 'Lyft')) * 100.0 / count(*), 2) as validity_cab_provider_rate,
        round(countif(trip_hour between 0 and 23) * 100.0 / count(*), 2) as validity_hour_rate,
        
        round(
            (
                countif(price > 0 or price is null) +
                countif(distance_miles >= 0) +
                countif(surge_multiplier >= 1.0) +
                countif(cab_provider in ('Uber', 'Lyft')) +
                countif(trip_hour between 0 and 23)
            ) * 100.0 / (count(*) * 5), 2
        ) as overall_validity_rate
    from stg 
),
-- ── 4. ACCURACY ──────────────────────────────────────────────────────────────
accuracy as (
    with stg_rev as (
        select sum(price) as stg_sum_price, count(*) as stg_cnt from stg where price is not null
    ),
    fact_rev as (
        select sum(price) as fact_sum_price, count(*) as fact_cnt from fact where price is not null
    )
    select
        coalesce(s.stg_sum_price, 0) as staging_revenue,
        coalesce(f.fact_sum_price, 0) as fact_revenue,
        abs(coalesce(s.stg_sum_price, 0) - coalesce(f.fact_sum_price, 0)) as revenue_discrepancy_usd,
        abs(s.stg_cnt - f.fact_cnt) as trip_count_discrepancy,
        case 
            when coalesce(s.stg_sum_price, 0) = 0 then 100.0
            else round((1.0 - (abs(coalesce(s.stg_sum_price, 0) - coalesce(f.fact_sum_price, 0)) / s.stg_sum_price)) * 100.0, 2)
        end as accuracy_rate
    from stg_rev s
    cross join fact_rev f
),

-- ── 5. TIMELINESS (Đã sửa lỗi TIMESTAMP_DIFF và bóc tách cấu trúc cửa sổ) ───
lag_calculated as (
    select
        timestamp_diff(ingestion_time, kafka_timestamp, MINUTE) as lag_min,
        percentile_cont(timestamp_diff(ingestion_time, kafka_timestamp, MINUTE), 0.5) over() as median_lag
    from fact
    where ingestion_time is not null
      and kafka_timestamp is not null
),

timeliness as (
    select
        round(avg(lag_min), 2) as avg_lag_minutes,
        round(any_value(median_lag), 2) as median_lag_minutes,
        max(lag_min) as max_lag_minutes,
        countif(lag_min > 120) as records_exceeding_sla,
        round(countif(lag_min <= 120) * 100.0 / count(*), 2) as timeliness_rate
    from lag_calculated
)

-- ── FINAL ASSEMBLY ───────────────────────────────────────────────────────────
select
    current_timestamp()                    as measured_at,

    -- Completeness
    c.total_rows                           as stg_total_rows,
    c.overall_completeness_rate,
    c.completeness_trip_id,
    c.completeness_price,
    c.completeness_distance,
    c.completeness_cab_provider,
    c.completeness_cab_tier,
    c.completeness_source_nbhd,
    c.completeness_surge,

    -- Uniqueness
    u.fact_total_rows,
    u.duplicate_count,
    u.uniqueness_rate,

    -- Validity
    v.overall_validity_rate,
    v.validity_price_rate,
    v.validity_distance_rate,
    v.validity_surge_rate,
    v.validity_cab_provider_rate,
    v.validity_hour_rate,

    -- Accuracy
    a.staging_revenue,
    a.fact_revenue,
    a.revenue_discrepancy_usd,
    a.trip_count_discrepancy,
    a.accuracy_rate,

    -- Timeliness
    t.avg_lag_minutes,
    t.median_lag_minutes,
    t.max_lag_minutes,
    t.records_exceeding_sla,
    t.timeliness_rate,

    -- Composite DQ Score
    round((c.overall_completeness_rate + u.uniqueness_rate + v.overall_validity_rate + a.accuracy_rate + t.timeliness_rate) / 5, 2) as composite_dq_score

from completeness c
cross join uniqueness u
cross join validity v
cross join accuracy a
cross join timeliness t