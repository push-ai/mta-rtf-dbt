{{ config(materialized='view') }}

with base as (
  select
    trip_uid,
    trip_uid_text,
    rt_trip_id,
    route_id,
    direction_id,
    service_date,
    rt_origin_code_hundredths,
    feed_ts_utc,
    as_of,
    trip_schedule_relationship,
    stop_sequence_int,
    stop_id,
    arrival_ts_utc,
    departure_ts_utc,
    event_ts_utc
  from {{ ref('fact_trips_stops') }}
)
select
  trip_uid,
  any_value(trip_uid_text) as trip_uid_text,
  any_value(rt_trip_id)    as rt_trip_id,
  any_value(route_id)      as route_id,
  any_value(direction_id)  as direction_id,
  any_value(service_date)  as service_date,
  any_value(rt_origin_code_hundredths) as rt_origin_code_hundredths,

  -- Feed window
  min(feed_ts_utc) as first_feed_ts_utc,
  max(feed_ts_utc) as last_feed_ts_utc,
  min(as_of)       as first_ingest_ts,
  max(as_of)       as last_ingest_ts,

  -- Terminal stops and sequences
  array_agg(struct(stop_id, stop_sequence_int) order by stop_sequence_int asc limit 1)[offset(0)].stop_id as first_stop_id,
  array_agg(struct(stop_id, stop_sequence_int) order by stop_sequence_int desc limit 1)[offset(0)].stop_id as last_stop_id,
  min(stop_sequence_int) as first_stop_sequence,
  max(stop_sequence_int) as last_stop_sequence,

  -- First/last event timestamps by stop order
  array_agg(coalesce(arrival_ts_utc, departure_ts_utc) order by stop_sequence_int asc limit 1)[offset(0)] as first_event_ts_utc,
  array_agg(coalesce(arrival_ts_utc, departure_ts_utc) order by stop_sequence_int desc limit 1)[offset(0)] as last_event_ts_utc,

  -- Simple duration (if both endpoints exist)
  timestamp_diff(
    array_agg(coalesce(arrival_ts_utc, departure_ts_utc) order by stop_sequence_int desc limit 1)[offset(0)],
    array_agg(coalesce(arrival_ts_utc, departure_ts_utc) order by stop_sequence_int asc limit 1)[offset(0)],
    second
  ) as trip_duration_s,

  -- Labels
  any_value(trip_schedule_relationship) as schedule_relationship
from base
group by trip_uid

