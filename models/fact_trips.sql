{{ config(materialized='view') }}

with base as (
  select
    trip_uid,
    trip_uid_text,
    rt_trip_id,
    route_id,
    trip_headsign,
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
),
agg as (
  select
    trip_uid,
    any_value(trip_uid_text) as trip_uid_text,
    any_value(rt_trip_id)    as rt_trip_id,
    any_value(route_id)      as route_id,
    any_value(direction_id)  as direction_id,
    any_value(trip_headsign) as trip_headsign,
    any_value(service_date)  as service_date,
    any_value(rt_origin_code_hundredths) as rt_origin_code_hundredths,

    -- Feed window
    min(feed_ts_utc) as first_feed_ts_utc,
    max(feed_ts_utc) as last_feed_ts_utc,
    min(as_of)       as first_ingest_ts,
    max(as_of)       as last_ingest_ts,

    -- Terminal stops and sequences
    -- Prefer rows with non-null stop_sequence; if all null, use event_ts_utc as fallback ordering
    array_agg(
      struct(stop_id, stop_sequence_int)
      order by
        case when stop_sequence_int is null then 1 else 0 end asc,
        stop_sequence_int asc,
        event_ts_utc asc
      limit 1
    )[offset(0)].stop_id as first_stop_id,
    array_agg(
      struct(stop_id, stop_sequence_int)
      order by
        case when stop_sequence_int is null then 1 else 0 end asc,
        stop_sequence_int desc,
        event_ts_utc desc
      limit 1
    )[offset(0)].stop_id as last_stop_id,
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
)
select
  agg.*,
  fs.stop_name as first_stop_name,
  fs.parent_station as first_parent_station,
  fs.stop_lat as first_stop_lat,
  fs.stop_lon as first_stop_lon,
  ls.stop_name as last_stop_name,
  ls.parent_station as last_parent_station,
  ls.stop_lat as last_stop_lat,
  ls.stop_lon as last_stop_lon
from agg
left join {{ source('mta', 'stops') }} as fs on fs.stop_id = agg.first_stop_id
left join {{ source('mta', 'stops') }} as ls on ls.stop_id = agg.last_stop_id

