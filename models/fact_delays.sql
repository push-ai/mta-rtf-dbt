{{ config(materialized='view') }}

-- Trip-level delays derived from the main GTFS-realtime feed
select
  -- Provenance
  trip_uid,
  trip_uid_text,
  tu_id,
  stu_id,
  as_of,
  feed,

  -- Trip identity and attributes
  rt_trip_id,
  route_id,
  direction_id,
  service_date,
  trip_schedule_relationship,
  trip_headsign,

  -- Stop attributes
  stop_id,
  stop_name,
  parent_station,
  stop_lat,
  stop_lon,
  stop_sequence,
  stop_sequence_int,

  -- Feed and event timestamps
  feed_ts_utc,
  arrival_ts_utc,
  departure_ts_utc,
  event_ts_utc,
  event_kind_primary,
  event_dt_local,
  service_day_local,

  -- Delay fields
  arrival__delay,
  departure__delay,
  arrival__uncertainty,
  departure__uncertainty,
  stu_schedule_relationship,

  -- Handy keys
  route_headsign_key,
  rt_origin_code_hundredths,

  -- Flags
  has_arrival_ts,
  has_departure_ts
from {{ ref('fact_trips_stops') }}
where feed = 'main'
  and (arrival__delay is not null or departure__delay is not null)