{{ config(materialized='view') }}

with tu as (
  select
    _dlt_id as tu_id,
    as_of,
    feed,
    trip_update__timestamp,
    trip_update__trip__trip_id,
    trip_update__trip__route_id,
    trip_update__trip__direction_id,
    trip_update__trip__start_date,
    trip_update__trip__schedule_relationship
  from {{ source('mta', 'trip_updates') }}
),
stu as (
  select
    _dlt_id as stu_id,
    _dlt_parent_id as tu_id,
    stop_id,
    stop_sequence,
    arrival__time,
    departure__time,
    arrival__delay,
    departure__delay,
    arrival__uncertainty,
    departure__uncertainty,
    schedule_relationship as stu_schedule_relationship
  from {{ source('mta', 'trip_updates__trip_update__stop_time_update') }}
),
stops as (
  select
    stop_id,
    stop_name,
    parent_station,
    stop_lat,
    stop_lon
  from {{ source('mta', 'stops') }}
),
static_trips as (
  select
    trip_id,
    route_id as static_route_id,
    service_id,
    trip_headsign,
    direction_id as static_direction_id
  from {{ source('mta', 'trips') }}
),
base as (
select
  -- Provenance
  tu.tu_id,
  stu.stu_id,
  tu.as_of,
  tu.feed,

  -- Trip identity and attributes
  tu.trip_update__trip__trip_id as rt_trip_id,
  tu.trip_update__trip__route_id as route_id,
  tu.trip_update__trip__direction_id as direction_id,
  tu.trip_update__trip__start_date as service_date,
  tu.trip_update__trip__schedule_relationship as trip_schedule_relationship,
  static_trips.trip_headsign as trip_headsign,

  -- Stop attributes
  stu.stop_id,
  stu.stop_sequence,
  safe_cast(stu.stop_sequence as int64) as stop_sequence_int,
  stops.stop_name,
  stops.parent_station,
  stops.stop_lat,
  stops.stop_lon,

  -- Feed and event timestamps
  timestamp_seconds(safe_cast(tu.trip_update__timestamp as int64)) as feed_ts_utc,
  case when stu.arrival__time   is not null then timestamp_seconds(safe_cast(stu.arrival__time   as int64)) end as arrival_ts_utc,
  case when stu.departure__time is not null then timestamp_seconds(safe_cast(stu.departure__time as int64)) end as departure_ts_utc,

  -- Delays as provided
  stu.arrival__delay,
  stu.departure__delay,
  stu.arrival__uncertainty,
  stu.departure__uncertainty,
  stu.stu_schedule_relationship,

  -- Handy keys
  concat(
    coalesce(tu.trip_update__trip__route_id, 'UNK'), '|',
    coalesce(static_trips.trip_headsign, 'UNK')
  ) as route_headsign_key,

  -- Origin code often embedded in rt_trip_id (not guaranteed unique; informative only)
  regexp_extract(tu.trip_update__trip__trip_id, r'^-?\d{1,8}') as rt_origin_code_hundredths,

  -- Stable trip identifiers
  concat(
    coalesce(tu.trip_update__trip__start_date, 'UNK'), '|',
    coalesce(tu.trip_update__trip__route_id, 'UNK'),    '|',
    coalesce(static_trips.trip_headsign, 'UNK'),'|',
    coalesce(
      regexp_extract(tu.trip_update__trip__trip_id, r'^-?\d{1,8}'),
      tu.trip_update__trip__trip_id,
      'UNK'
    )
  ) as trip_uid_text,
  to_hex(sha256(
    concat(
      coalesce(tu.trip_update__trip__start_date, 'UNK'), '|',
      coalesce(tu.trip_update__trip__route_id, 'UNK'),    '|',
      coalesce(static_trips.trip_headsign, 'UNK'),'|',
      coalesce(
        regexp_extract(tu.trip_update__trip__trip_id, r'^-?\d{1,8}'),
        tu.trip_update__trip__trip_id,
        'UNK'
      )
    )
  )) as trip_uid

from tu
join stu on stu.tu_id = tu.tu_id
left join stops on stops.stop_id = stu.stop_id
left join static_trips on static_trips.trip_id = tu.trip_update__trip__trip_id
)

select
  base.*,
  (base.arrival_ts_utc is not null) as has_arrival_ts,
  (base.departure_ts_utc is not null) as has_departure_ts,
  coalesce(base.arrival_ts_utc, base.departure_ts_utc) as event_ts_utc,
  case
    when base.arrival_ts_utc   is not null then 'arrival'
    when base.departure_ts_utc is not null then 'departure'
  end as event_kind_primary,
  datetime(coalesce(base.arrival_ts_utc, base.departure_ts_utc), 'America/New_York') as event_dt_local,
  date(coalesce(base.arrival_ts_utc, base.departure_ts_utc), 'America/New_York') as service_day_local
from base

