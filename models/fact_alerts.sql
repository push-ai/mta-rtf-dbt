{{ config(materialized='view') }}

with base_alerts as (
  select
    feed,
    entity_id,
    as_of,
    _dlt_id as alert_id,
    _dlt_load_id
  from {{ source('mta', 'alerts') }}
),
header_text as (
  select
    _dlt_parent_id as alert_id,
    any_value(text) as header_text
  from {{ source('mta', 'alerts__alert__header_text__translation') }}
  group by 1
),
description_text as (
  select
    _dlt_parent_id as alert_id,
    any_value(text) as description_text
  from {{ source('mta', 'alerts__alert__description_text__translation') }}
  group by 1
),
active_period as (
  select
    _dlt_parent_id as alert_id,
    min(case when start is not null then timestamp_seconds(safe_cast(start as int64)) end) as first_start_ts_utc,
    max(case when `end` is not null then timestamp_seconds(safe_cast(`end` as int64)) end) as last_end_ts_utc
  from {{ source('mta', 'alerts__alert__active_period') }}
  group by 1
),
informed_entity as (
  select
    _dlt_parent_id as alert_id,
    any_value(coalesce(trip__route_id, route_id)) as route_id,
    any_value(trip__trip_id) as rt_trip_id,
    any_value(stop_id) as stop_id,
    any_value(agency_id) as agency_id
  from {{ source('mta', 'alerts__alert__informed_entity') }}
  group by 1
),
routes as (
  select
    route_id,
    route_short_name,
    route_long_name,
    route_type,
    route_color,
    route_text_color,
    route_desc,
    agency_id
  from {{ source('mta', 'routes') }}
)

select
  b.feed,
  b.entity_id,
  b.as_of,
  b.alert_id,
  h.header_text,
  d.description_text,
  ap.first_start_ts_utc,
  ap.last_end_ts_utc,
  i.route_id,
  i.rt_trip_id as trip_id,
  regexp_extract(i.rt_trip_id, r'^-?\d{1,8}') as rt_origin_code_hundredths,
  cast(null as string) as route_dir_key,
  cast(null as string) as trip_uid_text,
  cast(null as string) as trip_uid,
  r.route_short_name,
  r.route_long_name,
  r.route_type,
  r.route_color,
  r.route_text_color,
  r.route_desc,
  r.agency_id
from base_alerts b
left join header_text h on h.alert_id = b.alert_id
left join description_text d on d.alert_id = b.alert_id
left join active_period ap on ap.alert_id = b.alert_id
left join informed_entity i on i.alert_id = b.alert_id
left join routes r on r.route_id = i.route_id
where b.feed = 'alerts'
