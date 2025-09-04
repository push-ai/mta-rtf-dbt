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
informed_entity as (
  select
    _dlt_parent_id as alert_id,
    any_value(trip__route_id) as route_id,
    any_value(trip__trip_id) as trip_id
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
  i.route_id,
  i.trip_id,
  r.route_short_name,
  r.route_long_name,
  r.route_type,
  r.route_color,
  r.route_text_color,
  r.route_desc,
  r.agency_id
from base_alerts b
left join header_text h on h.alert_id = b.alert_id
left join informed_entity i on i.alert_id = b.alert_id
left join routes r on r.route_id = i.route_id
