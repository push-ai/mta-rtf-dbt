## Realtime metrics guide using fact_trips_stops and fact_trips

This guide shows how to compute common GTFS-Realtime metrics from the dbt models in this project. Replace hard-coded dates/times as needed. References use dbt's `{{ ref('...') }}`.

### Models

- `fact_trips_stops` (stop-level): one row per stop_time_update enriched with stop metadata and timestamps.
  - Keys: `trip_uid`, `route_id`, `direction_id`, `stop_id`, `stop_sequence_int`, `route_dir_key`
  - Times: `feed_ts_utc`, `arrival_ts_utc`, `departure_ts_utc`, `event_ts_utc` (arrival-first), `as_of`

- `fact_trips` (trip-level rollup): one row per unique `trip_uid` with first/last event/feed info.
  - Times: `first_feed_ts_utc`, `last_feed_ts_utc`, `first_ingest_ts`, `last_ingest_ts`, `first_event_ts_utc`, `last_event_ts_utc`

Notes on timestamps:
- Use `event_ts_utc = COALESCE(arrival_ts_utc, departure_ts_utc)` for generic “pass” events (headways/throughput).
- For departure-centric metrics (terminal depart, OTP), prefer `COALESCE(departure_ts_utc, arrival_ts_utc)` within the metric query.

---

### Trips observed (per minute and per 5‑minute)

```sql
-- Per minute window between two UTC timestamps
DECLARE start_ts TIMESTAMP DEFAULT TIMESTAMP('2025-09-01 06:00:00+00');
DECLARE end_ts   TIMESTAMP DEFAULT TIMESTAMP('2025-09-01 10:00:00+00');

SELECT
  route_id,
  direction_id,
  TIMESTAMP_TRUNC(feed_ts_utc, MINUTE) AS ts_minute,
  COUNT(DISTINCT trip_uid) AS trips_observed
FROM {{ ref('fact_trips_stops') }}
WHERE feed_ts_utc BETWEEN start_ts AND end_ts
GROUP BY route_id, direction_id, ts_minute
ORDER BY ts_minute, route_id, direction_id;
```

```sql
-- Per 5‑minute aligned buckets
DECLARE start_ts TIMESTAMP DEFAULT TIMESTAMP('2025-09-01 06:00:00+00');
DECLARE end_ts   TIMESTAMP DEFAULT TIMESTAMP('2025-09-01 10:00:00+00');

SELECT
  route_id,
  direction_id,
  TIMESTAMP_SECONDS(300 * DIV(UNIX_SECONDS(feed_ts_utc), 300)) AS ts_5min,
  COUNT(DISTINCT trip_uid) AS trips_observed
FROM {{ ref('fact_trips_stops') }}
WHERE feed_ts_utc BETWEEN start_ts AND end_ts
GROUP BY route_id, direction_id, ts_5min
ORDER BY ts_5min, route_id, direction_id;
```

### Service Delivered (by route/direction within a local window)

```sql
DECLARE service_day DATE DEFAULT DATE('2025-09-01');
DECLARE tz STRING DEFAULT 'America/New_York';
DECLARE start_local_time STRING DEFAULT '06:00:00';
DECLARE end_local_time   STRING DEFAULT '10:00:00';

-- 1) Scheduled terminal departures in window
WITH cal AS (
  SELECT service_id
  FROM {{ source('mta', 'calendar') }}
  WHERE start_date <= service_day AND end_date >= service_day
    AND (
      (EXTRACT(DAYOFWEEK FROM service_day)=1 AND sunday=1) OR
      (EXTRACT(DAYOFWEEK FROM service_day)=2 AND monday=1) OR
      (EXTRACT(DAYOFWEEK FROM service_day)=3 AND tuesday=1) OR
      (EXTRACT(DAYOFWEEK FROM service_day)=4 AND wednesday=1) OR
      (EXTRACT(DAYOFWEEK FROM service_day)=5 AND thursday=1) OR
      (EXTRACT(DAYOFWEEK FROM service_day)=6 AND friday=1) OR
      (EXTRACT(DAYOFWEEK FROM service_day)=7 AND saturday=1)
    )
),
trips_active AS (
  SELECT t.trip_id, t.route_id, t.direction_id, t.service_id
  FROM {{ source('mta', 'trips') }} t
  JOIN cal USING (service_id)
),
first_stop AS (
  SELECT trip_id, MIN(CAST(stop_sequence AS INT64)) AS min_seq
  FROM {{ source('mta', 'stop_times') }}
  GROUP BY trip_id
),
sched AS (
  SELECT
    ta.route_id,
    ta.direction_id,
    st.trip_id,
    TIMESTAMP(
      DATETIME_ADD(DATETIME(service_day, TIME(0,0,0)), INTERVAL CAST(SPLIT(st.departure_time, ':')[OFFSET(0)] AS INT64) HOUR)
      + INTERVAL CAST(SPLIT(st.departure_time, ':')[OFFSET(1)] AS INT64) MINUTE
      + INTERVAL CAST(SPLIT(st.departure_time, ':')[OFFSET(2)] AS INT64) SECOND
    ) AS sched_departure_ts
  FROM trips_active ta
  JOIN first_stop fs  ON fs.trip_id = ta.trip_id
  JOIN {{ source('mta', 'stop_times') }} st
    ON st.trip_id = ta.trip_id AND st.stop_sequence = CAST(fs.min_seq AS STRING)
),
window_local AS (
  SELECT route_id, direction_id, trip_id, sched_departure_ts
  FROM sched
  WHERE TIME(FORMAT_TIMESTAMP('%T', sched_departure_ts, tz)) BETWEEN start_local_time AND end_local_time
),
-- 2) Actual terminal departures from TU: first event per trip
first_event AS (
  SELECT
    route_id,
    direction_id,
    trip_uid,
    ARRAY_AGG(STRUCT(stop_sequence_int, evt TIMESTAMP) ORDER BY stop_sequence_int ASC LIMIT 1)[OFFSET(0)] AS first_evt
  FROM (
    SELECT route_id, direction_id, trip_uid, stop_sequence_int,
           COALESCE(departure_ts_utc, arrival_ts_utc) AS evt
    FROM {{ ref('fact_trips_stops') }}
  )
  WHERE evt IS NOT NULL
  GROUP BY route_id, direction_id, trip_uid
),
actual_in_window AS (
  SELECT route_id, direction_id, trip_uid
  FROM first_event
  WHERE first_evt.evt IS NOT NULL
    AND TIME(FORMAT_TIMESTAMP('%T', first_evt.evt, tz)) BETWEEN start_local_time AND end_local_time
)
SELECT
  s.route_id,
  s.direction_id,
  COUNT(DISTINCT s.trip_id) AS scheduled_trips,
  COUNT(DISTINCT a.trip_uid) AS delivered_trips,
  SAFE_DIVIDE(COUNT(DISTINCT a.trip_uid), COUNT(DISTINCT s.trip_id)) AS service_delivered
FROM window_local s
LEFT JOIN actual_in_window a USING (route_id, direction_id)
GROUP BY s.route_id, s.direction_id
ORDER BY s.route_id, s.direction_id;
```

### Terminal On‑Time Performance (OTP)

```sql
-- Terminal on-time within 0–5 minutes vs scheduled terminal depart
WITH first_event AS (
  SELECT
    route_id,
    direction_id,
    trip_uid,
    ARRAY_AGG(COALESCE(departure_ts_utc, arrival_ts_utc)
              ORDER BY stop_sequence_int ASC LIMIT 1)[OFFSET(0)] AS actual_departure_ts
  FROM {{ ref('fact_trips_stops') }}
  GROUP BY route_id, direction_id, trip_uid
)
SELECT
  wl.route_id,
  wl.direction_id,
  100 * AVG(CASE WHEN TIMESTAMP_DIFF(fe.actual_departure_ts, wl.sched_departure_ts, MINUTE) BETWEEN 0 AND 5 THEN 1 ELSE 0 END) AS otp_pct
FROM (
  -- Replace with scheduled terminal window from the previous query (window_local)
  SELECT route_id, direction_id, trip_id, TIMESTAMP('2025-09-01 07:00:00+00') AS sched_departure_ts
  FROM UNNEST([])
) wl
LEFT JOIN first_event fe USING (route_id, direction_id)
GROUP BY wl.route_id, wl.direction_id
ORDER BY wl.route_id, wl.direction_id;
```

### Headways at a stop (screenline)

```sql
DECLARE screenline_stop_id STRING DEFAULT 'R14N';
DECLARE service_day DATE DEFAULT DATE('2025-09-01');
DECLARE tz STRING DEFAULT 'America/New_York';

WITH seen AS (
  SELECT
    TIMESTAMP_TRUNC(event_ts_utc, SECOND) AS pass_ts,
    route_id,
    direction_id,
    trip_uid
  FROM {{ ref('fact_trips_stops') }}
  WHERE stop_id = screenline_stop_id
    AND DATE(event_ts_utc, tz) = service_day
),
ordered AS (
  SELECT *, ROW_NUMBER() OVER (ORDER BY pass_ts) AS rn
  FROM (
    SELECT pass_ts, route_id, direction_id, trip_uid
    FROM seen
    QUALIFY ROW_NUMBER() OVER (PARTITION BY trip_uid ORDER BY pass_ts) = 1
  )
)
SELECT
  route_id,
  direction_id,
  pass_ts,
  TIMESTAMP_DIFF(pass_ts, LAG(pass_ts) OVER (PARTITION BY route_id, direction_id ORDER BY pass_ts), SECOND) AS headway_s
FROM ordered
WHERE rn > 1
ORDER BY pass_ts;
```

### Dwell time per stop

```sql
SELECT
  route_id,
  direction_id,
  stop_id,
  trip_uid,
  TIMESTAMP_DIFF(departure_ts_utc, arrival_ts_utc, SECOND) AS dwell_s
FROM {{ ref('fact_trips_stops') }}
WHERE arrival_ts_utc IS NOT NULL AND departure_ts_utc IS NOT NULL;
```

### Run time between two stops (A -> B) on the same trip

```sql
DECLARE stop_a STRING DEFAULT 'R14N';
DECLARE stop_b STRING DEFAULT 'R16N';

WITH a AS (
  SELECT trip_uid, COALESCE(departure_ts_utc, arrival_ts_utc) AS ts_a
  FROM {{ ref('fact_trips_stops') }}
  WHERE stop_id = stop_a
),
b AS (
  SELECT trip_uid, COALESCE(arrival_ts_utc, departure_ts_utc) AS ts_b
  FROM {{ ref('fact_trips_stops') }}
  WHERE stop_id = stop_b
)
SELECT
  ta.trip_uid,
  TIMESTAMP_DIFF(tb.ts_b, ta.ts_a, SECOND) AS runtime_s
FROM a ta
JOIN b tb USING (trip_uid)
WHERE tb.ts_b >= ta.ts_a;
```

### Excess delay vs schedule at a stop

```sql
DECLARE service_day DATE DEFAULT DATE('2025-09-01');
DECLARE stop_ref STRING DEFAULT 'R14N';

WITH sched AS (
  SELECT
    t.route_id,
    t.direction_id,
    st.trip_id,
    st.stop_id,
    TIMESTAMP(
      DATETIME_ADD(DATETIME(service_day, TIME(0,0,0)), INTERVAL CAST(SPLIT(st.departure_time, ':')[OFFSET(0)] AS INT64) HOUR)
      + INTERVAL CAST(SPLIT(st.departure_time, ':')[OFFSET(1)] AS INT64) MINUTE
      + INTERVAL CAST(SPLIT(st.departure_time, ':')[OFFSET(2)] AS INT64) SECOND
    ) AS sched_departure_ts
  FROM {{ source('mta', 'stop_times') }} st
  JOIN {{ source('mta', 'trips') }} t USING (trip_id)
  WHERE st.stop_id = stop_ref
),
actual AS (
  SELECT route_id, direction_id, stop_id, trip_uid, COALESCE(departure_ts_utc, arrival_ts_utc) AS actual_ts
  FROM {{ ref('fact_trips_stops') }}
  WHERE stop_id = stop_ref
)
SELECT
  a.route_id,
  a.direction_id,
  a.stop_id,
  APPROX_QUANTILES(TIMESTAMP_DIFF(a.actual_ts, s.sched_departure_ts, SECOND), 100)[OFFSET(50)] AS p50_delay_s,
  APPROX_QUANTILES(TIMESTAMP_DIFF(a.actual_ts, s.sched_departure_ts, SECOND), 100)[OFFSET(90)] AS p90_delay_s
FROM actual a
JOIN sched s USING (route_id, direction_id)
GROUP BY a.route_id, a.direction_id, a.stop_id;
```

### Trip completeness (first and last stop seen)

```sql
WITH agg AS (
  SELECT
    trip_uid,
    MIN(stop_sequence_int) AS min_seq,
    MAX(stop_sequence_int) AS max_seq,
    COUNTIF(COALESCE(arrival_ts_utc, departure_ts_utc) IS NOT NULL) AS stops_seen
  FROM {{ ref('fact_trips_stops') }}
  GROUP BY trip_uid
)
SELECT
  COUNT(*) AS trips_total,
  COUNTIF(stops_seen >= 2) AS trips_with_start_and_end,
  SAFE_DIVIDE(COUNTIF(stops_seen >= 2), COUNT(*)) AS completeness_rate
FROM agg;
```

### Added / Canceled trips share

```sql
SELECT
  route_id,
  direction_id,
  100 * AVG(CASE WHEN schedule_relationship = 'ADDED' THEN 1 ELSE 0 END)     AS added_pct,
  100 * AVG(CASE WHEN schedule_relationship = 'CANCELED' THEN 1 ELSE 0 END)  AS canceled_pct
FROM {{ ref('fact_trips') }}
GROUP BY route_id, direction_id
ORDER BY route_id, direction_id;
```

### Feed latency (ingest vs feed timestamp)

```sql
SELECT
  route_id,
  direction_id,
  TIMESTAMP_TRUNC(first_feed_ts_utc, MINUTE) AS ts_minute,
  AVG(TIMESTAMP_DIFF(first_ingest_ts, first_feed_ts_utc, SECOND)) AS avg_latency_s
FROM {{ ref('fact_trips') }}
GROUP BY route_id, direction_id, ts_minute
ORDER BY ts_minute;
```

### Wait Assessment at a stop (observed headway vs scheduled headway)

```sql
DECLARE stop_ref STRING DEFAULT 'R14N';
DECLARE tz STRING DEFAULT 'America/New_York';

-- Observed headways from realtime
WITH obs AS (
  SELECT
    route_id,
    direction_id,
    TIMESTAMP_TRUNC(event_ts_utc, SECOND) AS pass_ts
  FROM {{ ref('fact_trips_stops') }}
  WHERE stop_id = stop_ref
),
obs_hw AS (
  SELECT
    route_id,
    direction_id,
    pass_ts,
    TIMESTAMP_DIFF(pass_ts, LAG(pass_ts) OVER (PARTITION BY route_id, direction_id ORDER BY pass_ts), SECOND) AS headway_s
  FROM obs
),
-- Scheduled headways from static
sch AS (
  SELECT
    t.route_id,
    t.direction_id,
    TIMESTAMP(
      DATETIME '2025-09-01 00:00:00' +
      INTERVAL CAST(SPLIT(st.departure_time, ':')[OFFSET(0)] AS INT64) HOUR +
      INTERVAL CAST(SPLIT(st.departure_time, ':')[OFFSET(1)] AS INT64) MINUTE +
      INTERVAL CAST(SPLIT(st.departure_time, ':')[OFFSET(2)] AS INT64) SECOND
    ) AS sched_ts
  FROM {{ source('mta', 'stop_times') }} st
  JOIN {{ source('mta', 'trips') }} t USING (trip_id)
  WHERE st.stop_id = stop_ref
),
sch_hw AS (
  SELECT
    route_id,
    direction_id,
    sched_ts,
    TIMESTAMP_DIFF(sched_ts, LAG(sched_ts) OVER (PARTITION BY route_id, direction_id ORDER BY sched_ts), SECOND) AS sched_headway_s
  FROM sch
)
SELECT
  o.route_id,
  o.direction_id,
  100 * AVG(CASE WHEN o.headway_s <= 2 * s.sched_headway_s THEN 1 ELSE 0 END) AS wait_assessment_pct
FROM obs_hw o
JOIN sch_hw s USING (route_id, direction_id)
WHERE o.headway_s IS NOT NULL AND s.sched_headway_s IS NOT NULL
GROUP BY o.route_id, o.direction_id
ORDER BY o.route_id, o.direction_id;
```

---

### Tips

- Use `trip_uid` for stable trip identity across snapshots.
- Prefer `event_ts_utc` for generic event timing; switch to departure‑first logic when needed.
- For production dashboards, consider materializing and partitioning by `DATE(feed_ts_utc)` (stop-level) and `DATE(last_feed_ts_utc)` (trip-level).


