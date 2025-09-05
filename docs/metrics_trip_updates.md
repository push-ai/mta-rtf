## Core models and metrics using trip_updates views

This document defines two core BigQuery models for GTFS‑Realtime trip updates and provides ready‑to‑run SQL for key metrics. Replace `project.dataset` with your project and dataset names as needed.

- **Base realtime tables (from loader)**: `project.dataset.trip_updates` and `project.dataset.trip_updates__trip_update__stop_time_update`
- **Static tables** (for schedule joins): `project.dataset.trips`, `project.dataset.stop_times`, `project.dataset.calendar`, `project.dataset.stops`

### Models

- **trip_updates_stops**: one row per stop time update, enriched with stop metadata and helpful timestamps/keys.
- **trip_updates_rollup**: one row per unique trip per feed observation window (trip‑level rollup; earliest and latest events, terminal info, counts).

#### trip_updates_stops (view or incremental table)

```sql
-- Create as a view for agility, or as a partitioned table for performance
CREATE OR REPLACE VIEW `project.dataset.trip_updates_stops` AS
SELECT
  -- Original TU cols (excluding the internal id)
  tu.* EXCEPT(_dlt_id),
  -- Nested stop_time_update fields
  tus.*,
  -- Stop metadata
  s.stop_name,
  s.parent_station,
  s.stop_lat,
  s.stop_lon,

  -- Timestamps readable in UTC
  TIMESTAMP_SECONDS(SAFE_CAST(tu.trip_update__timestamp AS INT64)) AS feed_ts_utc,
  CASE WHEN tus.arrival__time IS NOT NULL THEN TIMESTAMP_SECONDS(SAFE_CAST(tus.arrival__time AS INT64)) END AS arrival_ts_utc,
  CASE WHEN tus.departure__time IS NOT NULL THEN TIMESTAMP_SECONDS(SAFE_CAST(tus.departure__time AS INT64)) END AS departure_ts_utc,

  -- Also provide local readable datetimes (America/New_York)
  CASE WHEN tus.arrival__time IS NOT NULL THEN DATETIME(TIMESTAMP_SECONDS(SAFE_CAST(tus.arrival__time AS INT64)), 'America/New_York') END AS arrival_dt_local,
  CASE WHEN tus.departure__time IS NOT NULL THEN DATETIME(TIMESTAMP_SECONDS(SAFE_CAST(tus.departure__time AS INT64)), 'America/New_York') END AS departure_dt_local,

  -- Unique trip identifier (text + hashed)
  REGEXP_EXTRACT(tu.trip_update__trip__trip_id, r'^-?\d{1,8}') AS rt_origin_code_hundredths,
  CONCAT(
    tu.trip_update__trip__start_date, '|',
    tu.trip_update__trip__route_id,    '|',
    tu.trip_update__trip__direction_id,'|',
    COALESCE(REGEXP_EXTRACT(tu.trip_update__trip__trip_id, r'^-?\d{1,8}'), tu.trip_update__trip__trip_id)
  ) AS trip_uid_text,
  TO_HEX(SHA256(
    CONCAT(
      tu.trip_update__trip__start_date, '|',
      tu.trip_update__trip__route_id,    '|',
      tu.trip_update__trip__direction_id,'|',
      COALESCE(REGEXP_EXTRACT(tu.trip_update__trip__trip_id, r'^-?\d{1,8}'), tu.trip_update__trip__trip_id)
    )
  )) AS trip_uid

FROM `project.dataset.trip_updates` tu
JOIN `project.dataset.trip_updates__trip_update__stop_time_update` tus
  ON tu._dlt_id = tus._dlt_parent_id
JOIN `project.dataset.stops` s
  ON tus.stop_id = s.stop_id;
```

Partitioned table option (faster scans):

```sql
-- One‑time backfill or scheduled write into a partitioned table
CREATE OR REPLACE TABLE `project.dataset.trip_updates_stops_tbl`
PARTITION BY DATE(feed_ts_utc)
CLUSTER BY trip_update__trip__route_id, trip_update__trip__direction_id, stop_id, trip_uid AS
SELECT * FROM `project.dataset.trip_updates_stops`;
```

#### trip_updates_rollup (trip‑level rollup)

```sql
-- View that rolls up stop‑level events to trip‑level features
CREATE OR REPLACE VIEW `project.dataset.trip_updates_rollup` AS
WITH base AS (
  SELECT
    trip_uid,
    trip_uid_text,
    trip_update__trip__trip_id AS rt_trip_id,
    trip_update__trip__route_id AS route_id,
    trip_update__trip__direction_id AS direction_id,
    trip_update__trip__start_date AS service_date,
    rt_origin_code_hundredths,
    feed_ts_utc,
    as_of,
    trip_update__trip__schedule_relationship AS schedule_relationship,
    SAFE_CAST(stop_sequence AS INT64) AS stop_sequence,
    stop_id,
    arrival_ts_utc,
    departure_ts_utc
  FROM `project.dataset.trip_updates_stops`
)
SELECT
  trip_uid,
  ANY_VALUE(trip_uid_text) AS trip_uid_text,
  ANY_VALUE(rt_trip_id)    AS rt_trip_id,
  ANY_VALUE(route_id)      AS route_id,
  ANY_VALUE(direction_id)  AS direction_id,
  ANY_VALUE(service_date)  AS service_date,
  ANY_VALUE(rt_origin_code_hundredths) AS rt_origin_code_hundredths,
  MIN(feed_ts_utc)  AS first_feed_ts_utc,
  MAX(feed_ts_utc)  AS last_feed_ts_utc,
  MIN(as_of)        AS first_ingest_ts,
  MAX(as_of)        AS last_ingest_ts,
  ARRAY_AGG(STRUCT(stop_id, stop_sequence) ORDER BY stop_sequence ASC LIMIT 1)[OFFSET(0)].stop_id AS first_stop_id,
  ARRAY_AGG(STRUCT(stop_id, stop_sequence) ORDER BY stop_sequence DESC LIMIT 1)[OFFSET(0)].stop_id AS last_stop_id,
  MIN(stop_sequence) AS first_stop_sequence,
  MAX(stop_sequence) AS last_stop_sequence,
  -- First/last event timestamps by stop order
  ARRAY_AGG(COALESCE(arrival_ts_utc, departure_ts_utc) ORDER BY stop_sequence ASC LIMIT 1)[OFFSET(0)] AS first_event_ts_utc,
  ARRAY_AGG(COALESCE(arrival_ts_utc, departure_ts_utc) ORDER BY stop_sequence DESC LIMIT 1)[OFFSET(0)] AS last_event_ts_utc,
  -- Labels
  ANY_VALUE(schedule_relationship) AS schedule_relationship
FROM base
GROUP BY trip_uid;
```

Materialized view alternative for the rollup (if base is partitioned and stable):

```sql
CREATE OR REPLACE MATERIALIZED VIEW `project.dataset.mv_trip_updates_rollup` AS
SELECT * FROM `project.dataset.trip_updates_rollup`;
```

### Materialization guidance

- **trip_updates_stops**: if query volume is high, materialize as a table partitioned by `DATE(feed_ts_utc)` and clustered by `(route_id, direction_id, stop_id, trip_uid)`.
- **trip_updates_rollup**: materialize as a view for flexibility; if performance is needed, mirror into a materialized view or incremental table partitioned by `DATE(last_feed_ts_utc)` and clustered by `(route_id, direction_id, trip_uid)`.
- Keep base realtime ingestion (`as_of`) for operational latency metrics; prefer `feed_ts_utc` for time‑bucketed analysis.

## Metrics built on these models

The queries below assume `project.dataset.trip_updates_stops` and `project.dataset.trip_updates_rollup` exist as above.

### Trips observed (per minute and per 5‑minute)

```sql
-- Parameters
DECLARE start_ts TIMESTAMP DEFAULT TIMESTAMP('2025-09-01 06:00:00+00');
DECLARE end_ts   TIMESTAMP DEFAULT TIMESTAMP('2025-09-01 10:00:00+00');

-- Per minute
SELECT
  route_id,
  direction_id,
  TIMESTAMP_TRUNC(feed_ts_utc, MINUTE) AS ts_minute,
  COUNT(DISTINCT trip_uid) AS trips_observed
FROM `project.dataset.trip_updates_stops`
WHERE feed_ts_utc BETWEEN start_ts AND end_ts
GROUP BY route_id, direction_id, ts_minute
ORDER BY ts_minute, route_id, direction_id;
```

```sql
-- Per 5‑minute aligned buckets
SELECT
  route_id,
  direction_id,
  TIMESTAMP_SECONDS(300 * DIV(UNIX_SECONDS(feed_ts_utc), 300)) AS ts_5min,
  COUNT(DISTINCT trip_uid) AS trips_observed
FROM `project.dataset.trip_updates_stops`
WHERE feed_ts_utc BETWEEN start_ts AND end_ts
GROUP BY route_id, direction_id, ts_5min
ORDER BY ts_5min, route_id, direction_id;
```

### Service Delivered (by route/direction within a local window)

```sql
-- Parameters
DECLARE service_day DATE DEFAULT DATE('2025-09-01');
DECLARE tz STRING DEFAULT 'America/New_York';
DECLARE start_local_time STRING DEFAULT '06:00:00';
DECLARE end_local_time   STRING DEFAULT '10:00:00';

-- 1) Scheduled terminal departures in window
WITH cal AS (
  SELECT service_id
  FROM `project.dataset.calendar`
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
  FROM `project.dataset.trips` t
  JOIN cal USING (service_id)
),
first_stop AS (
  SELECT trip_id, MIN(CAST(stop_sequence AS INT64)) AS min_seq
  FROM `project.dataset.stop_times`
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
  JOIN `project.dataset.stop_times` st
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
    tu.route_id,
    tu.direction_id,
    tu.trip_uid,
    ARRAY_AGG(STRUCT(stop_sequence, event_ts TIMESTAMP) ORDER BY stop_sequence ASC LIMIT 1)[OFFSET(0)] AS first_stu
  FROM (
    SELECT
      route_id,
      direction_id,
      trip_uid,
      SAFE_CAST(stop_sequence AS INT64) AS stop_sequence,
      COALESCE(arrival_ts_utc, departure_ts_utc) AS event_ts
    FROM `project.dataset.trip_updates_stops`
  ) tu
  WHERE event_ts IS NOT NULL
  GROUP BY route_id, direction_id, trip_uid
),
actual_in_window AS (
  SELECT route_id, direction_id, trip_uid
  FROM first_event
  WHERE first_stu.event_ts IS NOT NULL
    AND TIME(FORMAT_TIMESTAMP('%T', first_stu.event_ts, 'America/New_York'))
        BETWEEN start_local_time AND end_local_time
)
SELECT
  s.route_id,
  s.direction_id,
  COUNT(DISTINCT s.trip_id) AS scheduled_trips,
  COUNT(DISTINCT a.trip_uid) AS delivered_trips,
  SAFE_DIVIDE(COUNT(DISTINCT a.trip_uid), COUNT(DISTINCT s.trip_id)) AS service_delivered
FROM window_local s
LEFT JOIN (
  -- Map realtime trip_uid back to scheduled trips if you keep an id map; otherwise compare at route/direction granularity
  SELECT route_id, direction_id, trip_uid FROM actual_in_window
) a USING (route_id, direction_id)
GROUP BY s.route_id, s.direction_id
ORDER BY s.route_id, s.direction_id;
```

### Terminal On‑Time Performance (OTP)

```sql
-- Reuse window_local (scheduled) from previous query
WITH first_event AS (
  SELECT
    route_id,
    direction_id,
    trip_uid,
    ARRAY_AGG(COALESCE(arrival_ts_utc, departure_ts_utc) ORDER BY SAFE_CAST(stop_sequence AS INT64) ASC LIMIT 1)[OFFSET(0)] AS actual_departure_ts
  FROM `project.dataset.trip_updates_stops`
  GROUP BY route_id, direction_id, trip_uid
)
SELECT
  wl.route_id,
  wl.direction_id,
  100 * AVG(CASE WHEN TIMESTAMP_DIFF(fe.actual_departure_ts, wl.sched_departure_ts, MINUTE) BETWEEN 0 AND 5 THEN 1 ELSE 0 END) AS otp_pct
FROM (
  -- Scheduled terminal departures within a local window
  SELECT route_id, direction_id, trip_id, sched_departure_ts
  FROM UNNEST([
    STRUCT<route_id STRING, direction_id STRING, trip_id STRING, sched_departure_ts TIMESTAMP> []
  ]) -- Replace by window_local CTE from prior query
) wl
LEFT JOIN first_event fe USING (route_id, direction_id)
GROUP BY wl.route_id, wl.direction_id
ORDER BY wl.route_id, wl.direction_id;
```

### Headways at a screenline using stop updates

```sql
-- Parameters
DECLARE screenline_stop_id STRING DEFAULT 'R14N';
DECLARE service_day DATE DEFAULT DATE('2025-09-01');
DECLARE tz STRING DEFAULT 'America/New_York';

WITH seen AS (
  SELECT
    TIMESTAMP_TRUNC(COALESCE(arrival_ts_utc, departure_ts_utc), SECOND) AS pass_ts,
    route_id,
    direction_id,
    trip_uid
  FROM `project.dataset.trip_updates_stops`
  WHERE stop_id = screenline_stop_id
    AND DATE(COALESCE(arrival_ts_utc, departure_ts_utc), tz) = service_day
),
ordered AS (
  SELECT *, ROW_NUMBER() OVER (ORDER BY pass_ts) AS rn
  FROM (
    -- De‑dupe multiple updates per same trip within the same second
    SELECT pass_ts, route_id, direction_id, trip_uid
    FROM seen
    QUALIFY ROW_NUMBER() OVER (PARTITION BY trip_uid ORDER BY pass_ts) = 1
  )
),
headways AS (
  SELECT
    route_id,
    direction_id,
    pass_ts,
    TIMESTAMP_DIFF(pass_ts, LAG(pass_ts) OVER (PARTITION BY route_id, direction_id ORDER BY pass_ts), SECOND) AS headway_s
  FROM ordered
)
SELECT * FROM headways WHERE headway_s IS NOT NULL ORDER BY pass_ts;
```

### Dwell time per stop

```sql
SELECT
  route_id,
  direction_id,
  stop_id,
  trip_uid,
  TIMESTAMP_DIFF(departure_ts_utc, arrival_ts_utc, SECOND) AS dwell_s
FROM `project.dataset.trip_updates_stops`
WHERE arrival_ts_utc IS NOT NULL AND departure_ts_utc IS NOT NULL;
```

### Run time between two stops (A -> B) on the same trip

```sql
DECLARE stop_a STRING DEFAULT 'R14N';
DECLARE stop_b STRING DEFAULT 'R16N';

WITH a AS (
  SELECT trip_uid, COALESCE(departure_ts_utc, arrival_ts_utc) AS ts_a
  FROM `project.dataset.trip_updates_stops`
  WHERE stop_id = stop_a
),
b AS (
  SELECT trip_uid, COALESCE(arrival_ts_utc, departure_ts_utc) AS ts_b
  FROM `project.dataset.trip_updates_stops`
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
-- Compare actual vs scheduled times for a stop
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
  FROM `project.dataset.stop_times` st
  JOIN `project.dataset.trips` t USING (trip_id)
  WHERE st.stop_id = stop_ref
),
actual AS (
  SELECT route_id, direction_id, stop_id, trip_uid, COALESCE(departure_ts_utc, arrival_ts_utc) AS actual_ts
  FROM `project.dataset.trip_updates_stops`
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
    MIN(SAFE_CAST(stop_sequence AS INT64)) AS min_seq,
    MAX(SAFE_CAST(stop_sequence AS INT64)) AS max_seq,
    COUNTIF(COALESCE(arrival_ts_utc, departure_ts_utc) IS NOT NULL) AS stops_seen
  FROM `project.dataset.trip_updates_stops`
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
FROM `project.dataset.trip_updates_rollup`
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
FROM `project.dataset.trip_updates_rollup`
GROUP BY route_id, direction_id, ts_minute
ORDER BY ts_minute;
```

### Wait Assessment at a stop (observed headway vs scheduled headway)

```sql
DECLARE stop_ref STRING DEFAULT 'R14N';
DECLARE tz STRING DEFAULT 'America/New_York';

-- Observed headways from TU
WITH obs AS (
  SELECT
    route_id,
    direction_id,
    TIMESTAMP_TRUNC(COALESCE(arrival_ts_utc, departure_ts_utc), SECOND) AS pass_ts
  FROM `project.dataset.trip_updates_stops`
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
  FROM `project.dataset.stop_times` st
  JOIN `project.dataset.trips` t USING (trip_id)
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

### Notes

- Use `trip_uid` to uniquely identify a train run across feeds on a service day.
- Prefer `feed_ts_utc` and local conversions for analysis; keep `as_of` for ingestion latency.
- For production dashboards, materialize the models into partitioned/clustered tables and schedule refreshes aligned with feed cadence.


