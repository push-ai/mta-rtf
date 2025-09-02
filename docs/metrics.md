# NYC Subway GTFS Metrics Playbook

This document outlines key service metrics you can compute with the tables loaded by this project, along with practical BigQuery examples. Where relevant, the framing aligns with MTA KPIs such as Service Delivered and Wait Assessment shown on the public dashboard (see: [MTA Metrics – Service Delivered](https://metrics.mta.info/?subway/servicedelivered)).

## Data inputs in this project

- Final dataset: `mta_subway`
  - Static (names/metadata): `routes`, `stops`, `trips`, `stop_times`, `calendar`
  - Realtime (append with history): `trip_updates`, `vehicle_positions`, `alerts`
- Staging dataset: `mta_subway_staging` (raw landing managed by dlt; useful for debugging)

Notes
- Realtime tables store one row per GTFS-RT message entity with an ingestion timestamp column `as_of`.
- Realtime payloads are nested structures under columns like `trip_update`, `vehicle`, or `alert`.
- Static GTFS contains scheduled times by service day (from `calendar`) and `stop_times` contain HH:MM:SS strings which must be combined with a service date to get timestamps.

## Time conventions

- Use America/New_York where you present metrics. BigQuery examples below use UTC internally and convert to local time when needed.
- Define a window (e.g., 06:00–22:00 local) and compute metrics per route/direction and/or screenline station(s).

## Helper: scheduled trips in a window (by terminal departure)

This computes how many trips were scheduled to depart their first stop (terminal) in a time window.

```sql
-- Parameters
DECLARE start_date DATE DEFAULT DATE('2025-09-01');  -- service day
DECLARE tz STRING DEFAULT 'America/New_York';
DECLARE start_local_time STRING DEFAULT '06:00:00';   -- local time window start
DECLARE end_local_time   STRING DEFAULT '10:00:00';   -- local time window end

-- Scheduled terminal departures in window
WITH cal AS (
  SELECT service_id
  FROM `push-ai-internal.mta_subway.calendar`
  WHERE start_date <= start_date
    AND end_date   >= start_date
    AND (
      (EXTRACT(DAYOFWEEK FROM start_date)=1 AND sunday=1) OR
      (EXTRACT(DAYOFWEEK FROM start_date)=2 AND monday=1) OR
      (EXTRACT(DAYOFWEEK FROM start_date)=3 AND tuesday=1) OR
      (EXTRACT(DAYOFWEEK FROM start_date)=4 AND wednesday=1) OR
      (EXTRACT(DAYOFWEEK FROM start_date)=5 AND thursday=1) OR
      (EXTRACT(DAYOFWEEK FROM start_date)=6 AND friday=1) OR
      (EXTRACT(DAYOFWEEK FROM start_date)=7 AND saturday=1)
    )
),
trips_active AS (
  SELECT t.trip_id, t.route_id, t.direction_id, t.service_id
  FROM `push-ai-internal.mta_subway.trips` t
  JOIN cal USING (service_id)
),
first_stop AS (
  SELECT trip_id, MIN(CAST(stop_sequence AS INT64)) AS min_seq
  FROM `push-ai-internal.mta_subway.stop_times`
  GROUP BY trip_id
),
terminal_departures AS (
  SELECT
    ta.route_id,
    ta.direction_id,
    st.trip_id,
    -- HH:MM:SS to TIMESTAMP on service_date (handles times >= 24:00:00)
    TIMESTAMP(
      DATETIME_ADD(
        DATETIME(start_date, TIME(0,0,0)),
        INTERVAL CAST(SPLIT(st.departure_time, ':')[OFFSET(0)] AS INT64) HOUR
      )
      + INTERVAL CAST(SPLIT(st.departure_time, ':')[OFFSET(1)] AS INT64) MINUTE
      + INTERVAL CAST(SPLIT(st.departure_time, ':')[OFFSET(2)] AS INT64) SECOND
    ) AS sched_departure_ts
  FROM trips_active ta
  JOIN first_stop fs  ON fs.trip_id = ta.trip_id
  JOIN `push-ai-internal.mta_subway.stop_times` st
    ON st.trip_id = ta.trip_id AND st.stop_sequence = CAST(fs.min_seq AS STRING)
),
window_local AS (
  SELECT
    route_id,
    direction_id,
    trip_id,
    sched_departure_ts,
    FORMAT_TIMESTAMP('%F %T', sched_departure_ts, tz) AS sched_departure_local
  FROM terminal_departures
  WHERE TIME(FORMAT_TIMESTAMP('%T', sched_departure_ts, tz))
        BETWEEN start_local_time AND end_local_time
)
SELECT * FROM window_local;
```

## Helper: actual trips observed (by terminal)

We identify actual trips by taking the earliest `stop_time_update` in each `trip_update` for the trip/date and using its arrival/departure as the start. Depending on how dlt mapped the nested fields, you will either access them as STRUCTs or JSON. Example below assumes nested STRUCTs; adjust to `JSON_VALUE`/`JSON_QUERY` if your columns are STRING JSON.

```sql
-- Earliest observed stop_time_update per trip on the service date
WITH rt AS (
  SELECT
    tu.feed,
    tu.as_of,
    tu.trip_update.trip.trip_id AS trip_id,
    tu.trip_update.trip.route_id AS route_id,
    tu.trip_update.stop_time_update AS stu  -- ARRAY<STRUCT<...>>
  FROM `push-ai-internal.mta_subway.trip_updates` tu
  WHERE DATE(tu.as_of, 'America/New_York') = DATE('2025-09-01')
),
first_event AS (
  SELECT
    route_id,
    trip_id,
    (SELECT AS STRUCT s.* FROM UNNEST(stu) s ORDER BY s.stop_sequence LIMIT 1) AS first_stu,
    MIN(as_of) AS first_seen
  FROM rt
  GROUP BY route_id, trip_id, stu
)
SELECT
  route_id,
  trip_id,
  first_stu.stop_id AS terminal_stop_id,
  COALESCE(first_stu.departure.time, first_stu.arrival.time) AS first_epoch_seconds,
  TIMESTAMP_MILLIS(COALESCE(first_stu.departure.time, first_stu.arrival.time) * 1000) AS actual_departure_ts
FROM first_event;
```

If the `trip_update` column is stored as STRING JSON, extract via:

```sql
-- Example JSON extraction
SELECT
  JSON_VALUE(tu.trip_update, '$.trip.trip_id') AS trip_id,
  JSON_VALUE(tu.trip_update, '$.trip.route_id') AS route_id
FROM `push-ai-internal.mta_subway.trip_updates` tu;
```

---

## Metric 1: Service Delivered (by route/direction)

Definition
- Percentage of scheduled trips that were actually run (departed terminal) during a window.

Formula
- service_delivered = delivered_trips / scheduled_trips

Query pattern
1) Compute `scheduled_trips_in_window` (helper above).
2) Compute `actual_terminal_departures_in_window` from `trip_updates` earliest `stop_time_update` per trip falling in the same local window.
3) Join on `trip_id` and aggregate by `route_id`, `direction_id`.

```sql
-- Assume window_local (scheduled) from helper and actual_first (actuals) as:
WITH actual_first AS (
  SELECT DISTINCT
    route_id,
    trip_id,
    -- Convert epoch seconds to local time window and filter
    TIMESTAMP_MILLIS(COALESCE(stu.departure.time, stu.arrival.time) * 1000) AS actual_departure_ts
  FROM (
    SELECT
      tu.trip_update.trip.route_id AS route_id,
      tu.trip_update.trip.trip_id AS trip_id,
      (SELECT AS STRUCT s.* FROM UNNEST(tu.trip_update.stop_time_update) s ORDER BY s.stop_sequence LIMIT 1) AS stu
    FROM `push-ai-internal.mta_subway.trip_updates` tu
    WHERE DATE(tu.as_of, 'America/New_York') = DATE('2025-09-01')
  )
),
actual_in_window AS (
  SELECT route_id, trip_id
  FROM actual_first
  WHERE TIME(FORMAT_TIMESTAMP('%T', actual_departure_ts, 'America/New_York'))
        BETWEEN '06:00:00' AND '10:00:00'
)
SELECT
  s.route_id,
  s.direction_id,
  COUNT(DISTINCT s.trip_id)                       AS scheduled_trips,
  COUNT(DISTINCT a.trip_id)                       AS delivered_trips,
  SAFE_DIVIDE(COUNT(DISTINCT a.trip_id), COUNT(DISTINCT s.trip_id)) AS service_delivered
FROM window_local s
LEFT JOIN actual_in_window a USING (route_id, trip_id)
GROUP BY s.route_id, s.direction_id
ORDER BY s.route_id, s.direction_id;
```

Interpretation
- Compare to MTA’s “Service Delivered” on the dashboard for directional lines (see: [MTA Service Delivered](https://metrics.mta.info/?subway/servicedelivered)).

## Metric 2: Headway and Wait Assessment (at a screenline)

Concepts
- Observed headway: time between trains passing a reference stop in a direction.
- Scheduled headway: time between scheduled departures at the same stop.
- Average Wait Time (AWT): headway / 2 (random arrivals assumption).
- Additional Wait Time (AWT_excess): observed_AWT − scheduled_AWT.
- Wait Assessment: % of observed headways within tolerance vs schedule (e.g., ≤ 2× scheduled headway, or within ±x seconds).

Observed headways via `vehicle_positions` example

```sql
-- Choose a screenline stop_id and direction
DECLARE screenline_stop_id STRING DEFAULT 'R14N';  -- example stop ID
DECLARE tz STRING DEFAULT 'America/New_York';

WITH seen AS (
  SELECT
    TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(vehicle.timestamp * 1000), SECOND) AS ts,
    vehicle.trip.route_id AS route_id,
    vehicle.trip.trip_id  AS trip_id,
    vehicle.stop_id       AS stop_id,
    vehicle.current_status AS status
  FROM `push-ai-internal.mta_subway.vehicle_positions`
  WHERE vehicle.stop_id = screenline_stop_id
    AND DATE(TIMESTAMP_MILLIS(vehicle.timestamp * 1000), tz) = DATE('2025-09-01')
),
ordered AS (
  SELECT *, ROW_NUMBER() OVER (ORDER BY ts) AS rn
  FROM seen
),
headways AS (
  SELECT
    a.route_id,
    a.stop_id,
    a.ts AS pass_ts,
    TIMESTAMP_DIFF(a.ts, LAG(a.ts) OVER (ORDER BY a.ts), SECOND) AS headway_s
  FROM ordered a
)
SELECT * FROM headways WHERE headway_s IS NOT NULL ORDER BY pass_ts;
```

Scheduled headways (from `stop_times` at the same stop and window)

```sql
-- Build scheduled departures at the same stop across the window and compute diffs
WITH stop_sched AS (
  SELECT
    t.route_id,
    st.stop_id,
    TIMESTAMP(
      DATETIME_ADD(DATETIME('2025-09-01', TIME(0,0,0)),
        INTERVAL CAST(SPLIT(st.departure_time, ':')[OFFSET(0)] AS INT64) HOUR)
      + INTERVAL CAST(SPLIT(st.departure_time, ':')[OFFSET(1)] AS INT64) MINUTE
      + INTERVAL CAST(SPLIT(st.departure_time, ':')[OFFSET(2)] AS INT64) SECOND
    ) AS sched_ts
  FROM `push-ai-internal.mta_subway.stop_times` st
  JOIN `push-ai-internal.mta_subway.trips` t USING (trip_id)
  WHERE st.stop_id = 'R14N'
),
headways_sched AS (
  SELECT route_id, stop_id, sched_ts,
    TIMESTAMP_DIFF(sched_ts, LAG(sched_ts) OVER (ORDER BY sched_ts), SECOND) AS sched_headway_s
  FROM stop_sched
)
SELECT * FROM headways_sched WHERE sched_headway_s IS NOT NULL ORDER BY sched_ts;
```

Compute Wait Assessment / AWT

```sql
-- Example tolerance: observed headway <= 2x scheduled headway
WITH obs AS (... headways ...),
     sch AS (... headways_sched ...)
SELECT
  o.route_id,
  o.stop_id,
  AVG(o.headway_s) / 2.0 AS observed_awt_s,
  AVG(s.sched_headway_s) / 2.0 AS scheduled_awt_s,
  (AVG(o.headway_s) - AVG(s.sched_headway_s)) / 2.0 AS additional_wait_time_s,
  100 * AVG(CASE WHEN o.headway_s <= 2*s.sched_headway_s THEN 1 ELSE 0 END) AS wait_assessment_pct
FROM obs o
JOIN sch s USING (route_id, stop_id)
GROUP BY o.route_id, o.stop_id;
```

## Metric 3: Terminal On-Time Performance (OTP)

Definition
- Trip is “on time” if the actual terminal departure is within a tolerance of the scheduled terminal departure (e.g., ≤ 5 minutes late, and not early beyond a threshold if desired).

Query pattern
1) Scheduled terminal departures (`window_local`).
2) Actual terminal departures (`actual_first`).
3) Join and compute difference.

```sql
WITH sched AS (... window_local ...),
actual AS (... actual_in_window with actual_departure_ts ...)
SELECT
  s.route_id,
  s.direction_id,
  COUNT(*) AS trips,
  100 * AVG(CASE WHEN TIMESTAMP_DIFF(a.actual_departure_ts, s.sched_departure_ts, MINUTE) BETWEEN 0 AND 5 THEN 1 ELSE 0 END) AS otp_pct
FROM sched s
LEFT JOIN actual a USING (route_id, trip_id)
GROUP BY s.route_id, s.direction_id
ORDER BY s.route_id, s.direction_id;
```

## Metric 4: Disruption/Alert counts and durations

Alerts (cause/effect) over time by route.

```sql
SELECT
  a.as_of,
  eff.text AS effect,
  cause.text AS cause,
  r
FROM `push-ai-internal.mta_subway.alerts` a,
UNNEST(a.alert.effect) eff,
UNNEST(a.alert.cause) cause
-- Adjust to your actual STRUCT path; if stored as JSON use JSON_VALUE/JSON_QUERY
ORDER BY a.as_of DESC
LIMIT 100;
```

## Practical guidance

- Start with a subset of screenline stations per route/direction for headways/await; confirm stop_ids from `stops`.
- Normalize output tables (views) for: scheduled trips in window, actual terminal departures, and stop-level passages. Reuse them across metrics.
- Keep a small tolerance table to define thresholds (e.g., OTP tolerance minutes, headway multiplier for Wait Assessment).
- If `trip_update`/`vehicle` are JSON strings, replace STRUCT field access with `JSON_VALUE(JSON_QUERY(...))` logic.

## References
- GTFS Realtime Feeds: [MTA API – Subway Real-Time Feeds](https://api.mta.info/#/subwayRealTimeFeeds)
- KPI framing: [MTA Metrics – Service Delivered](https://metrics.mta.info/?subway/servicedelivered)


