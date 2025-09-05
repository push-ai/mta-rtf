--trip_upsates model
SELECT
  tu.* EXCEPT(_dlt_id),
  tus.*,
  s.stop_name,
  s.parent_station,
  s.stop_lat,
  s.stop_lon,

  -- Readability: convert epoch seconds to UTC TIMESTAMPs
  TIMESTAMP_SECONDS(SAFE_CAST(tu.trip_update__timestamp AS INT64)) AS feed_ts_utc,
  CASE WHEN tus.arrival__time IS NOT NULL
    THEN TIMESTAMP_SECONDS(SAFE_CAST(tus.arrival__time AS INT64))
  END AS arrival_ts_utc,
  CASE WHEN tus.departure__time IS NOT NULL
    THEN TIMESTAMP_SECONDS(SAFE_CAST(tus.departure__time AS INT64))
  END AS departure_ts_utc,

  -- Optional: keep local (America/New_York) readable times too
  CASE WHEN tus.arrival__time IS NOT NULL
    THEN DATETIME(TIMESTAMP_SECONDS(SAFE_CAST(tus.arrival__time AS INT64)), 'America/New_York')
  END AS arrival_dt_local,
  CASE WHEN tus.departure__time IS NOT NULL
    THEN DATETIME(TIMESTAMP_SECONDS(SAFE_CAST(tus.departure__time AS INT64)), 'America/New_York')
  END AS departure_dt_local,

  -- Unique trip identifier (text + hashed)
  -- Spec suggests uniqueness by OriginTime + Route + Direction within a service day.
  -- We use decoded origin code when present; otherwise fall back to full GTFS-rt trip_id.
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

FROM `mta_subway.trip_updates` tu
JOIN `push-ai-internal.mta_subway.trip_updates__trip_update__stop_time_update` tus
  ON tu._dlt_id = tus._dlt_parent_id
JOIN `mta_subway.stops` s
  ON tus.stop_id = s.stop_id
WHERE tu.trip_update__trip__route_id = 'L'
ORDER BY tu.trip_update__trip__trip_id, tus.stop_sequence DESC
LIMIT 25;