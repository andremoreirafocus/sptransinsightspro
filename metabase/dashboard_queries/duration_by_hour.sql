-- Dashboard query — Panel P8: Average trip duration by hour of day.
-- Anchor: trip start (started_at_time_dim_key).
-- Grain: one row per hour_of_day. The average is taken across all trips (and routes)
-- that fall in the hour, so the line chart plots a single honest mean per hour and
-- cannot sum across rows. The optional {{route}}/{{is_circular}} filters narrow which
-- trips are averaged.
-- Same construct as the P9 average-speed panel; only the trip_facts metric field differs
-- (duration_seconds instead of avg_speed_kmh, divided by 60 to report minutes).
-- Metabase native SQL. Map these field filters in the question editor:
--   {{date_range}}  Field Filter -> refined.dim_time.date_actual   (default: Previous 30 days)
--   {{route}}       Field Filter -> refined.trip_facts.route_id    (optional)
--   {{is_circular}} Field Filter -> refined.trip_facts.is_circular (optional)
SELECT
    refined.dim_time.hour_of_day,
    ROUND((AVG(refined.trip_facts.duration_seconds) / 60.0)::numeric, 1) AS avg_duration_minutes,
    COUNT(*) AS trip_count
FROM refined.trip_facts
JOIN refined.dim_time
  ON refined.dim_time.time_key = refined.trip_facts.started_at_time_dim_key
WHERE 1 = 1
    [[ AND {{date_range}} ]]
    [[ AND {{route}} ]]
    [[ AND {{is_circular}} ]]
GROUP BY
    refined.dim_time.hour_of_day
ORDER BY
    refined.dim_time.hour_of_day;
