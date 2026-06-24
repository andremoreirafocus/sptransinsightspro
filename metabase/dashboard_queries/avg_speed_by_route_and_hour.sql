-- Dashboard query — Panel P9: Average speed by route and hour.
-- Anchor: trip start (started_at_time_dim_key).
-- Metabase native SQL. Map these field filters in the question editor:
--   {{date_range}}  Field Filter -> refined.dim_time.date_actual   (default: Previous 30 days)
--   {{route}}       Field Filter -> refined.trip_facts.route_id    (optional)
--   {{is_circular}} Field Filter -> refined.trip_facts.is_circular (optional)
-- No is_circular correctness guard: circular trips are direction-split upstream, so
-- avg_speed_kmh is valid for them (see the circular-route note in the design doc).
SELECT
    refined.trip_facts.route_id,
    refined.dim_time.hour_of_day,
    refined.trip_facts.is_circular,
    AVG(refined.trip_facts.avg_speed_kmh) AS avg_speed_kmh,
    COUNT(*)                              AS trip_count
FROM refined.trip_facts
JOIN refined.dim_time
  ON refined.dim_time.time_key = refined.trip_facts.started_at_time_dim_key
WHERE 1 = 1
    [[ AND {{date_range}} ]]
    [[ AND {{route}} ]]
    [[ AND {{is_circular}} ]]
GROUP BY
    refined.trip_facts.route_id,
    refined.dim_time.hour_of_day,
    refined.trip_facts.is_circular
ORDER BY
    refined.trip_facts.route_id,
    refined.dim_time.hour_of_day;
