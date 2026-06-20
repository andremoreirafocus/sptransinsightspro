-- Dashboard query — Panel P10: Route summary with terminal names (drill-down).
-- Anchor: trip start (started_at_time_dim_key) for the date filter.
-- Metabase native SQL. Map these field filters / variables in the question editor:
--   {{route}}       Field Filter -> refined.trip_facts.route_id      (REQUIRED — the drill-down target)
--   {{date_range}}  Field Filter -> refined.dim_time.date_actual     (default: Previous 30 days)
--   {{is_circular}} Field Filter -> refined.trip_details.is_circular (optional)
--   {{min_trips}}   Number       -> low-sample guard (default 5)
-- total_trips / metrics are over the selected route and the selected period (not "today").
SELECT
    refined.trip_facts.route_id,
    refined.trip_details.first_stop_name,
    refined.trip_details.last_stop_name,
    refined.trip_details.is_circular,
    COUNT(*)                                                                          AS total_trips,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY refined.trip_facts.duration_seconds)  AS median_duration_seconds,
    AVG(refined.trip_facts.avg_speed_kmh)                                             AS avg_speed_kmh,
    STDDEV(refined.trip_facts.duration_seconds) / NULLIF(AVG(refined.trip_facts.duration_seconds), 0) AS reliability_index
FROM refined.trip_facts
JOIN refined.trip_details
  ON refined.trip_details.trip_id = refined.trip_facts.trip_id
JOIN refined.dim_time
  ON refined.dim_time.time_key = refined.trip_facts.started_at_time_dim_key
WHERE 1 = 1
    [[ AND {{route}} ]]
    [[ AND {{date_range}} ]]
    [[ AND {{is_circular}} ]]
GROUP BY
    refined.trip_facts.route_id,
    refined.trip_details.first_stop_name,
    refined.trip_details.last_stop_name,
    refined.trip_details.is_circular
[[ HAVING COUNT(*) >= {{min_trips}} ]]
ORDER BY refined.trip_facts.route_id;
