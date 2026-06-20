-- Dashboard query — Panel P6: Median and P95 duration by route.
-- Anchor: trip start (started_at_time_dim_key) — dim_time joined for the date/weekend filters.
-- Metabase native SQL. Map these field filters / variables in the question editor:
--   {{date_range}}  Field Filter -> refined.dim_time.date_actual   (default: Previous 30 days)
--   {{direction}}   Field Filter -> refined.trip_facts.direction   (optional)
--   {{is_weekend}}  Field Filter -> refined.dim_time.is_weekend    (optional)
--   {{route}}       Field Filter -> refined.trip_facts.route_id    (optional)
--   {{is_circular}} Field Filter -> refined.trip_facts.is_circular (optional)
--   {{min_trips}}   Number       -> low-sample guard (default 5; percentiles are noisy below it)
SELECT
    refined.trip_facts.route_id,
    refined.trip_facts.direction,
    refined.trip_facts.is_circular,
    PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY refined.trip_facts.duration_seconds) AS median_duration_seconds,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY refined.trip_facts.duration_seconds) AS p95_duration_seconds,
    COUNT(*) AS trip_count
FROM refined.trip_facts
JOIN refined.dim_time
  ON refined.dim_time.time_key = refined.trip_facts.started_at_time_dim_key
WHERE 1 = 1
    [[ AND {{date_range}} ]]
    [[ AND {{direction}} ]]
    [[ AND {{is_weekend}} ]]
    [[ AND {{route}} ]]
    [[ AND {{is_circular}} ]]
GROUP BY
    refined.trip_facts.route_id,
    refined.trip_facts.direction,
    refined.trip_facts.is_circular
[[ HAVING COUNT(*) >= {{min_trips}} ]]
ORDER BY
    refined.trip_facts.route_id,
    refined.trip_facts.direction;
