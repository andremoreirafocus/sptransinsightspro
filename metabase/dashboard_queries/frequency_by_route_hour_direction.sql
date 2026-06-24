-- Dashboard query — Panels P4 (route x hour heatmap) and P5 (split by direction).
-- Anchor: trip start (started_at_time_dim_key).
-- Metabase native SQL. Map these field filters / variables in the question editor:
--   {{date_range}}  Field Filter -> refined.dim_time.date_actual   (default dashboard value: Previous 30 days)
--   {{route}}       Field Filter -> refined.trip_facts.route_id    (optional)
--   {{direction}}   Field Filter -> refined.trip_facts.direction   (optional; used by P5)
--   {{is_weekend}}  Field Filter -> refined.dim_time.is_weekend    (optional)
--   {{is_circular}} Field Filter -> refined.trip_facts.is_circular (optional)
-- P4 aggregates over direction in the visualization; P5 uses the direction breakdown.
-- (Fully-qualified table names, no aliases, so Metabase field filters resolve correctly.)
SELECT
    refined.trip_facts.route_id,
    refined.trip_facts.direction,
    refined.dim_time.hour_of_day,
    refined.trip_facts.is_circular,
    COUNT(*) AS trip_count
FROM refined.trip_facts
JOIN refined.dim_time
  ON refined.dim_time.time_key = refined.trip_facts.started_at_time_dim_key
WHERE 1 = 1
    [[ AND {{date_range}} ]]
    [[ AND {{route}} ]]
    [[ AND {{direction}} ]]
    [[ AND {{is_weekend}} ]]
    [[ AND {{is_circular}} ]]
GROUP BY
    refined.trip_facts.route_id,
    refined.trip_facts.direction,
    refined.dim_time.hour_of_day,
    refined.trip_facts.is_circular
ORDER BY
    refined.trip_facts.route_id,
    refined.trip_facts.direction,
    refined.dim_time.hour_of_day;
