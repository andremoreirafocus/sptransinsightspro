-- Dashboard query — Panel P8: Reliability ranking (coefficient of variation).
-- Anchor: trip start (started_at_time_dim_key) — dim_time joined for the date filter.
-- reliability_index = STDDEV(duration_seconds) / AVG(duration_seconds); lower = more consistent.
-- Metabase native SQL. Map these field filters / variables in the question editor:
--   {{date_range}}  Field Filter -> refined.dim_time.date_actual   (default: Previous 30 days)
--   {{route}}       Field Filter -> refined.trip_facts.route_id    (optional)
--   {{is_circular}} Field Filter -> refined.trip_facts.is_circular (optional)
--   {{min_trips}}   Number       -> low-sample guard (default 5)
-- Sort ASC = most reliable first; flip to DESC for the least-reliable view (same question).
SELECT
    refined.trip_facts.route_id,
    refined.trip_facts.is_circular,
    STDDEV(refined.trip_facts.duration_seconds) / NULLIF(AVG(refined.trip_facts.duration_seconds), 0) AS reliability_index,
    COUNT(*) AS trip_count
FROM refined.trip_facts
JOIN refined.dim_time
  ON refined.dim_time.time_key = refined.trip_facts.started_at_time_dim_key
WHERE 1 = 1
    [[ AND {{date_range}} ]]
    [[ AND {{route}} ]]
    [[ AND {{is_circular}} ]]
GROUP BY
    refined.trip_facts.route_id,
    refined.trip_facts.is_circular
[[ HAVING COUNT(*) >= {{min_trips}} ]]
ORDER BY reliability_index ASC NULLS LAST;
