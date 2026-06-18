SELECT
    tf.route_id,
    tf.direction,
    tf.is_circular,
    PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY tf.duration_seconds) AS median_duration_seconds,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY tf.duration_seconds) AS p95_duration_seconds,
    COUNT(*) AS trip_count
FROM refined.trip_facts tf
GROUP BY tf.route_id, tf.direction, tf.is_circular
ORDER BY tf.route_id, tf.direction;
