SELECT
    tf.route_id,
    tf.is_circular,
    STDDEV(tf.duration_seconds) / NULLIF(AVG(tf.duration_seconds), 0) AS reliability_index,
    COUNT(*) AS trip_count
FROM refined.trip_facts tf
GROUP BY tf.route_id, tf.is_circular
ORDER BY reliability_index ASC NULLS LAST;
