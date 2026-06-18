SELECT
    tf.route_id,
    tf.direction,
    d.hour_of_day,
    tf.is_circular,
    COUNT(*) AS trip_count
FROM refined.trip_facts tf
JOIN refined.dim_time d ON d.time_key = tf.started_at_time_dim_key
GROUP BY tf.route_id, tf.direction, d.hour_of_day, tf.is_circular
ORDER BY tf.route_id, tf.direction, d.hour_of_day;
