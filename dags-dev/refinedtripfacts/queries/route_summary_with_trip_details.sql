SELECT
    tf.route_id,
    td.first_stop_name,
    td.last_stop_name,
    td.is_circular,
    COUNT(*)                                                                        AS total_trips,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tf.duration_seconds)               AS median_duration_seconds,
    AVG(tf.avg_speed_kmh)                                                           AS avg_speed_kmh,
    STDDEV(tf.duration_seconds) / NULLIF(AVG(tf.duration_seconds), 0)              AS reliability_index
FROM refined.trip_facts tf
JOIN refined.trip_details td ON td.trip_id = tf.trip_id
GROUP BY tf.route_id, td.first_stop_name, td.last_stop_name, td.is_circular
ORDER BY tf.route_id;
