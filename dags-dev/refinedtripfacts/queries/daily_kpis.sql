SELECT
    COUNT(*)                    AS trips_completed_today,
    COUNT(DISTINCT tf.route_id) AS active_routes_today,
    COUNT(DISTINCT tf.vehicle_id) AS active_vehicles_today
FROM refined.trip_facts tf
JOIN refined.dim_time d ON d.time_key = tf.ended_at_time_dim_key
WHERE d.date_actual = current_date;
