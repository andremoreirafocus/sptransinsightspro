-- Dashboard query — Panels P1/P2/P3: Today at a glance.
-- Anchor: trip completion (ended_at_time_dim_key). No parameters.
-- "today" = current_date, resolved São Paulo-local via the datasource Report Timezone
-- (America/Sao_Paulo). See metabase-complementary-implementation_plan_pending.md.
SELECT
    COUNT(*)                      AS trips_completed_today,
    COUNT(DISTINCT tf.route_id)   AS active_routes_today,
    COUNT(DISTINCT tf.vehicle_id) AS active_vehicles_today
FROM refined.trip_facts tf
JOIN refined.dim_time d ON d.time_key = tf.ended_at_time_dim_key
WHERE d.date_actual = current_date;
