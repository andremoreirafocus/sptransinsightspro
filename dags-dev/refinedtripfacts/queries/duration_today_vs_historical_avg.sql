WITH today_ref AS (
    SELECT
        d.weekday   AS today_weekday,
        d.date_actual AS today_date
    FROM refined.dim_time d
    WHERE d.date_actual = current_date
    LIMIT 1
),
historical AS (
    SELECT
        tf.route_id,
        tf.is_circular,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tf.duration_seconds) AS historical_median_seconds
    FROM refined.trip_facts tf
    JOIN refined.dim_time d  ON d.time_key = tf.started_at_time_dim_key
    JOIN today_ref       tr  ON d.weekday  = tr.today_weekday
    WHERE d.date_actual >= (tr.today_date - INTERVAL '4 weeks')
      AND d.date_actual <  tr.today_date
    GROUP BY tf.route_id, tf.is_circular
),
today AS (
    SELECT
        tf.route_id,
        tf.is_circular,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tf.duration_seconds) AS today_median_seconds
    FROM refined.trip_facts tf
    JOIN refined.dim_time d ON d.time_key = tf.started_at_time_dim_key
    JOIN today_ref       tr ON d.date_actual = tr.today_date
    GROUP BY tf.route_id, tf.is_circular
)
SELECT
    t.route_id,
    t.is_circular,
    t.today_median_seconds,
    h.historical_median_seconds,
    ROUND(
        (
            (t.today_median_seconds - h.historical_median_seconds)
                / NULLIF(h.historical_median_seconds, 0) * 100
        )::numeric,
        2
    ) AS deviation_pct
FROM today t
LEFT JOIN historical h ON h.route_id = t.route_id
ORDER BY deviation_pct DESC NULLS LAST;
