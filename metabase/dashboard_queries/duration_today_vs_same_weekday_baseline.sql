-- Dashboard query — Panel P7: Today vs same-weekday historical average.
-- Anchor: trip start (started_at_time_dim_key).
-- Window is FIXED: today plus the 4 prior same-weekday dates. The {{date_range}} dashboard
-- filter does NOT apply here.
-- baseline_median = AVG of the per-day medians of those 4 same-weekday dates
--                   (average of daily medians — NOT a single pooled median over the window).
-- Target weekday derived from current_date via EXTRACT(ISODOW ...) (1=Mon..7=Sun, matching
-- dim_time.weekday) — it does not depend on a dim_time row existing for today, so the panel
-- still works in the early hours before any trip has completed today.
-- Metabase native SQL. Field filter:
--   {{route}}  Field Filter -> refined.trip_facts.route_id  (optional)
WITH params AS (
    SELECT
        current_date                           AS today_date,
        EXTRACT(ISODOW FROM current_date)::int AS target_weekday
),
daily AS (   -- per-(route, date) median for each of the last 4 same-weekday dates
    SELECT
        refined.trip_facts.route_id  AS route_id,
        refined.dim_time.date_actual AS date_actual,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY refined.trip_facts.duration_seconds) AS daily_median
    FROM refined.trip_facts
    JOIN refined.dim_time
      ON refined.dim_time.time_key = refined.trip_facts.started_at_time_dim_key
    CROSS JOIN params
    WHERE refined.dim_time.weekday = params.target_weekday
      AND refined.dim_time.date_actual >= (params.today_date - INTERVAL '4 weeks')
      AND refined.dim_time.date_actual <  params.today_date
      [[ AND {{route}} ]]
    GROUP BY refined.trip_facts.route_id, refined.dim_time.date_actual
),
baseline AS (   -- average of the per-day medians, per route
    SELECT route_id, AVG(daily_median) AS baseline_median
    FROM daily
    GROUP BY route_id
),
today AS (   -- today's median per route
    SELECT
        refined.trip_facts.route_id AS route_id,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY refined.trip_facts.duration_seconds) AS today_median
    FROM refined.trip_facts
    JOIN refined.dim_time
      ON refined.dim_time.time_key = refined.trip_facts.started_at_time_dim_key
    CROSS JOIN params
    WHERE refined.dim_time.date_actual = params.today_date
      [[ AND {{route}} ]]
    GROUP BY refined.trip_facts.route_id
)
SELECT
    today.route_id,
    today.today_median        AS today_median_seconds,
    baseline.baseline_median  AS baseline_median_seconds,
    ROUND(
        ((today.today_median - baseline.baseline_median)
            / NULLIF(baseline.baseline_median, 0) * 100)::numeric,
        2
    ) AS deviation_pct
FROM today
LEFT JOIN baseline ON baseline.route_id = today.route_id
ORDER BY deviation_pct DESC NULLS LAST;
