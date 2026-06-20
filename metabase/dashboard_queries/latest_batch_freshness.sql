-- Dashboard query — Panel P0: Latest batch finished trips (freshness).
-- No parameters. Render latest_batch_ts in São Paulo local time
-- (datasource Report Timezone = America/Sao_Paulo).
SELECT
    MAX(logic_date)                                                                AS latest_batch_ts,
    COUNT(*) FILTER (
        WHERE logic_date = (SELECT MAX(logic_date) FROM refined.trip_facts)
    )                                                                              AS latest_batch_trip_count
FROM refined.trip_facts;
