SELECT
    MAX(logic_date)                                                                AS latest_batch_ts,
    COUNT(*) FILTER (
        WHERE logic_date = (SELECT MAX(logic_date) FROM refined.trip_facts)
    )                                                                              AS latest_batch_trip_count
FROM refined.trip_facts;
