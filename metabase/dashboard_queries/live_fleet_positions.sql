-- Dashboard query — Panel P11: Live vehicle positions (map).
-- Source: refined.latest_positions — a LIVE snapshot, full-replaced (TRUNCATE + append)
--   every updatelatestpositions run from the single most recent ~2-min SPTrans batch.
--   It has NO history: there is no dim_time anchor and the global Date range / Route
--   dashboard filters do NOT apply here (this panel is "the fleet, now").
-- Grain guard: refined.latest_positions has NO unique constraint on veiculo_id (only the
--   surrogate id PK). DISTINCT ON (veiculo_id) ... ORDER BY veiculo_id, veiculo_ts DESC
--   guarantees exactly one current marker per vehicle (safety net against duplicate rows;
--   within a single batch all veiculo_ts are effectively equal).
-- Display: render veiculo_lat x veiculo_long on a Metabase map (pin/marker). veiculo_ts is
--   TIMESTAMPTZ — Metabase renders it in the Report Timezone (America/Sao_Paulo); a
--   companion card can show MAX(veiculo_ts) + age (now() - MAX(veiculo_ts)) as freshness.
-- Enrichment: trip_id is derived as linha_lt || '-' || (sentido 1->'0', 2->'1'), identical
--   to refined.trip_details.trip_id, so the LEFT JOIN safely adds terminal names / is_circular
--   to each marker's tooltip. route_id equivalence is NOT asserted — trip_id is the only sound link.
-- Metabase native SQL. Map these field filters in the question editor (both panel-local, optional):
--   {{linha_lt}}       Field Filter -> refined.latest_positions.linha_lt       (line label)
--   {{linha_sentido}}  Field Filter -> refined.latest_positions.linha_sentido  (direction 1/2)
SELECT DISTINCT ON (refined.latest_positions.veiculo_id)
    refined.latest_positions.veiculo_id,
    refined.latest_positions.linha_lt,
    refined.latest_positions.linha_sentido,
    refined.latest_positions.veiculo_lat,
    refined.latest_positions.veiculo_long,
    refined.latest_positions.veiculo_ts,
    refined.trip_details.first_stop_name,
    refined.trip_details.last_stop_name,
    refined.trip_details.is_circular
FROM refined.latest_positions
LEFT JOIN refined.trip_details
  ON refined.trip_details.trip_id = refined.latest_positions.trip_id
WHERE 1 = 1
    [[ AND {{linha_lt}} ]]
    [[ AND {{linha_sentido}} ]]
ORDER BY
    refined.latest_positions.veiculo_id,
    refined.latest_positions.veiculo_ts DESC;
