-- Dashboard query — Panel P11 companion scalar/number card: live fleet count + freshness.
-- Source: refined.latest_positions — a LIVE snapshot, full-replaced (TRUNCATE + append)
--   every updatelatestpositions run from the single most recent ~2-min SPTrans batch.
--   It has NO history: there is no dim_time anchor and the global Date range / Route
--   dashboard filters do NOT apply here (this card is "the fleet, now").
-- Grain guard: refined.latest_positions has NO unique constraint on veiculo_id (only the
--   surrogate id PK). DISTINCT ON (veiculo_id) ... ORDER BY veiculo_id, veiculo_ts DESC
--   guarantees exactly one current vehicle per count/freshness computation.
-- Freshness: latest_snapshot_ts should be rendered in the Report Timezone
--   (America/Sao_Paulo). snapshot_age_seconds is the age of the latest visible snapshot
--   after panel-local filters are applied.
-- Metabase native SQL. Map these field filters in the question editor (both panel-local, optional):
--   {{linha_lt}}       Field Filter -> refined.latest_positions.linha_lt       (line label)
--   {{linha_sentido}}  Field Filter -> refined.latest_positions.linha_sentido  (direction 1/2)
WITH latest_positions_deduped AS (
    SELECT DISTINCT ON (refined.latest_positions.veiculo_id)
        refined.latest_positions.veiculo_id,
        refined.latest_positions.veiculo_ts
    FROM refined.latest_positions
    WHERE 1 = 1
        [[ AND {{linha_lt}} ]]
        [[ AND {{linha_sentido}} ]]
    ORDER BY
        refined.latest_positions.veiculo_id,
        refined.latest_positions.veiculo_ts DESC
)
SELECT
    COUNT(*) AS active_vehicle_count,
    MAX(latest_positions_deduped.veiculo_ts) AS latest_snapshot_ts,
    EXTRACT(EPOCH FROM (now() - MAX(latest_positions_deduped.veiculo_ts)))::bigint AS snapshot_age_seconds
FROM latest_positions_deduped;
