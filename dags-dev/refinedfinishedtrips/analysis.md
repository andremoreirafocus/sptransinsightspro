# refinedfinishedtrips-v2.py - Complete Analysis

## Overall Purpose
This DAG extracts "finished trips" (completed bus routes) from historical position data stored in S3, processes them to identify complete journeys, validates them, and persists them to PostgreSQL for analytics.

---

## Execution Flow (Step by Step)

### 1. Entry Point: `main()` → `extract_trips()`
```
refinedfinishedtrips-v2.py:main()
  ↓
extract_trips()
  ├─ get_config() → Reads configuration
  └─ extract_trips_for_all_Lines_and_vehicles(config)
```

**Configuration Source:**
- If `AIRFLOW_HOME` env var exists → Pulls from Airflow:
  - MinIO connection from `minio_conn`
  - PostgreSQL connection from `postgres_conn`
  - Job variables from `refinedfinishedtrips` JSON Variable
- Otherwise → Reads from `.env` file

**Config Keys Extracted:**
- `ANALYSIS_HOURS_WINDOW` - How many hours back to analyze
- `TRUSTED_BUCKET` - MinIO S3 bucket name
- `APP_FOLDER` - Folder prefix in bucket
- `POSITIONS_TABLE_NAME` - Name of positions table partition
- `FINISHED_TRIPS_TABLE_NAME` - Destination PostgreSQL table
- MinIO credentials (endpoint, access_key, secret_key)
- PostgreSQL credentials (host, port, database, user, password, sslmode)

---

### 2. Bulk Load Positions via DuckDB: `get_recent_positions()`

**What it does:**
- Creates in-memory DuckDB connection
- Configures S3/MinIO access in DuckDB
- Queries Hive-partitioned parquet files from S3

**S3 Path Construction:**
```
s3://{TRUSTED_BUCKET}/{APP_FOLDER}/{POSITIONS_TABLE_NAME}/
year={YYYY}/
month={MM}/
day={DD}/**
```

**Time Filtering Logic:**
1. Gets current time in São Paulo timezone
2. Calculates hour window:
   - `min_hour = current_hour - ANALYSIS_HOURS_WINDOW` (or 0 if negative)
   - `max_hour = current_hour`
3. Filters parquet files: `hour::INTEGER >= min_hour AND hour::INTEGER <= current_hour`

**Data Retrieved:**
- `veiculo_ts` - Timestamp of position
- `linha_lt` - Bus line identifier
- `veiculo_id` - Vehicle ID (unique bus)
- `linha_sentido` - Direction (1 or 2)
- `distance_to_first_stop` - Distance to first stop on route
- `distance_to_last_stop` - Distance to last stop on route
- `is_circular` - Boolean: whether line is circular
- `lt_origem` - Origin stop ID
- `lt_destino` - Destination stop ID

**Returns:** Pandas DataFrame sorted by `veiculo_ts` ASC

---

### 3. Group & Process: `extract_trips_for_all_Lines_and_vehicles_pandas()`

**Grouping:**
- Groups positions by `(linha_lt, veiculo_id)` → Creates groups for each line/vehicle combination
- For each group (e.g., all positions for Line 3, Vehicle #1234):
  - Calls `extract_trips_per_line_per_vehicle_pandas(linha_lt, veiculo_id, df_group)`

**Progress Tracking:**
- Logs every 500 groups processed
- Accumulates all trips in memory: `all_finished_trips` list

---

### 4. Extract Trips for Single Vehicle: `extract_trips_per_line_per_vehicle_pandas()`

**Process:**
1. Convert DataFrame to list of dictionaries (for easier indexing)
2. **Call 1:** `extract_raw_trips_metadata(position_records)`
   - Identifies trip boundaries
3. **Call 2:** `filter_healthy_trips(raw_trips_metadata, position_records)`
   - Validates trips meet duration requirements
4. **Call 3:** `generate_trips_table(position_records, clean_trips_metadata, linha_lt, veiculo_id)`
   - Formats into tuple structure

**Returns:** List of trip tuples (or None if no valid trips)

---

### 5A. Identify Trip Boundaries: `extract_raw_trips_metadata()`

**Core Logic: Detect Direction Changes**

A "trip" is a complete route from start to end. Trips are separated when the bus changes direction (`linha_sentido` changes).

**Algorithm:**
```
For each position record (i=1 to len):
  If linha_sentido[i] != linha_sentido[i-1]:
    → Found a direction change
    → Mark end of previous trip at index i-1
    → Mark start of new trip at index i
    → Add to trips_metadata: {start_index, end_index, sentido}
```

**Special Handling:**
- **First trip discarded** - Marked as incomplete because we don't know where journey started
- **Last trip discarded** - Marked as incomplete because journey may still be ongoing
- Only middle trips are kept

**Returns:** List of trip metadata with `start_position_index`, `end_position_index`, `sentido`

---

### 5B. Validate Trips: `filter_healthy_trips()`

**Duration Validation:**

For each raw trip, calculates duration from positions:
```
duration = position[end_index].veiculo_ts - position[start_index].veiculo_ts
```

**Duration Thresholds:**
- **Circular lines:**
  - Min: 1200 seconds (20 minutes)
  - Max: 10800 seconds (3 hours)
- **Non-circular lines:**
  - Min: 1800 seconds (30 minutes)
  - Max: 10800 seconds (3 hours)

**Filters Out:**
- Trips shorter than minimum (incomplete)
- Trips longer than maximum (vehicle likely went off-route or malfunctioned)

**Returns:** List of validated trip metadata

---

### 5C. Format Trip Data: `generate_trips_table()`

**For each valid trip, creates a tuple:**
```python
(
  trip_id,           # "3-0" or "3-1" (line-converted_direction)
  vehicle_id,        # 1234
  trip_start_time,   # datetime object
  trip_end_time,     # datetime object
  duration,          # timedelta object
  is_circular,       # boolean
  average_speed      # 0.0 (hardcoded, not calculated)
)
```

**Trip ID Conversion:**
```
sentido 1 → direction 0
sentido 2 → direction 1
else      → direction 999
```

**Returns:** List of trip tuples

---

### 6. Persist to PostgreSQL: `save_finished_trips_to_db()`

**3-Phase Process:**

**Phase 1: Setup Staging Table**
```sql
DROP TABLE IF EXISTS {finished_trips_table_name}_stg;
CREATE UNLOGGED TABLE {finished_trips_table_name}_stg 
AS SELECT * FROM {finished_trips_table_name} WITH NO DATA;
```
- Uses `UNLOGGED` table for speed (no transaction log, faster inserts)
- Clones schema from production table to ensure type matching

**Phase 2: Batch Insert to Staging**
- Uses SQLAlchemy parameterized queries
- Maps tuples to named parameters: `t_id, v_id, t_start, t_end, dur, circ, spd`
- Inserts with page_size=1000 for optimal batch size

**Phase 3: Atomic Upsert to Production**
```sql
INSERT INTO {finished_trips_table_name} (
  trip_id, vehicle_id, trip_start_time, trip_end_time, 
  duration, is_circular, average_speed
)
SELECT * FROM {staging_table}_stg
ON CONFLICT (trip_start_time, vehicle_id, trip_id) 
DO NOTHING;
```

**Deduplication Strategy:**
- Composite unique constraint: `(trip_start_time, vehicle_id, trip_id)`
- Any duplicate is silently ignored with `DO NOTHING`
- Atomic transaction ensures all-or-nothing semantics

**Phase 4: Cleanup**
```sql
ANALYZE {finished_trips_table_name};  -- Update table statistics for PowerBI
DROP TABLE IF EXISTS {staging_table}_stg;
```

**Returns:** Number of new rows added and duplicates skipped (logged)

---

## Data Lineage Summary

```
MinIO S3 Parquet Files
  ↓
DuckDB (reads via httpfs + S3 plugin)
  ↓
Pandas DataFrame (grouped by line/vehicle)
  ↓
Trip Extraction (detect direction changes)
  ↓
Trip Validation (duration filters)
  ↓
Trip Formatting (tuple structure)
  ↓
PostgreSQL (staging → production with dedup)
```

---

## Key Design Decisions

| Aspect | Implementation | Reason |
|--------|---|---|
| **Position Storage** | Hive-partitioned parquet in S3 | Fast columnar queries, partitionable by date |
| **Position Query** | DuckDB in-memory | Lightweight, SQL-capable, S3-native support |
| **Trip Detection** | Direction change marker | Buses naturally change direction at route endpoints |
| **Discard First/Last** | Always excluded | First may start mid-route, last may be incomplete |
| **Duration Thresholds** | Different for circular | Circular routes are faster (shorter loops) |
| **Staging Table** | UNLOGGED temp table | Speed optimization, discarded after use |
| **Deduplication** | ON CONFLICT DO NOTHING | Prevents duplicate trips on re-runs |

---

## Logging Architecture

- **Root Logger** configured in refinedfinishedtrips-v2.py with rotating file handler
- **5MB per file, 5 files max** (25MB total logs)
- All child modules inherit this configuration
- Both file and console output
