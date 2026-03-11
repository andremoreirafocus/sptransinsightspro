# TransformLiveData Validation and Lineage Analysis

Scope: `dags-dev/transformlivedata` validation + lineage only. No code changes requested.

## Findings (Ordered by Severity)

1. Raw validation schema path is likely wrong in `transformlivedata-v7.py`
- The file path is constructed as `.../transformlivedata/transformlivedata/config/raw_data_schema_config.json`, which likely does not exist. This will cause raw validation to fail at runtime due to a missing file.
- Files: `dags-dev/transformlivedata-v7.py`, `dags-dev/transformlivedata/config/raw_data_schema_config.json`

2. Validation is split across 3 mechanisms with inconsistent coverage
- There is a manual structural check in `transform_positions.raw_data_structure_is_valid`, JSON Schema validation in `validate_json_data_schema`, and expectation validation in `validate_expectations`.
- The structural check and JSON Schema overlap but are not aligned: JSON Schema does not require `metadata.source` or `metadata.total_vehicles` while `raw_data_structure_is_valid` requires them. This allows a payload to pass schema validation and then fail later.
- Files: `dags-dev/transformlivedata/services/transform_positions.py`, `dags-dev/transformlivedata/config/raw_data_schema_config.json`, `dags-dev/transformlivedata/quality/validate_json_data_schema.py`

3. Lineage config source is unclear vs. unused `lineage.json`
- `create_lineage_report` loads a file path from `config["DATA_QUALITY_CONFIG"]`, then extracts `columns` mappings from that file.
- The repo contains `dags-dev/transformlivedata/config/lineage.json` but the code doesn’t reference it, making the lineage source ambiguous.
- If `DATA_QUALITY_CONFIG` points to expectations JSON, it may not contain `columns` in the expected shape.
- Files: `dags-dev/transformlivedata/services/lineage_report.py`, `dags-dev/transformlivedata/config/lineage.json`, `dags-dev/transformlivedata/config/transformed_data_expectations.json`

4. Lineage artifacts are only written to local files, not persisted
- `create_lineage_report` writes `column_lineage_report_<execution_id>.txt` and `validation_report_<execution_id>.txt` locally.
- There is no upload to MinIO or DB, so lineage data may disappear in ephemeral Airflow workers.
- Files: `dags-dev/transformlivedata/services/lineage_report.py`

5. Validation results do not affect save behavior beyond “valid/invalid df”
- `validate_expectations` returns `valid_postions_df` and `invalid_positions_df`, but only the valid dataframe is saved.
- There is no quarantine or persistence of invalid rows for triage, which can mask data loss.
- Files: `dags-dev/transformlivedata-v7.py`, `dags-dev/transformlivedata/quality/validate_expectations.py`

6. Quality reporting mixes concerns and has incomplete coverage
- `get_transformation_metrics_and_issues_report` reports invalid trips, vehicles, distance errors, and count discrepancies but does not include counts of schema failures or expectation failures.
- Schema validation errors are thrown without a durable summary; expectation failures are not included in the final report.
- Files: `dags-dev/transformlivedata-v7.py`, `dags-dev/transformlivedata/services/transform_positions.py`, `dags-dev/transformlivedata/quality/validate_json_data_schema.py`

## Lower-Severity Observations

- JSON Schema allows `payload.hr` to be `array`, `string`, or `integer`, but downstream usage assumes a string in some places. This broad type can allow values that later logic can’t handle consistently.
- File: `dags-dev/transformlivedata/config/raw_data_schema_config.json`

- `transform_positions.py` has `from turtle import pd`, unused and likely a mistake. Not validation/lineage related but suggests code hygiene issues.
- File: `dags-dev/transformlivedata/services/transform_positions.py`

## Open Questions / Assumptions

1. What is the intended canonical source of lineage mappings: `lineage.json` or a section inside the data quality config?
2. Do you want invalid rows written to a quarantine location (e.g., MinIO) instead of being dropped?
3. Should raw schema validation be strict (fail run) or soft (log + continue)?

## Unified Quality Result (UQR) Proposal

Goal: produce one authoritative quality artifact per run that merges transformation metrics and expectation validation outcomes, and drives reporting, lineage, and quarantine routing.

### UQR Schema (Draft)

```json
{
  "execution_id": "uuid",
  "logical_date_utc": "2026-02-15T12:36:00Z",
  "source_file": "posicoes_onibus-202602150936.json",
  "row_counts": {
    "raw_records": 5251,
    "transformed_records": 5200,
    "accepted_records": 5150,
    "rejected_records": 50
  },
  "transformation_metrics": {
    "total_vehicles_processed": 5200,
    "valid_vehicles": 5150,
    "invalid_vehicles": 50,
    "expected_vehicles": 5251,
    "total_lines_processed": 120
  },
  "transformation_issues": {
    "invalid_trips": ["2104-10-0", "3063-1-1"],
    "invalid_vehicle_ids": [21300, 21303],
    "distance_calculation_errors": 3,
    "vehicle_count_discrepancies_per_line": 12
  },
  "expectations_summary": {
    "total_checks": 18,
    "checks_failed": 2,
    "rows_failed": 50,
    "top_failure_reasons": [
      {"rule": "veiculo_lat_in_range", "count": 20},
      {"rule": "veiculo_long_in_range", "count": 30}
    ]
  },
  "outcome": {
    "status": "WARN",
    "acceptance_rate": 0.9904,
    "policy_version": "v1"
  },
  "artifacts": {
    "quality_report_path": "trusted/quality/reports/...",
    "quarantine_path": "raw/quarantine/..."
  }
}
```

### Report Template (Human-Readable)

```
TRANSFORMLIVEDATA QUALITY REPORT
Execution ID: <uuid>
Logical Date (UTC): <timestamp>
Source File: <filename>

Record Counts
- Raw records: <n>
- Transformed records: <n>
- Accepted records: <n>
- Rejected records: <n>
- Acceptance rate: <pct>

Transformation Metrics
- Total vehicles processed: <n>
- Valid vehicles: <n>
- Invalid vehicles: <n>
- Expected vehicles: <n>
- Lines processed: <n>

Transformation Issues
- Invalid trips: <n> (sample: <first 10>)
- Invalid vehicle IDs: <n> (sample: <first 10>)
- Distance calculation errors: <n>
- Vehicle count discrepancies: <n>

Expectation Validation
- Total checks: <n>
- Checks failed: <n>
- Rows failed: <n>
- Top failure reasons:
  - <rule>: <count>
  - <rule>: <count>

Outcome
- Status: PASS | WARN | FAIL
- Policy version: <version>

Artifacts
- Quality report: <path>
- Quarantine folder: <path>
```

### Policy (Draft)

- PASS: acceptance_rate >= 0.995 and checks_failed == 0
- WARN: acceptance_rate >= 0.980 and checks_failed > 0
- FAIL: acceptance_rate < 0.980 or any critical check fails

### Quarantine Handling (High Level Only)

- All expectation-rejected records must be saved to a quarantined folder.
- Each quarantined record should include failure reasons and execution_id for traceability.
- No storage format or path details defined here (pending decision).
