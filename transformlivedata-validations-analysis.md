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
