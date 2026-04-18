# quality

Shared data quality modules used across pipelines.

## Modules

### `validate_json_data_schema.py`

Pre-transformation validation of raw data against a JSON Schema.

**`validate_json_data_schema(data, schema)`**
Validates a raw data dict against a JSON Schema (dict or file path).
Returns `(is_valid: bool, error_messages: List[str])`.

**`load_raw_schema(config_file)`**
Loads a JSON Schema from a file path. Returns the parsed dict.

---

### `validate_expectations.py`

Post-transformation validation of a DataFrame using a Great Expectations suite.

**`validate_expectations(df_to_be_validated, expectations_suite)`**
Runs an ephemeral GX checkpoint against the DataFrame.
Returns a dict with:
- `valid_df` — rows that passed all expectations
- `invalid_df` — rows that failed, with `invalid_reason` and `validation_failed_at` columns added
- `expectations_summary` — counts of checks, violations, exceptions, failure reasons

---

### `reporting.py`

Shared quality reporting infrastructure used by all pipelines. Provides the common contract for building, persisting, and notifying quality reports.

**`build_quality_report_path(metadata_bucket, quality_report_folder, pipeline_name, batch_ts, filename_suffix="")`**
Builds the partitioned MinIO path for a quality report.
Returns `"{bucket}/{folder}/{pipeline}/year=.../month=.../day=.../hour=.../quality-report-{pipeline}_{HHMM}{suffix}.json"`.

**`build_quality_summary(pipeline, execution_id, status, acceptance_rate, rows_failed, quality_report_path, failure_phase=None, failure_message=None, **extra_fields)`**
Builds the standardised summary block consumed by the alertservice.
Mandatory fields form the common contract (`contract_version`, `pipeline`, `execution_id`, `status`, `acceptance_rate`, `rows_failed`, `quality_report_path`, `failure_phase`, `failure_message`, `generated_at_utc`).
Pipeline-specific fields are passed as `**extra_fields` and merged flat.
`status` must be one of `"PASS"`, `"WARN"`, or `"FAIL"`. `acceptance_rate` is always a continuous float (0.0–1.0).

**`create_failure_quality_report(pipeline, execution_id, failure_phase, failure_message, quality_report_path, **extra_fields)`**
Builds a minimal `{summary, details}` envelope with `status` forced to `"FAIL"` and an empty `details` block.
Callers that have partial stage results should augment `details` before saving.

**`save_quality_report(report, path, connection_data, write_fn=...)`**
Persists a quality report to object storage. `path` format is `"{bucket}/{object_name}"`.
Storage failures are re-raised. Does not send any notification.

**`save_and_notify_quality_report(report, path, connection_data, webhook_url, write_fn=...)`**
Persists a quality report and sends the summary to a webhook in a single call.
Webhook failures are logged and swallowed so that a notification outage never blocks the pipeline.
Pass `"disabled"`, `"none"`, or `"null"` as `webhook_url` to skip notification.

## Usage

Validation modules (`validate_json_data_schema`, `validate_expectations`) are loaded by `pipeline_configurator` when the pipeline's general config declares a `data_validations` section. The loaded suites and schemas are stored in the pipeline config dict and passed directly to these functions.

`reporting.py` is imported directly by pipeline service modules that need to build or persist quality reports.
