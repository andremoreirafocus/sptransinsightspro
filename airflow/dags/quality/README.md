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

## Usage

Both modules are loaded by `pipeline_configurator` when the pipeline's general config declares a `data_validations` section. The loaded suites and schemas are stored in the pipeline config dict and passed directly to these functions.
