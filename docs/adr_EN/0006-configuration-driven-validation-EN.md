# ADR-0006: Configuration-driven validation (JSON Schema + Great Expectations)

**Date:** 2026-04-15  
**Status:** Accepted

## Context

[ADR-0005](./0005-multi-layer-data-quality-framework-EN.md) establishes that `transformlivedata` must treat data quality as a first-class concern, with multi-stage validation: JSON Schema before transformation and Great Expectations afterward. This decision assumes that framework is already adopted and addresses one specific operational question: **where validation rules should live**.

The simplest approach, hardcoding the rules in Python code, such as `if "field" not in data: raise` or fixed numeric thresholds, works for static rules but makes it impossible to update them without pipeline redeployment, and mixes validation logic with business logic. In production with Airflow, changing a single threshold would require pull request, merge, and promotion.

## Decision

Externalize all validation rules into configuration files loaded at runtime by `pipeline_configurator`:

- **JSON Schema (Draft 7)** for structural validation of the raw payload: file `transformlivedata_raw_data_json_schema.json`.
- **Great Expectations suite** for post-transformation validation: file `transformlivedata_data_expectations.json`.

The rules are loaded as part of the canonical configuration contract, through `config["raw_data_json_schema"]` and `config["data_expectations"]`, and interpreted by specialized libraries, `jsonschema` and `great_expectations`, without embedding validation logic in pipeline code.

In Airflow, the same rules are loaded from Airflow Variables, allowing updates without redeployment.

## Alternatives considered

**Hardcoded validation in Python:** Simple and dependency-free. However, any rule change requires code changes, linting, testing, and redeployment. In Airflow production, this means pull request, merge, and promotion even for changing a numeric threshold.

**Validation only through Great Expectations, without JSON Schema:** Great Expectations can validate DataFrame structure after transformation, but it does not validate raw JSON payloads before transformation. A structural failure in the API would cause a generic exception that is hard to diagnose instead of a clear error message pinpointing the problematic field.

**Validation with Pydantic only:** Excellent for modeling Python objects with type validation, but the overhead of creating and maintaining Pydantic models for external API schemas is high, and the framework has no native support for statistical expectations, such as “at least 95% of records must have non-null `trip_id`.”

## Consequences

**Positive:**
- Validation rules can be updated in Airflow Variables without redeployment; threshold changes or new fields do not require a development cycle.
- Clear separation between business logic in code and quality rules in configuration.
- JSON Schema produces precise error messages that identify exactly which field violated which rule.
- Great Expectations produces structured reports with per-expectation metrics, integrated into the pipeline quality report.
- The same validation framework can be reused in other pipelines simply by changing the configuration files.

**Negative / Tradeoffs:**
- Two separate validation systems must be maintained, external JSON Schema and GX suites, each with its own learning curve.
- Configuration files must be versioned together with the code; a transformation schema change must be reflected in expectations and vice versa. Synchronization is the developer’s responsibility and is not checked automatically.
- Great Expectations brings heavy dependencies, about 200 MB added to the environment. For projects with strict image-size constraints, lighter alternatives would need evaluation.
