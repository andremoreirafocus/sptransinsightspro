# ADR-0007: Pipeline promotion workflow (`dags-dev` → `airflow/dags`)

**Date:** 2026-04-15  
**Status:** Accepted

## Context

Pipelines are developed as directly executable Python code, without Airflow, in the `dags-dev/` directory. The production environment is Airflow, whose `dags_folder` points to `airflow/dags/`.

Two obvious approaches exist to keep these environments synchronized:

1. **Develop directly in `airflow/dags/`:** there is no synchronization friction, but production code remains constantly exposed to in-progress changes. Any saved file is immediately loaded by the Airflow scheduler and may cause runtime errors.
2. **Symlinks or shared volumes:** `airflow/dags/` points to `dags-dev/` through a symlink or Docker volume. This removes synchronization needs but also eliminates separation between development and production: test files, fixtures, and incomplete code become visible to Airflow.

Both approaches eliminate the concept of a quality gate before code reaches production.

## Decision

Adopt an **explicit promotion workflow** with an automated quality gate:

- All development and testing happens in `dags-dev/`.
- Promotion to production is performed exclusively through `automation/promote_pipeline.py <pipeline>`.
- The script automatically executes, in order:
  1. Lint with `ruff check`, failing if there are errors.
  2. Unit tests with `pytest tests/`, failing if there are failures and skipped if there is no `tests/` directory.
  3. `rsync` from the pipeline subdirectory to `airflow/dags/`, excluding `tests/`, `__pycache__`, and `.pytest_cache`.
  4. `rsync` of shared modules, `infra/` and `pipeline_configurator/`, to production.

The step counter shown in the output is automatically adjusted according to test presence, providing clear feedback on what was executed.

## Alternatives considered

**Direct development in `airflow/dags/`:** Minimizes friction, but exposes production to in-progress code. Airflow loads DAGs every `dag_file_processor_timeout` seconds, so a syntax error would cause an immediate scheduler failure visible to all users.

**Symlinks or shared Docker volumes:** An elegant solution that eliminates synchronization problems, but also removes development and production separation. Test files such as `conftest.py`, fixtures, and `tests/` would be loaded by Airflow as potential DAGs, generating warnings or parsing errors. Environment separation is compromised.

**External CI/CD, for example GitHub Actions:** The correct solution for teams and real production, where pull requests trigger lint and tests and merges to `main` trigger automatic promotion. For a solo-development project without CI configured, the local script is the functional equivalent with lower setup overhead.

## Consequences

**Positive:**
- No code without successful lint and tests reaches production; the gate is structural, not optional.
- The production environment, `airflow/dags/`, contains only production code, with no test files, fixtures, or stubs.
- Promotion is auditable: `rsync` with `--checksum` ensures that only changed files are transferred, and script output documents every executed step.
- Shared modules, `infra/` and `pipeline_configurator/`, are always synchronized together with the promoted pipeline, avoiding drift between versions.

**Negative / Tradeoffs:**
- Promotion is a manual step: it depends on the developer remembering to run the script. Without CI/CD, there is no guarantee that `airflow/dags/` reflects the latest approved state in `dags-dev/`.
- Drift risk: if changes are made directly in `airflow/dags/`, for example an emergency hotfix, they will not exist in `dags-dev/` and will be overwritten during the next promotion.
- The absence of explicit versioning for promoted artifacts makes rollbacks harder: reverting to a previous production version requires manually checking out the desired Git state of `airflow/dags/`.
