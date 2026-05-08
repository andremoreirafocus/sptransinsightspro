#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SQL_ROOT="${PROJECT_ROOT}/database/bootstrap"

SERVICE_NAME="airflow_postgres"
DB_USER="${AIRFLOW_DB_USER:-airflow}"

docker_compose_exec() {
  docker compose exec -T "${SERVICE_NAME}" "$@"
}

run_sql_file() {
  local file_path="$1"
  docker_compose_exec psql -v ON_ERROR_STOP=1 -U "${DB_USER}" -f - < "${file_path}"
}

echo "Bootstrapping ${SERVICE_NAME}..."

run_sql_file "${SQL_ROOT}/shared/001_create_sptrans_insights_database.sql"
run_sql_file "${SQL_ROOT}/airflow_postgres/001_to_be_processed_raw.sql"

echo "Bootstrap for ${SERVICE_NAME} completed."
