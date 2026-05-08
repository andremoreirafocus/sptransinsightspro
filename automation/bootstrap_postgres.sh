#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SQL_ROOT="${PROJECT_ROOT}/database/bootstrap"

SERVICE_NAME="postgres"
DB_USER="${POSTGRES_DB_USER:-postgres}"

docker_compose_exec() {
  docker compose exec -T "${SERVICE_NAME}" "$@"
}

run_sql_file() {
  local file_path="$1"
  docker_compose_exec psql -v ON_ERROR_STOP=1 -U "${DB_USER}" -f - < "${file_path}"
}

echo "Bootstrapping ${SERVICE_NAME}..."

run_sql_file "${SQL_ROOT}/shared/001_create_sptrans_insights_database.sql"
run_sql_file "${SQL_ROOT}/postgres/003_refined_finished_trips.sql"
run_sql_file "${SQL_ROOT}/postgres/002_refined_trip_details.sql"
run_sql_file "${SQL_ROOT}/postgres/001_refined_latest_positions.sql"

echo "Bootstrap for ${SERVICE_NAME} completed."
