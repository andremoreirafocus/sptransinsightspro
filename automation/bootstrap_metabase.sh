#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SQL_ROOT="${PROJECT_ROOT}/database/bootstrap"
ENV_FILE="${PROJECT_ROOT}/.env"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/wait_helpers.sh"

WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-180}"
WAIT_INTERVAL_SECONDS="${WAIT_INTERVAL_SECONDS:-5}"
METABASE_HEALTH_URL="${METABASE_HEALTH_URL:-http://localhost:3001/api/health}"
POSTGRES_SERVICE_NAME="postgres"

load_env_file() {
  if [ -f "${ENV_FILE}" ]; then
    set -a
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
    set +a
  fi
}

require_env() {
  local var_name="$1"
  local value="${!var_name:-}"
  if [ -z "${value}" ]; then
    echo "ERROR: required env var '${var_name}' is not set." >&2
    exit 1
  fi
}

wait_for_postgres_service() {
  local db_user="$1"
  wait_for_condition \
    "service '${POSTGRES_SERVICE_NAME}' to accept connections" \
    "${WAIT_TIMEOUT_SECONDS}" \
    "${WAIT_INTERVAL_SECONDS}" \
    docker compose exec -T "${POSTGRES_SERVICE_NAME}" pg_isready -U "${db_user}" -d postgres
}

run_metabase_sql_bootstrap() {
  local db_user="$1"
  docker compose exec -T "${POSTGRES_SERVICE_NAME}" psql \
    -v ON_ERROR_STOP=1 \
    -v metabase_db_name="${METABASE_DB_NAME}" \
    -v metabase_internal_user="${METABASE_INTERNAL_USER}" \
    -v metabase_internal_password="${METABASE_INTERNAL_PASSWORD}" \
    -v metabase_reader_user="${METABASE_READER_USER}" \
    -v metabase_reader_password="${METABASE_READER_PASSWORD}" \
    -U "${db_user}" \
    -f - < "${SQL_ROOT}/postgres/004_metabase.sql"
}

cd "${PROJECT_ROOT}"
load_env_file

DB_USER="${POSTGRES_DB_USER:-postgres}"

require_env "METABASE_DB_NAME"
require_env "METABASE_INTERNAL_USER"
require_env "METABASE_INTERNAL_PASSWORD"
require_env "METABASE_READER_USER"
require_env "METABASE_READER_PASSWORD"

wait_for_postgres_service "${DB_USER}"

echo "==> Running Metabase SQL bootstrap..."
run_metabase_sql_bootstrap "${DB_USER}"

echo "==> Starting Metabase service..."
docker compose up -d metabase

wait_for_condition \
  "service 'metabase' to become available" \
  "${WAIT_TIMEOUT_SECONDS}" \
  "${WAIT_INTERVAL_SECONDS}" \
  check_http_url "${METABASE_HEALTH_URL}"

echo "Metabase bootstrap completed."
