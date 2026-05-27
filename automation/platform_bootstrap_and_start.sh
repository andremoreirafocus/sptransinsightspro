#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${PROJECT_ROOT}/.env"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/wait_helpers.sh"

WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-120}"
WAIT_INTERVAL_SECONDS="${WAIT_INTERVAL_SECONDS:-2}"
MINIO_HEALTHCHECK_URL="${MINIO_HEALTHCHECK_URL:-http://localhost:9000/minio/health/live}"

load_env_file() {
  if [ -f "${ENV_FILE}" ]; then
    set -a
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
    set +a
  fi
}

wait_for_postgres_service() {
  local service_name="$1"
  local db_user="$2"
  wait_for_condition \
    "service '${service_name}' to accept connections" \
    "${WAIT_TIMEOUT_SECONDS}" \
    "${WAIT_INTERVAL_SECONDS}" \
    docker compose exec -T "${service_name}" pg_isready -U "${db_user}" -d postgres
}

wait_for_http_service() {
  local service_label="$1"
  local service_url="$2"
  wait_for_condition \
    "service '${service_label}' to become available" \
    "${WAIT_TIMEOUT_SECONDS}" \
    "${WAIT_INTERVAL_SECONDS}" \
    check_http_url "${service_url}"
}

cd "${PROJECT_ROOT}"
load_env_file

AIRFLOW_POSTGRES_USER="${AIRFLOW_DB_USER:-airflow}"
POSTGRES_USER_NAME="${POSTGRES_DB_USER:-postgres}"

echo "==> Starting infrastructure services..."
docker compose up -d airflow_postgres postgres minio

wait_for_postgres_service "airflow_postgres" "${AIRFLOW_POSTGRES_USER}"
wait_for_postgres_service "postgres" "${POSTGRES_USER_NAME}"
wait_for_http_service "minio" "${MINIO_HEALTHCHECK_URL}"

echo "==> Running MinIO bootstrap..."
bash "${SCRIPT_DIR}/bootstrap_minio.sh"

echo "==> Running Airflow database bootstrap..."
bash "${SCRIPT_DIR}/bootstrap_airflow_postgres.sh"

echo "==> Running analytical database bootstrap..."
bash "${SCRIPT_DIR}/bootstrap_postgres.sh"

echo "==> Starting Airflow application services..."
docker compose up -d airflow_webserver airflow_scheduler

echo "==> Running Airflow application bootstrap..."
bash "${SCRIPT_DIR}/bootstrap_airflow_app.sh"

echo "==> Running observability bootstrap..."
bash "${SCRIPT_DIR}/bootstrap_observability.sh"

echo "==> Starting the remaining platform services..."
docker compose up -d

echo "Platform started with bootstrap completed."
