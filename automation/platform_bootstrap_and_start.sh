#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${PROJECT_ROOT}/.env"

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
  local elapsed=0

  echo "Waiting for service '${service_name}' to accept connections..."

  until docker compose exec -T "${service_name}" pg_isready -U "${db_user}" -d postgres >/dev/null 2>&1; do
    sleep "${WAIT_INTERVAL_SECONDS}"
    elapsed=$((elapsed + WAIT_INTERVAL_SECONDS))

    if [ "${elapsed}" -ge "${WAIT_TIMEOUT_SECONDS}" ]; then
      echo "❌ Timed out waiting for service '${service_name}' to become available."
      exit 1
    fi
  done

  echo "✅ Service '${service_name}' is available."
}

wait_for_http_service() {
  local service_label="$1"
  local service_url="$2"
  local elapsed=0

  echo "Waiting for service '${service_label}' to become available..."

  until python3 -c 'import sys, urllib.request; urllib.request.urlopen(sys.argv[1], timeout=2)' \
    "${service_url}" >/dev/null 2>&1; do
    sleep "${WAIT_INTERVAL_SECONDS}"
    elapsed=$((elapsed + WAIT_INTERVAL_SECONDS))

    if [ "${elapsed}" -ge "${WAIT_TIMEOUT_SECONDS}" ]; then
      echo "Timed out waiting for service '${service_label}' to become available."
      exit 1
    fi
  done

  echo "Service '${service_label}' is available."
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

echo "==> Starting the remaining platform services..."
docker compose up -d

echo "Platform started with bootstrap completed."
