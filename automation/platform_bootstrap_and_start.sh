#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

AIRFLOW_POSTGRES_USER="${AIRFLOW_DB_USER:-airflow}"
POSTGRES_USER_NAME="${POSTGRES_DB_USER:-postgres}"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-120}"
WAIT_INTERVAL_SECONDS="${WAIT_INTERVAL_SECONDS:-2}"

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

echo "==> Starting database services..."
cd "${PROJECT_ROOT}"
docker compose up -d airflow_postgres postgres

wait_for_postgres_service "airflow_postgres" "${AIRFLOW_POSTGRES_USER}"
wait_for_postgres_service "postgres" "${POSTGRES_USER_NAME}"

echo "==> Running Airflow database bootstrap..."
bash "${SCRIPT_DIR}/bootstrap_airflow_postgres.sh"

echo "==> Running analytical database bootstrap..."
bash "${SCRIPT_DIR}/bootstrap_postgres.sh"

echo "==> Starting the remaining platform services..."
docker compose up -d

echo "✅ Platform started with bootstrap completed."
