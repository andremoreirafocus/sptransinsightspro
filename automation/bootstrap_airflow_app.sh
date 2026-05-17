#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VARIABLES_DIR="${PROJECT_ROOT}/airflow/variables_and_connections"
GENERATED_CONNECTIONS_FILE="${VARIABLES_DIR}/generated_connections.json"
ENV_FILE="${PROJECT_ROOT}/.env"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/wait_helpers.sh"

SERVICE_NAME="airflow_webserver"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-120}"
WAIT_INTERVAL_SECONDS="${WAIT_INTERVAL_SECONDS:-2}"

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
  if [ -z "${!var_name:-}" ]; then
    echo "Missing required environment variable: ${var_name}"
    exit 1
  fi
}

docker_compose_exec() {
  docker compose exec -T "${SERVICE_NAME}" "$@"
}

wait_for_airflow() {
  wait_for_condition \
    "Airflow CLI to become available" \
    "${WAIT_TIMEOUT_SECONDS}" \
    "${WAIT_INTERVAL_SECONDS}" \
    docker_compose_exec airflow users list --output json
}

admin_user_exists() {
  docker_compose_exec airflow users list --output json | \
    python3 -c 'import json, sys; username = sys.argv[1]; users = json.load(sys.stdin); sys.exit(0 if any(user.get("username") == username for user in users) else 1)' \
      "${AIRFLOW_ADMIN_USERNAME}"
}

import_variables() {
  docker_compose_exec airflow variables import /opt/airflow/variables_and_connections/variables.json
  docker_compose_exec airflow variables import /opt/airflow/variables_and_connections/transformlivedata_general.json
  docker_compose_exec airflow variables import /opt/airflow/variables_and_connections/transformlivedata_data_expectations.json
  docker_compose_exec airflow variables import /opt/airflow/variables_and_connections/transformlivedata_raw_data_json_schema.json
  docker_compose_exec airflow variables import /opt/airflow/variables_and_connections/gtfs_general.json
  docker_compose_exec airflow variables import /opt/airflow/variables_and_connections/gtfs_data_expectations_stops.json
  docker_compose_exec airflow variables import /opt/airflow/variables_and_connections/gtfs_data_expectations_stop_times.json
  docker_compose_exec airflow variables import /opt/airflow/variables_and_connections/gtfs_data_expectations_trip_details.json
  docker_compose_exec airflow variables import /opt/airflow/variables_and_connections/refinedfinishedtrips_general.json
}

trap 'rm -f "${GENERATED_CONNECTIONS_FILE}"' EXIT

cd "${PROJECT_ROOT}"
load_env_file

require_env "AIRFLOW_ADMIN_USERNAME"
require_env "AIRFLOW_ADMIN_PASSWORD"
require_env "AIRFLOW_ADMIN_FIRSTNAME"
require_env "AIRFLOW_ADMIN_LASTNAME"
require_env "AIRFLOW_ADMIN_EMAIL"
require_env "MINIO_PLATFORM_ACCESS_KEY"
require_env "MINIO_PLATFORM_SECRET_KEY"
require_env "GTFS_LOGIN"
require_env "GTFS_PASSWORD"
require_env "POSTGRES_DB_USER"
require_env "POSTGRES_DB_PASSWORD"
require_env "AIRFLOW_DB_USER"
require_env "AIRFLOW_DB_PASSWORD"

wait_for_airflow

echo "Bootstrapping Airflow application configuration..."

if admin_user_exists; then
  echo "Airflow admin user '${AIRFLOW_ADMIN_USERNAME}' already exists."
else
  docker_compose_exec airflow users create \
    --username "${AIRFLOW_ADMIN_USERNAME}" \
    --firstname "${AIRFLOW_ADMIN_FIRSTNAME}" \
    --lastname "${AIRFLOW_ADMIN_LASTNAME}" \
    --role Admin \
    --email "${AIRFLOW_ADMIN_EMAIL}" \
    --password "${AIRFLOW_ADMIN_PASSWORD}"
  echo "Created Airflow admin user '${AIRFLOW_ADMIN_USERNAME}'."
fi

echo "Rendering Airflow connections import file from the tracked template using runtime credentials..."
python3 "${SCRIPT_DIR}/render_airflow_connections.py" \
  "${VARIABLES_DIR}/connections.json" \
  "${GENERATED_CONNECTIONS_FILE}"

echo "Importing Airflow connections and variables..."
docker_compose_exec airflow connections import /opt/airflow/variables_and_connections/generated_connections.json
import_variables

echo "Airflow application bootstrap completed."
