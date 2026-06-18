#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/wait_helpers.sh"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/docker_helper.sh"

WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-120}"
WAIT_INTERVAL_SECONDS="${WAIT_INTERVAL_SECONDS:-2}"

cd "${PROJECT_ROOT}"

ACTIVE_DAGS=(
  "gtfs-v7"
  "orchestratetransform-v2"
  "transformlivedata-v10"
  "refinedfinishedtrips-v6"
  "refinedtripfacts-v1"
  "refinedsynctripdetails-v3"
  "updatelatestpositions-v4"
)

echo "Unpausing selected Airflow DAGs..."

airflow_dag_exists() {
  local dag_id="$1"
  ${DOCKER_COMPOSE} exec -T airflow_webserver env PYTHONWARNINGS=ignore airflow dags list | \
    awk 'NR > 2 {print $1}' | grep -Fx "${dag_id}" >/dev/null
}

reserialize_airflow_dags() {
  echo "Reserializing Airflow DAGs before unpause attempts..."
  if ! ${DOCKER_COMPOSE} exec -T airflow_webserver airflow dags reserialize; then
    echo "Warning: airflow dags reserialize failed. Continuing with best-effort unpause attempts."
  fi
}

print_unpause_failure_hint() {
  local dag_id="$1"
  echo "Warning: failed to unpause Airflow DAG '${dag_id}'."
  echo "The platform startup will continue."
  echo "You can retry later with:"
  echo "  ${DOCKER_COMPOSE} exec -T airflow_webserver airflow dags unpause ${dag_id}"
}

trigger_gtfs_v7() {
  echo "Triggering Airflow DAG: gtfs-v7"
  if ! ${DOCKER_COMPOSE} exec -T airflow_webserver airflow dags trigger gtfs-v7; then
    echo "Warning: failed to trigger Airflow DAG 'gtfs-v7'."
    echo "The platform startup will continue."
    echo "You can retry later with:"
    echo "  ${DOCKER_COMPOSE} exec -T airflow_webserver airflow dags trigger gtfs-v7"
  fi
}

reserialize_airflow_dags

for dag_id in "${ACTIVE_DAGS[@]}"; do
  if ! wait_for_condition \
    "Airflow DAG '${dag_id}' to be registered" \
    "${WAIT_TIMEOUT_SECONDS}" \
    "${WAIT_INTERVAL_SECONDS}" \
    airflow_dag_exists "${dag_id}"; then
    print_unpause_failure_hint "${dag_id}"
    continue
  fi
  echo "Unpausing Airflow DAG: ${dag_id}"
  if ! ${DOCKER_COMPOSE} exec -T airflow_webserver airflow dags unpause "${dag_id}"; then
    print_unpause_failure_hint "${dag_id}"
    continue
  fi
  if [ "${dag_id}" = "gtfs-v7" ]; then
    trigger_gtfs_v7
  fi
done

echo "Selected Airflow DAGs are unpaused."
