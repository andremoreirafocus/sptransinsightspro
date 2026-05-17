#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/wait_helpers.sh"

WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-120}"
WAIT_INTERVAL_SECONDS="${WAIT_INTERVAL_SECONDS:-2}"

LOKI_READY_URL="${LOKI_READY_URL:-http://localhost:3100/ready}"
PROMTAIL_READY_URL="${PROMTAIL_READY_URL:-http://localhost:9080/ready}"
GRAFANA_READY_URL="${GRAFANA_READY_URL:-http://localhost:3000/api/health}"
ALERTMANAGER_READY_URL="${ALERTMANAGER_READY_URL:-http://localhost:9093/-/ready}"

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

echo "Bootstrapping observability stack..."
docker compose up -d loki promtail grafana alertmanager

wait_for_http_service "loki" "${LOKI_READY_URL}"
wait_for_http_service "promtail" "${PROMTAIL_READY_URL}"
wait_for_http_service "grafana" "${GRAFANA_READY_URL}"
wait_for_http_service "alertmanager" "${ALERTMANAGER_READY_URL}"

echo "Observability stack bootstrap completed."
