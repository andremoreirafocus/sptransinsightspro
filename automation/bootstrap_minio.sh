#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${PROJECT_ROOT}/.env"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/wait_helpers.sh"

SERVICE_NAME="minio"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-120}"
WAIT_INTERVAL_SECONDS="${WAIT_INTERVAL_SECONDS:-2}"
MINIO_HEALTHCHECK_URL="${MINIO_HEALTHCHECK_URL:-http://localhost:9000/minio/health/live}"
MC_IMAGE="${MC_IMAGE:-minio/mc}"

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

wait_for_minio() {
  wait_for_condition \
    "MinIO to become available" \
    "${WAIT_TIMEOUT_SECONDS}" \
    "${WAIT_INTERVAL_SECONDS}" \
    check_http_url "${MINIO_HEALTHCHECK_URL}"
}

get_minio_network() {
  docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{println $k}}{{end}}' \
    "${SERVICE_NAME}" | head -n1
}

run_mc() {
  local network_name="$1"
  shift

  docker run --rm \
    --network "${network_name}" \
    -e "MC_HOST_local=http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio:9000" \
    "${MC_IMAGE}" "$@"
}

create_access_user_if_missing() {
  local add_output=""

  if run_mc "${MINIO_NETWORK}" admin user info local "${MINIO_PLATFORM_ACCESS_KEY}" >/dev/null 2>&1; then
    echo "MinIO platform access user '${MINIO_PLATFORM_ACCESS_KEY}' already exists."
    return 0
  fi

  if add_output="$(
    run_mc "${MINIO_NETWORK}" admin user add local \
      "${MINIO_PLATFORM_ACCESS_KEY}" "${MINIO_PLATFORM_SECRET_KEY}" 2>&1
  )"; then
    run_mc "${MINIO_NETWORK}" admin policy attach local readwrite \
      --user "${MINIO_PLATFORM_ACCESS_KEY}" >/dev/null
    echo "Created MinIO platform access user '${MINIO_PLATFORM_ACCESS_KEY}'."
    return 0
  fi

  if printf '%s' "${add_output}" | grep -qiE "already exists|same as admin access key"; then
    echo "MinIO platform access user '${MINIO_PLATFORM_ACCESS_KEY}' already exists or matches the current admin access key."
    return 0
  fi

  echo "${add_output}" >&2
  return 1
}

cd "${PROJECT_ROOT}"
load_env_file

require_env "MINIO_ROOT_USER"
require_env "MINIO_ROOT_PASSWORD"
require_env "MINIO_PLATFORM_ACCESS_KEY"
require_env "MINIO_PLATFORM_SECRET_KEY"

wait_for_minio

MINIO_NETWORK="$(get_minio_network)"
if [ -z "${MINIO_NETWORK}" ]; then
  echo "Could not determine the MinIO container network."
  exit 1
fi

echo "Bootstrapping MinIO access credentials..."
create_access_user_if_missing

echo "MinIO bootstrap completed."
