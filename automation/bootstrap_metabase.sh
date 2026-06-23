#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SQL_ROOT="${PROJECT_ROOT}/database/bootstrap"
ENV_FILE="${PROJECT_ROOT}/.env"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/wait_helpers.sh"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/docker_helper.sh"

WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-180}"
WAIT_INTERVAL_SECONDS="${WAIT_INTERVAL_SECONDS:-5}"
METABASE_PORT="${METABASE_PORT:-3001}"
METABASE_REPORT_TIMEZONE="America/Sao_Paulo"
POSTGRES_SERVICE_NAME="postgres"
ANALYTICAL_DB_NAME="sptrans_insights"

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

require_command() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "ERROR: required command '${cmd}' not found on host (needed for Metabase app provisioning)." >&2
    exit 1
  fi
}

wait_for_postgres_service() {
  local db_user="$1"
  wait_for_condition \
    "service '${POSTGRES_SERVICE_NAME}' to accept connections" \
    "${WAIT_TIMEOUT_SECONDS}" \
    "${WAIT_INTERVAL_SECONDS}" \
    ${DOCKER_COMPOSE} exec -T "${POSTGRES_SERVICE_NAME}" pg_isready -U "${db_user}" -d postgres
}

run_metabase_sql_bootstrap() {
  local db_user="$1"
  ${DOCKER_COMPOSE} exec -T "${POSTGRES_SERVICE_NAME}" psql \
    -v ON_ERROR_STOP=1 \
    -v metabase_db_name="${METABASE_DB_NAME}" \
    -v metabase_internal_user="${METABASE_INTERNAL_USER}" \
    -v metabase_internal_password="${METABASE_INTERNAL_PASSWORD}" \
    -v metabase_reader_user="${METABASE_READER_USER}" \
    -v metabase_reader_password="${METABASE_READER_PASSWORD}" \
    -U "${db_user}" \
    -f - < "${SQL_ROOT}/postgres/004_metabase.sql"
}

# Apply the São Paulo session timezone scoped to the Metabase reader role only.
# This is authoritative for the native-SQL panels (current_date), and leaves every
# pipeline role on UTC (no blast radius). Idempotent: ALTER ROLE ... SET overwrites.
apply_reader_session_timezone() {
  local db_user="$1"
  ${DOCKER_COMPOSE} exec -T "${POSTGRES_SERVICE_NAME}" psql \
    -v ON_ERROR_STOP=1 \
    -U "${db_user}" \
    -d "${ANALYTICAL_DB_NAME}" \
    -c "ALTER ROLE \"${METABASE_READER_USER}\" IN DATABASE ${ANALYTICAL_DB_NAME} SET timezone = '${METABASE_REPORT_TIMEZONE}';"
}

# Generic Metabase API call with mandatory HTTP-status checking.
# Args: METHOD PATH [SESSION_TOKEN] [JSON_BODY]
# - Echoes the response body on stdout for the caller to parse with jq.
# - Aborts (non-zero) on any non-2xx, printing Metabase's JSON error body.
# - Request bodies are streamed via stdin (never on argv) and never logged, so
#   the admin/reader passwords cannot leak into process lists or output.
metabase_api() {
  local method="$1"
  local path="$2"
  local session="${3:-}"
  local body="${4:-}"
  local url="${METABASE_BASE_URL}${path}"
  local body_file http_code
  body_file="$(mktemp)"

  local -a args=(-sS -X "${method}" -H "Content-Type: application/json" \
    -o "${body_file}" -w '%{http_code}')
  if [ -n "${session}" ]; then
    args+=(-H "X-Metabase-Session: ${session}")
  fi

  if [ -n "${body}" ]; then
    http_code="$(printf '%s' "${body}" | curl "${args[@]}" --data-binary @- "${url}")"
  else
    http_code="$(curl "${args[@]}" "${url}")"
  fi

  if [ -z "${http_code}" ] || [ "${http_code}" -lt 200 ] || [ "${http_code}" -ge 300 ]; then
    echo "ERROR: Metabase API ${method} ${path} returned HTTP ${http_code:-000}." >&2
    cat "${body_file}" >&2
    echo >&2
    rm -f "${body_file}"
    return 1
  fi

  cat "${body_file}"
  rm -f "${body_file}"
}

# One-time, idempotent Metabase application provisioning: admin user, query
# timezone (reader-scoped default + Report Timezone), the read-only refined
# datasource, and an initial schema sync. Safe on a fresh instance and on re-run.
provision_metabase_app() {
  local db_user="$1"

  echo "==> Provisioning Metabase application (admin, timezone, datasource)..."

  # 1. Read setup state. setup-token is only present pre-setup.
  local props has_setup setup_token
  props="$(metabase_api GET /api/session/properties)"
  has_setup="$(printf '%s' "${props}" | jq -r '.["has-user-setup"]')"

  # 2. Create the admin and clear the setup wizard, only if not yet set up.
  if [ "${has_setup}" != "true" ]; then
    setup_token="$(printf '%s' "${props}" | jq -r '.["setup-token"] // empty')"
    if [ -z "${setup_token}" ]; then
      echo "ERROR: Metabase reports has-user-setup=false but exposed no setup-token." >&2
      return 1
    fi
    echo "    - creating admin user and completing the setup wizard"
    local setup_body
    setup_body="$(jq -n \
      --arg token "${setup_token}" \
      --arg first "${METABASE_ADMIN_FIRST_NAME}" \
      --arg last "${METABASE_ADMIN_LAST_NAME}" \
      --arg email "${METABASE_ADMIN_EMAIL}" \
      --arg password "${METABASE_ADMIN_PASSWORD}" \
      --arg site "${METABASE_SITE_NAME}" \
      '{token: $token,
        user: {first_name: $first, last_name: $last, email: $email, password: $password},
        prefs: {site_name: $site, site_locale: "en", allow_tracking: false}}')"
    metabase_api POST /api/setup "" "${setup_body}" >/dev/null
  else
    echo "    - Metabase already set up; skipping admin creation"
  fi

  # 3. Log in (single path for fresh and re-run) → session token for later calls.
  local session_body session_id
  session_body="$(jq -n \
    --arg u "${METABASE_ADMIN_EMAIL}" \
    --arg p "${METABASE_ADMIN_PASSWORD}" \
    '{username: $u, password: $p}')"
  session_id="$(metabase_api POST /api/session "" "${session_body}" | jq -r '.id // empty')"
  if [ -z "${session_id}" ]; then
    echo "ERROR: failed to obtain a Metabase admin session (check METABASE_ADMIN_* credentials)." >&2
    return 1
  fi

  # 4. Timezone in two layers so native-SQL current_date is correct from scratch.
  echo "    - applying reader-scoped session timezone (${METABASE_REPORT_TIMEZONE})"
  apply_reader_session_timezone "${db_user}"
  echo "    - setting Metabase Report Timezone (${METABASE_REPORT_TIMEZONE})"
  metabase_api PUT /api/setting/report-timezone "${session_id}" \
    "$(jq -n --arg v "${METABASE_REPORT_TIMEZONE}" '{value: $v}')" >/dev/null

  # 5. Ensure the read-only refined datasource exists (envelope: read .data[]).
  local databases db_id
  databases="$(metabase_api GET /api/database "${session_id}")"
  db_id="$(printf '%s' "${databases}" \
    | jq -r '.data[] | select(.name == "sptrans_insights") | .id' | head -n1)"
  if [ -z "${db_id}" ]; then
    echo "    - creating 'sptrans_insights' datasource (read-only reader, refined schema)"
    # host is the compose service name 'postgres' (resolved by the Metabase
    # container over the compose network), NOT localhost. Do not "correct" it.
    local db_body
    db_body="$(jq -n \
      --arg user "${METABASE_READER_USER}" \
      --arg password "${METABASE_READER_PASSWORD}" \
      --arg dbname "${ANALYTICAL_DB_NAME}" \
      '{engine: "postgres",
        name: "sptrans_insights",
        details: {host: "postgres",
                  port: 5432,
                  dbname: $dbname,
                  user: $user,
                  password: $password,
                  "schema-filters-type": "inclusion",
                  "schema-filters-patterns": "refined",
                  ssl: false}}')"
    db_id="$(metabase_api POST /api/database "${session_id}" "${db_body}" | jq -r '.id // empty')"
  else
    echo "    - 'sptrans_insights' datasource already exists (id=${db_id})"
  fi
  if [ -z "${db_id}" ]; then
    echo "ERROR: could not determine the 'sptrans_insights' datasource id." >&2
    return 1
  fi

  # 6. Populate the refined tables (async; visibility may lag a few seconds).
  #    Endpoint is /sync_schema (the REST route); "sync_schema_now" is only the UI label.
  echo "    - triggering schema sync for datasource id=${db_id}"
  metabase_api "POST" "/api/database/${db_id}/sync_schema" "${session_id}" >/dev/null

  echo "Metabase application provisioning completed."
}

cd "${PROJECT_ROOT}"
load_env_file

METABASE_BASE_URL="${METABASE_BASE_URL:-http://localhost:${METABASE_PORT}}"
METABASE_HEALTH_URL="${METABASE_HEALTH_URL:-${METABASE_BASE_URL}/api/health}"

DB_USER="${POSTGRES_DB_USER:-postgres}"

require_env "METABASE_DB_NAME"
require_env "METABASE_INTERNAL_USER"
require_env "METABASE_INTERNAL_PASSWORD"
require_env "METABASE_READER_USER"
require_env "METABASE_READER_PASSWORD"

# App-provisioning inputs (admin init + datasource). Required even on a grant-fix
# re-run, since the idempotent provisioning phase always runs after the health wait.
require_env "METABASE_ADMIN_EMAIL"
require_env "METABASE_ADMIN_PASSWORD"
METABASE_ADMIN_FIRST_NAME="${METABASE_ADMIN_FIRST_NAME:-Admin}"
METABASE_ADMIN_LAST_NAME="${METABASE_ADMIN_LAST_NAME:-User}"
METABASE_SITE_NAME="${METABASE_SITE_NAME:-SPTrans Insights Pro}"

# Host tooling needed by the provisioning phase (precedent: bootstrap_minio.sh uses jq).
require_command "curl"
require_command "jq"

wait_for_postgres_service "${DB_USER}"

echo "==> Running Metabase SQL bootstrap..."
run_metabase_sql_bootstrap "${DB_USER}"

echo "==> Starting Metabase service..."
${DOCKER_COMPOSE} up -d metabase

wait_for_condition \
  "service 'metabase' to become available" \
  "${WAIT_TIMEOUT_SECONDS}" \
  "${WAIT_INTERVAL_SECONDS}" \
  check_http_url "${METABASE_HEALTH_URL}"

provision_metabase_app "${DB_USER}"

echo "Metabase bootstrap completed."
