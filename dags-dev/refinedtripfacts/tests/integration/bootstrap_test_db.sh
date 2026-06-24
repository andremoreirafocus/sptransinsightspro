#!/usr/bin/env bash
# One-time setup: creates test_sptrans and mirrors the prod DDL needed by the integration suite.
# Run from the project root before executing integration tests.
#
# Usage:
#   bash dags-dev/refinedtripfacts/tests/integration/bootstrap_test_db.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
SQL_ROOT="${PROJECT_ROOT}/database/bootstrap"
ENV_FILE="${SCRIPT_DIR}/.env"

if [ ! -f "${ENV_FILE}" ]; then
  echo "ERROR: ${ENV_FILE} not found. Copy .env.example and fill in your values."
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

DOCKER_COMPOSE="${DOCKER_COMPOSE:-docker compose}"

pg_exec() {
  ${DOCKER_COMPOSE} exec -T postgres psql -v ON_ERROR_STOP=1 -U "${DB_USER}" "$@"
}

run_sql_file_on_test_db() {
  local file_path="$1"
  # Strip \connect directives — the test DB name differs from prod.
  grep -vF '\connect' "${file_path}" | pg_exec -d "${DB_DATABASE}" -f -
}

echo "Creating database '${DB_DATABASE}' if it does not exist..."
echo "SELECT 'CREATE DATABASE ${DB_DATABASE}' WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = '${DB_DATABASE}')\gexec" \
  | pg_exec -f -

echo "Applying schema: refined schema + pg_partman..."
run_sql_file_on_test_db "${SQL_ROOT}/postgres/003_refined_finished_trips.sql"

echo "Applying schema: dim_time + trip_facts + pg_partman config..."
run_sql_file_on_test_db "${SQL_ROOT}/postgres/005_refined_trip_facts.sql"

echo "Bootstrap for '${DB_DATABASE}' completed."
