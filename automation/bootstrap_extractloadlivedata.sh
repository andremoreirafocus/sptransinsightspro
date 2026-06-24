#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/docker_helper.sh"

cd "${PROJECT_ROOT}"

echo "Bootstrapping extractloadlivedata service..."
echo "Trying Docker BuildKit build for extractloadlivedata..."

if ${DOCKER_COMPOSE} build extractloadlivedata; then
  echo "BuildKit build for extractloadlivedata succeeded."
else
  echo "BuildKit build for extractloadlivedata failed. Retrying with DOCKER_BUILDKIT=0..."
  DOCKER_BUILDKIT=0 ${DOCKER_COMPOSE} build extractloadlivedata
fi

echo "Starting extractloadlivedata service..."
${DOCKER_COMPOSE} up -d extractloadlivedata

echo "extractloadlivedata bootstrap completed."
