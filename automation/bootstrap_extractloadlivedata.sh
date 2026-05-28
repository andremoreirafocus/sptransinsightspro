#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

echo "Bootstrapping extractloadlivedata service..."
echo "Trying Docker BuildKit build for extractloadlivedata..."

if docker compose build extractloadlivedata; then
  echo "BuildKit build for extractloadlivedata succeeded."
else
  echo "BuildKit build for extractloadlivedata failed. Retrying with DOCKER_BUILDKIT=0..."
  DOCKER_BUILDKIT=0 docker compose build extractloadlivedata
fi

echo "Starting extractloadlivedata service..."
docker compose up -d extractloadlivedata

echo "extractloadlivedata bootstrap completed."
