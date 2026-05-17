#!/usr/bin/env bash

# Shared waiting helpers for bootstrap scripts.

wait_for_condition() {
  local label="$1"
  local timeout_seconds="$2"
  local interval_seconds="$3"
  shift 3

  local elapsed=0
  echo "Waiting for ${label}..."

  until "$@"; do
    sleep "${interval_seconds}"
    elapsed=$((elapsed + interval_seconds))

    if [ "${elapsed}" -ge "${timeout_seconds}" ]; then
      echo "❌ Timed out waiting for ${label}."
      return 1
    fi
  done

  echo "✅ ${label} is available."
}

check_http_url() {
  local service_url="$1"
  python3 -c 'import sys, urllib.request; urllib.request.urlopen(sys.argv[1], timeout=2)' \
    "${service_url}" >/dev/null 2>&1
}
