#!/usr/bin/env bash

# Use DOCKER_CMD_PREFIX to add sudo if needed, e.g. DOCKER_CMD_PREFIX=sudo
DOCKER_CMD_PREFIX="${DOCKER_CMD_PREFIX:-}"

# Define standard docker commands
DOCKER_COMPOSE="${DOCKER_CMD_PREFIX} docker compose"
DOCKER="${DOCKER_CMD_PREFIX} docker"

# Export them so they can be used by child processes if needed, 
# although sourcing this file is the preferred way.
export DOCKER_COMPOSE
export DOCKER
